# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sqlite3

import pytest

import petl as etl
import petl.io.db as db


class TrackedCursor(sqlite3.Cursor):
    def __init__(self, connection):
        super(TrackedCursor, self).__init__(connection)
        self.close_calls = 0
        self.close_error = None

    def close(self):
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error
        return super(TrackedCursor, self).close()


class TrackedConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super(TrackedConnection, self).__init__(*args, **kwargs)
        self.created_cursors = []
        self.commit_calls = 0

    def cursor(self, *args, **kwargs):
        kwargs['factory'] = TrackedCursor
        cursor = super(TrackedConnection, self).cursor(*args, **kwargs)
        self.created_cursors.append(cursor)
        return cursor

    def commit(self):
        self.commit_calls += 1
        return super(TrackedConnection, self).commit()


@pytest.fixture
def connection():
    conn = sqlite3.connect(':memory:', factory=TrackedConnection)
    conn.execute('CREATE TABLE target(value INTEGER UNIQUE)')
    conn.execute('INSERT INTO target VALUES (1)')
    conn.commit()
    conn.commit_calls = 0
    conn.created_cursors = []
    try:
        yield conn
    finally:
        conn.close()


def assert_owned_cursors_closed(connection):
    """Check that each loader-owned native cursor was closed once."""
    assert connection.created_cursors
    for cursor in connection.created_cursors:
        assert cursor.close_calls == 1
        with pytest.raises(sqlite3.ProgrammingError):
            cursor.execute('SELECT 1')


@pytest.mark.parametrize('use_factory', [False, True])
@pytest.mark.parametrize('replace', [False, True])
@pytest.mark.parametrize('source_error', [False, True])
def test_owned_load_cursor_closes_after_error(connection, use_factory,
                                             replace, source_error):
    failure = ValueError('source iteration failed')

    def rows():
        yield ('value',)
        yield (2,)
        if source_error:
            raise failure
        yield (2,)

    database = connection.cursor if use_factory else connection
    load = etl.todb if replace else etl.appenddb
    expected_error = ValueError if source_error else sqlite3.IntegrityError
    with pytest.raises(expected_error) as caught:
        load(rows(), database, 'target')
    if source_error:
        assert caught.value is failure

    assert_owned_cursors_closed(connection)
    assert connection.commit_calls == 0
    assert connection.execute('SELECT 42').fetchone() == (42,)
    connection.rollback()
    assert connection.execute('SELECT value FROM target').fetchall() == [(1,)]


@pytest.mark.parametrize('use_factory', [False, True])
def test_owned_cursor_closes_after_truncate_error(connection, use_factory):
    database = connection.cursor if use_factory else connection
    with pytest.raises(sqlite3.OperationalError):
        etl.todb([('value',), (2,)], database, 'missing')
    assert_owned_cursors_closed(connection)
    assert connection.commit_calls == 0


@pytest.mark.parametrize('use_factory', [False, True])
@pytest.mark.parametrize('commit', [False, True])
def test_owned_load_cursor_closes_with_commit_control(connection, use_factory,
                                                      commit):
    database = connection.cursor if use_factory else connection
    etl.appenddb([('value',), (2,)], database, 'target', commit=commit)
    assert_owned_cursors_closed(connection)
    assert connection.commit_calls == int(commit)
    assert connection.execute(
        'SELECT value FROM target ORDER BY value',
    ).fetchall() == [(1,), (2,)]


@pytest.mark.parametrize('replace', [False, True])
def test_load_does_not_close_borrowed_cursor_after_error(connection, replace):
    cursor = connection.cursor()
    load = etl.todb if replace else etl.appenddb
    try:
        with pytest.raises(sqlite3.IntegrityError):
            load([('value',), (2,), (2,)], cursor, 'target')
        assert cursor.close_calls == 0
        assert cursor.execute('SELECT 42').fetchone() == (42,)
        assert connection.commit_calls == 0
    finally:
        cursor.close()


def test_replacement_cursor_factory_error_closes_first_cursor_once(connection):
    failure = RuntimeError('cannot create replacement cursor')

    def make_cursor():
        if connection.created_cursors:
            raise failure
        return connection.cursor()

    with pytest.raises(RuntimeError) as caught:
        etl.todb([('value',), (2,)], make_cursor, 'target')
    assert caught.value is failure
    assert_owned_cursors_closed(connection)
    assert connection.commit_calls == 0


def test_truncate_cursor_close_error_is_not_retried(connection):
    failure = RuntimeError('cannot close truncate cursor')

    def make_cursor():
        cursor = connection.cursor()
        cursor.close_error = failure
        return cursor

    with pytest.raises(RuntimeError) as caught:
        etl.todb([('value',), (2,)], make_cursor, 'target')
    assert caught.value is failure
    assert len(connection.created_cursors) == 1
    assert connection.created_cursors[0].close_calls == 1
    assert connection.commit_calls == 0


@pytest.mark.parametrize('replace', [False, True])
def test_clickhouse_adapter_closes_cursor_after_driver_error(
        connection, monkeypatch, replace):
    # Route SQLite through this adapter to exercise cleanup on driver errors.
    monkeypatch.setattr(db, '_is_clikchouse_dbapi_connection',
                        lambda value: value is connection)
    load = etl.todb if replace else etl.appenddb
    with pytest.raises(sqlite3.OperationalError):
        load([('value',), (2,)], connection, 'target')
    assert_owned_cursors_closed(connection)
    assert connection.commit_calls == 0


def fail_cursor_close(connection, monkeypatch, skip_first=False):
    failure = RuntimeError('cursor cleanup failed')
    make_cursor = connection.cursor

    def failing_cursor():
        cursor = make_cursor()
        if not (skip_first and len(connection.created_cursors) == 1):
            cursor.close_error = failure
        return cursor

    monkeypatch.setattr(connection, 'cursor', failing_cursor)
    return failure


@pytest.mark.parametrize('use_factory', [False, True])
@pytest.mark.parametrize('replace', [False, True])
@pytest.mark.parametrize('source_error', [False, True])
def test_load_error_survives_cursor_close_error(
        connection, monkeypatch, use_factory, replace, source_error):
    fail_cursor_close(connection, monkeypatch, skip_first=replace)
    source_failure = ValueError('source iteration failed')

    def rows():
        yield ('value',)
        yield (2,)
        if source_error:
            raise source_failure
        yield (2,)

    database = connection.cursor if use_factory else connection
    load = etl.todb if replace else etl.appenddb
    expected = ValueError if source_error else sqlite3.IntegrityError
    with pytest.raises(expected) as caught:
        load(rows(), database, 'target')
    if source_error:
        assert caught.value is source_failure
    assert all(cursor.close_calls == 1
               for cursor in connection.created_cursors)
    assert connection.commit_calls == 0


@pytest.mark.parametrize('adapter', ['connection', 'factory', 'clickhouse'])
def test_truncate_error_survives_cursor_close_error(
        connection, monkeypatch, adapter):
    fail_cursor_close(connection, monkeypatch)
    if adapter == 'clickhouse':
        monkeypatch.setattr(db, '_is_clikchouse_dbapi_connection',
                            lambda value: value is connection)
    database = connection.cursor if adapter == 'factory' else connection
    with pytest.raises(sqlite3.OperationalError):
        etl.todb([('value',), (2,)], database, 'missing')
    assert len(connection.created_cursors) == 1
    assert connection.created_cursors[0].close_calls == 1
    assert connection.commit_calls == 0


def test_clickhouse_insert_error_survives_cursor_close_error(
        connection, monkeypatch):
    fail_cursor_close(connection, monkeypatch)
    monkeypatch.setattr(db, '_is_clikchouse_dbapi_connection',
                        lambda value: value is connection)
    with pytest.raises(sqlite3.OperationalError):
        etl.appenddb([('value',), (2,)], connection, 'target')
    assert connection.created_cursors[0].close_calls == 1
    assert connection.commit_calls == 0


@pytest.mark.parametrize('use_factory', [False, True])
@pytest.mark.parametrize('handling_other_error', [False, True])
def test_successful_load_still_reports_cursor_close_error(
        connection, monkeypatch, use_factory, handling_other_error):
    close_error = fail_cursor_close(connection, monkeypatch)
    database = connection.cursor if use_factory else connection

    def load():
        with pytest.raises(RuntimeError) as caught:
            etl.appenddb([('value',), (2,)], database, 'target')
        assert caught.value is close_error

    if handling_other_error:
        try:
            raise ValueError('unrelated outer error')
        except ValueError:
            load()
    else:
        load()
    assert connection.created_cursors[0].close_calls == 1
    assert connection.commit_calls == 0


@pytest.mark.parametrize('use_factory', [False, True])
def test_source_interrupt_survives_cursor_close_error(
        connection, monkeypatch, use_factory):
    failure = KeyboardInterrupt()
    fail_cursor_close(connection, monkeypatch)

    def rows():
        yield ('value',)
        raise failure

    database = connection.cursor if use_factory else connection
    with pytest.raises(KeyboardInterrupt) as caught:
        etl.appenddb(rows(), database, 'target')
    assert caught.value is failure
    assert connection.created_cursors[0].close_calls == 1
    assert connection.commit_calls == 0
