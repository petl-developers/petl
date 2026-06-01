# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import sqlite3
from tempfile import NamedTemporaryFile
from petl.compat import next


import petl.io.db as db
from petl.test.helpers import ieq, eq_
from petl.io.db import fromdb, todb, appenddb
from petl.io.db_create import drop_table


# N.B., this file only tests the DB-related functions using sqlite3,
# as anything else requires database connection configuration. See
# docs/dbtests.py for a script to exercise the DB-related functions with
# MySQL and Postgres.


class LoggingConnection(object):

    def __init__(self):
        self.sql = []

    def cursor(self):
        return self

    def execute(self, sql):
        self.sql.append(sql)

    def close(self):
        pass

    def commit(self):
        pass


def test_drop_table_if_exists_sql():
    dbo = LoggingConnection()

    drop_table(dbo, 'test_create', commit=False)
    drop_table(dbo, 'test_create', commit=False, if_exists=True)

    eq_([u'DROP TABLE "test_create"',
         u'DROP TABLE IF EXISTS "test_create"'],
        dbo.sql)


def _record_todb_create_calls(monkeypatch, drop):
    calls = []

    def fake_drop_table(dbo, tablename, schema=None, commit=True,
                        if_exists=False):
        calls.append(('drop', tablename, if_exists))

    def fake_create_table(table, dbo, tablename, schema=None, commit=True,
                          constraints=True, metadata=None, dialect=None,
                          sample=1000):
        calls.append(('create', tablename))

    def fake_todb(table, dbo, tablename, schema=None, commit=True,
                  truncate=False):
        calls.append(('load', tablename, truncate))

    monkeypatch.setattr(db, 'drop_table', fake_drop_table)
    monkeypatch.setattr(db, 'create_table', fake_create_table)
    monkeypatch.setattr(db, '_todb', fake_todb)

    table = (('foo',), ('a',))
    todb(table, object(), 'foobar', create=True, drop=drop)
    return calls


def test_todb_drop_false_skips_drop(monkeypatch):
    calls = _record_todb_create_calls(monkeypatch, False)

    eq_([('create', 'foobar'),
         ('load', 'foobar', True)],
        calls)


def test_todb_drop_true_uses_plain_drop(monkeypatch):
    calls = _record_todb_create_calls(monkeypatch, True)

    eq_([('drop', 'foobar', False),
         ('create', 'foobar'),
         ('load', 'foobar', True)],
        calls)


def test_todb_drop_if_exists_uses_if_exists(monkeypatch):
    calls = _record_todb_create_calls(monkeypatch, 'if_exists')

    eq_([('drop', 'foobar', True),
         ('create', 'foobar'),
         ('load', 'foobar', True)],
        calls)


def test_fromdb():

    # initial data
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(':memory:')
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    actual = fromdb(connection, 'select * from foobar', ownsdb=True)
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2.0))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice

    # test iterators are isolated
    i1 = iter(actual)
    i2 = iter(actual)
    eq_(('foo', 'bar'), next(i1))
    eq_(('a', 1), next(i1))
    eq_(('foo', 'bar'), next(i2))
    eq_(('b', 2), next(i1))


def test_fromdb_mkcursor():

    # initial data
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(':memory:')
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    mkcursor = lambda: connection.cursor()
    actual = fromdb(mkcursor, 'select * from foobar')
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2.0))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice

    # test iterators are isolated
    i1 = iter(actual)
    i2 = iter(actual)
    eq_(('foo', 'bar'), next(i1))
    eq_(('a', 1), next(i1))
    eq_(('foo', 'bar'), next(i2))
    eq_(('b', 2), next(i1))


def test_fromdb_withargs():

    # initial data
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(':memory:')
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    actual = fromdb(
        connection,
        'select * from foobar where bar > ? and bar < ?',
        (1, 3)
    )
    expect = (('foo', 'bar'),
              ('b', 2),
              ('c', 2.0))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    connection.close()


def test_todb_appenddb():

    f = NamedTemporaryFile(delete=False)
    conn = sqlite3.connect(f.name)
    conn.execute('create table foobar (foo, bar)')
    conn.commit()

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    todb(table, conn, 'foobar')

    # check what it did
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)

    # try appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appenddb(table2, conn, 'foobar')

    # check what it did
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    ieq(expect, actual)
    conn.close()


def test_todb_appenddb_cursor():

    f = NamedTemporaryFile(delete=False)
    conn = sqlite3.connect(f.name)
    conn.execute('create table foobar (foo, bar)')
    conn.commit()

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    cursor = conn.cursor()
    todb(table, cursor, 'foobar')

    # check what it did
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)

    # try appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appenddb(table2, cursor, 'foobar')

    # check what it did
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    ieq(expect, actual)
    conn.close()
