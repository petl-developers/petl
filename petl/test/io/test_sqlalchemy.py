from __future__ import absolute_import, division, print_function

import pytest

import petl as etl
from petl.io.db_create import create_table, drop_table

sa = pytest.importorskip('sqlalchemy')
from sqlalchemy.orm import sessionmaker  # noqa: E402 (optional dependency)
from sqlalchemy.pool import QueuePool  # noqa: E402 (optional dependency)


@pytest.fixture(params=['default', 'future'])
def engine(tmpdir, request):
    kwargs = {}
    if request.param == 'future':
        if not hasattr(sa.engine.Connection, 'exec_driver_sql'):
            pytest.skip('future mode requires SQLAlchemy 1.4 or later')
        kwargs['future'] = True
    engine = sa.create_engine('sqlite:///' + str(tmpdir.join('data.db')),
                              poolclass=QueuePool, **kwargs)
    yield engine
    engine.dispose()


@pytest.fixture(params=['engine', 'connection', 'session'])
def dbo(request, engine):
    if request.param == 'connection':
        dbo = engine.connect()
    elif request.param == 'session':
        dbo = sessionmaker(bind=engine)()
    else:
        dbo = engine
    yield dbo
    if dbo is not engine:
        dbo.close()


def rows(engine):
    with engine.connect() as connection:
        return [tuple(row) for row in connection.execute(
            sa.text('SELECT id, name FROM records ORDER BY id'))]


def test_sqlalchemy_create_write_read_append_drop(dbo, engine):
    table = [('id', 'name'), (1, 'Alice'), (2, 'Bob')]
    etl.todb(table, dbo, 'records', create=True)
    assert rows(engine) == table[1:]
    assert list(etl.fromdb(dbo, 'SELECT id, name FROM records ORDER BY id')) == table

    etl.appenddb([table[0], (3, 'Carol')], dbo, 'records')
    assert rows(engine) == table[1:] + [(3, 'Carol')]
    etl.todb([table[0], (4, 'Dana')], dbo, 'records')
    assert rows(engine) == [(4, 'Dana')]

    drop_table(dbo, 'records')
    with engine.connect() as connection:
        assert connection.execute(sa.text(
            "SELECT name FROM sqlite_master WHERE name='records'")).fetchall() == []


def test_sqlalchemy_fromdb_parameters_and_expressions(dbo, engine):
    with engine.begin() as connection:
        connection.execute(sa.text('CREATE TABLE records (id INTEGER, name TEXT)'))
        connection.execute(sa.text("INSERT INTO records VALUES (1, 'Alice'), (2, 'Bob')"))
    expected = [('id', 'name'), (2, 'Bob')]
    query = 'SELECT id, name FROM records WHERE id=:id'
    assert list(etl.fromdb(dbo, query, {'id': 2})) == expected
    assert list(etl.fromdb(dbo, sa.text(query), {'id': 2})) == expected


def test_sqlalchemy_failed_write_rolls_back(dbo, engine):
    with engine.begin() as connection:
        connection.execute(sa.text('CREATE TABLE records (id INTEGER UNIQUE, name TEXT)'))
        connection.execute(sa.text("INSERT INTO records VALUES (1, 'original')"))
    table = [('id', 'name'), (2, 'new'), (2, 'duplicate')]
    with pytest.raises(sa.exc.IntegrityError):
        etl.todb(table, dbo, 'records')
    assert rows(engine) == [(1, 'original')]
    etl.appenddb([table[0], (3, 'retry')], dbo, 'records')
    assert rows(engine) == [(1, 'original'), (3, 'retry')]


@pytest.mark.parametrize('kind', ['connection', 'session'])
@pytest.mark.parametrize('commit', [False, True])
def test_sqlalchemy_commit_false_preserves_caller_transaction(engine, kind, commit):
    with engine.begin() as connection:
        connection.execute(sa.text('CREATE TABLE records (id INTEGER, name TEXT)'))
    dbo = engine.connect() if kind == 'connection' else sessionmaker(bind=engine)()
    try:
        if kind == 'session' and not hasattr(dbo, 'get_transaction'):
            transaction = dbo.transaction
        else:
            transaction = dbo.begin()
        table = [('id', 'name'), (1, 'Alice')]
        etl.todb(table, dbo, 'records', commit=False)
        assert transaction.is_active
        assert rows(engine) == []
        if commit:
            transaction.commit()
        else:
            transaction.rollback()
        assert rows(engine) == (table[1:] if commit else [])
    finally:
        dbo.close()


def test_sqlalchemy_owned_connections_are_returned(engine):
    table = [('id',), (1,)]
    etl.todb(table, engine, 'records', create=True)
    assert engine.pool.checkedout() == 0
    iterator = iter(etl.fromdb(engine, 'SELECT * FROM records'))
    assert next(iterator) == ('id',)
    iterator.close()
    assert engine.pool.checkedout() == 0
    with pytest.raises(sa.exc.DatabaseError):
        list(etl.fromdb(engine, 'SELECT * FROM missing'))
    assert engine.pool.checkedout() == 0


def test_sqlalchemy_connection_raw_positional_parameters(engine):
    with engine.connect() as connection:
        assert list(etl.fromdb(connection, 'SELECT ? AS value', (123,))) == [
            ('value',), (123,)]


def test_sqlalchemy_create_drop_commit_false(engine):
    with engine.connect() as connection:
        transaction = connection.begin()
        create_table([('id',), (1,)], connection, 'records', commit=False)
        drop_table(connection, 'records', commit=False)
        assert transaction.is_active
        transaction.rollback()
