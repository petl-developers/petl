# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division
import logging
import pytest

import petl as etl
from petl.compat import PY3
from petl.test.helpers import ieq
from petl.test.io.pytest_helpers import get_db_args, get_mysql_args, get_pg_args, require_package, skip_unless_connect


logger = logging.getLogger(__name__)
debug = logger.debug


def _test_dbo(write_dbo, read_dbo=None):
    if read_dbo is None:
        read_dbo = write_dbo

    expect_empty = (('foo', 'bar'),)
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2))
    expect_appended = (('foo', 'bar'),
                       ('a', 1),
                       ('b', 2),
                       ('a', 1),
                       ('b', 2))
    actual = etl.fromdb(read_dbo, 'SELECT * FROM test')

    debug('verify empty to start with...')
    debug(etl.look(actual))
    ieq(expect_empty, actual)

    debug('write some data and verify...')
    etl.todb(expect, write_dbo, 'test')
    debug(etl.look(actual))
    ieq(expect, actual)

    debug('append some data and verify...')
    etl.appenddb(expect, write_dbo, 'test')
    debug(etl.look(actual))
    ieq(expect_appended, actual)

    debug('overwrite and verify...')
    etl.todb(expect, write_dbo, 'test')
    debug(etl.look(actual))
    ieq(expect, actual)

    debug('cut, overwrite and verify')
    etl.todb(etl.cut(expect, 'bar', 'foo'), write_dbo, 'test')
    debug(etl.look(actual))
    ieq(expect, actual)

    debug('cut, append and verify')
    etl.appenddb(etl.cut(expect, 'bar', 'foo'), write_dbo, 'test')
    debug(etl.look(actual))
    ieq(expect_appended, actual)

    debug('try a single row')
    etl.todb(etl.head(expect, 1), write_dbo, 'test')
    debug(etl.look(actual))
    ieq(etl.head(expect, 1), actual)


def _test_with_schema(dbo, schema):

    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2))
    expect_appended = (('foo', 'bar'),
                       ('a', 1),
                       ('b', 2),
                       ('a', 1),
                       ('b', 2))
    actual = etl.fromdb(dbo, 'SELECT * FROM test')

    print('write some data and verify...')
    etl.todb(expect, dbo, 'test', schema=schema)
    ieq(expect, actual)
    print(etl.look(actual))

    print('append some data and verify...')
    etl.appenddb(expect, dbo, 'test', schema=schema)
    ieq(expect_appended, actual)
    print(etl.look(actual))


def _test_unicode(dbo):
    expect = ((u'name', u'id'),
              (u'Արամ Խաչատրյան', 1),
              (u'Johann Strauß', 2),
              (u'Вагиф Сәмәдоғлу', 3),
              (u'章子怡', 4),
              )
    actual = etl.fromdb(dbo, 'SELECT * FROM test_unicode')

    print('write some data and verify...')
    etl.todb(expect, dbo, 'test_unicode')
    ieq(expect, actual)
    print(etl.look(actual))


def _setup_mysql(dbapi_connection):
    # setup table
    cursor = dbapi_connection.cursor()
    # deal with quote compatibility
    cursor.execute('SET SQL_MODE=ANSI_QUOTES')
    cursor.execute('DROP TABLE IF EXISTS test')
    cursor.execute('CREATE TABLE test (foo TEXT, bar INT)')
    cursor.execute('DROP TABLE IF EXISTS test_unicode')
    cursor.execute('CREATE TABLE test_unicode (name TEXT, id INT) '
                   'CHARACTER SET utf8')
    cursor.close()
    dbapi_connection.commit()


def _setup_postgresql(dbapi_connection):
    # setup table
    cursor = dbapi_connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS test')
    cursor.execute('CREATE TABLE test (foo TEXT, bar INT)')
    cursor.execute('DROP TABLE IF EXISTS test_unicode')
    # assume character encoding UTF-8 already set at database level
    cursor.execute('CREATE TABLE test_unicode (name TEXT, id INT)')
    cursor.close()
    dbapi_connection.commit()


def _setup_sqlalchemy_quotes(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("SET sql_mode = 'ANSI_QUOTES'")


def test_pymysql():
    pymysql = require_package('pymysql')
    require_package('sqlalchemy')
    myhost, myuser, mypassword, mydatabase = get_mysql_args()

    def connect():
        return pymysql.connect(host=myhost,
                               user=myuser,
                               password=mypassword,
                               database=mydatabase)

    dbapi_connection = skip_unless_connect(connect, 'SKIP pymysql tests')

    try:
        # exercise using a dbapi_connection
        _setup_mysql(dbapi_connection)
        _test_dbo(dbapi_connection)

        # exercise using a dbapi_cursor
        _setup_mysql(dbapi_connection)
        dbapi_cursor = dbapi_connection.cursor()
        _test_dbo(dbapi_cursor)
        dbapi_cursor.close()

        # exercise sqlalchemy dbapi_connection
        _setup_mysql(dbapi_connection)
        from sqlalchemy import create_engine
        sqlalchemy_engine = create_engine('mysql+pymysql://%s:%s@%s/%s' %
                                         (myuser, mypassword, myhost, mydatabase))
        from sqlalchemy.event import listen
        listen(sqlalchemy_engine, "connect", _setup_sqlalchemy_quotes)
        sqlalchemy_connection = sqlalchemy_engine.connect()
        _test_dbo(sqlalchemy_connection)
        sqlalchemy_connection.close()

        # exercise sqlalchemy session
        _setup_mysql(dbapi_connection)
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=sqlalchemy_engine)
        sqlalchemy_session = Session()
        _test_dbo(sqlalchemy_session)
        sqlalchemy_session.close()

        # exercise sqlalchemy engine
        _setup_mysql(dbapi_connection)
        sqlalchemy_engine2 = create_engine('mysql+pymysql://%s:%s@%s/%s' %
                                          (myuser, mypassword, myhost, mydatabase), echo_pool='debug')
        listen(sqlalchemy_engine2, "connect", _setup_sqlalchemy_quotes)
        _test_dbo(sqlalchemy_engine2)
        sqlalchemy_engine2.dispose()

        # other exercises
        _test_with_schema(dbapi_connection, mydatabase)
        utf8_connection = pymysql.connect(host=myhost,
                                  user=myuser,
                                  password=mypassword,
                                  database=mydatabase,
                                  charset='utf8')
        try:
            utf8_connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
            _test_unicode(utf8_connection)
        finally:
            utf8_connection.close()
    finally:
        dbapi_connection.close()


@pytest.mark.skipif(PY3, reason='MySQLdb is Python 2 only')
def test_mysqldb():
    MySQLdb = require_package('MySQLdb')
    require_package('sqlalchemy')
    myhost, myuser, mypassword, mydatabase = get_db_args('MYSQLDB', 'MYSQL')

    def connect():
        return MySQLdb.connect(host=myhost,
                               user=myuser,
                               passwd=mypassword,
                               db=mydatabase)

    dbapi_connection = skip_unless_connect(connect, 'SKIP MySQLdb tests')

    try:
        # exercise using a dbapi_connection
        _setup_mysql(dbapi_connection)
        _test_dbo(dbapi_connection)

        # exercise using a dbapi_cursor
        _setup_mysql(dbapi_connection)
        dbapi_cursor = dbapi_connection.cursor()
        _test_dbo(dbapi_cursor)
        dbapi_cursor.close()

        # exercise sqlalchemy dbapi_connection
        _setup_mysql(dbapi_connection)
        from sqlalchemy import create_engine
        sqlalchemy_engine = create_engine('mysql+mysqldb://%s:%s@%s/%s' %
                                         (myuser, mypassword, myhost, mydatabase))
        from sqlalchemy.event import listen
        listen(sqlalchemy_engine, "connect", _setup_sqlalchemy_quotes)
        sqlalchemy_connection = sqlalchemy_engine.connect()
        sqlalchemy_connection.execute(sqlalchemy.text('SET SQL_MODE=ANSI_QUOTES'))
        _test_dbo(sqlalchemy_connection)
        sqlalchemy_connection.close()

        # exercise sqlalchemy session
        _setup_mysql(dbapi_connection)
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=sqlalchemy_engine)
        sqlalchemy_session = Session()
        _test_dbo(sqlalchemy_session)
        sqlalchemy_session.close()

        # other exercises
        _test_with_schema(dbapi_connection, mydatabase)
        utf8_connection = MySQLdb.connect(host=myhost,
                                  user=myuser,
                                  passwd=mypassword,
                                  db=mydatabase,
                                  charset='utf8')
        try:
            utf8_connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
            _test_unicode(utf8_connection)
        finally:
            utf8_connection.close()
    finally:
        dbapi_connection.close()


def test_postgresql():
    psycopg2 = require_package('psycopg2')
    require_package('sqlalchemy')
    pghost, pguser, pgpassword, pgdatabase = get_pg_args()

    def connect():
        import psycopg2.extensions
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
        return psycopg2.connect(
            'host=%s dbname=%s user=%s password=%s'
            % (pghost, pgdatabase, pguser, pgpassword)
        )

    dbapi_connection = skip_unless_connect(connect, 'SKIP psycopg2 tests')

    try:
        # exercise using a dbapi_connection
        _setup_postgresql(dbapi_connection)
        _test_dbo(dbapi_connection)

        # exercise using a dbapi_cursor
        _setup_postgresql(dbapi_connection)
        dbapi_cursor = dbapi_connection.cursor()
        _test_dbo(dbapi_cursor)
        dbapi_cursor.close()

        # exercise sqlalchemy dbapi_connection
        _setup_postgresql(dbapi_connection)
        from sqlalchemy import create_engine
        sqlalchemy_engine = create_engine('postgresql+psycopg2://%s:%s@%s/%s' %
                                          (pguser, pgpassword, pghost, pgdatabase))
        sqlalchemy_connection = sqlalchemy_engine.connect()
        _test_dbo(sqlalchemy_connection)
        sqlalchemy_connection.close()

        # exercise sqlalchemy session
        _setup_postgresql(dbapi_connection)
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=sqlalchemy_engine)
        sqlalchemy_session = Session()
        _test_dbo(sqlalchemy_session)
        sqlalchemy_session.close()

        # other exercises
        # The Session exercise now commits its rows; start the next case empty.
        _setup_postgresql(dbapi_connection)
        _test_dbo(dbapi_connection,
                  lambda: dbapi_connection.cursor(name='arbitrary'))
        _test_with_schema(dbapi_connection, 'public')
        _test_unicode(dbapi_connection)
    finally:
        dbapi_connection.close()
