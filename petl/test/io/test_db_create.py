# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import logging
from datetime import datetime, date
import sqlite3

import pytest

from petl.io.db import fromdb, todb
from petl.io.db_create import make_sqlalchemy_column
from petl.test.helpers import ieq, eq_
from petl.util.vis import look
from petl.test.io.test_db_server import user, password, host, database


logger = logging.getLogger(__name__)
debug = logger.debug


def _test_create(dbo):

    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2))

    expect_extended = (('foo', 'bar', 'baz'),
                       ('a', 1, 2.3),
                       ('b', 2, 4.1))
    actual = fromdb(dbo, 'SELECT * FROM test_create')

    debug('verify table does not exist to start with')
    try:
        debug(look(actual))
    except Exception as e:
        debug('expected exception: ' + str(e))
    else:
        raise Exception('expected exception not raised')

    debug('verify cannot write without create')
    try:
        todb(expect, dbo, 'test_create')
    except Exception as e:
        debug('expected exception: ' + str(e))
    else:
        raise Exception('expected exception not raised')

    debug('create table and verify')
    todb(expect, dbo, 'test_create', create=True)
    ieq(expect, actual)
    debug(look(actual))

    debug('verify cannot overwrite with new cols without recreate')
    try:
        todb(expect_extended, dbo, 'test_create')
    except Exception as e:
        debug('expected exception: ' + str(e))
    else:
        raise Exception('expected exception not raised')

    debug('verify recreate')
    todb(expect_extended, dbo, 'test_create', create=True, drop=True)
    ieq(expect_extended, actual)
    debug(look(actual))

    debug('horrendous identifiers')
    table = (('foo foo', 'bar.baz."spong`'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    todb(table, dbo, 'foo " bar`', create=True)
    actual = fromdb(dbo, 'SELECT * FROM "foo "" bar`"')
    ieq(table, actual)


def _setup_mysql(dbapi_connection):
    # setup table
    cursor = dbapi_connection.cursor()
    # deal with quote compatibility
    cursor.execute('SET SQL_MODE=ANSI_QUOTES')
    cursor.execute('DROP TABLE IF EXISTS test_create')
    cursor.execute('DROP TABLE IF EXISTS "foo "" bar`"')
    cursor.close()
    dbapi_connection.commit()


def _setup_generic(dbapi_connection):
    # setup table
    cursor = dbapi_connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS test_create')
    cursor.execute('DROP TABLE IF EXISTS "foo "" bar`"')
    cursor.close()
    dbapi_connection.commit()


try:
    # noinspection PyUnresolvedReferences
    import sqlalchemy
except ImportError as e:
    pytest.skip('SKIP generic create tests: %s' % e, allow_module_level=True)
else:

    from sqlalchemy import Column, DateTime, Date

    def test_make_datetime_column():
        sql_col = make_sqlalchemy_column([datetime(2014, 1, 1, 1, 1, 1, 1),
                                          datetime(2014, 1, 1, 1, 1, 1, 2)],
                                         'name')
        expect = Column('name', DateTime(), nullable=False)
        eq_(str(expect.type), str(sql_col.type))

    def test_make_date_column():
        sql_col = make_sqlalchemy_column([date(2014, 1, 1),
                                          date(2014, 1, 2)],
                                         'name')
        expect = Column('name', Date(), nullable=False)
        eq_(str(expect.type), str(sql_col.type))

    def test_sqlite3_create():

        dbapi_connection = sqlite3.connect(':memory:')

        # exercise using a dbapi_connection
        _setup_generic(dbapi_connection)
        _test_create(dbapi_connection)

        # exercise using a dbapi_cursor
        _setup_generic(dbapi_connection)
        dbapi_cursor = dbapi_connection.cursor()
        _test_create(dbapi_cursor)
        dbapi_cursor.close()

SKIP_PYMYSQL = False
try:
    import pymysql
    import sqlalchemy
    pymysql.connect(host=host,
                    user=user,
                    password=password,
                    database=database)
except Exception as e:
    SKIP_PYMYSQL = 'SKIP pymysql create tests: %s' % e
finally:
    @pytest.mark.skipif(bool(SKIP_PYMYSQL), reason=str(SKIP_PYMYSQL))
    def test_mysql_create():

        import pymysql
        connect = pymysql.connect

        # assume database already created
        dbapi_connection = connect(host=host,
                                   user=user,
                                   password=password,
                                   database=database)

        # exercise using a dbapi_connection
        _setup_mysql(dbapi_connection)
        _test_create(dbapi_connection)

        # exercise using a dbapi_cursor
        _setup_mysql(dbapi_connection)
        dbapi_cursor = dbapi_connection.cursor()
        _test_create(dbapi_cursor)
        dbapi_cursor.close()

        # exercise sqlalchemy dbapi_connection
        _setup_mysql(dbapi_connection)
        from sqlalchemy import create_engine
        sqlalchemy_engine = create_engine('mysql+pymysql://%s:%s@%s/%s'
                                          % (user, password, host, database))
        sqlalchemy_connection = sqlalchemy_engine.connect()
        sqlalchemy_connection.execute('SET SQL_MODE=ANSI_QUOTES')
        _test_create(sqlalchemy_connection)
        sqlalchemy_connection.close()

        # exercise sqlalchemy session
        _setup_mysql(dbapi_connection)
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=sqlalchemy_engine)
        sqlalchemy_session = Session()
        _test_create(sqlalchemy_session)
        sqlalchemy_session.close()


SKIP_POSTGRES = False
try:
    import psycopg2
    import sqlalchemy
    psycopg2.connect(
        'host=%s dbname=%s user=%s password=%s'
        % (host, database, user, password)
    )
except Exception as e:
    SKIP_POSTGRES = 'SKIP psycopg2 create tests: %s' % e
finally:
    @pytest.mark.skipif(bool(SKIP_POSTGRES), reason=str(SKIP_POSTGRES))
    def test_postgresql_create():
        import psycopg2
        import psycopg2.extensions
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

        # assume database already created
        dbapi_connection = psycopg2.connect(
            'host=%s dbname=%s user=%s password=%s'
            % (host, database, user, password)
        )
        dbapi_connection.autocommit = True

        # exercise using a dbapi_connection
        _setup_generic(dbapi_connection)
        _test_create(dbapi_connection)

        # exercise using a dbapi_cursor
        _setup_generic(dbapi_connection)
        dbapi_cursor = dbapi_connection.cursor()
        _test_create(dbapi_cursor)
        dbapi_cursor.close()

        # # ignore these for now, having trouble with autocommit
        #
        # # exercise sqlalchemy dbapi_connection
        # _setup_generic(dbapi_connection)
        # from sqlalchemy import create_engine
        # sqlalchemy_engine = create_engine(
        #     'postgresql+psycopg2://%s:%s@%s/%s'
        #     % (user, password, host, database)
        # )
        # sqlalchemy_connection = sqlalchemy_engine.connect()
        # _test_create(sqlalchemy_connection)
        # sqlalchemy_connection.close()
        #
        # # exercise sqlalchemy session
        # _setup_generic(dbapi_connection)
        # from sqlalchemy.orm import sessionmaker
        # Session = sessionmaker(bind=sqlalchemy_engine)
        # sqlalchemy_session = Session()
        # _test_create(sqlalchemy_session)
        # sqlalchemy_session.close()
