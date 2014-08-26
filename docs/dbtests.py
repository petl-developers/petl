# -*- coding: utf-8 -*-


"""
DB-related tests, separated from main unit tests because they need local database
setup prior to running.

"""


from __future__ import absolute_import, print_function, division


import sys
sys.path.insert(0, './src')
import petl
from petl.testutils import ieq
from petl import look, fromdb, todb, appenddb, cut


def exercise(dbo):
    print('=' * len(repr(dbo)))
    print(repr(dbo))
    print('=' * len(repr(dbo)))
    print(    )
    expect_empty = (('foo', 'bar'),)
    expect = (('foo', 'bar'), ('a', 1), ('b', 1))
    expect_appended = (('foo', 'bar'), ('a', 1), ('b', 1), ('a', 1), ('b', 1))
    actual = fromdb(dbo, 'SELECT * FROM test')

    print('verify empty to start with...')
    ieq(expect_empty, actual)
    print(look(actual))
    
    print('write some data and verify...')
    todb(expect, dbo, 'test')
    ieq(expect, actual)
    print(look(actual))
    
    print('append some data and verify...')
    appenddb(expect, dbo, 'test')
    ieq(expect_appended, actual)
    print(look(actual))
    
    print('overwrite and verify...')
    todb(expect, dbo, 'test')
    ieq(expect, actual)
    print(look(actual))
    
    print('cut, overwrite and verify')
    todb(cut(expect, 'bar', 'foo'), dbo, 'test')
    ieq(expect, actual)
    print(look(actual))

    print('cut, append and verify')
    appenddb(cut(expect, 'bar', 'foo'), dbo, 'test')
    ieq(expect_appended, actual)
    print(look(actual))


def exercise_ss_cursor(setup_dbo, ss_dbo):
    print('=' * len(repr(ss_dbo)))
    print('EXERCISE WITH SERVER-SIDE CURSOR')
    print(repr(ss_dbo))
    print('=' * len(repr(ss_dbo)))
    print()
    expect_empty = (('foo', 'bar'),)
    expect = (('foo', 'bar'), ('a', 1), ('b', 1))
    expect_appended = (('foo', 'bar'), ('a', 1), ('b', 1), ('a', 1), ('b', 1))
    actual = fromdb(ss_dbo, 'SELECT * FROM test')

    print('verify empty to start with...')
    ieq(expect_empty, actual)
    print(look(actual))

    print('write some data and verify...')
    todb(expect, setup_dbo, 'test')
    ieq(expect, actual)
    print(look(actual))

    print('append some data and verify...')
    appenddb(expect, setup_dbo, 'test')
    ieq(expect_appended, actual)
    print(look(actual))

    print('overwrite and verify...')
    todb(expect, setup_dbo, 'test')
    ieq(expect, actual)
    print(look(actual))

    print('cut, overwrite and verify')
    todb(cut(expect, 'bar', 'foo'), setup_dbo, 'test')
    ieq(expect, actual)
    print(look(actual))

    print('cut, append and verify')
    appenddb(cut(expect, 'bar', 'foo'), setup_dbo, 'test')
    ieq(expect_appended, actual)
    print(look(actual))


def exercise_with_schema(dbo, db):
    print('=' * len(repr(dbo)))
    print('EXERCISE WITH EXPLICIT SCHEMA NAME')
    print(repr(dbo))
    print('=' * len(repr(dbo)))
    print(    )
    expect = (('foo', 'bar'), ('a', 1), ('b', 1))
    expect_appended = (('foo', 'bar'), ('a', 1), ('b', 1), ('a', 1), ('b', 1))
    actual = fromdb(dbo, 'SELECT * FROM test')

    print('write some data and verify...')
    todb(expect, dbo, 'test', schema=db)
    ieq(expect, actual)
    print(look(actual))
    
    print('append some data and verify...')
    appenddb(expect, dbo, 'test', schema=db)
    ieq(expect_appended, actual)
    print(look(actual))
    

def exercise_unicode(dbo):
    print('=' * len(repr(dbo)))
    print('EXERCISE UNICODE')
    print(repr(dbo))
    print('=' * len(repr(dbo)))
    print()
    expect = ((u'name', u'id'),
              (u'Արամ Խաչատրյան', 1),
              (u'Johann Strauß', 2),
              (u'Вагиф Сәмәдоғлу', 3),
              (u'章子怡', 4),
              )
    actual = fromdb(dbo, 'SELECT * FROM test_unicode')
    print('write some data and verify...')
    todb(expect, dbo, 'test_unicode')
    ieq(expect, actual)
    print(look(actual))


def setup_mysql(dbapi_connection):
    # setup table
    cursor = dbapi_connection.cursor()
    # deal with quote compatibility
    cursor.execute('SET SQL_MODE=ANSI_QUOTES')
    cursor.execute('DROP TABLE IF EXISTS test')
    cursor.execute('CREATE TABLE test (foo TEXT, bar INT)')
    cursor.execute('DROP TABLE IF EXISTS test_unicode')
    cursor.execute('CREATE TABLE test_unicode (name TEXT, id INT) CHARACTER SET utf8')
    cursor.close()
    dbapi_connection.commit()


def exercise_mysql(host, user, passwd, db):
    import MySQLdb
    
    # assume database already created
    dbapi_connection = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
    
    # exercise using a dbapi_connection
    setup_mysql(dbapi_connection)
    exercise(dbapi_connection)
    
    # exercise using a dbapi_cursor
    setup_mysql(dbapi_connection)
    dbapi_cursor = dbapi_connection.cursor()
    exercise(dbapi_cursor)
    dbapi_cursor.close()
    
    # exercise sqlalchemy dbapi_connection
    setup_mysql(dbapi_connection)
    from sqlalchemy import create_engine
    sqlalchemy_engine = create_engine('mysql://%s:%s@%s/%s' % (user, passwd, host, db))
    sqlalchemy_connection = sqlalchemy_engine.connect()
    sqlalchemy_connection.execute('SET SQL_MODE=ANSI_QUOTES')
    exercise(sqlalchemy_connection)
    sqlalchemy_connection.close()
    
    # exercise sqlalchemy session
    setup_mysql(dbapi_connection)
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=sqlalchemy_engine)
    sqlalchemy_session = Session()
    exercise(sqlalchemy_session)
    sqlalchemy_session.close()

    # other exercises
    exercise_ss_cursor(dbapi_connection, lambda: dbapi_connection.cursor(MySQLdb.cursors.SSCursor))
    exercise_with_schema(dbapi_connection, db)
    utf8_connection = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    utf8_connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    exercise_unicode(utf8_connection)


def setup_postgresql(dbapi_connection):
    # setup table
    cursor = dbapi_connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS test')
    cursor.execute('CREATE TABLE test (foo TEXT, bar INT)')
    cursor.execute('DROP TABLE IF EXISTS test_unicode')
    # assume character encoding UTF-8 already set at database level
    cursor.execute('CREATE TABLE test_unicode (name TEXT, id INT)')
    cursor.close()
    dbapi_connection.commit()
    
    
def exercise_postgresql(host, user, passwd, db):
    import psycopg2
    import psycopg2.extensions
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

    # assume database already created
    dbapi_connection = psycopg2.connect('host=%s dbname=%s user=%s password=%s' % (host, db, user, passwd))

    # exercise using a dbapi_connection
    setup_postgresql(dbapi_connection)
    exercise(dbapi_connection)
    
    # exercise using a dbapi_cursor
    setup_postgresql(dbapi_connection)
    dbapi_cursor = dbapi_connection.cursor()
    exercise(dbapi_cursor)
    dbapi_cursor.close()
    
    # exercise sqlalchemy dbapi_connection
    setup_postgresql(dbapi_connection)
    from sqlalchemy import create_engine
    sqlalchemy_engine = create_engine('postgresql+psycopg2://%s:%s@%s/%s' % (user, passwd, host, db))
    sqlalchemy_connection = sqlalchemy_engine.connect()
    exercise(sqlalchemy_connection)
    sqlalchemy_connection.close()
    
    # exercise sqlalchemy session
    setup_postgresql(dbapi_connection)
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=sqlalchemy_engine)
    sqlalchemy_session = Session()
    exercise(sqlalchemy_session)
    sqlalchemy_session.close()

    # other exercises
    exercise_ss_cursor(dbapi_connection, lambda: dbapi_connection.cursor(name='arbitrary'))
    exercise_with_schema(dbapi_connection, 'public')
    exercise_unicode(dbapi_connection)


if __name__ == '__main__':
    print(petl.VERSION)
    
    # setup logging
    import logging
    logger = logging.getLogger('petl.io')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(levelname)s] %(name)s - %(funcName)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    if sys.argv[1] == 'mysql':
        exercise_mysql(*sys.argv[2:])
    elif sys.argv[1] == 'postgresql':
        exercise_postgresql(*sys.argv[2:])
    else:
        print('either mysql or postgresql')
    
    
    
    
        
    
    
    
