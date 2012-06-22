#!/usr/bin/env python

import petl
print petl.VERSION
from petl.testutils import ieq

import sys
user = sys.argv[1]
passwd = sys.argv[2]

import MySQLdb
# assume database petl_test already created
connection = MySQLdb.connect(user=user, passwd=passwd, db='petl_test')
print 'setup table'
cursor = connection.cursor()
# deal with quote compatibility
cursor.execute('SET SQL_MODE=ANSI_QUOTES')
cursor.execute('DROP TABLE IF EXISTS test')
cursor.execute('CREATE TABLE test (foo TEXT, bar INT)')
connection.commit()
cursor.close()

print 'exercise the petl functions using a connection'
from petl import look, fromdb, todb, appenddb
t1 = fromdb(connection, 'SELECT * FROM test')
print look(t1)
t2 = (('foo', 'bar'), ('a', 1), ('b', 1))
t2app = (('foo', 'bar'), ('a', 1), ('b', 1), ('a', 1), ('b', 1))
todb(t2, connection, 'test')
print look(t1)
ieq(t2, t1)
appenddb(t2, connection, 'test')
print look(t1)
ieq(t2app, t1)
todb(t2, connection, 'test')
print look(t1)
ieq(t2, t1)

print 'exercise the petl functions using a cursor'
cursor = connection.cursor()
todb(t2, cursor, 'test')
print look(t1)
ieq(t2, t1)
appenddb(t2, cursor, 'test')
print look(t1)
ieq(t2app, t1)
todb(t2, cursor, 'test')
print look(t1)
ieq(t2, t1)

print 'exercise using sqlalchemy engine'
from sqlalchemy import create_engine
engine = create_engine('mysql://%s:%s@localhost/petl_test' % (user, passwd))
# deal with quote compatibility
engine.execute('SET SQL_MODE=ANSI_QUOTES')
t1 = fromdb(engine, 'SELECT * FROM test')
print look(t1)
ieq(t2, t1)
appenddb(t2, engine, 'test')
print look(t1)
ieq(t2app, t1)
todb(t2, engine, 'test')
print look(t1)
ieq(t2, t1)

print 'exercise using sqlalchemy connection'
connection = engine.connect()
t1 = fromdb(connection, 'SELECT * FROM test')
print look(t1)
ieq(t2, t1)
appenddb(t2, connection, 'test')
print look(t1)
ieq(t2app, t1)
todb(t2, connection, 'test')
print look(t1)
ieq(t2, t1)

