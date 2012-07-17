#!/usr/bin/env python

import petl
print petl.VERSION
from petl.testutils import ieq

import sys
user = sys.argv[1]
passwd = sys.argv[2]


# set up logging
import logging
logger = logging.getLogger('petl.io')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(name)s - %(funcName)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


import MySQLdb
# assume database petl_test already created
connection = MySQLdb.connect(user=user, passwd=passwd, db='petl_test')

def prompt(msg):
    i = raw_input(msg + '? ([y]/n)\n')
    if i not in ('', 'y', 'Y'):
        sys.exit(0)
     
prompt('setup table')
cursor = connection.cursor()
# deal with quote compatibility
cursor.execute('SET SQL_MODE=ANSI_QUOTES')
cursor.execute('DROP TABLE IF EXISTS test')
cursor.execute('CREATE TABLE test (foo TEXT, bar INT)')
cursor.close()
connection.commit()

prompt('exercise the petl functions using a connection')
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

prompt('exercise the petl functions using a cursor')
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

from sqlalchemy import create_engine
engine = create_engine('mysql://%s:%s@localhost/petl_test' % (user, passwd))

prompt('exercise using sqlalchemy connection')
connection = engine.connect()
connection.execute('SET SQL_MODE=ANSI_QUOTES')
prompt('fromdb')
t1 = fromdb(connection, 'SELECT * FROM test')
print look(t1)
ieq(t2, t1)
prompt('appenddb')
appenddb(t2, connection, 'test')
prompt('look')
print look(t1)
ieq(t2app, t1)
prompt('todb')
todb(t2, connection, 'test')
prompt('look')
print look(t1)
ieq(t2, t1)

prompt('exercise using sqlalchemy engine')
prompt('fromdb')
# N.B., I'm not sure why we have to set SQL_MODE multiple times here, this
# means that using an engine directly is probably not recommended, especially
# where other statements have to be executed to ensure SQL-92 quote character
# compatibility
engine.execute('SET SQL_MODE=ANSI_QUOTES')
t1 = fromdb(engine, 'SELECT * FROM "test"')
print look(t1)
ieq(t2, t1)
prompt('appenddb')
appenddb(t2, engine, 'test')
prompt('look')
engine.execute('SET SQL_MODE=ANSI_QUOTES')
print look(t1)
ieq(t2app, t1)
prompt('todb')
todb(t2, engine, 'test')
prompt('look')
engine.execute('SET SQL_MODE=ANSI_QUOTES')
print look(t1)
ieq(t2, t1)


prompt('exercise using sqlalchemy session')
prompt('fromdb')
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()
t1 = fromdb(session, 'SELECT * FROM test')
print look(t1)
ieq(t2, t1)
prompt('appenddb')
appenddb(t2, session, 'test')
prompt('look')
print look(t1)
ieq(t2app, t1)
prompt('todb')
todb(t2, session, 'test')
prompt('look')
print look(t1)
ieq(t2, t1)

