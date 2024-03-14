# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <headingcell level=1>

# Using server-side cursors with PostgreSQL and MySQL 

# <codecell>

# see http://pynash.org/2013/03/06/timing-and-profiling.html for setup of profiling magics

# <codecell>

import MySQLdb
import psycopg2

import petl
from petl.fluent import etl

# <codecell>
print(petl.VERSION)
tbl_dummy_data = etl().dummytable(100000)
tbl_dummy_data.look()

# <codecell>

print(tbl_dummy_data.nrows())

# <headingcell level=2>

# PostgreSQL

# <codecell>

psql_connection = psycopg2.connect(host='localhost', dbname='petl', user='petl', password='petl')

# <codecell>

cursor = psql_connection.cursor()
cursor.execute('DROP TABLE IF EXISTS issue_219;')
cursor.execute('CREATE TABLE issue_219 (foo INTEGER, bar TEXT, baz FLOAT);')

# <codecell>

tbl_dummy_data.progress(10000).todb(psql_connection, 'issue_219')

# <codecell>

# memory usage using default cursor
print(etl.fromdb(psql_connection, 'select * from issue_219 order by foo').look(2))

# <codecell>

# memory usage using server-side cursor
print(etl.fromdb(lambda: psql_connection.cursor(name='server-side'), 'select * from issue_219 order by foo').look(2))

# <headingcell level=2>

# MySQL

# <codecell>

mysql_connection = MySQLdb.connect(host='127.0.0.1', db='petl', user='petl', passwd='petl')

# <codecell>

cursor = mysql_connection.cursor()
cursor.execute('SET SQL_MODE=ANSI_QUOTES')
cursor.execute('DROP TABLE IF EXISTS issue_219;')
cursor.execute('CREATE TABLE issue_219 (foo INTEGER, bar TEXT, baz FLOAT);')

# <codecell>

tbl_dummy_data.progress(10000).todb(mysql_connection, 'issue_219')

# <codecell>

# memory usage with default cursor
print(etl.fromdb(mysql_connection, 'select * from issue_219 order by foo').look(2))

# <codecell>

# memory usage with server-side cursor
print(etl.fromdb(lambda: mysql_connection.cursor(MySQLdb.cursors.SSCursor), 'select * from issue_219 order by foo').look(2))

