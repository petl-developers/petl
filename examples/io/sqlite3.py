from __future__ import division, print_function, absolute_import
import os

# fromsqlite3()
###############

os.remove('example.db')
import sqlite3
# set up a database to demonstrate with
data = [['a', 1],
        ['b', 2],
        ['c', 2.0]]
connection = sqlite3.connect('example.db')
c = connection.cursor()
_ = c.execute('drop table if exists foobar')
_ = c.execute('create table foobar (foo, bar)')
for row in data:
    _ = c.execute('insert into foobar values (?, ?)', row)

connection.commit()
c.close()
# now demonstrate the petl.fromsqlite3 function
from petl import look, fromsqlite3
foobar = fromsqlite3('example.db', 'select * from foobar')
look(foobar)


# tosqlite3()
##############

os.remove('example.db')
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
from petl import tosqlite3, look
_ = tosqlite3(table1, 'example.db', 'foobar', create=True)
# look what it did
from petl import fromsqlite3
table2 = fromsqlite3('example.db', 'select * from foobar')
look(table2)

