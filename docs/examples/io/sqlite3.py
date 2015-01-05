from __future__ import division, print_function, absolute_import
import os

# fromsqlite3()
###############

import sqlite3
# set up a database to demonstrate with
data = [['a', 1],
        ['b', 2],
        ['c', 2.0]]
connection = sqlite3.connect('test.db')
c = connection.cursor()
_ = c.execute('drop table if exists foobar')
_ = c.execute('create table foobar (foo, bar)')
for row in data:
    _ = c.execute('insert into foobar values (?, ?)', row)

connection.commit()
c.close()
# now demonstrate the petl.fromsqlite3 function
from petl import look, fromsqlite3
foobar = fromsqlite3('test.db', 'select * from foobar')
look(foobar)


# tosqlite3()
##############

os.remove('test.db')
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
from petl import tosqlite3, look
_ = tosqlite3(table1, 'test.db', 'foobar', create=True)
# look what it did
from petl import fromsqlite3
table2 = fromsqlite3('test.db', 'select * from foobar')
look(table2)

