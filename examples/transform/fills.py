from __future__ import division, print_function, absolute_import


# filldown()
############

table1 = [['foo', 'bar', 'baz'],
          [1, 'a', None],
          [1, None, .23],
          [1, 'b', None],
          [2, None, None],
          [2, None, .56],
          [2, 'c', None],
          [None, 'c', .72]]
from petl import filldown, lookall
table2 = filldown(table1)
lookall(table2)
table3 = filldown(table1, 'bar')
lookall(table3)
table4 = filldown(table1, 'bar', 'baz')
lookall(table4)


# fillright()
#############

table1 = [['foo', 'bar', 'baz'],
          [1, 'a', None],
          [1, None, .23],
          [1, 'b', None],
          [2, None, None],
          [2, None, .56],
          [2, 'c', None],
          [None, 'c', .72]]
from petl import fillright, lookall
table2 = fillright(table1)
lookall(table2)


# fillleft()
############

table1 = [['foo', 'bar', 'baz'],
          [1, 'a', None],
          [1, None, .23],
          [1, 'b', None],
          [2, None, None],
          [2, None, .56],
          [2, 'c', None],
          [None, 'c', .72]]
from petl import fillleft, lookall
table2 = fillleft(table1)
lookall(table2)
