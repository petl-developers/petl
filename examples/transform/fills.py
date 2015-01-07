from __future__ import division, print_function, absolute_import


# filldown()
############

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          [1, 'a', None],
          [1, None, .23],
          [1, 'b', None],
          [2, None, None],
          [2, None, .56],
          [2, 'c', None],
          [None, 'c', .72]]
table2 = etl.filldown(table1)
table2.lookall()
table3 = etl.filldown(table1, 'bar')
table3.lookall()
table4 = etl.filldown(table1, 'bar', 'baz')
table4.lookall()


# fillright()
#############

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          [1, 'a', None],
          [1, None, .23],
          [1, 'b', None],
          [2, None, None],
          [2, None, .56],
          [2, 'c', None],
          [None, 'c', .72]]
table2 = etl.fillright(table1)
table2.lookall()


# fillleft()
############

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          [1, 'a', None],
          [1, None, .23],
          [1, 'b', None],
          [2, None, None],
          [2, None, .56],
          [2, 'c', None],
          [None, 'c', .72]]
table2 = etl.fillleft(table1)
table2.lookall()
