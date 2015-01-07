from __future__ import division, print_function, absolute_import


# rename()
##########

import petl as etl
table1 = [['sex', 'age'],
          ['m', 12],
          ['f', 34],
          ['-', 56]]
# rename a single field
table2 = etl.rename(table1, 'sex', 'gender')
table2
# rename multiple fields by passing a dictionary as the second argument
table3 = etl.rename(table1, {'sex': 'gender', 'age': 'age_years'})
table3


# setheader()
#############

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2]]
table2 = etl.setheader(table1, ['foofoo', 'barbar'])
table2


# extendheader()
################

import petl as etl
table1 = [['foo'],
          ['a', 1, True],
          ['b', 2, False]]
table2 = etl.extendheader(table1, ['bar', 'baz'])
table2


# pushheader()
##############

import petl as etl
table1 = [['a', 1],
          ['b', 2]]
table2 = etl.pushheader(table1, ['foo', 'bar'])
table2


# skip()
#########

import petl as etl
table1 = [['#aaa', 'bbb', 'ccc'],
          ['#mmm'],
          ['foo', 'bar'],
          ['a', 1],
          ['b', 2]]
table2 = etl.skip(table1, 2)
table2


