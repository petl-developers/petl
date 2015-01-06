from __future__ import division, print_function, absolute_import


# rename()
##########

table1 = [['sex', 'age'],
          ['m', 12],
          ['f', 34],
          ['-', 56]]
from petl import look, rename
# rename a single field
table2 = rename(table1, 'sex', 'gender')
look(table2)
# rename multiple fields by passing a dictionary as the second argument
table3 = rename(table1, {'sex': 'gender', 'age': 'age_years'})
look(table3)


# setheader()
#############

table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2]]
from petl import setheader, look
table2 = setheader(table1, ['foofoo', 'barbar'])
look(table2)


# extendheader()
################

table1 = [['foo'],
          ['a', 1, True],
          ['b', 2, False]]
from petl import extendheader, look
table2 = extendheader(table1, ['bar', 'baz'])
look(table2)


# pushheader()
##############

table1 = [['a', 1],
          ['b', 2]]
from petl import pushheader, look
table2 = pushheader(table1, ['foo', 'bar'])
look(table2)


# skip()
#########

table1 = [['#aaa', 'bbb', 'ccc'],
          ['#mmm'],
          ['foo', 'bar'],
          ['a', 1],
          ['b', 2]]
from petl import skip, look
table2 = skip(table1, 2)
look(table2)


