from __future__ import absolute_import, print_function, division


# sort()
########

from petl import sort, look
table1 = [['foo', 'bar'],
          ['C', 2],
          ['A', 9],
          ['A', 6],
          ['F', 1],
          ['D', 10]]
table2 = sort(table1, 'foo')
look(table2)
# sorting by compound key is supported
table3 = sort(table1, key=['foo', 'bar'])
look(table3)
# if no key is specified, the default is a lexical sort
table4 = sort(table1)
look(table4)


# mergesort()
#############

from petl import mergesort, look
table1 = [['foo', 'bar'],
          ['A', 9],
          ['C', 2],
          ['D', 10],
          ['A', 6],
          ['F', 1]]
table2 = [['foo', 'bar'],
          ['B', 3],
          ['D', 10],
          ['A', 10],
          ['F', 4]]
table3 = mergesort(table1, table2, key='foo')
look(table3)


