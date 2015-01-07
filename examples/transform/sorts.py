from __future__ import absolute_import, print_function, division


# sort()
########

import petl as etl
table1 = [['foo', 'bar'],
          ['C', 2],
          ['A', 9],
          ['A', 6],
          ['F', 1],
          ['D', 10]]
table2 = etl.sort(table1, 'foo')
table2
# sorting by compound key is supported
table3 = etl.sort(table1, key=['foo', 'bar'])
table3
# if no key is specified, the default is a lexical sort
table4 = etl.sort(table1)
table4


# mergesort()
#############

import petl as etl
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
table3 = etl.mergesort(table1, table2, key='foo')
table3.lookall()


# issorted()
############

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 3, True],
          ['b', 2]]
etl.issorted(table1, key='foo')
etl.issorted(table1, key='bar')
etl.issorted(table1, key='foo', strict=True)
etl.issorted(table1, key='foo', reverse=True)

