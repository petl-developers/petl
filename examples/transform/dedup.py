from __future__ import division, print_function, absolute_import


# duplicates()
##############

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['A', 1, 2.0],
          ['B', 2, 3.4],
          ['D', 6, 9.3],
          ['B', 3, 7.8],
          ['B', 2, 12.3],
          ['E', None, 1.3],
          ['D', 4, 14.5]]
table2 = etl.duplicates(table1, 'foo')
table2
# compound keys are supported
table3 = etl.duplicates(table1, key=['foo', 'bar'])
table3


# unique()
##########

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['A', 1, 2],
          ['B', '2', '3.4'],
          ['D', 'xyz', 9.0],
          ['B', u'3', u'7.8'],
          ['B', '2', 42],
          ['E', None, None],
          ['D', 4, 12.3],
          ['F', 7, 2.3]]
table2 = etl.unique(table1, 'foo')
table2


# conflicts()
#############

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['A', 1, 2.7],
          ['B', 2, None],
          ['D', 3, 9.4],
          ['B', None, 7.8],
          ['E', None],
          ['D', 3, 12.3],
          ['A', 2, None]]
table2 = etl.conflicts(table1, 'foo')
table2


# isunique()
############

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b'],
          ['b', 2],
          ['c', 3, True]]
etl.isunique(table1, 'foo')
etl.isunique(table1, 'bar')


