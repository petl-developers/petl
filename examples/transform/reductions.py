from __future__ import absolute_import, print_function, division


# rowreduce()
#############

from petl import rowreduce, look
table1 = [['foo', 'bar'],
          ['a', 3],
          ['a', 7],
          ['b', 2],
          ['b', 1],
          ['b', 9],
          ['c', 4]]
def sumbar(key, rows):
    return [key, sum(row[1] for row in rows)]

table2 = rowreduce(table1, key='foo', reducer=sumbar,
                   fields=['foo', 'barsum'])
look(table2)


# aggregate()
#############

from petl import aggregate, look

table1 = [['foo', 'bar', 'baz'],
          ['a', 3, True],
          ['a', 7, False],
          ['b', 2, True],
          ['b', 2, False],
          ['b', 9, False],
          ['c', 4, True]]
# aggregate whole rows
table2 = aggregate(table1, 'foo', len)
look(table2)
# aggregate single field
table3 = aggregate(table1, 'foo', sum, 'bar')
look(table3)
# alternative signature using keyword args
table4 = aggregate(table1, key=('foo', 'bar'),
                   aggregation=list, value=('bar', 'baz'))
look(table4)
# aggregate multiple fields
from collections import OrderedDict
from petl import strjoin

aggregation = OrderedDict()
aggregation['count'] = len
aggregation['minbar'] = 'bar', min
aggregation['maxbar'] = 'bar', max
aggregation['sumbar'] = 'bar', sum
# default aggregation function is list
aggregation['listbar'] = 'bar'
aggregation['listbarbaz'] = ('bar', 'baz'), list
aggregation['bars'] = 'bar', strjoin(', ')
table5 = aggregate(table1, 'foo', aggregation)
look(table5)


# mergeduplicates()
###################

from petl import mergeduplicates, look
table1 = [['foo', 'bar', 'baz'],
          ['A', 1, 2.7],
          ['B', 2, None],
          ['D', 3, 9.4],
          ['B', None, 7.8],
          ['E', None, 42.],
          ['D', 3, 12.3],
          ['A', 2, None]]
table2 = mergeduplicates(table1, 'foo')
look(table2)


# merge()
#########

from petl import look, merge
table1 = [['foo', 'bar', 'baz'],
          [1, 'A', True],
          [2, 'B', None],
          [4, 'C', True]]
table2 = [['bar', 'baz', 'quux'],
          ['A', True, 42.0],
          ['B', False, 79.3],
          ['C', False, 12.4]]
table3 = merge(table1, table2, key='bar')
look(table3)


# fold()
########

from petl import fold, look
table1 = [['id', 'count'], 
          [1, 3], 
          [1, 5],
          [2, 4], 
          [2, 8]]        
import operator
table2 = fold(table1, 'id', operator.add, 'count', 
              presorted=True)
look(table2)
