from __future__ import absolute_import, print_function, division


import petl as etl
table = [['foo', 'bar'],
         ['a', 1],
         ['b', None]]

# raises exception under Python 3
etl.select(table, 'bar', lambda v: v > 0)
# no error under Python 3
etl.selectgt(table, 'bar', 0)
# or ...
etl.select(table, 'bar', lambda v: v > etl.Comparable(0))

