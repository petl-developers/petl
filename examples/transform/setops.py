from __future__ import absolute_import, print_function, division


# complement()
##############

import petl as etl
a = [['foo', 'bar', 'baz'],
     ['A', 1, True],
     ['C', 7, False],
     ['B', 2, False],
     ['C', 9, True]]
b = [['x', 'y', 'z'],
     ['B', 2, False],
     ['A', 9, False],
     ['B', 3, True],
     ['C', 9, True]]
aminusb = etl.complement(a, b)
aminusb
bminusa = etl.complement(b, a)
bminusa


# recordcomplement()
####################

import petl as etl
a = [['foo', 'bar', 'baz'],
     ['A', 1, True],
     ['C', 7, False],
     ['B', 2, False],
     ['C', 9, True]]
b = [['bar', 'foo', 'baz'],
     [2, 'B', False],
     [9, 'A', False],
     [3, 'B', True],
     [9, 'C', True]]
aminusb = etl.recordcomplement(a, b)
aminusb
bminusa = etl.recordcomplement(b, a)
bminusa


# diff()
########

import petl as etl
a = [['foo', 'bar', 'baz'],
     ['A', 1, True],
     ['C', 7, False],
     ['B', 2, False],
     ['C', 9, True]]
b = [['x', 'y', 'z'],
     ['B', 2, False],
     ['A', 9, False],
     ['B', 3, True],
     ['C', 9, True]]
added, subtracted = etl.diff(a, b)
# rows in b not in a
added
# rows in a not in b
subtracted


# recorddiff()
##############

import petl as etl
a = [['foo', 'bar', 'baz'],
     ['A', 1, True],
     ['C', 7, False],
     ['B', 2, False],
     ['C', 9, True]]
b = [['bar', 'foo', 'baz'],
     [2, 'B', False],
     [9, 'A', False],
     [3, 'B', True],
     [9, 'C', True]]
added, subtracted = etl.recorddiff(a, b)
added
subtracted


# intersection()
################

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['A', 1, True],
          ['C', 7, False],
          ['B', 2, False],
          ['C', 9, True]]
table2 = [['x', 'y', 'z'],
          ['B', 2, False],
          ['A', 9, False],
          ['B', 3, True],
          ['C', 9, True]]
table3 = etl.intersection(table1, table2)
table3


