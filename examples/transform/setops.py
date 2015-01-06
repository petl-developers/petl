from __future__ import absolute_import, print_function, division


# complement()
##############

from petl import complement, look
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
aminusb = complement(a, b)
look(aminusb)
bminusa = complement(b, a)
look(bminusa)


# recordcomplement()
####################

from petl import recordcomplement, look
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
aminusb = recordcomplement(a, b)
look(aminusb)
bminusa = recordcomplement(b, a)
look(bminusa)


# diff()
########

from petl import diff, look

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
added, subtracted = diff(a, b)
# rows in b not in a
look(added)
# rows in a not in b
look(subtracted)


# recorddiff()
##############

from petl import recorddiff, look    
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
added, subtracted = recorddiff(a, b)
look(added)
look(subtracted)


# intersection()
################

from petl import intersection, look
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
table3 = intersection(table1, table2)
look(table3)


