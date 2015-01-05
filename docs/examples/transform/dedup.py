from __future__ import division, print_function, absolute_import


# duplicates()
##############

from petl import duplicates, look
table1 = [
    ['foo', 'bar', 'baz'],
    ['A', 1, 2.0],
    ['B', 2, 3.4],
    ['D', 6, 9.3],
    ['B', 3, 7.8],
    ['B', 2, 12.3],
    ['E', None, 1.3],
    ['D', 4, 14.5]
]
table2 = duplicates(table1, 'foo')
look(table2)
# compound keys are supported
table3 = duplicates(table1, key=['foo', 'bar'])
look(table3)


# unique()
##########

from petl import unique, look
table1 = [
    ['foo', 'bar', 'baz'],
    ['A', 1, 2],
    ['B', '2', '3.4'],
    ['D', 'xyz', 9.0],
    ['B', u'3', u'7.8'],
    ['B', '2', 42],
    ['E', None, None],
    ['D', 4, 12.3],
    ['F', 7, 2.3]
]
table2 = unique(table1, 'foo')
look(table2)


# conflicts()
#############

from petl import conflicts, look
table1 = [
    ['foo', 'bar', 'baz'],
    ['A', 1, 2.7],
    ['B', 2, None],
    ['D', 3, 9.4],
    ['B', None, 7.8],
    ['E', None],
    ['D', 3, 12.3],
    ['A', 2, None]
]
table2 = conflicts(table1, 'foo')
look(table2)



