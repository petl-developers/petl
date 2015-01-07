from __future__ import division, print_function, absolute_import


# limits()
##########

import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
minv, maxv = etl.limits(table, 'bar')
minv
maxv


# stats()
#########

import petl as etl
table = [['foo', 'bar', 'baz'],
         ['A', 1, 2],
         ['B', '2', '3.4'],
         [u'B', u'3', u'7.8', True],
         ['D', 'xyz', 9.0],
         ['E', None]]
etl.stats(table, 'bar')


