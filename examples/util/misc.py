from __future__ import division, print_function, absolute_import


# typeset()
###########

import petl as etl
table = [['foo', 'bar', 'baz'],
         ['A', 1, '2'],
         ['B', u'2', '3.4'],
         [u'B', u'3', '7.8', True],
         ['D', u'xyz', 9.0],
         ['E', 42]]
sorted(etl.typeset(table, 'foo'))
sorted(etl.typeset(table, 'bar'))
sorted(etl.typeset(table, 'baz'))


# diffheaders()
###############

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['a', 1, .3]]
table2 = [['baz', 'bar', 'quux'],
          ['a', 1, .3]]
add, sub = etl.diffheaders(table1, table2)
add
sub


# diffvalues()
##############

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 3]]
table2 = [['bar', 'foo'],
          [1, 'a'],
          [3, 'c']]
add, sub = etl.diffvalues(table1, table2, 'foo')
add
sub


# nthword()
###########

import petl as etl
s = 'foo bar'
f = etl.nthword(0)
f(s)
g = etl.nthword(1)
g(s)

