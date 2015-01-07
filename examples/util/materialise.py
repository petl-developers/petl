from __future__ import division, print_function, absolute_import


# columns()
###########

import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
cols = etl.columns(table)
cols['foo']
cols['bar']


# facetcolumns()
################

import petl as etl
table = [['foo', 'bar', 'baz'],
         ['a', 1, True],
         ['b', 2, True],
         ['b', 3]]
fc = etl.facetcolumns(table, 'foo')
fc['a']
fc['b']
