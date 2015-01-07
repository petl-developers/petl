from __future__ import division, print_function, absolute_import


# nrows()
#########

import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2]]
etl.nrows(table)


# valuecount()
##############

import petl as etl
table = [['foo', 'bar'],
         ['a', 1],
         ['b', 2],
         ['b', 7]]
etl.valuecount(table, 'foo', 'b')


# valuecounter()
################

import petl as etl
table = [['foo', 'bar'],
         ['a', True],
         ['b'],
         ['b', True],
         ['c', False]]
etl.valuecounter(table, 'foo').most_common()


# valuecounts()
###############

import petl as etl
table = [['foo', 'bar', 'baz'],
         ['a', True, 0.12],
         ['a', True, 0.17],
         ['b', False, 0.34],
         ['b', False, 0.44],
         ['b']]
etl.valuecounts(table, 'foo')
etl.valuecounts(table, 'foo', 'bar')


# parsecounter()
################

import petl as etl
table = [['foo', 'bar', 'baz'],
         ['A', 'aaa', 2],
         ['B', u'2', '3.4'],
         [u'B', u'3', u'7.8', True],
         ['D', '3.7', 9.0],
         ['E', 42]]
counter, errors = etl.parsecounter(table, 'bar')
counter.most_common()
errors.most_common()


# parsecounts()
###############

import petl as etl
table = [['foo', 'bar', 'baz'],
         ['A', 'aaa', 2],
         ['B', u'2', '3.4'],
         [u'B', u'3', u'7.8', True],
         ['D', '3.7', 9.0],
         ['E', 42]]
etl.parsecounts(table, 'bar')


# typecounter()
###############

import petl as etl
table = [['foo', 'bar', 'baz'],
         ['A', 1, 2],
         ['B', u'2', '3.4'],
         [u'B', u'3', u'7.8', True],
         ['D', u'xyz', 9.0],
         ['E', 42]]
etl.typecounter(table, 'foo').most_common()
etl.typecounter(table, 'bar').most_common()
etl.typecounter(table, 'baz').most_common()


# typecounts()
##############

import petl as etl
table = [['foo', 'bar', 'baz'],
         [b'A', 1, 2],
         [b'B', '2', b'3.4'],
         ['B', '3', '7.8', True],
         ['D', u'xyz', 9.0],
         ['E', 42]]
etl.typecounts(table, 'foo')
etl.typecounts(table, 'bar')
etl.typecounts(table, 'baz')


# stringpatterns()
##################

import petl as etl
table = [['foo', 'bar'],
         ['Mr. Foo', '123-1254'],
         ['Mrs. Bar', '234-1123'],
         ['Mr. Spo', '123-1254'],
         [u'Mr. Baz', u'321 1434'],
         [u'Mrs. Baz', u'321 1434'],
         ['Mr. Quux', '123-1254-XX']]
etl.stringpatterns(table, 'foo')
etl.stringpatterns(table, 'bar')


# rowlengths()
###############

import petl as etl
table = [['foo', 'bar', 'baz'],
         ['A', 1, 2],
         ['B', '2', '3.4'],
         [u'B', u'3', u'7.8', True],
         ['D', 'xyz', 9.0],
         ['E', None],
         ['F', 9]]
etl.rowlengths(table)
