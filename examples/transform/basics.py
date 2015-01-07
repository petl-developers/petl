from __future__ import division, print_function, absolute_import


# cut()
#######

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['A', 1, 2.7],
          ['B', 2, 3.4],
          ['B', 3, 7.8],
          ['D', 42, 9.0],
          ['E', 12]]
table2 = etl.cut(table1, 'foo', 'baz')
table2
# fields can also be specified by index, starting from zero
table3 = etl.cut(table1, 0, 2)
table3
# field names and indices can be mixed
table4 = etl.cut(table1, 'bar', 0)
table4
# select a range of fields
table5 = etl.cut(table1, *range(0, 2))
table5


# cutout()
##########

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['A', 1, 2.7],
          ['B', 2, 3.4],
          ['B', 3, 7.8],
          ['D', 42, 9.0],
          ['E', 12]]
table2 = etl.cutout(table1, 'bar')
table2


# cat()
#######

import petl as etl
table1 = [['foo', 'bar'],
          [1, 'A'],
          [2, 'B']]
table2 = [['bar', 'baz'],
          ['C', True],
          ['D', False]]
table3 = etl.cat(table1, table2)
table3
# can also be used to square up a single table with uneven rows
table4 = [['foo', 'bar', 'baz'],
          ['A', 1, 2],
          ['B', '2', '3.4'],
          [u'B', u'3', u'7.8', True],
          ['D', 'xyz', 9.0],
          ['E', None]]
table5 = etl.cat(table4)
table5
# use the header keyword argument to specify a fixed set of fields
table6 = [['bar', 'foo'],
          ['A', 1],
          ['B', 2]]
table7 = etl.cat(table6, header=['A', 'foo', 'B', 'bar', 'C'])
table7
# using the header keyword argument with two input tables
table8 = [['bar', 'foo'],
          ['A', 1],
          ['B', 2]]
table9 = [['bar', 'baz'],
          ['C', True],
          ['D', False]]
table10 = etl.cat(table8, table9, header=['A', 'foo', 'B', 'bar', 'C'])
table10


# addfield()
############

import petl as etl
table1 = [['foo', 'bar'],
          ['M', 12],
          ['F', 34],
          ['-', 56]]
# using a fixed value
table2 = etl.addfield(table1, 'baz', 42)
table2
# calculating the value
table2 = etl.addfield(table1, 'baz', lambda rec: rec['bar'] * 2)
table2


# rowslice()
############

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 5],
          ['d', 7],
          ['f', 42]]
table2 = etl.rowslice(table1, 2)
table2
table3 = etl.rowslice(table1, 1, 4)
table3
table4 = etl.rowslice(table1, 0, 5, 2)
table4


# head()
########

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 5],
          ['d', 7],
          ['f', 42],
          ['f', 3],
          ['h', 90]]
table2 = etl.head(table1, 4)
table2


# tail()
########

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 5],
          ['d', 7],
          ['f', 42],
          ['f', 3],
          ['h', 90],
          ['k', 12],
          ['l', 77],
          ['q', 2]]
table2 = etl.tail(table1, 4)
table2


# skipcomments()
################

import petl as etl
table1 = [['##aaa', 'bbb', 'ccc'],
          ['##mmm',],
          ['#foo', 'bar'],
          ['##nnn', 1],
          ['a', 1],
          ['b', 2]]
table2 = etl.skipcomments(table1, '##')
table2


# annex()
#########

import petl as etl
table1 = [['foo', 'bar'],
          ['A', 9],
          ['C', 2],
          ['F', 1]]
table2 = [['foo', 'baz'],
          ['B', 3],
          ['D', 10]]
table3 = etl.annex(table1, table2)
table3


# addrownumbers()
#################

import petl as etl
table1 = [['foo', 'bar'],
          ['A', 9],
          ['C', 2],
          ['F', 1]]
table2 = etl.addrownumbers(table1)
table2


# addcolumn()
#############

import petl as etl
table1 = [['foo', 'bar'],
          ['A', 1],
          ['B', 2]]
col = [True, False]
table2 = etl.addcolumn(table1, 'baz', col)
table2


# addfieldusingcontext()
########################

import petl as etl
table1 = [['foo', 'bar'],
          ['A', 1],
          ['B', 4],
          ['C', 5],
          ['D', 9]]
def upstream(prv, cur, nxt):
    if prv is None:
        return None
    else:
        return cur.bar - prv.bar

def downstream(prv, cur, nxt):
    if nxt is None:
        return None
    else:
        return nxt.bar - cur.bar

table2 = etl.addfieldusingcontext(table1, 'baz', upstream)
table3 = etl.addfieldusingcontext(table2, 'quux', downstream)
table3

