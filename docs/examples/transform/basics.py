from __future__ import division, print_function, absolute_import


# cut()
#######

from petl import look, cut
table1 = [
    ['foo', 'bar', 'baz'],
    ['A', 1, 2.7],
    ['B', 2, 3.4],
    ['B', 3, 7.8],
    ['D', 42, 9.0],
    ['E', 12]
]
table2 = cut(table1, 'foo', 'baz')
look(table2)
# fields can also be specified by index, starting from zero
table3 = cut(table1, 0, 2)
look(table3)
# field names and indices can be mixed
table4 = cut(table1, 'bar', 0)
look(table4)
# select a range of fields
table5 = cut(table1, *range(0, 2))
look(table5)


# cutout()
##########

from petl import cutout, look
table1 = [
    ['foo', 'bar', 'baz'],
    ['A', 1, 2.7],
    ['B', 2, 3.4],
    ['B', 3, 7.8],
    ['D', 42, 9.0],
    ['E', 12]
]
table2 = cutout(table1, 'bar')
look(table2)


# cat()
#######

from petl import look, cat
table1 = [
    ['foo', 'bar'],
    [1, 'A'],
    [2, 'B']
]
table2 = [
    ['bar', 'baz'],
    ['C', True],
    ['D', False]
]
table3 = cat(table1, table2)
look(table3)
# can also be used to square up a single table with uneven rows
table4 = [
    ['foo', 'bar', 'baz'],
    ['A', 1, 2],
    ['B', '2', '3.4'],
    [u'B', u'3', u'7.8', True],
    ['D', 'xyz', 9.0],
    ['E', None]
]
table5 = cat(table4)
look(table5)
# use the header keyword argument to specify a fixed set of fields
table6 = [
    ['bar', 'foo'],
    ['A', 1],
    ['B', 2]
]
table7 = cat(table6, header=['A', 'foo', 'B', 'bar', 'C'])
look(table7)
# using the header keyword argument with two input tables
table8 = [
    ['bar', 'foo'],
    ['A', 1],
    ['B', 2]
]
table9 = [
    ['bar', 'baz'],
    ['C', True],
    ['D', False]
]
table10 = cat(table8, table9, header=['A', 'foo', 'B', 'bar', 'C'])
look(table10)


# addfield()
############

from petl import addfield, look
table1 = [['foo', 'bar'],
          ['M', 12],
          ['F', 34],
          ['-', 56]]
# using a fixed value
table2 = addfield(table1, 'baz', 42)
look(table2)
# calculating the value
table2 = addfield(table1, 'baz', lambda rec: rec['bar'] * 2)
look(table2)
# an expression string can also be used via expr
from petl import expr
table3 = addfield(table1, 'baz', expr('{bar} * 2'))
look(table3)


# rowslice()
############

from petl import rowslice, look
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 5],
          ['d', 7],
          ['f', 42]]
table2 = rowslice(table1, 2)
look(table2)
table3 = rowslice(table1, 1, 4)
look(table3)
table4 = rowslice(table1, 0, 5, 2)
look(table4)


# head()
########

from petl import head, look
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 5],
          ['d', 7],
          ['f', 42],
          ['f', 3],
          ['h', 90]]
table2 = head(table1, 4)
look(table2)


# tail()
########

from petl import tail, look
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
table2 = tail(table1, 4)
look(table2)


# skipcomments()
################

from petl import skipcomments, look
table1 = [['##aaa', 'bbb', 'ccc'],
          ['##mmm',],
          ['#foo', 'bar'],
          ['##nnn', 1],
          ['a', 1],
          ['b', 2]]
table2 = skipcomments(table1, '##')
look(table2)


# annex()
#########

from petl import annex, look
table1 = [['foo', 'bar'],
          ['A', 9],
          ['C', 2],
          ['F', 1]]
table2 = [['foo', 'baz'],
          ['B', 3],
          ['D', 10]]
table3 = annex(table1, table2)
look(table3)


# addrownumbers()
#################

from petl import addrownumbers, look
table1 = [['foo', 'bar'],
          ['A', 9],
          ['C', 2],
          ['F', 1]]
table2 = addrownumbers(table1)
look(table2)


# addcolumn()
#############

from petl import addcolumn, look
table1 = [['foo', 'bar'],
          ['A', 1],
          ['B', 2]]
col = [True, False]
table2 = addcolumn(table1, 'baz', col)
look(table2)


# addfieldusingcontext()
########################

from petl import look, addfieldusingcontext
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

table2 = addfieldusingcontext(table1, 'baz', upstream)
table3 = addfieldusingcontext(table2, 'quux', downstream)
look(table3)

