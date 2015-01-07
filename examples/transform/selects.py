from __future__ import absolute_import, print_function, division


# select()
##########

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['a', 4, 9.3],
          ['a', 2, 88.2],
          ['b', 1, 23.3],
          ['c', 8, 42.0],
          ['d', 7, 100.9],
          ['c', 2]]
# the second positional argument can be a function accepting
# a row
table2 = etl.select(table1,
                    lambda rec: rec.foo == 'a' and rec.baz > 88.1)
table2
# the second positional argument can also be an expression
# string, which will be converted to a function using petl.expr()
table3 = etl.select(table1, "{foo} == 'a' and {baz} > 88.1")
table3
# the condition can also be applied to a single field
table4 = etl.select(table1, 'foo', lambda v: v == 'a')
table4


# selectre()
############

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['aa', 4, 9.3],
          ['aaa', 2, 88.2],
          ['b', 1, 23.3],
          ['ccc', 8, 42.0],
          ['bb', 7, 100.9],
          ['c', 2]]
table2 = etl.selectre(table1, 'foo', '[ab]{2}')
table2


# selectusingcontext()
######################

import petl as etl
table1 = [['foo', 'bar'],
          ['A', 1],
          ['B', 4],
          ['C', 5],
          ['D', 9]]
def query(prv, cur, nxt):
    return ((prv is not None and (cur.bar - prv.bar) < 2)
            or (nxt is not None and (nxt.bar - cur.bar) < 2))

table2 = etl.selectusingcontext(table1, query)
table2


# facet()
#########

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['a', 4, 9.3],
          ['a', 2, 88.2],
          ['b', 1, 23.3],
          ['c', 8, 42.0],
          ['d', 7, 100.9],
          ['c', 2]]
foo = etl.facet(table1, 'foo')
sorted(foo.keys())
foo['a']
foo['c']


