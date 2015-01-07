from __future__ import division, print_function, absolute_import, \
    unicode_literals


# values()
##########

import petl as etl
table1 = [['foo', 'bar'],
          ['a', True],
          ['b'],
          ['b', True],
          ['c', False]]
foo = etl.values(table1, 'foo')
foo
list(foo)
bar = etl.values(table1, 'bar')
bar
list(bar)
# values from multiple fields
table2 = [['foo', 'bar', 'baz'],
          [1, 'a', True],
          [2, 'bb', True],
          [3, 'd', False]]
foobaz = etl.values(table2, 'foo', 'baz')
foobaz
list(foobaz)


# header()
##########


import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2]]
etl.header(table)


# fieldnames()
##############

import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2]]
etl.fieldnames(table)
etl.header(table)


# data()
########

import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2]]
d = etl.data(table)
list(d)


# dicts()
#########

import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2]]
d = etl.dicts(table)
d
list(d)


# namedtuples()
###############

import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2]]
d = etl.namedtuples(table)
d
list(d)


# records()
###############

import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2]]
d = etl.records(table)
d
list(d)


# rowgroupby()
##############

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 3, True],
          ['b', 2]]
# group entire rows
for key, group in etl.rowgroupby(table1, 'foo'):
    print(key, list(group))

# group specific values
for key, group in etl.rowgroupby(table1, 'foo', 'bar'):
    print(key, list(group))


# empty()
#########

import petl as etl
table = (
    etl
    .empty()
    .addcolumn('foo', ['A', 'B'])
    .addcolumn('bar', [1, 2])
)
table
