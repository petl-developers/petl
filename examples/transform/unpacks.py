from __future__ import absolute_import, print_function, division


# unpack()
##########

import petl as etl
table1 = [['foo', 'bar'],
          [1, ['a', 'b']],
          [2, ['c', 'd']],
          [3, ['e', 'f']]]
table2 = etl.unpack(table1, 'bar', ['baz', 'quux'])
table2


# unpackdict()
##############

import petl as etl
table1 = [['foo', 'bar'],
          [1, {'baz': 'a', 'quux': 'b'}],
          [2, {'baz': 'c', 'quux': 'd'}],
          [3, {'baz': 'e', 'quux': 'f'}]]
table2 = etl.unpackdict(table1, 'bar')
table2
