from __future__ import absolute_import, print_function, division


# unpack()
##########

from petl import unpack, look
table1 = [['foo', 'bar'],
          [1, ['a', 'b']],
          [2, ['c', 'd']],
          [3, ['e', 'f']]]
table2 = unpack(table1, 'bar', ['baz', 'quux'])
look(table2)


# unpackdict()
##############

from petl import unpackdict, look
table1 = [['foo', 'bar'],
          [1, {'baz': 'a', 'quux': 'b'}],
          [2, {'baz': 'c', 'quux': 'd'}],
          [3, {'baz': 'e', 'quux': 'f'}]]
table2 = unpackdict(table1, 'bar')
look(table2)
