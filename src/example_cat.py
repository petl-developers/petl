#!/usr/bin/env python

"""
TODO doc me
"""

from petl import Cat, look

table1 = [['foo', 'bar'],
          [1, 'A'],
          [2, 'B']]
table2 = [['bar', 'baz'],
          ['C', True],
          ['D', False]]
table3 = Cat(table1, table2, missing=None)
look(table3)
