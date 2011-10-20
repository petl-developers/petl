#!/usr/bin/env python

"""
TODO doc me
"""

from petl import cat, look

table1 = [['foo', 'bar'],
          [1, 'A'],
          [2, 'B']]
table2 = [['bar', 'baz'],
          ['C', True],
          ['D', False]]
table3 = cat(table1, table2, missing=None)
look(table3)
