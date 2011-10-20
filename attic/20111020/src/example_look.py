#!/usr/bin/env python

"""
TODO doc me
"""

from petl import ExtractCsv, look
table = ExtractCsv('../fixture/example1.csv')
look(table)