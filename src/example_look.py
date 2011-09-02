#!/usr/bin/env python

"""
TODO doc me
"""

from petl.extract import ExtractCsv
from petl.interact import look

table = ExtractCsv('../fixture/example1.csv')
look(table)