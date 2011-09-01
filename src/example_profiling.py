#!/usr/bin/env python

"""
TODO doc me
"""

from petl.profile import *
from petl.extract import *

table = ExtractCsv('../fixture/example1.csv')
profiler = Profiler()
profiler.add(RowLengths)
profiler.add(DataTypes)
profiler.add(DistinctValues, field='foo')
profiler.add(BasicStatistics, field='baz')
report = profiler.profile(table)
print report
