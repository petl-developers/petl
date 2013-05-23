"""
DB-related tests, separated from main unit tests because they need local database
setup prior to running.

"""

import sys
sys.path.insert(0, './src')
from petl.interactive import etl
import logging
logging.basicConfig(level=logging.DEBUG)

t = (('foo', 'bar'),
     ('C', 2),
     ('A', 9),
     ('B', 6),
     ('E', 1),
     ('D', 10))
t = etl(t)
t.nrows()
t.nrows()