"""
DB-related tests, separated from main unit tests because they need local database
setup prior to running.

"""

import sys
sys.path.insert(0, './src')
from petl import cache, nrows
import logging
logging.basicConfig(level=logging.DEBUG)

t = (('foo', 'bar'),
     ('C', 2),
     ('A', 9),
     ('B', 6),
     ('E', 1),
     ('D', 10))
u = cache(t)
nrows(u)
nrows(u)