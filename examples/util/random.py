from __future__ import division, print_function, absolute_import


# randomtable()
###############

import petl as etl
table = etl.randomtable(3, 100, seed=42)
table


# dummytable()
##############

import petl as etl
table1 = etl.dummytable(100, seed=42)
table1
# customise fields
import random
from functools import partial
fields = [('foo', random.random),
          ('bar', partial(random.randint, 0, 500)),
          ('baz', partial(random.choice,
                          ['chocolate', 'strawberry', 'vanilla']))]
table2 = etl.dummytable(100, fields=fields, seed=42)
table2
