from __future__ import division, print_function, absolute_import


# toarray()
###########

import petl as etl
table = [('foo', 'bar', 'baz'),
         ('apples', 1, 2.5),
         ('oranges', 3, 4.4),
         ('pears', 7, .1)]
a = etl.toarray(table)
a
# the dtype can be specified as a string
a = etl.toarray(table, dtype='a4, i2, f4')
a
# the dtype can also be partially specified
a = etl.toarray(table, dtype={'foo': 'a4'})
a


# fromarray()
#############

import petl as etl
import numpy as np
a = np.array([('apples', 1, 2.5),
              ('oranges', 3, 4.4),
              ('pears', 7, 0.1)],
             dtype='U8, i4,f4')
table = etl.fromarray(a)
table


# valuestoarray()
#################

import petl as etl
table = [('foo', 'bar', 'baz'),
         ('apples', 1, 2.5),
         ('oranges', 3, 4.4),
         ('pears', 7, .1)]
table = etl.wrap(table)
table.values('bar').array()
# specify dtype
table.values('bar').array(dtype='i4')
