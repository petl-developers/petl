from __future__ import division, print_function, absolute_import


# progress()
############

import petl as etl
table = etl.dummytable(100000)
table.progress(10000).tocsv('example.csv')


# clock()
#########

import petl as etl
t1 = etl.dummytable(100000)
c1 = etl.clock(t1)
t2 = etl.convert(c1, 'foo', lambda v: v**2)
c2 = etl.clock(t2)
p = etl.progress(c2, 10000)
etl.tocsv(p, 'example.csv')
# time consumed retrieving rows from t1
c1.time
# time consumed retrieving rows from t2
c2.time
# actual time consumed by the convert step
c2.time - c1.time


