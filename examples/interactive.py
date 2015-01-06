from __future__ import division, print_function, absolute_import


import petl.interactive as etl
l = [['foo', 'bar'], ['a', 1], ['b', 3]]
table1 = etl.wrap(l)
table1.look()
table1.cut('foo').look()
table1.tocsv('example.csv')
etl.fromcsv('example.csv').look()
