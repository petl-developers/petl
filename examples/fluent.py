from __future__ import division, print_function, absolute_import


import petl.fluent as etl
table1 = etl.dummytable()
table1.look()


import petl.fluent as etl
l = [['foo', 'bar'], ['a', 1], ['b', 3]]
table2 = etl.wrap(l)
table2.look()
table2.cut('foo').look()
table2.tocsv('example.csv')
etl.fromcsv('example.csv').look()
