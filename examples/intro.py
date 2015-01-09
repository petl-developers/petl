from __future__ import division, print_function, absolute_import

example_data = """foo,bar,baz
a,1,3.4
b,2,7.4
c,6,2.2
d,9,8.1
"""
with open('example.csv', 'w') as f:
    f.write(example_data)

import petl as etl
table1 = etl.fromcsv('example.csv')
table2 = etl.convert(table1, 'foo', 'upper')
table3 = etl.convert(table2, 'bar', int)
table4 = etl.convert(table3, 'baz', float)
table5 = etl.addfield(table4, 'quux', lambda row: row.bar * row.baz)
table5

table = (
    etl
    .fromcsv('example.csv')
    .convert('foo', 'upper')
    .convert('bar', int)
    .convert('baz', float)
    .addfield('quux', lambda row: row.bar * row.baz)
)
table

l = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
table = etl.wrap(l)
table.look()

l = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
table = etl.wrap(l)
table

etl.config.look_index_header = True

table
