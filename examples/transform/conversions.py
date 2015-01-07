from __future__ import division, print_function, absolute_import


# convert()
###########

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['A', '2.4', 12],
          ['B', '5.7', 34],
          ['C', '1.2', 56]]
# using a built-in function:
table2 = etl.convert(table1, 'bar', float)
table2
# using a lambda function::
table3 = etl.convert(table1, 'baz', lambda v: v*2)
table3
# a method of the data value can also be invoked by passing
# the method name
table4 = etl.convert(table1, 'foo', 'lower')
table4
# arguments to the method invocation can also be given
table5 = etl.convert(table1, 'foo', 'replace', 'A', 'AA')
table5
# values can also be translated via a dictionary
table7 = etl.convert(table1, 'foo', {'A': 'Z', 'B': 'Y'})
table7
# the same conversion can be applied to multiple fields
table8 = etl.convert(table1, ('foo', 'bar', 'baz'), str)
table8
# multiple conversions can be specified at the same time
table9 = etl.convert(table1, {'foo': 'lower',
                              'bar': float,
                              'baz': lambda v: v * 2})
table9
# ...or alternatively via a list
table10 = etl.convert(table1, ['lower', float, lambda v: v*2])
table10
# conversion can be conditional
table11 = etl.convert(table1, 'baz', lambda v: v * 2,
                      where=lambda r: r.foo == 'B')
table11
# conversion can access other values from the same row
table12 = etl.convert(table1, 'baz',
                      lambda v, row: v * float(row.bar),
                      pass_row=True)
table12


# convertnumbers()
##################

import petl as etl
table1 = [['foo', 'bar', 'baz', 'quux'],
          ['1', '3.0', '9+3j', 'aaa'],
          ['2', '1.3', '7+2j', None]]
table2 = etl.convertnumbers(table1)
table2


