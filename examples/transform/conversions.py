from __future__ import division, print_function, absolute_import


# convert()
###########

table1 = [['foo', 'bar', 'baz'],
          ['A', '2.4', 12],
          ['B', '5.7', 34],
          ['C', '1.2', 56]]
from petl import convert, look
# using a built-in function:
table2 = convert(table1, 'bar', float)
look(table2)
# using a lambda function::
table3 = convert(table1, 'baz', lambda v: v*2)
look(table3)
# a method of the data value can also be invoked by passing
# the method name
table4 = convert(table1, 'foo', 'lower')
look(table4)
# arguments to the method invocation can also be given
table5 = convert(table1, 'foo', 'replace', 'A', 'AA')
look(table5)
# values can also be translated via a dictionary
table7 = convert(table1, 'foo', {'A': 'Z', 'B': 'Y'})
look(table7)
# the same conversion can be applied to multiple fields
table8 = convert(table1, ('foo', 'bar', 'baz'), str)
look(table8)
# multiple conversions can be specified at the same time
table9 = convert(table1, {'foo': 'lower',
                          'bar': float,
                          'baz': lambda v: v*2})
look(table9)
# ...or alternatively via a list
table10 = convert(table1, ['lower', float, lambda v: v*2])
look(table10)
# conversion can be conditional
table11 = convert(table1, 'baz', lambda v: v*2,
                  where=lambda r: r.foo == 'B')
look(table11)
# conversion can access other values from the same row
table12 = convert(table1, 'baz', lambda v, row: v * float(row.bar),
                  pass_row=True)
look(table12)


# convertnumbers()
##################

table1 = [['foo', 'bar', 'baz', 'quux'],
          ['1', '3.0', '9+3j', 'aaa'],
          ['2', '1.3', '7+2j', None]]
from petl import convertnumbers, look
table2 = convertnumbers(table1)
look(table2)


