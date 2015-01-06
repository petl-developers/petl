from __future__ import absolute_import, print_function, division


# capture()
############

from petl import capture, look
table1 = [['id', 'variable', 'value'],
          ['1', 'A1', '12'],
          ['2', 'A2', '15'],
          ['3', 'B1', '18'],
          ['4', 'C12', '19']]
table2 = capture(table1, 'variable', '(\\w)(\\d+)',
                 ['treat', 'time'])
look(table2)
# using the include_original argument
table3 = capture(table1, 'variable', '(\\w)(\\d+)',
                 ['treat', 'time'],
                 include_original=True)
look(table3)


# split()
#########

from petl import split, look
table1 = [['id', 'variable', 'value'],
          ['1', 'parad1', '12'],
          ['2', 'parad2', '15'],
          ['3', 'tempd1', '18'],
          ['4', 'tempd2', '19']]
table2 = split(table1, 'variable', 'd', ['variable', 'day'])
look(table2)


# search()
##########

from petl import search, look
table1 = [['foo', 'bar', 'baz'],
          ['orange', 12, 'oranges are nice fruit'],
          ['mango', 42, 'I like them'],
          ['banana', 74, 'lovely too'],
          ['cucumber', 41, 'better than mango']]
# search any field
table2 = search(table1, '.g.')
look(table2)
# search a specific field
table3 = search(table1, 'foo', '.g.')
look(table3)


# searchcomplement()
####################

from petl import searchcomplement, look
table1 = [['foo', 'bar', 'baz'],
          ['orange', 12, 'oranges are nice fruit'],
          ['mango', 42, 'I like them'],
          ['banana', 74, 'lovely too'],
          ['cucumber', 41, 'better than mango']]
# search any field
table2 = searchcomplement(table1, '.g.')
look(table2)
# search a specific field
table3 = searchcomplement(table1, 'foo', '.g.')
look(table3)

