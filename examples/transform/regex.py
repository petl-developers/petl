from __future__ import absolute_import, print_function, division


# capture()
############

import petl as etl
table1 = [['id', 'variable', 'value'],
          ['1', 'A1', '12'],
          ['2', 'A2', '15'],
          ['3', 'B1', '18'],
          ['4', 'C12', '19']]
table2 = etl.capture(table1, 'variable', '(\\w)(\\d+)',
                     ['treat', 'time'])
table2
# using the include_original argument
table3 = etl.capture(table1, 'variable', '(\\w)(\\d+)',
                     ['treat', 'time'],
                     include_original=True)
table3


# split()
#########

import petl as etl
table1 = [['id', 'variable', 'value'],
          ['1', 'parad1', '12'],
          ['2', 'parad2', '15'],
          ['3', 'tempd1', '18'],
          ['4', 'tempd2', '19']]
table2 = etl.split(table1, 'variable', 'd', ['variable', 'day'])
table2


# search()
##########

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['orange', 12, 'oranges are nice fruit'],
          ['mango', 42, 'I like them'],
          ['banana', 74, 'lovely too'],
          ['cucumber', 41, 'better than mango']]
# search any field
table2 = etl.search(table1, '.g.')
table2
# search a specific field
table3 = etl.search(table1, 'foo', '.g.')
table3


# searchcomplement()
####################

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['orange', 12, 'oranges are nice fruit'],
          ['mango', 42, 'I like them'],
          ['banana', 74, 'lovely too'],
          ['cucumber', 41, 'better than mango']]
# search any field
table2 = etl.searchcomplement(table1, '.g.')
table2
# search a specific field
table3 = etl.searchcomplement(table1, 'foo', '.g.')
table3

