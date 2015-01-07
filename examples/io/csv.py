from __future__ import division, print_function, absolute_import


# fromcsv()
###########

import petl as etl
import csv
# set up a CSV file to demonstrate with
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
with open('example.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(table1)

# now demonstrate the use of fromcsv()
table2 = etl.fromcsv('example.csv')
table2


# tocsv()
#########

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
etl.tocsv(table1, 'example.csv')
# look what it did
print(open('example.csv').read())
