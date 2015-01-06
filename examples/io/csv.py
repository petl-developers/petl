from __future__ import division, print_function, absolute_import


# fromcsv()
###########

# set up a CSV file to demonstrate with
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
import csv
with open('example.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(table1)

# now demonstrate the use of petl.fromcsv
from petl import fromcsv, look
table2 = fromcsv('example.csv')
look(table2)


# tocsv()
#########

table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
from petl import tocsv
tocsv(table1, 'example.csv')
# look what it did
print(open('example.csv').read())
