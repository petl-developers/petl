from __future__ import division, print_function, absolute_import


# tohtml()
##########


import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
etl.tohtml(table1, 'example.html', caption='example table')
print(open('example.html').read())
