from __future__ import division, print_function, absolute_import


# tohtml()
##########


table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
from petl import tohtml
tohtml(table1, 'example.html', caption='example table')
print(open('example.html').read())
