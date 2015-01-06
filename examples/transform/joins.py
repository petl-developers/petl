from __future__ import absolute_import, print_function, division


# join()
########

from petl import join, look
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square'],
          [4, 'ellipse']]
table3 = join(table1, table2, key='id')
look(table3)
# if no key is given, a natural join is tried
table4 = join(table1, table2)
look(table4)
# note behaviour if the key is not unique in either or both tables
table5 = [['id', 'colour'],
          [1, 'blue'],
          [1, 'red'],
          [2, 'purple']]
table6 = [['id', 'shape'],
          [1, 'circle'],
          [1, 'square'],
          [2, 'ellipse']]
table7 = join(table5, table6, key='id')
look(table7)
# compound keys are supported
table8 = [['id', 'time', 'height'],
          [1, 1, 12.3],
          [1, 2, 34.5],
          [2, 1, 56.7]]
table9 = [['id', 'time', 'weight'],
          [1, 2, 4.5],
          [2, 1, 6.7],
          [2, 2, 8.9]]
table10 = join(table8, table9, key=['id', 'time'])
look(table10)


# leftjoin()
############

from petl import leftjoin, look
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square'],
          [4, 'ellipse']]
table3 = leftjoin(table1, table2, key='id')
look(table3)


# rightjoin()
#############

from petl import rightjoin, look
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square'],
          [4, 'ellipse']]
table3 = rightjoin(table1, table2, key='id')
look(table3)


# outerjoin()
#############

from petl import outerjoin, look
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square'],
          [4, 'ellipse']]
table3 = outerjoin(table1, table2, key='id')
look(table3)


# crossjoin()
#############

from petl import crossjoin, look
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square']]
table3 = crossjoin(table1, table2)
look(table3)


# antijoin()
############

from petl import antijoin, look
table1 = [['id', 'colour'],
          [0, 'black'],
          [1, 'blue'],
          [2, 'red'],
          [4, 'yellow'],
          [5, 'white']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square']]
table3 = antijoin(table1, table2, key='id')
look(table3)


# lookupjoin()
##############

from petl import lookupjoin, look
table1 = [['id', 'color', 'cost'], 
          [1, 'blue', 12], 
          [2, 'red', 8], 
          [3, 'purple', 4]]
table2 = [['id', 'shape', 'size'], 
          [1, 'circle', 'big'], 
          [1, 'circle', 'small'], 
          [2, 'square', 'tiny'], 
          [2, 'square', 'big'], 
          [3, 'ellipse', 'small'], 
          [3, 'ellipse', 'tiny']]
table3 = lookupjoin(table1, table2, key='id')
look(table3)


# unjoin()
##########

from petl import look, unjoin
# join key is present in the table
table1 = (('foo', 'bar', 'baz'),
          ('A', 1, 'apple'),
          ('B', 1, 'apple'),
          ('C', 2, 'orange'))
table2, table3 = unjoin(table1, 'baz', key='bar')
look(table2)
look(table3)
# an integer join key can also be reconstructed
table4 = (('foo', 'bar'),
          ('A', 'apple'),
          ('B', 'apple'),
          ('C', 'orange'))
table5, table6 = unjoin(table4, 'bar')
look(table5)
look(table6)





