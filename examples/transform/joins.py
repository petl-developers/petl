from __future__ import absolute_import, print_function, division


# join()
########

import petl as etl
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square'],
          [4, 'ellipse']]
table3 = etl.join(table1, table2, key='id')
table3
# if no key is given, a natural join is tried
table4 = etl.join(table1, table2)
table4
# note behaviour if the key is not unique in either or both tables
table5 = [['id', 'colour'],
          [1, 'blue'],
          [1, 'red'],
          [2, 'purple']]
table6 = [['id', 'shape'],
          [1, 'circle'],
          [1, 'square'],
          [2, 'ellipse']]
table7 = etl.join(table5, table6, key='id')
table7
# compound keys are supported
table8 = [['id', 'time', 'height'],
          [1, 1, 12.3],
          [1, 2, 34.5],
          [2, 1, 56.7]]
table9 = [['id', 'time', 'weight'],
          [1, 2, 4.5],
          [2, 1, 6.7],
          [2, 2, 8.9]]
table10 = etl.join(table8, table9, key=['id', 'time'])
table10


# leftjoin()
############

import petl as etl
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square'],
          [4, 'ellipse']]
table3 = etl.leftjoin(table1, table2, key='id')
table3


# rightjoin()
#############

import petl as etl
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square'],
          [4, 'ellipse']]
table3 = etl.rightjoin(table1, table2, key='id')
table3


# outerjoin()
#############

import petl as etl
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square'],
          [4, 'ellipse']]
table3 = etl.outerjoin(table1, table2, key='id')
table3


# crossjoin()
#############

import petl as etl
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square']]
table3 = etl.crossjoin(table1, table2)
table3


# antijoin()
############

import petl as etl
table1 = [['id', 'colour'],
          [0, 'black'],
          [1, 'blue'],
          [2, 'red'],
          [4, 'yellow'],
          [5, 'white']]
table2 = [['id', 'shape'],
          [1, 'circle'],
          [3, 'square']]
table3 = etl.antijoin(table1, table2, key='id')
table3


# lookupjoin()
##############

import petl as etl
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
table3 = etl.lookupjoin(table1, table2, key='id')
table3


# unjoin()
##########

import petl as etl
# join key is present in the table
table1 = (('foo', 'bar', 'baz'),
          ('A', 1, 'apple'),
          ('B', 1, 'apple'),
          ('C', 2, 'orange'))
table2, table3 = etl.unjoin(table1, 'baz', key='bar')
table2
table3
# an integer join key can also be reconstructed
table4 = (('foo', 'bar'),
          ('A', 'apple'),
          ('B', 'apple'),
          ('C', 'orange'))
table5, table6 = etl.unjoin(table4, 'bar')
table5
table6





