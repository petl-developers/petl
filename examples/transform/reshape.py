from __future__ import absolute_import, print_function, division


# melt()
########

import petl as etl
table1 = [['id', 'gender', 'age'],
          [1, 'F', 12],
          [2, 'M', 17],
          [3, 'M', 16]]
table2 = etl.melt(table1, 'id')
table2.lookall()
# compound keys are supported
table3 = [['id', 'time', 'height', 'weight'],
          [1, 11, 66.4, 12.2],
          [2, 16, 53.2, 17.3],
          [3, 12, 34.5, 9.4]]
table4 = etl.melt(table3, key=['id', 'time'])
table4.lookall()
# a subset of variable fields can be selected
table5 = etl.melt(table3, key=['id', 'time'],
                  variables=['height'])
table5.lookall()


# recast()
##########

import petl as etl
table1 = [['id', 'variable', 'value'],
          [3, 'age', 16],
          [1, 'gender', 'F'],
          [2, 'gender', 'M'],
          [2, 'age', 17],
          [1, 'age', 12],
          [3, 'gender', 'M']]
table2 = etl.recast(table1)
table2
# specifying variable and value fields
table3 = [['id', 'vars', 'vals'],
          [3, 'age', 16],
          [1, 'gender', 'F'],
          [2, 'gender', 'M'],
          [2, 'age', 17],
          [1, 'age', 12],
          [3, 'gender', 'M']]
table4 = etl.recast(table3, variablefield='vars', valuefield='vals')
table4
# if there are multiple values for each key/variable pair, and no
# reducers function is provided, then all values will be listed
table6 = [['id', 'time', 'variable', 'value'],
          [1, 11, 'weight', 66.4],
          [1, 14, 'weight', 55.2],
          [2, 12, 'weight', 53.2],
          [2, 16, 'weight', 43.3],
          [3, 12, 'weight', 34.5],
          [3, 17, 'weight', 49.4]]
table7 = etl.recast(table6, key='id')
table7
# multiple values can be reduced via an aggregation function
def mean(values):
    return float(sum(values)) / len(values)

table8 = etl.recast(table6, key='id', reducers={'weight': mean})
table8
# missing values are padded with whatever is provided via the
# missing keyword argument (None by default)
table9 = [['id', 'variable', 'value'],
          [1, 'gender', 'F'],
          [2, 'age', 17],
          [1, 'age', 12],
          [3, 'gender', 'M']]
table10 = etl.recast(table9, key='id')
table10


# transpose()
#############

import petl as etl
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple'],
          [5, 'yellow'],
          [7, 'orange']]
table2 = etl.transpose(table1)
table2


# pivot()
#########

import petl as etl
table1 = [['region', 'gender', 'style', 'units'],
          ['east', 'boy', 'tee', 12],
          ['east', 'boy', 'golf', 14],
          ['east', 'boy', 'fancy', 7],
          ['east', 'girl', 'tee', 3],
          ['east', 'girl', 'golf', 8],
          ['east', 'girl', 'fancy', 18],
          ['west', 'boy', 'tee', 12],
          ['west', 'boy', 'golf', 15],
          ['west', 'boy', 'fancy', 8],
          ['west', 'girl', 'tee', 6],
          ['west', 'girl', 'golf', 16],
          ['west', 'girl', 'fancy', 1]]
table2 = etl.pivot(table1, 'region', 'gender', 'units', sum)
table2
table3 = etl.pivot(table1, 'region', 'style', 'units', sum)
table3
table4 = etl.pivot(table1, 'gender', 'style', 'units', sum)
table4


# flatten()
###########

import petl as etl
table1 = [['foo', 'bar', 'baz'],
          ['A', 1, True],
          ['C', 7, False],
          ['B', 2, False],
          ['C', 9, True]]
list(etl.flatten(table1))


# unflatten()
#############

import petl as etl
a = ['A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9]
table1 = etl.unflatten(a, 3)
table1
# a table and field name can also be provided as arguments
table2 = [['lines'],
          ['A'],
          [1],
          [True],
          ['C'],
          [7],
          [False],
          ['B'],
          [2],
          [False],
          ['C'],
          [9]]
table3 = etl.unflatten(table2, 'lines', 3)
table3
