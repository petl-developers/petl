from __future__ import absolute_import, print_function, division


# melt()
########

from petl import melt, lookall
table1 = [['id', 'gender', 'age'],
          [1, 'F', 12],
          [2, 'M', 17],
          [3, 'M', 16]]
table2 = melt(table1, 'id')
lookall(table2)
# compound keys are supported
table3 = [['id', 'time', 'height', 'weight'],
          [1, 11, 66.4, 12.2],
          [2, 16, 53.2, 17.3],
          [3, 12, 34.5, 9.4]]
table4 = melt(table3, key=['id', 'time'])
lookall(table4)
# a subset of variable fields can be selected
table5 = melt(table3, key=['id', 'time'],
              variables=['height'])
lookall(table5)


# recast()
##########

from petl import recast, look
table1 = [['id', 'variable', 'value'],
          [3, 'age', 16],
          [1, 'gender', 'F'],
          [2, 'gender', 'M'],
          [2, 'age', 17],
          [1, 'age', 12],
          [3, 'gender', 'M']]
table2 = recast(table1)
look(table2)
# specifying variable and value fields
table3 = [['id', 'vars', 'vals'],
          [3, 'age', 16],
          [1, 'gender', 'F'],
          [2, 'gender', 'M'],
          [2, 'age', 17],
          [1, 'age', 12],
          [3, 'gender', 'M']]
table4 = recast(table3, variablefield='vars', valuefield='vals')
look(table4)
# if there are multiple values for each key/variable pair, and no
# reducers function is provided, then all values will be listed
table6 = [['id', 'time', 'variable', 'value'],
          [1, 11, 'weight', 66.4],
          [1, 14, 'weight', 55.2],
          [2, 12, 'weight', 53.2],
          [2, 16, 'weight', 43.3],
          [3, 12, 'weight', 34.5],
          [3, 17, 'weight', 49.4]]
table7 = recast(table6, key='id')
look(table7)
# multiple values can be reduced via an aggregation function
def mean(values):
    return float(sum(values)) / len(values)

table8 = recast(table6, key='id', reducers={'weight': mean})
look(table8)
# missing values are padded with whatever is provided via the
# missing keyword argument (None by default)
table9 = [['id', 'variable', 'value'],
          [1, 'gender', 'F'],
          [2, 'age', 17],
          [1, 'age', 12],
          [3, 'gender', 'M']]
table10 = recast(table9, key='id')
look(table10)


# transpose()
#############

from petl import transpose, look    
table1 = [['id', 'colour'],
          [1, 'blue'],
          [2, 'red'],
          [3, 'purple'],
          [5, 'yellow'],
          [7, 'orange']]
table2 = transpose(table1)
look(table2)


# pivot()
#########

from petl import pivot, look
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
table2 = pivot(table1, 'region', 'gender', 'units', sum)
look(table2)
table3 = pivot(table1, 'region', 'style', 'units', sum)
look(table3)
table4 = pivot(table1, 'gender', 'style', 'units', sum)
look(table4)


# flatten()
###########

from petl import flatten
table1 = [['foo', 'bar', 'baz'],
          ['A', 1, True],
          ['C', 7, False],
          ['B', 2, False],
          ['C', 9, True]]
list(flatten(table1))


# unflatten()
#############

from petl import unflatten, look
a = ['A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9]
table1 = unflatten(a, 3)
look(table1)
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
table3 = unflatten(table2, 'lines', 3)
look(table3)
