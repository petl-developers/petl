from __future__ import division, print_function, absolute_import


# fromjson()
############

import petl as etl
data = '''
[{"foo": "a", "bar": 1},
{"foo": "b", "bar": 2},
{"foo": "c", "bar": 2}]
'''
with open('example.json', 'w') as f:
    f.write(data)

table1 = etl.fromjson('example.json')
table1


# fromdicts()
#############

import petl as etl
dicts = [{"foo": "a", "bar": 1},
         {"foo": "b", "bar": 2},
         {"foo": "c", "bar": 2}]
table1 = etl.fromdicts(dicts)
table1


# tojson()
##########

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
etl.tojson(table1, 'example.json', sort_keys=True)
# check what it did
print(open('example.json').read())


# tojsonarrays()
################

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
etl.tojsonarrays(table1, 'example.json')
# check what it did
print(open('example.json').read())

