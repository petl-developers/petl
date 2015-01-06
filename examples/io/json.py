from __future__ import division, print_function, absolute_import


# fromjson()
############

data = '''
[{"foo": "a", "bar": 1},
{"foo": "b", "bar": 2},
{"foo": "c", "bar": 2}]
'''
with open('example.json', 'w') as f:
    _ = f.write(data)

from petl import fromjson, look
table1 = fromjson('example.json')
look(table1)


# fromdicts()
#############

dicts = [{"foo": "a", "bar": 1},
         {"foo": "b", "bar": 2},
         {"foo": "c", "bar": 2}]
from petl import fromdicts, look
table1 = fromdicts(dicts)
look(table1)


# tojson()
##########

table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
from petl import tojson
tojson(table1, 'example.json', sort_keys=True)
# check what it did
print(open('example.json').read())


# tojsonarrays()
################

table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
from petl import tojsonarrays
tojsonarrays(table1, 'example.json')
# check what it did
print(open('example.json').read())

