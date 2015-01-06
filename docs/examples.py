"""
Examples used in docstrings.

"""

# valuecounts
##############

table = (('foo', 'bar', 'baz'),
         ('a', True, .12),
         ('a', True, .17),
         ('b', False, .34),
         ('b', False, .44),
         ('b',),
         ('b', False, .56))

from petl import look, valuecounts
look(table)
look(valuecounts(table, 'foo'))
look(valuecounts(table, 'foo', 'bar'))


# facetcolumns

from petl import facetcolumns
table = [['foo', 'bar', 'baz'], 
         ['a', 1, True], 
         ['b', 2, True], 
         ['b', 3]]
fc = facetcolumns(table, 'foo')
fc['a']
fc['a']['foo']
fc['a']['bar']
fc['a']['baz']
fc['b']
fc['b']['foo']
fc['b']['bar']
fc['b']['baz']
fc['c']

# sort

table1 = [['foo', 'bar'],
          ['C', 2],
          ['A', 9],
          ['A', 6],
          ['F', 1],
          ['D', 10]]

from petl import sort, look
look(table1)
table2 = sort(table1, 'foo')
look(table2)
# sorting by compound key is supported
table3 = sort(table1, key=['foo', 'bar'])
look(table3)
# if no key is specified, the default is a lexical sort
table4 = sort(table1)
look(table4)


# melt

table1 = [['id', 'gender', 'age'],
          [1, 'F', 12],
          [2, 'M', 17],
          [3, 'M', 16]]
table3 = [['id', 'time', 'height', 'weight'],
          [1, 11, 66.4, 12.2],
          [2, 16, 53.2, 17.3],
          [3, 12, 34.5, 9.4]]

from petl import melt, look
look(table1)
table2 = melt(table1, 'id')
look(table2)
# compound keys are supported
look(table3)
table4 = melt(table3, key=['id', 'time'])
look(table4)
# a subset of variable fields can be selected
table5 = melt(table3, key=['id', 'time'], variables=['height'])    
look(table5)


# recast

table1 = [['id', 'variable', 'value'],
          [3, 'age', 16],
          [1, 'gender', 'F'],
          [2, 'gender', 'M'],
          [2, 'age', 17],
          [1, 'age', 12],
          [3, 'gender', 'M']]
table3 = [['id', 'vars', 'vals'],
          [3, 'age', 16],
          [1, 'gender', 'F'],
          [2, 'gender', 'M'],
          [2, 'age', 17],
          [1, 'age', 12],
          [3, 'gender', 'M']]
table6 = [['id', 'time', 'variable', 'value'],
          [1, 11, 'weight', 66.4],
          [1, 14, 'weight', 55.2],
          [2, 12, 'weight', 53.2],
          [2, 16, 'weight', 43.3],
          [3, 12, 'weight', 34.5],
          [3, 17, 'weight', 49.4]]
table9 = [['id', 'variable', 'value'],
          [1, 'gender', 'F'],
          [2, 'age', 17],
          [1, 'age', 12],
          [3, 'gender', 'M']]

from petl import recast, look
look(table1)
table2 = recast(table1)
look(table2)
# specifying variable and value fields
look(table3)
table4 = recast(table3, variablefield='vars', valuefield='vals')
look(table4)
# if there are multiple values for each key/variable pair, and no reducers
# function is provided, then all values will be listed
look(table6)
table7 = recast(table6, key='id')
look(table7)
# multiple values can be reduced via an aggregation function
def mean(values):
    return float(sum(values)) / len(values)

table8 = recast(table6, key='id', reducers={'weight': mean})
look(table8)    
# missing values are padded with whatever is provided via the missing 
# keyword argument (None by default)
look(table9)
table10 = recast(table9, key='id')
look(table10)



# complement

a = [['foo', 'bar', 'baz'],
     ['A', 1, True],
     ['C', 7, False],
     ['B', 2, False],
     ['C', 9, True]]
b = [['x', 'y', 'z'],
     ['B', 2, False],
     ['A', 9, False],
     ['B', 3, True],
     ['C', 9, True]]

from petl import complement, look
look(a)
look(b)
aminusb = complement(a, b)
look(aminusb)
bminusa = complement(b, a)
look(bminusa)


# recordcomplement

a = (('foo', 'bar', 'baz'),
     ('A', 1, True),
     ('C', 7, False),
     ('B', 2, False),
     ('C', 9, True))
b = (('bar', 'foo', 'baz'),
     (2, 'B', False),
     (9, 'A', False),
     (3, 'B', True),
     (9, 'C', True))

from petl import recordcomplement, look
look(a)
look(b)
aminusb = recordcomplement(a, b)
look(aminusb)
bminusa = recordcomplement(b, a)
look(bminusa)

# diff

a = [['foo', 'bar', 'baz'],
     ['A', 1, True],
     ['C', 7, False],
     ['B', 2, False],
     ['C', 9, True]]
b = [['x', 'y', 'z'],
     ['B', 2, False],
     ['A', 9, False],
     ['B', 3, True],
     ['C', 9, True]]

from petl import diff, look
look(a)
look(b)
added, subtracted = diff(a, b)
# rows in b not in a
look(added)
# rows in a not in b
look(subtracted)


# recorddiff

a = (('foo', 'bar', 'baz'),
     ('A', 1, True),
     ('C', 7, False),
     ('B', 2, False),
     ('C', 9, True))
b = (('bar', 'foo', 'baz'),
     (2, 'B', False),
     (9, 'A', False),
     (3, 'B', True),
     (9, 'C', True))

from petl import recorddiff, look    
look(a)
look(b)
added, subtracted = recorddiff(a, b)
look(added)
look(subtracted)


# select

table1 = [['foo', 'bar', 'baz'],
          ['a', 4, 9.3],
          ['a', 2, 88.2],
          ['b', 1, 23.3],
          ['c', 8, 42.0],
          ['d', 7, 100.9],
          ['c', 2]]

from petl import select, look     
look(table1)
# the second positional argument can be a function accepting a record (i.e., a 
# dictionary representation of a row).
table2 = select(table1, lambda rec: rec['foo'] == 'a' and rec['baz'] > 88.1)
look(table2)
# the second positional argument can also be an expression string, which 
# will be converted to a function using expr()
table3 = select(table1, "{foo} == 'a' and {baz} > 88.1")
look(table3)
# the condition can also be applied to a single field
table4 = select(table1, 'foo', lambda v: v == 'a')
look(table4)


# facet

table1 = [['foo', 'bar', 'baz'],
          ['a', 4, 9.3],
          ['a', 2, 88.2],
          ['b', 1, 23.3],
          ['c', 8, 42.0],
          ['d', 7, 100.9],
          ['c', 2]]

from petl import facet, look
look(table1)
foo = facet(table1, 'foo')
foo.keys()
look(foo['a'])
look(foo['c'])


# selectre

table1 = (('foo', 'bar', 'baz'),
          ('aa', 4, 9.3),
          ('aaa', 2, 88.2),
          ('b', 1, 23.3),
          ('ccc', 8, 42.0),
          ('bb', 7, 100.9),
          ('c', 2))

from petl import selectre, look    
look(table1)
table2 = selectre(table1, 'foo', '[ab]{2}')
look(table2)


# unpack

table1 = [['foo', 'bar'],
          [1, ['a', 'b']],
          [2, ['c', 'd']],
          [3, ['e', 'f']]]

from petl import unpack, look    
look(table1)
table2 = unpack(table1, 'bar', ['baz', 'quux'])
look(table2)


# transpose

table1 = (('id', 'colour'),
          (1, 'blue'),
          (2, 'red'),
          (3, 'purple'),
          (5, 'yellow'),
          (7, 'orange'))

from petl import transpose, look    
look(table1)
table2 = transpose(table1)
look(table2)


# intersection

table1 = (('foo', 'bar', 'baz'),
          ('A', 1, True),
          ('C', 7, False),
          ('B', 2, False),
          ('C', 9, True))
table2 = (('x', 'y', 'z'),
          ('B', 2, False),
          ('A', 9, False),
          ('B', 3, True),
          ('C', 9, True))

from petl import intersection, look
look(table1)
look(table2)
table3 = intersection(table1, table2)
look(table3)


# pivot

table1 = (('region', 'gender', 'style', 'units'),
          ('east', 'boy', 'tee', 12),
          ('east', 'boy', 'golf', 14),
          ('east', 'boy', 'fancy', 7),
          ('east', 'girl', 'tee', 3),
          ('east', 'girl', 'golf', 8),
          ('east', 'girl', 'fancy', 18),
          ('west', 'boy', 'tee', 12),
          ('west', 'boy', 'golf', 15),
          ('west', 'boy', 'fancy', 8),
          ('west', 'girl', 'tee', 6),
          ('west', 'girl', 'golf', 16),
          ('west', 'girl', 'fancy', 1))

from petl import pivot, look
look(table1)
table2 = pivot(table1, 'region', 'gender', 'units', sum)
look(table2)
table3 = pivot(table1, 'region', 'style', 'units', sum)
look(table3)
table4 = pivot(table1, 'gender', 'style', 'units', sum)
look(table4)


# flatten

table1 = [['foo', 'bar', 'baz'],
          ['A', 1, True],
          ['C', 7, False],
          ['B', 2, False],
          ['C', 9, True]]

from petl import flatten, look
look(table1)
list(flatten(table1))


# unflatten

table1 = [['lines',],
          ['A',], 
          [1,], 
          [True,], 
          ['C',], 
          [7,], 
          [False,],
          ['B',], 
          [2,], 
          [False,],
          ['C'], 
          [9,]]

from petl import unflatten, look
input = ['A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9]
table = unflatten(input, 3)
look(table)
# a table and field name can also be provided as arguments
look(table1)
table2 = unflatten(table1, 'lines', 3)
look(table2)


# tocsv

table = [['foo', 'bar'],
         ['a', 1],
         ['b', 2],
         ['c', 2]]

from petl import tocsv, look
look(table)
tocsv(table, 'test.csv', delimiter='\t')
# look what it did
from petl import fromcsv
look(fromcsv('test.csv', delimiter='\t'))


# appendcsv

table = [['foo', 'bar'],
         ['d', 7],
         ['e', 42],
         ['f', 12]]

# look at an existing CSV file
from petl import look, fromcsv
testcsv = fromcsv('test.csv', delimiter='\t')
look(testcsv)
# append some data
look(table)
from petl import appendcsv 
appendcsv(table, 'test.csv', delimiter='\t')
# look what it did
look(testcsv)


# topickle

table = [['foo', 'bar'],
         ['a', 1],
         ['b', 2],
         ['c', 2]]

from petl import topickle, look
look(table)
topickle(table, 'test.dat')
# look what it did
from petl import frompickle
look(frompickle('test.dat'))


# appendpickle

table = [['foo', 'bar'],
         ['d', 7],
         ['e', 42],
         ['f', 12]]

from petl import look, frompickle
# inspect an existing pickle file
testdat = frompickle('test.dat')
look(testdat)
# append some data
from petl import appendpickle
look(table)
appendpickle(table, 'test.dat')
# look what it did
look(testdat)


# tosqlite3

table = [['foo', 'bar'],
         ['a', 1],
         ['b', 2],
         ['c', 2]]

from petl import tosqlite3, look
look(table)
# by default, if the table does not already exist, it will be created
tosqlite3(table, 'test.db', 'foobar')
# look what it did
from petl import fromsqlite3
look(fromsqlite3('test.db', 'select * from foobar'))


# appendsqlite3

moredata = [['foo', 'bar'],
            ['d', 7],
            ['e', 9],
            ['f', 1]]

from petl import appendsqlite3, look
look(moredata)
appendsqlite3(moredata, 'test.db', 'foobar') 
# look what it did
from petl import look, fromsqlite3
look(fromsqlite3('test.db', 'select * from foobar'))


# tojson

table = [['foo', 'bar'],
         ['a', 1],
         ['b', 2],
         ['c', 2]]

from petl import tojson, look
look(table)
tojson(table, 'example.json')
# check what it did
with open('example.json') as f:
    print f.read()


# tojsonarrays

table = [['foo', 'bar'],
         ['a', 1],
         ['b', 2],
         ['c', 2]]

from petl import tojsonarrays, look
look(table)
tojsonarrays(table, 'example.json')
# check what it did
with open('example.json') as f:
    print f.read()


# mergesort

table1 = (('foo', 'bar'),
          ('A', 9),
          ('C', 2),
          ('D', 10),
          ('A', 6),
          ('F', 1))
table2 = (('foo', 'bar'),
          ('B', 3),
          ('D', 10),
          ('A', 10),
          ('F', 4))

from petl import mergesort, look
look(table1)
look(table2)
table3 = mergesort(table1, table2, key='foo')
look(table3)


# mergesort - heterogeneous tables

table4 = (('foo', 'bar'),
          ('A', 9),
          ('C', 2),
          ('D', 10),
          ('A', 6),
          ('F', 1))

table5 = (('foo', 'baz'),
          ('B', 3),
          ('D', 10),
          ('A', 10),
          ('F', 4))

from petl import mergesort, look
table6 = mergesort(table4, table5, key='foo')
look(table6)


# mergesort - heterogeneous tables, reverse sorting

table1 = (('foo', 'bar'),
          ('A', 9),
          ('C', 2),
          ('D', 10),
          ('A', 6),
          ('F', 1))

table2 = (('foo', 'baz'),
          ('B', 3),
          ('D', 10),
          ('A', 10),
          ('F', 4))

from petl import mergesort, sort, cat, look
expect = sort(cat(table1, table2), key='foo', reverse=True) 
look(expect)
actual = mergesort(table1, table2, key='foo', reverse=True)
look(actual)
actual = mergesort(sort(table1, key='foo'), reverse=True, sort(table2, key='foo', reverse=True), key='foo', reverse=True, presorted=True)
look(actual)


# progress

from petl import dummytable, progress, tocsv
d = dummytable(100500)
p = progress(d, 10000)
tocsv(p, 'output.csv')


# clock

from petl import dummytable, clock, convert, progress, tocsv
t1 = dummytable(100000)
c1 = clock(t1)
t2 = convert(c1, 'foo', lambda v: v**2)
c2 = clock(t2)
p = progress(c2, 10000)
tocsv(p, 'dummy.csv')
# time consumed retrieving rows from t1
c1.time
# time consumed retrieving rows from t2
c2.time
# actual time consumed by the convert step
c2.time - c1.time 


# unpackdict

table1 = (('foo', 'bar'),
          (1, {'baz': 'a', 'quux': 'b'}),
          (2, {'baz': 'c', 'quux': 'd'}),
          (3, {'baz': 'e', 'quux': 'f'}))

from petl import unpackdict, look
look(table1)
table2 = unpackdict(table1, 'bar')
look(table2)



# isordered
table = (('foo', 'bar', 'baz'), 
         ('a', 1, True), 
         ('b', 3, True), 
         ('b', 2))

from petl import isordered, look
look(table)
isordered(table, key='foo')
isordered(table, key='foo', strict=True)
isordered(table, key='foo', reverse=True)


# rowgroupby
table = (('foo', 'bar', 'baz'), 
         ('a', 1, True), 
         ('b', 3, True), 
         ('b', 2))

from petl import rowgroupby, look
look(table)
# group entire rows
for key, group in rowgroupby(table, 'foo'):
    print key, list(group)

# group specific values
for key, group in rowgroupby(table, 'foo', 'bar'):
    print key, list(group)


# nthword
from petl import nthword
s = 'foo bar'
f = nthword(0)
f(s)
g = nthword(1)
g(s)


# selectwithcontext
###################

table1 = (('foo', 'bar'),
          ('A', 1),
          ('B', 4),
          ('C', 5),
          ('D', 9))

from petl import look, selectwithcontext
look(table1)
def query(prv, cur, nxt):
    return ((prv is not None and (cur.bar - prv.bar) < 2)
            or (nxt is not None and (nxt.bar - cur.bar) < 2))

table2 = selectwithcontext(table1, query)
look(table2)


