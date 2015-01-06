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


# unpack

table1 = [['foo', 'bar'],
          [1, ['a', 'b']],
          [2, ['c', 'd']],
          [3, ['e', 'f']]]

from petl import unpack, look    
look(table1)
table2 = unpack(table1, 'bar', ['baz', 'quux'])
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



