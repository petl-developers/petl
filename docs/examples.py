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



