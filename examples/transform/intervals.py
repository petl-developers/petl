from __future__ import division, print_function, absolute_import


# intervallookup()
##################

import petl as etl
table = [['start', 'stop', 'value'],
         [1, 4, 'foo'],
         [3, 7, 'bar'],
         [4, 9, 'baz']]
lkp = etl.intervallookup(table, 'start', 'stop')
lkp.search(0, 1)
lkp.search(1, 2)
lkp.search(2, 4)
lkp.search(2, 5)
lkp.search(9, 14)
lkp.search(19, 140)
lkp.search(0)
lkp.search(1)
lkp.search(2)
lkp.search(4)
lkp.search(5)

import petl as etl
table = [['start', 'stop', 'value'],
         [1, 4, 'foo'],
         [3, 7, 'bar'],
         [4, 9, 'baz']]
lkp = etl.intervallookup(table, 'start', 'stop', include_stop=True,
                         value='value')
lkp.search(0, 1)
lkp.search(1, 2)
lkp.search(2, 4)
lkp.search(2, 5)
lkp.search(9, 14)
lkp.search(19, 140)
lkp.search(0)
lkp.search(1)
lkp.search(2)
lkp.search(4)
lkp.search(5)


# intervallookupone()
#####################

import petl as etl
table = [['start', 'stop', 'value'],
         [1, 4, 'foo'],
         [3, 7, 'bar'],
         [4, 9, 'baz']]
lkp = etl.intervallookupone(table, 'start', 'stop', strict=False)
lkp.search(0, 1)
lkp.search(1, 2)
lkp.search(2, 4)
lkp.search(2, 5)
lkp.search(9, 14)
lkp.search(19, 140)
lkp.search(0)
lkp.search(1)
lkp.search(2)
lkp.search(4)
lkp.search(5)


# facetintervallookup()
#######################

import petl as etl
table = (('type', 'start', 'stop', 'value'),
         ('apple', 1, 4, 'foo'),
         ('apple', 3, 7, 'bar'),
         ('orange', 4, 9, 'baz'))
lkp = etl.facetintervallookup(table, key='type', start='start', stop='stop')
lkp['apple'].search(1, 2)
lkp['apple'].search(2, 4)
lkp['apple'].search(2, 5)
lkp['orange'].search(2, 5)
lkp['orange'].search(9, 14)
lkp['orange'].search(19, 140)
lkp['apple'].search(1)
lkp['apple'].search(2)
lkp['apple'].search(4)
lkp['apple'].search(5)
lkp['orange'].search(5)


# intervaljoin()
################

import petl as etl
left = [['begin', 'end', 'quux'],
        [1, 2, 'a'],
        [2, 4, 'b'],
        [2, 5, 'c'],
        [9, 14, 'd'],
        [1, 1, 'e'],
        [10, 10, 'f']]
right = [['start', 'stop', 'value'],
         [1, 4, 'foo'],
         [3, 7, 'bar'],
         [4, 9, 'baz']]
table1 = etl.intervaljoin(left, right, 
                          lstart='begin', lstop='end', 
                          rstart='start', rstop='stop')
table1.lookall()
# include stop coordinate in intervals
table2 = etl.intervaljoin(left, right, 
                          lstart='begin', lstop='end', 
                          rstart='start', rstop='stop',
                          include_stop=True)
table2.lookall()

# with facet key
import petl as etl
left = (('fruit', 'begin', 'end'),
        ('apple', 1, 2),
        ('apple', 2, 4),
        ('apple', 2, 5),
        ('orange', 2, 5),
        ('orange', 9, 14),
        ('orange', 19, 140),
        ('apple', 1, 1))
right = (('type', 'start', 'stop', 'value'),
         ('apple', 1, 4, 'foo'),
         ('apple', 3, 7, 'bar'),
         ('orange', 4, 9, 'baz'))
table3 = etl.intervaljoin(left, right,
                          lstart='begin', lstop='end', lkey='fruit',
                          rstart='start', rstop='stop', rkey='type')
table3.lookall()

# intervalleftjoin()
####################

import petl as etl
left = [['begin', 'end', 'quux'],
        [1, 2, 'a'],
        [2, 4, 'b'],
        [2, 5, 'c'],
        [9, 14, 'd'],
        [1, 1, 'e'],
        [10, 10, 'f']]
right = [['start', 'stop', 'value'],
         [1, 4, 'foo'],
         [3, 7, 'bar'],
         [4, 9, 'baz']]
table1 = etl.intervalleftjoin(left, right,
                              lstart='begin', lstop='end',
                              rstart='start', rstop='stop')
table1.lookall()
