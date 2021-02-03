from __future__ import absolute_import, print_function, division


import operator


from collections import OrderedDict
from petl.test.helpers import ieq
from petl.util import strjoin
from petl.transform.reductions import rowreduce, aggregate, \
    mergeduplicates, Conflict, fold


def test_rowreduce():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def sumbar(key, rows):
        return [key, sum(row[1] for row in rows)]
        
    table2 = rowreduce(table1, key='foo', reducer=sumbar, 
                       header=['foo', 'barsum'])
    expect2 = (('foo', 'barsum'),
               ('a', 10),
               ('b', 12),
               ('c', 4))
    ieq(expect2, table2)
    

def test_rowreduce_fieldnameaccess():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def sumbar(key, records):
        return [key, sum([rec['bar'] for rec in records])]
        
    table2 = rowreduce(table1, key='foo', reducer=sumbar, 
                       header=['foo', 'barsum'])
    expect2 = (('foo', 'barsum'),
               ('a', 10),
               ('b', 12),
               ('c', 4))
    ieq(expect2, table2)
    

def test_rowreduce_more():
    
    table1 = (('foo', 'bar'),
              ('aa', 3),
              ('aa', 7),
              ('bb', 2),
              ('bb', 1),
              ('bb', 9),
              ('cc', 4))
    
    def sumbar(key, records):
        return [key, sum(rec['bar'] for rec in records)]
        
    table2 = rowreduce(table1, key='foo', reducer=sumbar, 
                       header=['foo', 'barsum'])
    expect2 = (('foo', 'barsum'),
               ('aa', 10),
               ('bb', 12),
               ('cc', 4))
    ieq(expect2, table2)
    

def test_rowreduce_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    reducer = lambda key, rows: (key, [r[0] for r in rows])
    actual = rowreduce(table, key='foo', reducer=reducer, 
                       header=('foo', 'bar'))
    ieq(expect, actual)


def test_aggregate_simple():
    
    table1 = (('foo', 'bar', 'baz'),
              ('a', 3, True),
              ('a', 7, False),
              ('b', 2, True),
              ('b', 2, False),
              ('b', 9, False),
              ('c', 4, True))

    # simplest signature - aggregate whole rows
    table2 = aggregate(table1, 'foo', len)
    expect2 = (('foo', 'value'),
               ('a', 2),
               ('b', 3),
               ('c', 1))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # next simplest signature - aggregate single field
    table3 = aggregate(table1, 'foo', sum, 'bar')
    expect3 = (('foo', 'value'),
               ('a', 10),
               ('b', 13),
               ('c', 4))
    ieq(expect3, table3)
    ieq(expect3, table3)
    
    # alternative signature for simple aggregation
    table4 = aggregate(table1, key=('foo', 'bar'), aggregation=list, 
                       value=('bar', 'baz'))
    expect4 = (('foo', 'bar', 'value'),
               ('a', 3, [(3, True)]),
               ('a', 7, [(7, False)]),
               ('b', 2, [(2, True), (2, False)]),
               ('b', 9, [(9, False)]),
               ('c', 4, [(4, True)]))
    ieq(expect4, table4)
    ieq(expect4, table4)
    
    table5 = aggregate(table1, 'foo', len, field='nrows')
    expect5 = (('foo', 'nrows'),
               ('a', 2),
               ('b', 3),
               ('c', 1))
    ieq(expect5, table5)
    ieq(expect5, table5)


def test_aggregate_simple_key_is_None():

    table1 = (('foo', 'bar', 'baz'),
              ('a', 3, True),
              ('a', 7, False),
              ('b', 2, True),
              ('b', 2, False),
              ('b', 9, False),
              ('c', 4, True))

    # simplest signature - aggregate whole rows
    table2 = aggregate(table1, None, len)
    expect2 = (('value',), (6,))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # next simplest signature - aggregate single field
    table3 = aggregate(table1, None, sum, 'bar')
    expect3 = (('value',), (27,))
    ieq(expect3, table3)
    ieq(expect3, table3)

    # alternative signature for simple aggregation
    table4 = aggregate(table1, key=None, aggregation=list,
                       value=('bar', 'baz'))
    expect4 = (('value',),
               ( [(3, True), (7, False),
                 (2, True), (2, False), (9, False),
                 (4, True)],),)

    ieq(expect4, table4)
    ieq(expect4, table4)

    table5 = aggregate(table1, None, len, field='nrows')
    expect5 = (('nrows',), (6,))
    ieq(expect5, table5)
    ieq(expect5, table5)

def test_aggregate_multifield():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    # dict arg
    
    aggregators = OrderedDict()
    aggregators['count'] = len
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum
    aggregators['listbar'] = 'bar', list
    aggregators['bars'] = 'bar', strjoin(', ')

    table2 = aggregate(table1, 'foo', aggregators)
    expect2 = (('foo', 'count', 'minbar', 'maxbar', 'sumbar', 'listbar', 
                'bars'),
               ('a', 2, 3, 7, 10, [3, 7], '3, 7'),
               ('b', 3, 1, 9, 12, [2, 1, 9], '2, 1, 9'),
               ('c', 1, 4, 4, 4, [4], '4'))
    ieq(expect2, table2)
    ieq(expect2, table2)  # check can iterate twice
    
    # use suffix notation
    
    table3 = aggregate(table1, 'foo')
    table3['count'] = len
    table3['minbar'] = 'bar', min
    table3['maxbar'] = 'bar', max
    table3['sumbar'] = 'bar', sum
    table3['listbar'] = 'bar'  # default aggregation is list
    table3['bars'] = 'bar', strjoin(', ')
    ieq(expect2, table3)
    
    # list arg

    aggregators = [('count', len),
                   ('minbar', 'bar', min),
                   ('maxbar', 'bar', max),
                   ('sumbar', 'bar', sum),
                   ('listbar', 'bar', list),
                   ('bars', 'bar', strjoin(', '))]

    table4 = aggregate(table1, 'foo', aggregators)
    ieq(expect2, table4)
    ieq(expect2, table4)  # check can iterate twice

def test_aggregate_multifield_key_is_None():

    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))

    # dict arg
    aggregators = OrderedDict()
    aggregators['count'] = len
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum
    aggregators['listbar'] = 'bar', list
    aggregators['bars'] = 'bar', strjoin(', ')

    table2 = aggregate(table1, None, aggregators)
    expect2 = (('count', 'minbar', 'maxbar', 'sumbar', 'listbar',
                'bars'),
               (6, 1, 9, 26, [3, 7, 2, 1, 9, 4], '3, 7, 2, 1, 9, 4'))
    ieq(expect2, table2)
    ieq(expect2, table2)  # check can iterate twice

    # use suffix notation
    table3 = aggregate(table1, None)
    table3['count'] = len
    table3['minbar'] = 'bar', min
    table3['maxbar'] = 'bar', max
    table3['sumbar'] = 'bar', sum
    table3['listbar'] = 'bar'  # default aggregation is list
    table3['bars'] = 'bar', strjoin(', ')
    ieq(expect2, table3)

    # list arg
    aggregators = [('count', len),
                   ('minbar', 'bar', min),
                   ('maxbar', 'bar', max),
                   ('sumbar', 'bar', sum),
                   ('listbar', 'bar', list),
                   ('bars', 'bar', strjoin(', '))]

    table4 = aggregate(table1, None, aggregators)
    ieq(expect2, table4)
    ieq(expect2, table4)  # check can iterate twice

def test_aggregate_more():

    table1 = (('foo', 'bar'),
              ('aa', 3),
              ('aa', 7),
              ('bb', 2),
              ('bb', 1),
              ('bb', 9),
              ('cc', 4),
              ('dd', 3))

    aggregators = OrderedDict()
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum
    aggregators['listbar'] = 'bar'  # default aggregation is list
    aggregators['bars'] = 'bar', strjoin(', ')

    table2 = aggregate(table1, 'foo', aggregators)
    expect2 = (('foo', 'minbar', 'maxbar', 'sumbar', 'listbar', 'bars'),
               ('aa', 3, 7, 10, [3, 7], '3, 7'),
               ('bb', 1, 9, 12, [2, 1, 9], '2, 1, 9'),
               ('cc', 4, 4, 4, [4], '4'),
               ('dd', 3, 3, 3, [3], '3'))
    ieq(expect2, table2)
    ieq(expect2, table2)  # check can iterate twice

    table3 = aggregate(table1, 'foo')
    table3['minbar'] = 'bar', min
    table3['maxbar'] = 'bar', max
    table3['sumbar'] = 'bar', sum
    table3['listbar'] = 'bar'  # default aggregation is list
    table3['bars'] = 'bar', strjoin(', ')
    ieq(expect2, table3)

def test_aggregate_more_key_is_None():

    table1 = (('foo', 'bar'),
              ('aa', 3),
              ('aa', 7),
              ('bb', 2),
              ('bb', 1),
              ('bb', 9),
              ('cc', 4),
              ('dd', 3))

    aggregators = OrderedDict()
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum
    aggregators['listbar'] = 'bar'  # default aggregation is list
    aggregators['bars'] = 'bar', strjoin(', ')

    table2 = aggregate(table1, None, aggregators)
    expect2 = (('minbar', 'maxbar', 'sumbar', 'listbar', 'bars'),
               (1, 9, 29,
                   [3, 7, 2, 1, 9, 4, 3],
                   '3, 7, 2, 1, 9, 4, 3'))
    ieq(expect2, table2)
    ieq(expect2, table2)  # check can iterate twice

    table3 = aggregate(table1, None)
    table3['minbar'] = 'bar', min
    table3['maxbar'] = 'bar', max
    table3['sumbar'] = 'bar', sum
    table3['listbar'] = 'bar'  # default aggregation is list
    table3['bars'] = 'bar', strjoin(', ')
    ieq(expect2, table3)

def test_aggregate_multiple_source_fields():

    table = (('foo', 'bar', 'baz'),
             ('a', 3, True),
             ('a', 7, False),
             ('b', 2, True),
             ('b', 2, False),
             ('b', 9, False),
             ('c', 4, True))

    expect = (('foo', 'bar', 'value'),
              ('a', 3, [(3, True)]),
              ('a', 7, [(7, False)]),
              ('b', 2, [(2, True), (2, False)]),
              ('b', 9, [(9, False)]),
              ('c', 4, [(4, True)]))

    actual = aggregate(table, ('foo', 'bar'), list, ('bar', 'baz'))
    ieq(expect, actual)
    ieq(expect, actual)

    actual = aggregate(table, key=('foo', 'bar'), aggregation=list, 
                       value=('bar', 'baz'))
    ieq(expect, actual)
    ieq(expect, actual)
    
    actual = aggregate(table, key=('foo', 'bar'))
    actual['value'] = ('bar', 'baz'), list
    ieq(expect, actual)
    ieq(expect, actual)    

def test_aggregate_multiple_source_fields_key_is_None():

    table = (('foo', 'bar', 'baz'),
             ('a', 3, True),
             ('a', 7, False),
             ('b', 2, True),
             ('b', 2, False),
             ('b', 9, False),
             ('c', 4, True))

    expect  = (('value',),
               ( [(3, True), (7, False),
                 (2, True), (2, False), (9, False),
                 (4, True)],),)

    actual = aggregate(table, None, list, ('bar', 'baz'))
    ieq(expect, actual)
    ieq(expect, actual)

    actual = aggregate(table, key=None, aggregation=list,
                       value=('bar', 'baz'))
    ieq(expect, actual)
    ieq(expect, actual)

    actual = aggregate(table, key=None)
    actual['value'] = ('bar', 'baz'), list
    ieq(expect, actual)
    ieq(expect, actual)


def test_aggregate_empty():

    table = (('foo', 'bar'),)

    aggregators = OrderedDict()
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum

    actual = aggregate(table, 'foo', aggregators)
    expect = (('foo', 'minbar', 'maxbar', 'sumbar'),)
    ieq(expect, actual)

def test_aggregate_empty_key_is_None():

    table = (('foo', 'bar'),)

    aggregators = OrderedDict()
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum

    actual = aggregate(table, None, aggregators)
    expect = (('minbar', 'maxbar', 'sumbar'),)
    ieq(expect, actual)

def test_mergeduplicates():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', None),
             ('D', 'xyz', 9.4),
             ('B', None, u'7.8', True),
             ('E', None, 42.),
             ('D', 'xyz', 12.3),
             ('A', 2, None))

    # value overrides missing
    result = mergeduplicates(table, 'foo', missing=None)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', Conflict([1, 2]), 2),
                   ('B', '2', u'7.8'),
                   ('D', 'xyz', Conflict([9.4, 12.3])),
                   ('E', None, 42.))
    ieq(expectation, result)
    
    
def test_mergeduplicates_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = mergeduplicates(table, key='foo')
    ieq(expect, actual)


def test_mergeduplicates_shortrows():
    table = [['foo', 'bar', 'baz'], 
             ['a', 1, True], 
             ['b', 2, True], 
             ['b', 3]]
    actual = mergeduplicates(table, 'foo')
    expect = [('foo', 'bar', 'baz'), 
              ('a', 1, True), 
              ('b', Conflict([2, 3]), True)]
    ieq(expect, actual)
        
    
def test_mergeduplicates_compoundkey():
    table = [['foo', 'bar', 'baz'], 
             ['a', 1, True], 
             ['a', 1, True], 
             ['a', 2, False],
             ['a', 2, None],
             ['c', 3, True],
             ['c', 3, False],
             ]
    actual = mergeduplicates(table, key=('foo', 'bar'))
    expect = [('foo', 'bar', 'baz'), 
              ('a', 1, True), 
              ('a', 2, False), 
              ('c', 3, Conflict([True, False]))]
    ieq(expect, actual)
        

def test_fold():

    t1 = (('id', 'count'), (1, 3), (1, 5), (2, 4), (2, 8))
    t2 = fold(t1, 'id', operator.add, 'count', presorted=True)
    expect = (('key', 'value'), (1, 8), (2, 12))
    ieq(expect, t2)
    ieq(expect, t2)
