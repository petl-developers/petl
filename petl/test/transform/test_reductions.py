from __future__ import absolute_import, print_function, division


import operator

import pytest


from collections import OrderedDict
from petl.test.helpers import ieq
from petl.errors import FieldSelectionError
from petl.util import strjoin
from petl.transform.reductions import rowreduce, aggregate, \
    mergeduplicates, merge, Conflict, fold


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

def test_aggregate_simple_whole_records():
    table = (('foo', 'bar'), ('a', 3), ('b', 7))

    def aggregation(rows):
        return sum(row['bar'] for row in rows)

    actual = aggregate(table, None, aggregation)
    expect = (('value',), (10,))
    ieq(expect, actual)


def test_aggregate_simple_whole_records_single_pass():
    table = iter((('foo', 'bar'), ('a', 3), ('b', 7)))
    actual = aggregate(table, None, list)
    expect = (('value',), ([('a', 3), ('b', 7)],))
    ieq(expect, actual)


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

def _aggregate_multifield(table, key, form, presorted):
    if form == 'dict':
        aggregation = OrderedDict([('total', ('bar', sum)), ('count', len)])
    elif form == 'list':
        aggregation = [('total', 'bar', sum), ('count', len)]
    else:
        aggregation = None
    result = aggregate(table, key, aggregation, presorted=presorted)
    if form == 'suffix':
        result['total'] = 'bar', sum
        result['count'] = len
    return result


@pytest.mark.parametrize('key_type', [list, tuple])
@pytest.mark.parametrize('key_values', [
    ('alpha', 'beta'),
    (1, 2),
    (None, 1),
    ('', 'a'),
    ((1, 2), (3, 4)),
    (False, True),
])
@pytest.mark.parametrize('form', ['dict', 'list', 'suffix'])
@pytest.mark.parametrize('presorted', [False, True])
def test_aggregate_multifield_singleton_key(key_type, key_values, form,
                                          presorted):
    first, second = key_values
    rows = [(first, 3), (first, 7), (second, 5)]
    if not presorted:
        rows.reverse()
    table = [('foo', 'bar')] + rows
    original = list(table)
    key = key_type(['foo'])
    result = _aggregate_multifield(table, key, form, presorted)
    expected = [('foo', 'total', 'count'), (first, 10, 2), (second, 5, 1)]

    assert list(result) == expected
    assert list(result) == expected
    assert key == key_type(['foo'])
    assert table == original


@pytest.mark.parametrize('key_type', [list, tuple])
@pytest.mark.parametrize('form', ['dict', 'list', 'suffix'])
@pytest.mark.parametrize('presorted', [False, True])
def test_aggregate_multifield_singleton_key_empty(key_type, form, presorted):
    result = _aggregate_multifield([('foo', 'bar')], key_type(['foo']),
                                  form, presorted)
    expected = [('foo', 'total', 'count')]
    assert list(result) == expected
    assert list(result) == expected


@pytest.mark.parametrize('presorted', [False, True])
@pytest.mark.parametrize('key, expected', [
    ('foo', [('foo', 'total', 'count'), ('alpha', 10, 2), ('beta', 5, 1)]),
    (('foo', 'baz'), [('foo', 'baz', 'total', 'count'),
                      ('alpha', 1, 10, 2), ('beta', 2, 5, 1)]),
    (operator.itemgetter(0, 2), [('key', 'total', 'count'),
                               (('alpha', 1), 10, 2), (('beta', 2), 5, 1)]),
    (None, [('total', 'count'), (15, 3)]),
])
def test_aggregate_multifield_key_shapes(key, expected, presorted):
    rows = [('alpha', 3, 1), ('alpha', 7, 1), ('beta', 5, 2)]
    if not presorted:
        rows.reverse()
    result = _aggregate_multifield([('foo', 'bar', 'baz')] + rows, key,
                                  'dict', presorted)
    if callable(key) and not presorted:
        # Sorting does not accept callable field selectors.
        with pytest.raises(FieldSelectionError):
            list(result)
        return
    assert list(result) == expected
    assert list(result) == expected


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
        

def test_mergeduplicates_field_index():
    table = [['foo', 'bar', 'baz'],
             ['aa', 1, True],
             ['aa', None, False],
             ['bb', 2, True]]
    expect = [('foo', 'bar', 'baz'),
              ('aa', 1, Conflict([True, False])),
              ('bb', 2, True)]
    for key in 0, -3, [0], ['foo'], (-3,):
        ieq(expect, mergeduplicates(table, key))
        ieq(expect, mergeduplicates(table, key, presorted=True))
        ieq(expect, merge(table[:2], table[:1] + table[2:], key=key))

    # key fields come first in the output, as with field names
    expect = [('baz', 'foo', 'bar'),
              (False, 'aa', None),
              (True, Conflict(['aa', 'bb']), Conflict([1, 2]))]
    ieq(expect, mergeduplicates(table, -1))
    ieq(expect, mergeduplicates(table, 'baz'))


def test_mergeduplicates_compoundkey_field_index():
    table = [['foo', 'bar', 'baz'],
             ['a', 1, True],
             ['a', 1, None],
             ['a', 2, False],
             ['c', 3, True],
             ['c', 3, False]]
    expect = [('foo', 'bar', 'baz'),
              ('a', 1, True),
              ('a', 2, False),
              ('c', 3, Conflict([True, False]))]
    for key in (0, 1), [-3, -2], ('foo', 1):
        ieq(expect, mergeduplicates(table, key))
        ieq(expect, mergeduplicates(table, key, presorted=True))


def test_mergeduplicates_field_index_out_of_range():
    table = [['foo', 'bar'],
             ['a', 1]]
    for key in 2, -3, [0, 2]:
        with pytest.raises(FieldSelectionError):
            mergeduplicates(table, key).nrows()
        with pytest.raises(FieldSelectionError):
            mergeduplicates(table, key, presorted=True).nrows()


def test_mergeduplicates_field_index_ragged_rows():
    # a negative index counts from the end of the header, not of the row
    table = [['foo', 'bar', 'baz'],
             ['s1', 'a'],
             ['f', 0, 'x'],
             ['s2', 'z']]
    expect = [('baz', 'foo', 'bar'),
              (None, Conflict(['s1', 's2']), Conflict(['a', 'z'])),
              ('x', 'f', 0)]
    for key in 'baz', 2, -1:
        ieq(expect, mergeduplicates(table, key))

    table = [['foo', 'bar'],
             ['a', 1, 'zz'],
             ['b', 2, 'mm'],
             ['c', 1, 'aa']]
    expect = [('bar', 'foo'),
              (1, Conflict(['a', 'c'])),
              (2, 'b')]
    for key in 'bar', 1, -1:
        ieq(expect, mergeduplicates(table, key))


def test_merge_field_index_reordered_fields():
    # an index refers to the combined header, whatever the order of the
    # fields in each table
    table1 = [['id', 'value'],
              [1, 'z'],
              [3, 'c']]
    table2 = [['value', 'id'],
              ['a', 2],
              ['b', 1]]
    expect = [('id', 'value'),
              (1, Conflict(['z', 'b'])),
              (2, 'a'),
              (3, 'c')]
    for key in 'id', 0, -2, [0]:
        ieq(expect, merge(table1, table2, key=key))
        ieq(expect[:1] + expect[:0:-1],
            merge(table1, table2, key=key, reverse=True))
    for key in 2, -3:
        with pytest.raises(FieldSelectionError):
            merge(table1, table2, key=key).nrows()


def test_fold():

    t1 = (('id', 'count'), (1, 3), (1, 5), (2, 4), (2, 8))
    t2 = fold(t1, 'id', operator.add, 'count', presorted=True)
    expect = (('key', 'value'), (1, 8), (2, 12))
    ieq(expect, t2)
    ieq(expect, t2)
