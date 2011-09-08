"""
TODO doc me

"""

import logging
import sys
from itertools import izip
from datetime import date, time


from petl import cut, cat, convert, sort, filterduplicates,\
    filterconflicts, mergeduplicates, melt, stringcapture, stringsplit, recast,\
    meanf, rslice, head, tail, parsedate, parsetime, count, fields, complement,\
    diff, data, translate, rename, addfield


logger = logging.getLogger('petl')
d, i, w, e = logger.debug, logger.info, logger.warn, logger.error # abbreviations
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stderr)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def iter_compare(it1, it2):
    for a, b in izip(it1, it2):
        assert tuple(a) == tuple(b), (a, b)
        

def test_cut():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    cut1 = cut(table, 'foo')
    expectation = [['foo'],
                   ['A'],
                   ['B'],
                   [u'B'],
                   ['D'],
                   ['E']]
    iter_compare(expectation, cut1)
    
    cut2 = cut(table, 'foo', 'baz')
    expectation = [['foo', 'baz'],
                   ['A', 2],
                   ['B', '3.4'],
                   [u'B', u'7.8'],
                   ['D', 9.0],
                   ['E', Ellipsis]]
    iter_compare(expectation, cut2)
    
    cut3 = cut(table, 1, 3)
    expectation = [['foo', 'baz'],
                   ['A', 2],
                   ['B', '3.4'],
                   [u'B', u'7.8'],
                   ['D', 9.0],
                   ['E', Ellipsis]]
    iter_compare(expectation, cut3)
    
    cut4 = cut(table, 'bar', 1)
    expectation = [['bar', 'foo'],
                   [1, 'A'],
                   ['2', 'B'],
                   [u'3', u'B'],
                   ['xyz', 'D'],
                   [None, 'E']]
    iter_compare(expectation, cut4)
    
        
def test_cat():
    
    table1 = [['foo', 'bar'],
              [1, 'A'],
              [2, 'B']]

    table2 = [['bar', 'baz'],
              ['C', True],
              ['D', False]]
    
    cat1 = cat(table1, table2, missing=None)
    expectation = [['foo', 'bar', 'baz'],
                   [1, 'A', None],
                   [2, 'B', None],
                   [None, 'C', True],
                   [None, 'D', False]]
    iter_compare(expectation, cat1)

    # how does cat cope with uneven rows?
    
    table3 = [['foo', 'bar', 'baz'],
              ['A', 1, 2],
              ['B', '2', '3.4'],
              [u'B', u'3', u'7.8', True],
              ['D', 'xyz', 9.0],
              ['E', None]]

    cat3 = cat(table3, missing=None)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2],
                   ['B', '2', '3.4'],
                   [u'B', u'3', u'7.8'],
                   ['D', 'xyz', 9.0],
                   ['E', None, None]]
    iter_compare(expectation, cat3)
    
    
def test_convert():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]
    
    # test the style where the conversion functions are passed in as a dictionary
    conversion = {'foo': str, 'bar': int, 'baz': float}
    conv = convert(table, conversion=conversion, error_value='error')
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2.0],
                   ['B', 2, 3.4],
                   ['B', 3, 7.8, True], # N.B., long rows are preserved
                   ['D', 'error', 9.0],
                   ['E', 'error']] # N.B., short rows are preserved
    iter_compare(expectation, conv) 
    
    # test the style where the conversion functions are added one at a time
    conv = convert(table, error_value='err')
    conv.set('foo', str)
    conv.set('bar', int)
    conv['baz'] = float # can also use __setitem__
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2.0],
                   ['B', 2, 3.4],
                   ['B', 3, 7.8, True],
                   ['D', 'err', 9.0],
                   ['E', 'err']]
    iter_compare(expectation, conv) 
    
    
def test_translate():
    
    table = [['foo', 'bar'],
             ['M', 12],
             ['F', 34],
             ['-', 56]]
    
    trans = {'M': 'male', 'F': 'female'}
    result = translate(table, 'foo', trans, default=None)
    
    expectation = [['foo', 'bar'],
                   ['male', 12],
                   ['female', 34],
                   [None, 56]]
    iter_compare(expectation, result)
    
    
def test_rename():

    table = [['foo', 'bar'],
             ['M', 12],
             ['F', 34],
             ['-', 56]]
    
    result = rename(table, 'foo', 'foofoo')
    assert list(fields(result)) == ['foofoo', 'bar']
    
    result = rename(table, {'foo': 'foofoo', 'bar': 'barbar'})
    assert list(fields(result)) == ['foofoo', 'barbar']


def test_addfield():

    table = [['foo', 'bar'],
             ['M', 12],
             ['F', 34],
             ['-', 56]]
    
    result = addfield(table, 'baz', 42)
    d(list(result))
    expectation = [['foo', 'bar', 'baz'],
                   ['M', 12, 42],
                   ['F', 34, 42],
                   ['-', 56, 42]]
    iter_compare(expectation, result)

    result = addfield(table, 'baz', lambda d: d['bar'] * 2)
    d(list(result))
    expectation = [['foo', 'bar', 'baz'],
                   ['M', 12, 24],
                   ['F', 34, 68],
                   ['-', 56, 112]]
    iter_compare(expectation, result)


def test_sort_1():
    
    data = [['foo', 'bar'],
            ['C', '2'],
            ['A', '9'],
            ['A', '6'],
            ['F', '1'],
            ['D', '10']]
    
    result = sort(data, 'foo')
    expectation = [['foo', 'bar'],
                   ['A', '9'],
                   ['A', '6'],
                   ['C', '2'],
                   ['D', '10'],
                   ['F', '1']]
    iter_compare(expectation, result)
    
    
def test_sort_2():
    
    data = [['foo', 'bar'],
            ['C', '2'],
            ['A', '9'],
            ['A', '6'],
            ['F', '1'],
            ['D', '10']]
    
    result = sort(data, key=('foo', 'bar'))
    expectation = [['foo', 'bar'],
                   ['A', '6'],
                   ['A', '9'],
                   ['C', '2'],
                   ['D', '10'],
                   ['F', '1']]
    iter_compare(expectation, result)
    
    result = sort(data) # default is lexical sort
    expectation = [['foo', 'bar'],
                   ['A', '6'],
                   ['A', '9'],
                   ['C', '2'],
                   ['D', '10'],
                   ['F', '1']]
    iter_compare(expectation, result)
    
    
def test_sort_3():
    
    data = [['foo', 'bar'],
            ['C', '2'],
            ['A', '9'],
            ['A', '6'],
            ['F', '1'],
            ['D', '10']]
    
    result = sort(data, 'bar')
    expectation = [['foo', 'bar'],
                   ['F', '1'],
                   ['D', '10'],
                   ['C', '2'],
                   ['A', '6'],
                   ['A', '9']]
    iter_compare(expectation, result)
    
    
def test_sort_4():
    
    data = [['foo', 'bar'],
            ['C', 2],
            ['A', 9],
            ['A', 6],
            ['F', 1],
            ['D', 10]]
    
    result = sort(data, 'bar')
    expectation = [['foo', 'bar'],
                   ['F', 1],
                   ['C', 2],
                   ['A', 6],
                   ['A', 9],
                   ['D', 10]]
    iter_compare(expectation, result)
    
    
def test_sort_5():
    
    data = [['foo', 'bar'],
            [2.3, 2],
            [1.2, 9],
            [2.3, 6],
            [3.2, 1],
            [1.2, 10]]
    
    expectation = [['foo', 'bar'],
                   [1.2, 9],
                   [1.2, 10],
                   [2.3, 2],
                   [2.3, 6],
                   [3.2, 1]]

    # can use either field names or indices (from 1) to specify sort key
    result = sort(data, key=('foo', 'bar'))
    iter_compare(expectation, result)
    result = sort(data, key=(1, 2))
    iter_compare(expectation, result)
    result = sort(data, key=('foo', 2))
    iter_compare(expectation, result)
    result = sort(data, key=(1, 'bar'))
    iter_compare(expectation, result)
    
    
def test_sort_6():
    
    data = [['foo', 'bar'],
            [2.3, 2],
            [1.2, 9],
            [2.3, 6],
            [3.2, 1],
            [1.2, 10]]
    
    expectation = [['foo', 'bar'],
                   [3.2, 1],
                   [2.3, 6],
                   [2.3, 2],
                   [1.2, 10],
                   [1.2, 9]]

    # can use either field names or indices (from 1) to specify sort key
    result = sort(data, key=('foo', 'bar'), reverse=True)
    iter_compare(expectation, result)
    
    
def test_filter_duplicates():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             ['D', 'xyz', 9.0],
             ['B', u'3', u'7.8', True],
             ['E', None],
             ['D', 4, 12.3]]

    result = filterduplicates(table, 'foo')
    expectation = [['foo', 'bar', 'baz'],
                   ['B', '2', '3.4'],
                   ['B', u'3', u'7.8', True],
                   ['D', 'xyz', 9.0],
                   ['D', 4, 12.3]]
    iter_compare(expectation, result)
    
    
def test_filter_conflicts():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', None],
             ['D', 'xyz', 9.4],
             ['B', None, u'7.8', True],
             ['E', None],
             ['D', 'xyz', 12.3],
             ['A', 2, None]]

    result = filterconflicts(table, 'foo', missing=None)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2],
                   ['A', 2, None],
                   ['D', 'xyz', 9.4],
                   ['D', 'xyz', 12.3]]
    iter_compare(expectation, result)
    
    
def test_merge_duplicates():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', None],
             ['D', 'xyz', 9.4],
             ['B', None, u'7.8', True],
             ['E', None],
             ['D', 'xyz', 12.3],
             ['A', 2, None]]

    # value overrides missing; last value wins
    result = mergeduplicates(table, 'foo', missing=None)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 2, 2],
                   ['B', '2', u'7.8', True],
                   ['D', 'xyz', 12.3],
                   ['E', None]]
    d(list(result))
    iter_compare(expectation, result)
    
    
def test_melt_1():
    
    data = [
            ['id', 'gender', 'age'],
            ['1', 'F', '12'],
            ['2', 'M', '17'],
            ['3', 'M', '16']
            ]
    
    expectation = [
                   ['id', 'variable', 'value'],
                   ['1', 'gender', 'F'],
                   ['1', 'age', '12'],
                   ['2', 'gender', 'M'],
                   ['2', 'age', '17'],
                   ['3', 'gender', 'M'],
                   ['3', 'age', '16']
                   ]
    
    result = melt(data, key='id')
    iter_compare(expectation, result)

    result = melt(data, key='id', variable_field='variable', value_field='value')
    iter_compare(expectation, result)


def test_melt_2():
    
    data = [
            ['id', 'time', 'height', 'weight'],
            ['1', '11', '66.4', '12.2'],
            ['2', '16', '53.2', '17.3'],
            ['3', '12', '34.5', '9.4']
            ]
    
    expectation = [
                   ['id', 'time', 'variable', 'value'],
                   ['1', '11', 'height', '66.4'],
                   ['1', '11', 'weight', '12.2'],
                   ['2', '16', 'height', '53.2'],
                   ['2', '16', 'weight', '17.3'],
                   ['3', '12', 'height', '34.5'],
                   ['3', '12', 'weight', '9.4']
                   ]
    
    result = melt(data, key=('id', 'time'))
    iter_compare(expectation, result)


def test_string_capture():
    
    data = [
            ['id', 'variable', 'value'],
            ['1', 'A1', '12'],
            ['2', 'A2', '15'],
            ['3', 'B1', '18'],
            ['4', 'C12', '19']
            ]
    
    expectation = [
                   ['id', 'value', 'treat', 'time'],
                   ['1', '12', 'A', '1'],  
                   ['2', '15', 'A', '2'],
                   ['3', '18', 'B', '1'],
                   ['4', '19', 'C', '1']
                   ]
    
    result = stringcapture(data, 'variable', '(\\w)(\\d)', ('treat', 'time'))
    iter_compare(expectation, result)

    result = stringcapture(data, 'variable', '(\\w)(\\d)', ('treat', 'time'),
                           include_original=False)
    iter_compare(expectation, result)

    expectation = [
                   ['id', 'variable', 'value', 'treat', 'time'],
                   ['1', 'A1', '12', 'A', '1'],  
                   ['2', 'A2', '15', 'A', '2'],
                   ['3', 'B1', '18', 'B', '1'],
                   ['4', 'C12', '19', 'C', '1']
                   ]
    
    result = stringcapture(data, 'variable', '(\\w)(\\d)', ('treat', 'time'),
                           include_original=True)
    iter_compare(expectation, result)
    
    
def test_string_split():
    
    data = [
            ['id', 'variable', 'value'],
            ['1', 'parad1', '12'],
            ['2', 'parad2', '15'],
            ['3', 'tempd1', '18'],
            ['4', 'tempd2', '19']
            ]
    
    expectation = [
                   ['id', 'value', 'variable', 'day'],
                   ['1', '12', 'para', '1'],  
                   ['2', '15', 'para', '2'],
                   ['3', '18', 'temp', '1'],
                   ['4', '19', 'temp', '2']
                   ]
    
    result = stringsplit(data, 'variable', 'd', ('variable', 'day'))
    iter_compare(expectation, result)

    result = stringsplit(data, 'variable', 'd', ('variable', 'day'),
                         include_original=False)
    iter_compare(expectation, result)

    expectation = [
                   ['id', 'variable', 'value', 'variable', 'day'],
                   ['1', 'parad1', '12', 'para', '1'],  
                   ['2', 'parad2', '15', 'para', '2'],
                   ['3', 'tempd1', '18', 'temp', '1'],
                   ['4', 'tempd2', '19', 'temp', '2']
                   ]
    
    result = stringsplit(data, 'variable', 'd', ('variable', 'day'),
                         include_original=True)
    iter_compare(expectation, result)
    
    
def test_melt_and_capture():
    
    data = [
            ['id', 'parad0', 'parad1', 'parad2'],
            ['1', '12', '34', '56'],
            ['2', '23', '45', '67']
            ]
    
    expectation = [
                   ['id', 'parasitaemia', 'day'],
                   ['1', '12', '0'],
                   ['1', '34', '1'],
                   ['1', '56', '2'],
                   ['2', '23', '0'],
                   ['2', '45', '1'],
                   ['2', '67', '2']
                   ]
    
    step1 = melt(data, key='id', value_field='parasitaemia')
    step2 = stringcapture(step1, 'variable', 'parad(\\d+)', ('day',))
    iter_compare(expectation, step2)


def test_melt_and_split():
    
    data = [
            ['id', 'parad0', 'parad1', 'parad2', 'tempd0', 'tempd1', 'tempd2'],
            ['1', '12', '34', '56', '37.2', '37.4', '37.9'],
            ['2', '23', '45', '67', '37.1', '37.8', '36.9']
            ]
    
    expectation = [
                   ['id', 'value', 'variable', 'day'],
                   ['1', '12', 'para', '0'],
                   ['1', '34', 'para', '1'],
                   ['1', '56', 'para', '2'],
                   ['1', '37.2', 'temp', '0'],
                   ['1', '37.4', 'temp', '1'],
                   ['1', '37.9', 'temp', '2'],
                   ['2', '23', 'para', '0'],
                   ['2', '45', 'para', '1'],
                   ['2', '67', 'para', '2'],
                   ['2', '37.1', 'temp', '0'],
                   ['2', '37.8', 'temp', '1'],
                   ['2', '36.9', 'temp', '2']
                   ]
    
    step1 = melt(data, key='id')
    step2 = stringsplit(step1, 'variable', 'd', ('variable', 'day'))
    iter_compare(expectation, step2)


def test_recast_1():
    
    data = [
            ['id', 'variable', 'value'],
            ['3', 'age', '16'],
            ['1', 'gender', 'F'],
            ['2', 'gender', 'M'],
            ['2', 'age', '17'],
            ['1', 'age', '12'],
            ['3', 'gender', 'M']
            ]
    
    expectation = [
                   ['id', 'age', 'gender'],
                   ['1', '12', 'F'],
                   ['2', '17', 'M'],
                   ['3', '16', 'M']
                   ]
    
    result = recast(data) # by default lift 'variable' field, hold everything else
    d(list(result))
    iter_compare(expectation, result)

    result = recast(data, variable_field='variable')
    iter_compare(expectation, result)

    result = recast(data, key='id', variable_field='variable')
    iter_compare(expectation, result)

    result = recast(data, key='id', variable_field='variable', value_field='value')
    iter_compare(expectation, result)


def test_recast_2():
    
    data = [
            ['id', 'variable', 'value'],
            ['3', 'age', '16'],
            ['1', 'gender', 'F'],
            ['2', 'gender', 'M'],
            ['2', 'age', '17'],
            ['1', 'age', '12'],
            ['3', 'gender', 'M']
            ]
    
    expectation = [
                   ['id', 'gender'],
                   ['1', 'F'],
                   ['2', 'M'],
                   ['3', 'M']
                   ]
    
    # can manually pick which variables you want to recast as fields
    result = recast(data, key='id', variable_field={'variable':['gender']})
    iter_compare(expectation, result)


def test_recast_3():
    
    data = [
            ['id', 'time', 'variable', 'value'],
            [1, 11, 'weight', 66.4],
            [1, 14, 'weight', 55.2],
            [2, 12, 'weight', 53.2],
            [2, 16, 'weight', 43.3],
            [3, 12, 'weight', 34.5],
            [3, 17, 'weight', 49.4]
            ]
    
    expectation = [
                   ['id', 'time', 'weight'],
                   [1, 11, 66.4],
                   [1, 14, 55.2],
                   [2, 12, 53.2],
                   [2, 16, 43.3],
                   [3, 12, 34.5],
                   [3, 17, 49.4]
                   ]
    
    result = recast(data)
    iter_compare(expectation, result)

    # in the absence of an aggregation function, pick the first item by default
    expectation = [
                   ['id', 'weight'],
                   [1, 66.4],
                   [2, 53.2],
                   [3, 34.5]
                   ]
    
    result = recast(data, key='id')
    iter_compare(expectation, result)

    # max aggregation
    expectation = [
                   ['id', 'weight'],
                   [1, 66.4],
                   [2, 53.2],
                   [3, 49.4]
                   ]
    
    result = recast(data, key='id', reduce={'weight': max})
    iter_compare(expectation, result)

    # min aggregation
    expectation = [
                   ['id', 'weight'],
                   [1, 55.2],
                   [2, 43.3],
                   [3, 34.5]
                   ]
    
    result = recast(data, key='id', reduce={'weight': min})
    iter_compare(expectation, result)

    # mean aggregation
    expectation = [
                   ['id', 'weight'],
                   [1, 60.80],
                   [2, 48.25],
                   [3, 41.95]
                   ]

    result = recast(data, key='id', reduce={'weight': meanf(precision=2)})
    iter_compare(expectation, result)

    
def test_rslice():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    result = rslice(table, 2)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2],
                   ['B', '2', '3.4']]
    iter_compare(expectation, result)

    result = rslice(table, 1, 2)
    expectation = [['foo', 'bar', 'baz'],
                   ['B', '2', '3.4']]
    iter_compare(expectation, result)

    result = rslice(table, 1, 5, 2)
    expectation = [['foo', 'bar', 'baz'],
                   ['B', '2', '3.4'],
                   ['D', 'xyz', 9.0]]
    iter_compare(expectation, result)


def test_head():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    result = head(table, 2)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2],
                   ['B', '2', '3.4']]
    iter_compare(expectation, result)


def test_tail():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    result = tail(table, 2)
    expectation = [['foo', 'bar', 'baz'],
                   ['D', 'xyz', 9.0],
                   ['E', None]]
    iter_compare(expectation, result)



def test_parsedate():

    dates = ['31/12/99',
             ' 31/12/1999 ', # throw some ws in as well
             u'31 Dec 99',
             '31 Dec 1999',
             '31. Dec. 1999',
             '31 December 1999', 
             '31. December 1999', 
             'Fri 31 Dec 99',
             'Fri 31/Dec 99',
             'Fri 31 December 1999',
             'Friday 31 December 1999',
             '12-31',
             '99-12-31',
             '1999-12-31', # iso 8601
             '12/99',
             '31/Dec',
             '12/31/99',
             '12/31/1999',
             'Dec 31, 99',
             'Dec 31, 1999',
             'December 31, 1999',
             'Fri, Dec 31, 99',
             'Fri, December 31, 1999',
             'Friday, 31. December 1999'] 

    for d in dates:
        p = parsedate(d)
        assert isinstance(p, date)
    
    try:
        parsedate('not a date')
    except:
        pass
    else:
        assert False


def test_parsetime():        

    times = ['13:37',
             ' 13:37:46 ', # throw some ws in as well
             u'01:37 PM',
             '01:37:46 PM',
             '37:46.00',
             '13:37:46.00', 
             '01:37:46.00 PM',
             '01:37PM',
             '01:37:46PM',
             '01:37:46.00PM'] 

    for t in times:
        p = parsetime(t)
        assert isinstance(p, time)
    
    try:
        parsetime('not a time')
    except:
        pass
    else:
        assert False

 
def test_count():
        
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    result = count(table)
    assert result == 5
    
    
def test_fields():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    result = fields(table)
    assert result == ['foo', 'bar', 'baz']


def test_data():
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    result = data(table)
    expectation = [['A', 1, 2],
                   ['B', '2', '3.4'],
                   [u'B', u'3', u'7.8', True],
                   ['D', 'xyz', 9.0],
                   ['E', None]]
    iter_compare(expectation, result)


def test_complement_1():

    table1 = [['foo', 'bar'],
              ['A', 1],
              ['B', 2],
              ['C', 7]]
    
    table2 = [['foo', 'bar'],
              ['A', 9],
              ['B', 2],
              ['B', 3]]
    
    expectation = [['foo', 'bar'],
                   ['A', 1],
                   ['C', 7]]
    
    result = complement(table1, table2)
    iter_compare(expectation, result)
    
    
def test_complement_2():

    tablea = [['foo', 'bar', 'baz'],
              ['A', 1, True],
              ['C', 7, False],
              ['B', 2, False],
              ['C', 9, True]]
    
    tableb = [['x', 'y', 'z'],
              ['B', 2, False],
              ['A', 9, False],
              ['B', 3, True],
              ['C', 9, True]]
    
    aminusb = [['foo', 'bar', 'baz'],
               ['A', 1, True],
               ['C', 7, False]]
    
    result = complement(tablea, tableb)
    iter_compare(aminusb, result)
    
    bminusa = [['x', 'y', 'z'],
               ['A', 9, False],
               ['B', 3, True]]
    
    result = complement(tableb, tablea)
    iter_compare(bminusa, result)
    

def test_diff():

    tablea = [['foo', 'bar', 'baz'],
              ['A', 1, True],
              ['C', 7, False],
              ['B', 2, False],
              ['C', 9, True]]
    
    tableb = [['x', 'y', 'z'],
              ['B', 2, False],
              ['A', 9, False],
              ['B', 3, True],
              ['C', 9, True]]
    
    aminusb = [['foo', 'bar', 'baz'],
               ['A', 1, True],
               ['C', 7, False]]
    
    bminusa = [['x', 'y', 'z'],
               ['A', 9, False],
               ['B', 3, True]]

    added, subtracted = diff(tablea, tableb)    
    iter_compare(added, bminusa)
    iter_compare(subtracted, aminusb)
    


    
    