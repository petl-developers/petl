"""
TODO doc me

"""

import logging
import sys
from itertools import izip


from petl.transform import Cut, Cat, Convert, Sort, FilterDuplicates,\
    FilterConflicts, MergeDuplicates, Melt, StringCapture, StringSplit, Recast


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

    cut1 = Cut(table, 'foo')
    expectation = [['foo'],
                   ['A'],
                   ['B'],
                   [u'B'],
                   ['D'],
                   ['E']]
    iter_compare(expectation, cut1)
    
    cut2 = Cut(table, 'foo', 'baz')
    expectation = [['foo', 'baz'],
                   ['A', 2],
                   ['B', '3.4'],
                   [u'B', u'7.8'],
                   ['D', 9.0],
                   ['E', Ellipsis]]
    iter_compare(expectation, cut2)
    
    cut3 = Cut(table, 1, 3)
    expectation = [['foo', 'baz'],
                   ['A', 2],
                   ['B', '3.4'],
                   [u'B', u'7.8'],
                   ['D', 9.0],
                   ['E', Ellipsis]]
    iter_compare(expectation, cut3)
    
    cut4 = Cut(table, 'bar', 1)
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
    
    cat1 = Cat(table1, table2, missing=None)
    expectation = [['foo', 'bar', 'baz'],
                   [1, 'A', None],
                   [2, 'B', None],
                   [None, 'C', True],
                   [None, 'D', False]]
    iter_compare(expectation, cat1)

    # how does Cat cope with uneven rows?
    
    table3 = [['foo', 'bar', 'baz'],
              ['A', 1, 2],
              ['B', '2', '3.4'],
              [u'B', u'3', u'7.8', True],
              ['D', 'xyz', 9.0],
              ['E', None]]

    cat3 = Cat(table3, missing=None)
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
    conv = Convert(table, conversion=conversion, error_value='error')
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2.0],
                   ['B', 2, 3.4],
                   ['B', 3, 7.8, True], # N.B., long rows are preserved
                   ['D', 'error', 9.0],
                   ['E', 'error']] # N.B., short rows are preserved
    iter_compare(expectation, conv) 
    
    # test the style where the conversion functions are added one at a time
    conv = Convert(table, error_value='err')
    conv.add('foo', str)
    conv.add('bar', int)
    conv.add('baz', float)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2.0],
                   ['B', 2, 3.4],
                   ['B', 3, 7.8, True],
                   ['D', 'err', 9.0],
                   ['E', 'err']]
    iter_compare(expectation, conv) 
    
    
def test_sort_1():
    
    data = [['foo', 'bar'],
            ['C', '2'],
            ['A', '9'],
            ['A', '6'],
            ['F', '1'],
            ['D', '10']]
    
    result = Sort(data, 'foo')
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
    
    result = Sort(data, 'foo', 'bar')
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
    
    result = Sort(data, 'bar')
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
    
    result = Sort(data, 'bar')
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
    result = Sort(data, 'foo', 'bar')
    iter_compare(expectation, result)
    result = Sort(data, 1, 2)
    iter_compare(expectation, result)
    result = Sort(data, 'foo', 2)
    iter_compare(expectation, result)
    result = Sort(data, 1, 'bar')
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
    result = Sort(data, 'foo', 'bar', reverse=True)
    iter_compare(expectation, result)
    
    
def test_filter_duplicates():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             ['D', 'xyz', 9.0],
             ['B', u'3', u'7.8', True],
             ['E', None],
             ['D', 4, 12.3]]

    result = FilterDuplicates(table, 'foo')
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

    result = FilterConflicts(table, 'foo', missing=None)
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
    result = MergeDuplicates(table, 'foo', missing=None)
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
    
    result = Melt(data, key='id')
    iter_compare(expectation, result)

    result = Melt(data, key='id', variable_field='variable', value_field='value')
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
    
    result = Melt(data, key=('id', 'time'))
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
    
    result = StringCapture(data, 'variable', '(\\w)(\\d)', ('treat', 'time'))
    iter_compare(expectation, result)

    result = StringCapture(data, 'variable', '(\\w)(\\d)', ('treat', 'time'),
                           include_original=False)
    iter_compare(expectation, result)

    expectation = [
                   ['id', 'variable', 'value', 'treat', 'time'],
                   ['1', 'A1', '12', 'A', '1'],  
                   ['2', 'A2', '15', 'A', '2'],
                   ['3', 'B1', '18', 'B', '1'],
                   ['4', 'C12', '19', 'C', '1']
                   ]
    
    result = StringCapture(data, 'variable', '(\\w)(\\d)', ('treat', 'time'),
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
    
    result = StringSplit(data, 'variable', 'd', ('variable', 'day'))
    iter_compare(expectation, result)

    result = StringSplit(data, 'variable', 'd', ('variable', 'day'),
                         include_original=False)
    iter_compare(expectation, result)

    expectation = [
                   ['id', 'variable', 'value', 'variable', 'day'],
                   ['1', 'parad1', '12', 'para', '1'],  
                   ['2', 'parad2', '15', 'para', '2'],
                   ['3', 'tempd1', '18', 'temp', '1'],
                   ['4', 'tempd2', '19', 'temp', '2']
                   ]
    
    result = StringSplit(data, 'variable', 'd', ('variable', 'day'),
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
    
    step1 = Melt(data, key='id', value_field='parasitaemia')
    step2 = StringCapture(step1, 'variable', 'parad(\\d+)', ('day',))
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
    
    step1 = Melt(data, key='id')
    step2 = StringSplit(step1, 'variable', 'd', ('variable', 'day'))
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
    
    result = Recast(data) # by default lift 'variable' field, hold everything else
    iter_compare(expectation, result)

    result = Recast(data, variable_field='variable')
    iter_compare(expectation, result)

    result = Recast(data, key='id', variable_field='variable')
    iter_compare(expectation, result)

    result = Recast(data, key='id', variable_field='variable', value_field='value')
    iter_compare(expectation, result)





    
    
