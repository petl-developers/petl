"""
TODO doc me

"""


import logging
import sys
from itertools import izip
from petl import rowlengths, values, stats, types, parsetypes, valueset, unique


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
        

def test_rowlengths():
    """TODO doc me"""

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None],
             ['F', 9]]
    result = rowlengths(table)
    expectation = (('length', 'count'), (3, 3), (2, 2), (4, 1))
    iter_compare(expectation, result) 


def test_values():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['B', 'xyz', 9.0],
             ['A', None]]
    result = values(table, 'foo')
    expectation = (('value', 'count'), ('B', 3), ('A', 2))
    iter_compare(expectation, result) 


def test_valueset():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['B', 'xyz', 9.0],
             ['A', None]]
    result = valueset(table, 'foo')
    expectation = {'B', 'A'}
    assert expectation == result, result


def test_unique():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['B', 'xyz', 9.0],
             ['A', None]]
    assert unique(table, 'bar')
    assert not unique(table, 'foo')


def test_stats():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    result = stats(table, 'bar')    
    assert result['min'] == 1.0
    assert result['max'] == 3.0
    assert result['sum'] == 6.0
    assert result['count'] == 3
    assert result['errors'] == 2
    assert result['mean'] == 2.0


def test_types():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', u'2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', u'xyz', 9.0],
             ['E', 42]]

    result = types(table, 'foo') 
    expectation = (('type', 'count'), ('str', 4), ('unicode', 1))
    iter_compare(expectation, result)

    result = types(table, 'bar') 
    expectation = (('type', 'count'), ('unicode', 3), ('int', 2))
    iter_compare(expectation, result)


def test_parsetypes():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', u'2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', '3.7', 9.0],
             ['E', 42]]

    result = parsetypes(table, 'bar') 
    expectation = (('type', 'count'), ('float', 3), ('int', 2))
    iter_compare(expectation, result)

