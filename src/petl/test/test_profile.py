"""
TODO doc me

"""


import logging
import sys
from datetime import date, time
from collections import Counter
from itertools import izip
from petl.profile import rowlengths, values, stats, types, parsedate, parsetime,\
    parsetypes


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

