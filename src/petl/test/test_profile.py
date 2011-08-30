"""
TODO doc me

"""


import logging
import sys


from petl.profile import *


logger = logging.getLogger('petl')
d, i, w, e = logger.debug, logger.info, logger.warn, logger.error # abbreviations
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stderr)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def test_profile_default():
    """TODO doc me"""

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    profiler = Profiler(table)

    d('profile the table with default analyses')
    report = profiler.profile()
    default = report['table']['default'] 
    assert default['fields'] == ('foo', 'bar', 'baz')
    assert default['sample_size'] == 5


def test_profile_row_lengths():
    """TODO doc me"""

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    profiler = Profiler(table)
        
    d('profile with row lengths analysis')
    profiler.add(RowLengths)
    report = profiler.profile()
    row_lengths = report['table']['row_lengths'] 
    assert row_lengths['max_row_length'] == 4
    assert row_lengths['min_row_length'] == 2
    assert row_lengths['mean_row_length'] == 3.


def test_profile_distinct_values():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    profiler = Profiler(table)

    d('profile with distinct values analysis on field "foo"')
    profiler.add(DistinctValues, field='foo')
    report = profiler.profile()
    distinct_values = report['field']['foo']['distinct_values'] 
    assert distinct_values == Counter({'A': 1, 'B': 2, 'D': 1, 'E': 1})


def test_profile_basic_statistics():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    profiler = Profiler(table)
    
    d('add basic statistics analysis on field "bar"')
    profiler.add(BasicStatistics, field='bar')
    report = profiler.profile()
    basic_statistics = report['field']['bar']['basic_statistics']
    assert basic_statistics['min'] == 1.0
    assert basic_statistics['max'] == 3.0
    assert basic_statistics['mean'] == 2.0
    assert basic_statistics['sum'] == 6.0
    assert basic_statistics['count'] == 3
    assert basic_statistics['errors'] == 2


def test_profile_types():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    profiler = Profiler(table)
    
    d('add types analysis on all fields')
    profiler.add(Types) 
    report = profiler.profile()

    foo_types = report['field']['foo']['types'] 
    assert foo_types['actual_types'] == Counter({'str': 4, 'unicode': 1})
    assert foo_types['parse_types'] == Counter()
    assert foo_types['consensus_types'] == Counter({'str': 4, 'unicode': 1})

    bar_types = report['field']['bar']['types'] 
    assert bar_types['actual_types'] == Counter({'int': 1, 
                                                 'str': 2, 
                                                 'unicode': 1, 
                                                 'NoneType': 1})
    assert bar_types['parse_types'] == Counter({'int': 2, 
                                                'float': 2})
    assert bar_types['consensus_types'] == Counter({'int': 3, 
                                                    'float': 2, 
                                                    'str': 2, 
                                                    'unicode': 1,
                                                    'NoneType': 1})

    baz_types = report['field']['baz']['types']
    assert baz_types['actual_types'] == Counter({'int': 1, 
                                                 'float': 1,
                                                 'str': 1,
                                                 'unicode': 1, 
                                                 'ellipsis': 1})
    assert baz_types['parse_types'] == Counter({'float': 2})     
    assert baz_types['consensus_types'] == Counter({'float': 3, 
                                                    'int': 1,
                                                    'str': 1,
                                                    'unicode': 1,
                                                    'ellipsis': 1})



def test_profile_types_datetime():

    table = [['when',],
             ['2009-08-12',], # iso 8601
             ['12/08/2009',], # TODO
             [u'12/31/2009'], # TODO
             ] 

    profiler = Profiler(table)
    
    d('add types analysis on "when" field')
    profiler.add(Types, field='when') 
    report = profiler.profile()

    types = report['field']['when']['types']
    assert types['actual_types'] = Counter({'str': 2, 'unicode': 1})
    assert types['parse_types'] = Counter({'date_iso8601': 1})

    # TODO dates, times etc.

