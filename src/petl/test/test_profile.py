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


d('begin tests')


def test_profile():
    """
    TODO doc me

    """

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 0.2],
             ['B', '2', 3.4],
             [u'B', u'3', 7.8, True],
             ['D', 'xyz', 9],
             ['E', None]]

    profiler = Profiler(table)

    d('profile the table with default analyses')
    report = profiler.profile()
    assert report['table']['default']['fields'] == ('foo', 'bar', 'baz')
    assert report['table']['default']['sample_size'] == 5

    d('add row lengths analysis')
    profiler.add(RowLengths)
    report = profiler.profile()
    assert report['table']['row_lengths']['max_row_length'] == 4
    assert report['table']['row_lengths']['min_row_length'] == 2
    assert report['table']['row_lengths']['mean_row_length'] == 3.

    d('add distinct values analysis on field "foo"')
    profiler.add(DistinctValues, field='foo')
    report = profiler.profile()
    assert report['field']['foo']['distinct_values'] == {'A': 1, 'B': 2, 'D': 1, 'E': 1}

    d('add basic statistics analysis on field "bar"')
    profiler.add(BasicStatistics, field='bar')
    report = profiler.profile()
    assert report['field']['bar']['basic_statistics']['min'] == 1.0
    assert report['field']['bar']['basic_statistics']['max'] == 3.0
    assert report['field']['bar']['basic_statistics']['mean'] == 2.0
    assert report['field']['bar']['basic_statistics']['sum'] == 6.0
    assert report['field']['bar']['basic_statistics']['count'] == 3
    assert report['field']['bar']['basic_statistics']['errors'] == 2

    d('add types analysis on all fields')
    profiler.add(Types) 
    report = profiler.profile()
    assert report['field']['foo']['types']['actual_types'] == Counter({'str': 4, 'unicode': 1})
    assert report['field']['foo']['types']['applicable_types'] == Counter({'str': 5, 'unicode': 5})     
    assert report['field']['foo']['types']['inferred_type'] == 'unicode'    
    assert report['field']['bar']['types']['actual_types'] == Counter({'int': 1, 'str': 2, 'unicode': 1, 'NoneType': 1})
    assert report['field']['bar']['types']['applicable_types'] == Counter({'int': 3, 'float': 3, 'str': 5, 'unicode': 5})     
    assert report['field']['bar']['types']['inferred_type'] == 'int'    
    assert report['field']['baz']['types']['actual_types'] == Counter({'int': 1, 'float': 3})
    assert report['field']['baz']['types']['applicable_types'] == Counter({'int': 4, 'float': 4, 'str': 4, 'unicode': 4})     
    assert report['field']['baz']['types']['inferred_type'] == 'float'    

