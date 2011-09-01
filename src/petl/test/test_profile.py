"""
TODO doc me

"""


import logging
import sys
from datetime import date, time, datetime
from collections import Counter


from petl.profile import Profiler, RowLengths, DistinctValues, BasicStatistics,\
    DataTypes


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

    d('profile the table with default analyses')
    profiler = Profiler()
    report = profiler.profile(table)
    assert report.fields == ('foo', 'bar', 'baz')
    assert report.sample_size == 5


def test_profile_row_lengths():
    """TODO doc me"""

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    d('profile with row lengths analysis')
    profiler = Profiler()
    profiler.add(RowLengths)
    report = profiler.profile(table)
    row_lengths = report.get_analysis(RowLengths) 
    assert row_lengths.counter == Counter({2: 1, 3: 3, 4: 1})


def test_profile_distinct_values():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    d('profile with distinct values analysis on field "foo"')
    profiler = Profiler()
    profiler.add(DistinctValues, field='foo')
    report = profiler.profile(table)
    distinct_values = report.get_analysis(DistinctValues, field='foo') 
    assert distinct_values.counter == Counter({'A': 1, 'B': 2, 'D': 1, 'E': 1})


def test_profile_basic_statistics():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]
    
    d('add basic statistics analysis on field "bar"')
    profiler = Profiler()
    profiler.add(BasicStatistics, field='bar')
    report = profiler.profile(table)
    basic_statistics = report.get_analysis(BasicStatistics, field='bar')
    assert basic_statistics.min == 1.0
    assert basic_statistics.max == 3.0
    assert basic_statistics.sum == 6.0
    assert basic_statistics.count == 3
    assert basic_statistics.errors == 2
    assert basic_statistics.mean() == 2.0


def test_profile_types():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    d('add types analysis on all fields')
    profiler = Profiler()
    profiler.add(DataTypes) 
    report = profiler.profile(table)

    foo_types = report.get_analysis(DataTypes, field='foo') 
    assert foo_types.actual_types == Counter({'str': 4, 'unicode': 1})
    assert foo_types.parse_types == Counter()
    assert foo_types.combined_types == Counter({'str': 4, 'unicode': 1})

    bar_types = report.get_analysis(DataTypes, field='bar') 
    assert bar_types.actual_types == Counter({'int': 1, 
                                              'str': 2, 
                                              'unicode': 1, 
                                              'NoneType': 1})
    assert bar_types.parse_types == Counter({'int': 2, 
                                             'float': 2})
    assert bar_types.combined_types == Counter({'int': 3, 
                                                'float': 2, 
                                                'str': 2, 
                                                'unicode': 1,
                                                'NoneType': 1})

    baz_types = report.get_analysis(DataTypes, field='baz')
    assert baz_types.actual_types == Counter({'int': 1, 
                                              'float': 1,
                                              'str': 1,
                                              'unicode': 1, 
                                              'ellipsis': 1})
    assert baz_types.parse_types == Counter({'float': 2})     
    assert baz_types.combined_types == Counter({'float': 3, 
                                                'int': 1,
                                                'str': 1,
                                                'unicode': 1,
                                                'ellipsis': 1})



def test_profile_types_datetime():

    table = [['date'],
             [date(1999, 12, 31)],
             ['31/12/99'],
             [' 31/12/1999 '], # throw some ws in as well
             [u'31 Dec 99'],
             ['31 Dec 1999'],
             ['31. Dec. 1999'],
             ['31 December 1999'], 
             ['31. December 1999'], 
             ['Fri 31 Dec 99'],
             ['Fri 31/Dec 99'],
             ['Fri 31 December 1999'],
             ['Friday 31 December 1999'],
             ['12-31'],
             ['99-12-31'],
             ['1999-12-31'], # iso 8601
             ['12/99'],
             ['31/Dec'],
             ['12/31/99'],
             ['12/31/1999'],
             ['Dec 31, 99'],
             ['Dec 31, 1999'],
             ['December 31, 1999'],
             ['Fri, Dec 31, 99'],
             ['Fri, December 31, 1999'],
             ['Friday, 31. December 1999'],
             ['I am not a date.'],
             [None]
             ] 

    d('add types analysis on "date" field')
    profiler = Profiler()
    profiler.add(DataTypes, field='date') 
    report = profiler.profile(table)

    date_types = report.get_analysis(DataTypes, field='date')
    assert date_types.actual_types == Counter({'date': 1, 
                                               'str': 24, 
                                               'unicode': 1, 
                                               'NoneType': 1})
    assert date_types.parse_types == Counter({'date': 24})
    assert date_types.combined_types == Counter({'date': 25, 
                                                 'str': 24, 
                                                 'unicode': 1, 
                                                 'NoneType': 1})

    table = [['time'],
             [time(13, 37, 46)],
             ['13:37'],
             [' 13:37:46 '], # throw some ws in as well
             [u'01:37 PM'],
             ['01:37:46 PM'],
             ['37:46.00'],
             ['13:37:46.00'], 
             ['01:37:46.00 PM'],
             ['01:37PM'],
             ['01:37:46PM'],
             ['01:37:46.00PM'],
             ['I am not a time.'], 
             [None]
             ] 

    d('add types analysis on "time" field')
    profiler = Profiler()
    profiler.add(DataTypes, field='time') 
    report = profiler.profile(table)
    
    time_types = report.get_analysis(DataTypes, field='time')
    assert time_types.actual_types == Counter({'time': 1, 
                                               'str': 10, 
                                               'unicode': 1, 
                                               'NoneType': 1})
    assert time_types.parse_types == Counter({'time': 10})
    assert time_types.combined_types == Counter({'time': 11, 
                                                 'str': 10,
                                                 'unicode': 1,
                                                 'NoneType': 1})


def test_profile_types_bool():

    table = [['bool'],
             [True],
             [False],
             ['true'],
             [' True '], # throw some ws in as well
             [u'false'],
             ['T'],
             ['f'],
             ['yes'], 
             ['No'], 
             ['Y'],
             ['n'],
             ['1'],
             ['0'],
             ['I am not a bool.'],
             [None]
             ] 

    d('add types analysis on "bool" field')
    profiler = Profiler()
    profiler.add(DataTypes, field='bool') 
    report = profiler.profile(table)

    bool_types = report.get_analysis(DataTypes, field='bool')
    assert bool_types.actual_types == Counter({'bool': 2, 
                                               'str': 11, 
                                               'unicode': 1, 
                                               'NoneType': 1})
    assert bool_types.parse_types == Counter({'bool': 11,
                                              'int': 2,
                                              'float': 2})
    assert bool_types.combined_types == Counter({'bool': 13, 
                                                 'str': 11, 
                                                 'int': 2,
                                                 'float': 2,
                                                 'unicode': 1, 
                                                 'NoneType': 1})

