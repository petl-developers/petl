"""
TODO doc me

"""


from petl.profile import *


def test_profile():
    """
    TODO doc me

    """

    table = [['foo', 'bar'],
             ['A', 1],
             ['B', 2],
             ['B', '3', True],
             ['D', 'xyz'],
             ['E']]

    profiler = Profiler(table)

    # profile the table with default analyses - list field names and
    # report the sample size used for profiling
    report = profiler.profile()
    assert report['table']['default']['fields'] == ('foo', 'bar')
    assert report['table']['default']['sample_size'] == 5

    # add row lengths analysis
    profiler.add(RowLengths)
    report = profiler.profile()
    assert report['table']['row_lengths']['max_row_length'] == 3
    assert report['table']['row_lengths']['min_row_length'] == 1
    assert report['table']['row_lengths']['mean_row_length'] == 2

    # add distinct values analysis on field 'foo'
    profiler.add(DistinctValues, field='foo')
    report = profiler.profile()
    assert report['field']['foo']['distinct_values'] == {'A': 1, 'B': 2, 'D': 1, 'E': 1}

    # add basic statistics analysis on field 'bar'
    profiler.add(BasicStatistics, field='foo')
    report = profiler.profile()
    assert report['field']['bar']['basic_statistics']['min'] == 1
    assert report['field']['bar']['basic_statistics']['max'] == 3
    assert report['field']['bar']['basic_statistics']['mean'] == 2
    assert report['field']['bar']['basic_statistics']['sum'] == 6
    assert report['field']['bar']['basic_statistics']['count'] == 3
    assert report['field']['bar']['basic_statistics']['errors'] == 2

    # add types analysis on all fields
    profiler.add(Types) 
    report = profiler.profile()
    assert report['field']['foo']['types']['actual_types'] == {'string': 5}
    assert report['field']['foo']['types']['applicable_types'] == {'string': 5}     
    assert report['field']['foo']['types']['inferred_type'] == 'string'    
    assert report['field']['bar']['types']['actual_types'] == {'int': 2, 'string': 2}
    assert report['field']['bar']['types']['applicable_types'] == {'int': 3, 'float': 3, 'string': 5}     
    assert report['field']['bar']['types']['inferred_type'] == 'int'    
