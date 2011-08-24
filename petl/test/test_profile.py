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

    profile = profiler.profile()
    assert profile['general']['field_names'] == ('foo', 'bar')
    assert profile['general']['sample_size'] == 5

    profiler.add(record_lengths)
    profile = profiler.profile()
    assert profile['general']['max_row_length'] == 3
    assert profile['general']['min_row_length'] == 1
    assert profile['general']['mean_row_length'] == 2

    profiler.add(distinct_values, field='foo')
    profile = profiler.profile()
    assert profile['fields']['foo']['distinct_values']['values'] == {'A', 'B', 'D', 'E'}
    assert profile['fields']['foo']['distinct_values']['counts']['A'] == 1
    assert profile['fields']['foo']['distinct_values']['counts']['B'] == 2
    assert profile['fields']['foo']['distinct_values']['counts']['D'] == 1
    assert profile['fields']['foo']['distinct_values']['counts']['E'] == 1
    assert 'C' not in profile['fields']['foo']['distinct_values']['counts']

    
    
