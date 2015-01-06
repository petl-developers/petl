from __future__ import absolute_import, print_function, division


from petl.util.statistics import stats


def test_stats():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '3', '7.8', True),
             ('D', 'xyz', 9.0),
             ('E', None))

    result = stats(table, 'bar')
    assert result['min'] == 1.0
    assert result['max'] == 3.0
    assert result['sum'] == 6.0
    assert result['count'] == 3
    assert result['errors'] == 2
    assert result['mean'] == 2.0
