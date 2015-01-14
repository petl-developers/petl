from __future__ import absolute_import, print_function, division


from petl.test.helpers import eq_
from petl.util.statistics import stats


def test_stats():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '3', '7.8', True),
             ('D', 'xyz', 9.0),
             ('E', None))

    result = stats(table, 'bar')
    eq_(1.0, result.min)
    eq_(3.0, result.max)
    eq_(6.0, result.sum)
    eq_(3, result.count)
    eq_(2, result.errors)
    eq_(2.0, result.mean)
    eq_(2/3, result.pvariance)
    eq_((2/3)**.5, result.pstdev)
