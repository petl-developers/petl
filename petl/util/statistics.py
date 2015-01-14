from __future__ import absolute_import, print_function, division


from collections import namedtuple


from petl.util.base import values, Table


def limits(table, field):
    """
    Find minimum and maximum values under the given field. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> minv, maxv = etl.limits(table, 'bar')
        >>> minv
        1
        >>> maxv
        3

    The `field` argument can be a field name or index (starting from zero).

    """

    vals = iter(values(table, field))
    try:
        minv = maxv = next(vals)
    except StopIteration:
        return None, None
    else:
        for v in vals:
            if v < minv:
                minv = v
            if v > maxv:
                maxv = v
        return minv, maxv


Table.limits = limits


_stats = namedtuple('stats', ('count', 'errors', 'sum', 'min', 'max', 'mean',
                              'pvariance', 'pstdev'))


def stats(table, field):
    """
    Calculate basic descriptive statistics on a given field. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', '2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', 'xyz', 9.0],
        ...          ['E', None]]
        >>> etl.stats(table, 'bar')
        stats(count=3, errors=2, sum=6.0, min=1.0, max=3.0, mean=2.0, pvariance=0.6666666666666666, pstdev=0.816496580927726)

    The `field` argument can be a field name or index (starting from zero).

    """

    _min = None
    _max = None
    _sum = 0
    _mean = 0
    _var = 0
    _count = 0
    _errors = 0
    for v in values(table, field):
        try:
            v = float(v)
        except (ValueError, TypeError):
            _errors += 1
        else:
            _count += 1
            if _min is None or v < _min:
                _min = v
            if _max is None or v > _max:
                _max = v
            _sum += v
            _mean, _var = onlinestats(v, _count, mean=_mean, variance=_var)
    _std = _var**.5
    return _stats(_count, _errors, _sum, _min, _max, _mean, _var, _std)


Table.stats = stats


def onlinestats(xi, n, mean=0, variance=0):
    # function to calculate online mean and variance
    meanprv = mean
    varianceprv = variance
    mean = (((n - 1)*meanprv) + xi)/n
    variance = (((n - 1)*varianceprv) + ((xi - meanprv)*(xi - mean)))/n
    return mean, variance
