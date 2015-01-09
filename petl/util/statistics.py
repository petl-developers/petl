from __future__ import absolute_import, print_function, division


from petl.compat import OrderedDict


from petl.util.base import itervalues, Table


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

    vals = itervalues(table, field)
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
        OrderedDict([('min', 1.0), ('max', 3.0), ('sum', 6.0), ('mean', 2.0), ('count', 3), ('errors', 2)])

    The `field` argument can be a field name or index (starting from zero).

    """

    output = OrderedDict([('min', None),
                          ('max', None),
                          ('sum', None),
                          ('mean', None),
                          ('count', 0),
                          ('errors', 0)])
    for v in itervalues(table, field):
        try:
            v = float(v)
        except (ValueError, TypeError):
            output['errors'] += 1
        else:
            if output['min'] is None or v < output['min']:
                output['min'] = v
            if output['max'] is None or v > output['max']:
                output['max'] = v
            if output['sum'] is None:
                output['sum'] = v
            else:
                output['sum'] += v
            output['count'] += 1
    if output['count'] > 0:
        output['mean'] = output['sum'] / output['count']
    return output


Table.stats = stats
