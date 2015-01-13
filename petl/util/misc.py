from __future__ import absolute_import, print_function, division


from petl.util.base import values, header, Table


def typeset(table, field):
    """
    Return a set containing all Python types found for values in the given
    field. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, '2'],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', '7.8', True],
        ...          ['D', u'xyz', 9.0],
        ...          ['E', 42]]
        >>> sorted(etl.typeset(table, 'foo'))
        ['str']
        >>> sorted(etl.typeset(table, 'bar'))
        ['int', 'str']
        >>> sorted(etl.typeset(table, 'baz'))
        ['NoneType', 'float', 'str']

    The `field` argument can be a field name or index (starting from zero).

    """

    s = set()
    for v in values(table, field):
        try:
            s.add(type(v).__name__)
        except IndexError:
            pass  # ignore short rows
    return s


Table.typeset = typeset


def diffheaders(t1, t2):
    """
    Return the difference between the headers of the two tables as a pair of
    sets. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 1, .3]]
        >>> table2 = [['baz', 'bar', 'quux'],
        ...           ['a', 1, .3]]
        >>> add, sub = etl.diffheaders(table1, table2)
        >>> add
        {'quux'}
        >>> sub
        {'foo'}

    """

    t1h = set(header(t1))
    t2h = set(header(t2))
    return t2h - t1h, t1h - t2h


Table.diffheaders = diffheaders


def diffvalues(t1, t2, f):
    """
    Return the difference between the values under the given field in the two
    tables, e.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 3]]
        >>> table2 = [['bar', 'foo'],
        ...           [1, 'a'],
        ...           [3, 'c']]
        >>> add, sub = etl.diffvalues(table1, table2, 'foo')
        >>> add
        {'c'}
        >>> sub
        {'b'}

    """

    t1v = set(values(t1, f))
    t2v = set(values(t2, f))
    return t2v - t1v, t1v - t2v


Table.diffvalues = diffvalues


def strjoin(s):
    """
    Return a function to join sequences using `s` as the separator. Intended
    for use with :func:`petl.transform.conversions.convert`.

    """

    return lambda l: s.join(map(str, l))


def nthword(n, sep=None):
    """
    Construct a function to return the nth word in a string. E.g.::

        >>> import petl as etl
        >>> s = 'foo bar'
        >>> f = etl.nthword(0)
        >>> f(s)
        'foo'
        >>> g = etl.nthword(1)
        >>> g(s)
        'bar'

    Intended for use with :func:`petl.transform.conversions.convert`.

    """

    return lambda s: s.split(sep)[n]


def coalesce(*fields, **kwargs):
    """
    Return a function which accepts a row and returns the first non-missing
    value from the specified fields. Intended for use with
    :func:`petl.transform.basics.addfield`.

    """
    missing = kwargs.get('missing', None)
    default = kwargs.get('default', None)

    def _coalesce(row):
        for f in fields:
            v = row[f]
            if v is not missing:
                return v
        return default

    return _coalesce
