from __future__ import absolute_import, print_function, division, \
    unicode_literals


import operator
from itertools import islice
from petl.compat import izip_longest


from petl.util.base import asindices, RowContainer


def listoflists(tbl):
    return [list(row) for row in tbl]


def tupleoftuples(tbl):
    return tuple(tuple(row) for row in tbl)


def listoftuples(tbl):
    return [tuple(row) for row in tbl]


def tupleoflists(tbl):
    return tuple(list(row) for row in tbl)


def columns(table, missing=None):
    """
    Construct a :class:`dict` mapping field names to lists of values. E.g.::

        >>> from petl import columns
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> cols = columns(table)
        >>> cols['foo']
        ['a', 'b', 'b']
        >>> cols['bar']
        [1, 2, 3]

    See also :func:`facetcolumns`.

    """

    cols = dict()
    it = iter(table)
    fields = [str(f) for f in next(it)]
    for f in fields:
        cols[f] = list()
    for row in it:
        for f, v in izip_longest(fields, row, fillvalue=missing):
            if f in cols:
                cols[f].append(v)
    return cols


def facetcolumns(table, key, missing=None):
    """
    Like :func:`columns` but stratified by values of the given key field. E.g.::

        >>> from petl import facetcolumns
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['a', 1, True],
        ...          ['b', 2, True],
        ...          ['b', 3]]
        >>> fc = facetcolumns(table, 'foo')
        >>> fc['a']
        {'baz': [True], 'foo': ['a'], 'bar': [1]}
        >>> fc['b']
        {'baz': [True, None], 'foo': ['b', 'b'], 'bar': [2, 3]}
        >>> fc['c']
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        KeyError: 'c'

    .. versionadded:: 0.8

    """

    fct = dict()
    it = iter(table)
    fields = [str(f) for f in next(it)]
    indices = asindices(fields, key)
    assert len(indices) > 0, 'no key field selected'
    getkey = operator.itemgetter(*indices)

    for row in it:
        kv = getkey(row)
        if kv not in fct:
            cols = dict()
            for f in fields:
                cols[f] = list()
            fct[kv] = cols
        else:
            cols = fct[kv]
        for f, v in izip_longest(fields, row, fillvalue=missing):
            if f in cols:
                cols[f].append(v)

    return fct


def cache(table, n=10000):
    """
    Wrap the table with a cache that caches up to `n` rows as they are initially
    requested via iteration.

    .. versionadded:: 0.16

    """

    return CacheContainer(table, n=n)


class CacheContainer(RowContainer):

    def __init__(self, inner, n=10000):
        self._inner = inner
        self._n = n
        self._cache = list()
        self._cachecomplete = False

    def clearcache(self):
        self._cache = list()
        self._cachecomplete = False

    def __iter__(self):

        # serve whatever is in the cache first
        for row in self._cache:
            yield row

        if not self._cachecomplete:

            # serve the remainder from the inner iterator
            it = iter(self._inner)
            for row in islice(it, len(self._cache), None):
                # maybe there's more room in the cache?
                if len(self._cache) < self._n:
                    self._cache.append(row)
                yield row

            # does the cache contain a complete copy of the inner table?
            if len(self._cache) < self._n:
                object.__setattr__(self, '_cachecomplete', True)
