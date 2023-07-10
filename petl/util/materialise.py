from __future__ import absolute_import, print_function, division


import operator
from collections import OrderedDict
from itertools import islice
from petl.compat import izip_longest, text_type, next


from petl.util.base import asindices, Table


def listoflists(tbl):
    return [list(row) for row in tbl]


Table.listoflists = listoflists
Table.lol = listoflists


def tupleoftuples(tbl):
    return tuple(tuple(row) for row in tbl)


Table.tupleoftuples = tupleoftuples
Table.tot = tupleoftuples


def listoftuples(tbl):
    return [tuple(row) for row in tbl]


Table.listoftuples = listoftuples
Table.lot = listoftuples


def tupleoflists(tbl):
    return tuple(list(row) for row in tbl)


Table.tupleoflists = tupleoflists
Table.tol = tupleoflists


def columns(table, missing=None):
    """
    Construct a :class:`dict` mapping field names to lists of values. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> cols = etl.columns(table)
        >>> cols['foo']
        ['a', 'b', 'b']
        >>> cols['bar']
        [1, 2, 3]

    See also :func:`petl.util.materialise.facetcolumns`.

    """

    cols = OrderedDict()
    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    for f in flds:
        cols[f] = list()
    for row in it:
        for f, v in izip_longest(flds, row, fillvalue=missing):
            if f in cols:
                cols[f].append(v)
    return cols


Table.columns = columns


def facetcolumns(table, key, missing=None):
    """
    Like :func:`petl.util.materialise.columns` but stratified by values of the
    given key field. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['a', 1, True],
        ...          ['b', 2, True],
        ...          ['b', 3]]
        >>> fc = etl.facetcolumns(table, 'foo')
        >>> fc['a']
        {'foo': ['a'], 'bar': [1], 'baz': [True]}
        >>> fc['b']
        {'foo': ['b', 'b'], 'bar': [2, 3], 'baz': [True, None]}

    """

    fct = dict()
    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    indices = asindices(hdr, key)
    assert len(indices) > 0, 'no key field selected'
    getkey = operator.itemgetter(*indices)

    for row in it:
        kv = getkey(row)
        if kv not in fct:
            cols = dict()
            for f in flds:
                cols[f] = list()
            fct[kv] = cols
        else:
            cols = fct[kv]
        for f, v in izip_longest(flds, row, fillvalue=missing):
            if f in cols:
                cols[f].append(v)

    return fct


Table.facetcolumns = facetcolumns


def cache(table, n=None):
    """
    Wrap the table with a cache that caches up to `n` rows as they are initially
    requested via iteration (cache all rows be default).

    """

    return CacheView(table, n=n)


Table.cache = cache


class CacheView(Table):

    def __init__(self, inner, n=None):
        self.inner = inner
        self.n = n
        self.cache = list()
        self.cachecomplete = False

    def clearcache(self):
        self.cache = list()
        self.cachecomplete = False

    def __iter__(self):

        # serve whatever is in the cache first
        for row in self.cache:
            yield row

        if not self.cachecomplete:

            # serve the remainder from the inner iterator
            it = iter(self.inner)
            for row in islice(it, len(self.cache), None):
                # maybe there's more room in the cache?
                if not self.n or len(self.cache) < self.n:
                    self.cache.append(row)
                yield row

            # does the cache contain a complete copy of the inner table?
            if not self.n or len(self.cache) < self.n:
                self.cachecomplete = True
