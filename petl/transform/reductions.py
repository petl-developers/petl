from __future__ import absolute_import, print_function, division


import itertools
import operator
from collections import OrderedDict
from petl.compat import next, string_types, reduce, text_type


from petl.errors import ArgumentError
from petl.util.base import Table, iterpeek, rowgroupby
from petl.util.base import values
from petl.util.counting import nrows
from petl.transform.sorts import sort, mergesort
from petl.transform.basics import cut
from petl.transform.dedup import distinct


def rowreduce(table, key, reducer, header=None, presorted=False,
              buffersize=None, tempdir=None, cache=True):
    """
    Group rows under the given key then apply `reducer` to produce a single
    output row for each input group of rows. E.g.::
    
        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 3],
        ...           ['a', 7],
        ...           ['b', 2],
        ...           ['b', 1],
        ...           ['b', 9],
        ...           ['c', 4]]
        >>> def sumbar(key, rows):
        ...     return [key, sum(row[1] for row in rows)]
        ...
        >>> table2 = etl.rowreduce(table1, key='foo', reducer=sumbar,
        ...                        header=['foo', 'barsum'])
        >>> table2
        +-----+--------+
        | foo | barsum |
        +=====+========+
        | 'a' |     10 |
        +-----+--------+
        | 'b' |     12 |
        +-----+--------+
        | 'c' |      4 |
        +-----+--------+
    
    N.B., this is not strictly a "reduce" in the sense of the standard Python
    :func:`reduce` function, i.e., the `reducer` function is *not* applied 
    recursively to values within a group, rather it is applied once to each row 
    group as a whole.
    
    See also :func:`petl.transform.reductions.aggregate` and
    :func:`petl.transform.reductions.fold`.
    
    """

    return RowReduceView(table, key, reducer, header=header,
                         presorted=presorted, 
                         buffersize=buffersize, tempdir=tempdir, cache=cache)


Table.rowreduce = rowreduce


class RowReduceView(Table):
    
    def __init__(self, source, key, reducer, header=None,
                 presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, 
                               tempdir=tempdir, cache=cache)
        self.key = key
        self.header = header
        self.reducer = reducer

    def __iter__(self):
        return iterrowreduce(self.source, self.key, self.reducer, self.header)

    
def iterrowreduce(source, key, reducer, header):
    if header is None:
        # output header from source
        header, source = iterpeek(source)
    yield tuple(header)
    for key, rows in rowgroupby(source, key):
        yield tuple(reducer(key, rows))
        

def aggregate(table, key, aggregation=None, value=None, presorted=False,
              buffersize=None, tempdir=None, cache=True, field='value'):
    """Apply aggregation functions.
    E.g.::

        >>> import petl as etl
        >>>
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 3, True],
        ...           ['a', 7, False],
        ...           ['b', 2, True],
        ...           ['b', 2, False],
        ...           ['b', 9, False],
        ...           ['c', 4, True]]
        >>> # aggregate whole rows
        ... table2 = etl.aggregate(table1, 'foo', len)
        >>> table2
        +-----+-------+
        | foo | value |
        +=====+=======+
        | 'a' |     2 |
        +-----+-------+
        | 'b' |     3 |
        +-----+-------+
        | 'c' |     1 |
        +-----+-------+

        >>> # aggregate whole rows without a key
        >>> etl.aggregate(table1, None, len)
        +-------+
        | value |
        +=======+
        |     6 |
        +-------+

        >>> # aggregate single field
        ... table3 = etl.aggregate(table1, 'foo', sum, 'bar')
        >>> table3
        +-----+-------+
        | foo | value |
        +=====+=======+
        | 'a' |    10 |
        +-----+-------+
        | 'b' |    13 |
        +-----+-------+
        | 'c' |     4 |
        +-----+-------+

        >>> # aggregate single field without a key
        >>> etl.aggregate(table1, None, sum, 'bar')
        +-------+
        | value |
        +=======+
        |    27 |
        +-------+

        >>> # alternative signature using keyword args
        ... table4 = etl.aggregate(table1, key=('foo', 'bar'),
        ...                        aggregation=list, value=('bar', 'baz'))
        >>> table4
        +-----+-----+-------------------------+
        | foo | bar | value                   |
        +=====+=====+=========================+
        | 'a' |   3 | [(3, True)]             |
        +-----+-----+-------------------------+
        | 'a' |   7 | [(7, False)]            |
        +-----+-----+-------------------------+
        | 'b' |   2 | [(2, True), (2, False)] |
        +-----+-----+-------------------------+
        | 'b' |   9 | [(9, False)]            |
        +-----+-----+-------------------------+
        | 'c' |   4 | [(4, True)]             |
        +-----+-----+-------------------------+

        >>> # alternative signature using keyword args without a key
        >>> etl.aggregate(table1, key=None,
        ...                 aggregation=list, value=('bar', 'baz'))
        +-----------------------------------------------------------------------+
        | value                                                                 |
        +=======================================================================+
        | [(3, True), (7, False), (2, True), (2, False), (9, False), (4, True)] |
        +-----------------------------------------------------------------------+

        >>> # aggregate multiple fields
        ... from collections import OrderedDict
        >>> import petl as etl
        >>>
        >>> aggregation = OrderedDict()
        >>> aggregation['count'] = len
        >>> aggregation['minbar'] = 'bar', min
        >>> aggregation['maxbar'] = 'bar', max
        >>> aggregation['sumbar'] = 'bar', sum
        >>> # default aggregation function is list
        ... aggregation['listbar'] = 'bar'
        >>> aggregation['listbarbaz'] = ('bar', 'baz'), list
        >>> aggregation['bars'] = 'bar', etl.strjoin(', ')
        >>> table5 = etl.aggregate(table1, 'foo', aggregation)
        >>> table5
        +-----+-------+--------+--------+--------+-----------+-------------------------------------+-----------+
        | foo | count | minbar | maxbar | sumbar | listbar   | listbarbaz                          | bars      |
        +=====+=======+========+========+========+===========+=====================================+===========+
        | 'a' |     2 |      3 |      7 |     10 | [3, 7]    | [(3, True), (7, False)]             | '3, 7'    |
        +-----+-------+--------+--------+--------+-----------+-------------------------------------+-----------+
        | 'b' |     3 |      2 |      9 |     13 | [2, 2, 9] | [(2, True), (2, False), (9, False)] | '2, 2, 9' |
        +-----+-------+--------+--------+--------+-----------+-------------------------------------+-----------+
        | 'c' |     1 |      4 |      4 |      4 | [4]       | [(4, True)]                         | '4'       |
        +-----+-------+--------+--------+--------+-----------+-------------------------------------+-----------+

        >>> # aggregate multiple fields without a key
        >>> etl.aggregate(table1, None, aggregation)
        +-------+--------+--------+--------+--------------------+-----------------------------------------------------------------------+--------------------+
        | count | minbar | maxbar | sumbar | listbar            | listbarbaz                                                            | bars               |
        +=======+========+========+========+====================+=======================================================================+====================+
        |     6 |      2 |      9 |     27 | [3, 7, 2, 2, 9, 4] | [(3, True), (7, False), (2, True), (2, False), (9, False), (4, True)] | '3, 7, 2, 2, 9, 4' |
        +-------+--------+--------+--------+--------------------+-----------------------------------------------------------------------+--------------------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are 
    ignored. Otherwise, the data are sorted, see also the discussion of the 
    `buffersize`, `tempdir` and `cache` arguments under the
    :func:`petl.transform.sorts.sort` function.

    If `key` is None, sorting is not necessary.

    """

    if callable(aggregation):
        return SimpleAggregateView(table, key, aggregation=aggregation, 
                                   value=value, presorted=presorted, 
                                   buffersize=buffersize, tempdir=tempdir, 
                                   cache=cache, field=field)
    elif aggregation is None or isinstance(aggregation, (list, tuple, dict)):
        # ignore value arg
        return MultiAggregateView(table, key, aggregation=aggregation,  
                                  presorted=presorted, buffersize=buffersize, 
                                  tempdir=tempdir, cache=cache)
    else:
        raise ArgumentError('expected aggregation is callable, list, tuple, dict '
                        'or None')


Table.aggregate = aggregate


class SimpleAggregateView(Table):
    
    def __init__(self, table, key, aggregation=list, value=None, 
                 presorted=False, buffersize=None, tempdir=None,
                 cache=True, field='value'):
        if presorted or key is None:
            self.table = table
        else:
            self.table = sort(table, key, buffersize=buffersize, 
                              tempdir=tempdir, cache=cache)    
        self.key = key
        self.aggregation = aggregation
        self.value = value
        self.field = field
        
    def __iter__(self):
        return itersimpleaggregate(self.table, self.key, self.aggregation, 
                                   self.value, self.field)


def itersimpleaggregate(table, key, aggregation, value, field):

    # special case counting
    if aggregation == len and key is not None:
        aggregation = lambda g: sum(1 for _ in g)  # count length of iterable

    # special case where length of key is 1
    if isinstance(key, (list, tuple)) and len(key) == 1:
        key = key[0]

    # determine output header
    if isinstance(key, (list, tuple)):
        outhdr = tuple(key) + (field,)
    elif callable(key):
        outhdr = ('key', field)
    elif key is None:
        outhdr = field,
    else:
        outhdr = (key, field)
    yield outhdr

    # generate data
    if isinstance(key, (list, tuple)):
        for k, grp in rowgroupby(table, key, value):
            yield tuple(k) + (aggregation(grp),)
    elif key is None:
        # special case counting
        if aggregation == len:
            yield nrows(table),
        else:
            yield aggregation(values(table, value)),
    else:
        for k, grp in rowgroupby(table, key, value):
            yield k, aggregation(grp)


class MultiAggregateView(Table):
    
    def __init__(self, source, key, aggregation=None, presorted=False, 
                 buffersize=None, tempdir=None, cache=True):
        if presorted or key is None:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, 
                               tempdir=tempdir, cache=cache)
        self.key = key
        if aggregation is None:
            self.aggregation = OrderedDict()
        elif isinstance(aggregation, (list, tuple)):
            self.aggregation = OrderedDict()
            for t in aggregation:
                self.aggregation[t[0]] = t[1:]
        elif isinstance(aggregation, dict):
            self.aggregation = aggregation
        else:
            raise ArgumentError(
                'expected aggregation is None, list, tuple or dict, found %r'
                % aggregation
            )

    def __iter__(self):
        return itermultiaggregate(self.source, self.key, self.aggregation)
    
    def __setitem__(self, key, value):
        self.aggregation[key] = value

    
def itermultiaggregate(source, key, aggregation):
    aggregation = OrderedDict(aggregation.items())  # take a copy
    it = iter(source)
    hdr = next(it)
    # push back header to ensure we iterate only once
    it = itertools.chain([hdr], it)

    # normalise aggregators
    for outfld in aggregation:
        agg = aggregation[outfld]
        if callable(agg):
            aggregation[outfld] = None, agg
        elif isinstance(agg, string_types):
            aggregation[outfld] = agg, list  # list is default
        elif len(agg) == 1 and isinstance(agg[0], string_types):
            aggregation[outfld] = agg[0], list  # list is default
        elif len(agg) == 1 and callable(agg[0]):
            aggregation[outfld] = None, agg[0]  # aggregate whole rows
        elif len(agg) == 2:
            pass  # no need to normalise
        else:
            raise ArgumentError('invalid aggregation: %r, %r' % (outfld, agg))

    # determine output header
    if isinstance(key, (list, tuple)):
        outhdr = list(key)
    elif callable(key):
        outhdr = ['key']
    elif key is None:
        outhdr = []
    else:
        outhdr = [key]
    for outfld in aggregation:
        outhdr.append(outfld)
    yield tuple(outhdr)

    if key is None:
        grouped = rowgroupby(it, lambda x: None)
    else:
        grouped = rowgroupby(it, key)

    # generate data
    for k, rows in grouped:
        rows = list(rows)  # may need to iterate over these more than once
        # handle compound key
        if isinstance(key, (list, tuple)):
            outrow = list(k)
        elif key is None:
            outrow = []
        else:
            outrow = [k]
        for outfld in aggregation:
            srcfld, aggfun = aggregation[outfld]
            if srcfld is None:
                aggval = aggfun(rows)
                outrow.append(aggval)
            elif isinstance(srcfld, (list, tuple)):
                idxs = [hdr.index(f) for f in srcfld]
                valgetter = operator.itemgetter(*idxs)
                vals = (valgetter(row) for row in rows)
                aggval = aggfun(vals)
                outrow.append(aggval)
            else:
                idx = hdr.index(srcfld)
                # try using generator comprehension
                vals = (row[idx] for row in rows)
                aggval = aggfun(vals)
                outrow.append(aggval)
        yield tuple(outrow)


def groupcountdistinctvalues(table, key, value):
    """Group by the `key` field then count the number of distinct values in the
    `value` field."""
    
    s1 = cut(table, key, value)
    s2 = distinct(s1)
    s3 = aggregate(s2, key, len)
    return s3


Table.groupcountdistinctvalues = groupcountdistinctvalues


def groupselectfirst(table, key, presorted=False, buffersize=None,
                     tempdir=None, cache=True):
    """Group by the `key` field then return the first row within each group.
    E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, True],
        ...           ['C', 7, False],
        ...           ['B', 2, False],
        ...           ['C', 9, True]]
        >>> table2 = etl.groupselectfirst(table1, key='foo')
        >>> table2
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'A' |   1 | True  |
        +-----+-----+-------+
        | 'B' |   2 | False |
        +-----+-----+-------+
        | 'C' |   7 | False |
        +-----+-----+-------+

    See also :func:`petl.transform.reductions.groupselectlast`,
    :func:`petl.transform.dedup.distinct`.

    """

    def _reducer(k, rows):
        return next(rows)

    return rowreduce(table, key, reducer=_reducer, presorted=presorted,
                     buffersize=buffersize, tempdir=tempdir, cache=cache)


Table.groupselectfirst = groupselectfirst


def groupselectlast(table, key, presorted=False, buffersize=None,
                    tempdir=None, cache=True):
    """Group by the `key` field then return the last row within each group.
    E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, True],
        ...           ['C', 7, False],
        ...           ['B', 2, False],
        ...           ['C', 9, True]]
        >>> table2 = etl.groupselectlast(table1, key='foo')
        >>> table2
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'A' |   1 | True  |
        +-----+-----+-------+
        | 'B' |   2 | False |
        +-----+-----+-------+
        | 'C' |   9 | True  |
        +-----+-----+-------+

    See also :func:`petl.transform.reductions.groupselectfirst`,
    :func:`petl.transform.dedup.distinct`.

    .. versionadded:: 1.1.0

    """

    def _reducer(k, rows):
        row = None
        for row in rows:
            pass
        return row

    return rowreduce(table, key, reducer=_reducer, presorted=presorted,
                     buffersize=buffersize, tempdir=tempdir, cache=cache)


Table.groupselectlast = groupselectlast


def groupselectmin(table, key, value, presorted=False, buffersize=None,
                   tempdir=None, cache=True):
    """Group by the `key` field then return the row with the minimum of the
    `value` field within each group. N.B., will only return one row for each
    group, even if multiple rows have the same (minimum) value."""

    return groupselectfirst(sort(table, value, reverse=False), key,
                            presorted=presorted, buffersize=buffersize,
                            tempdir=tempdir, cache=cache)


Table.groupselectmin = groupselectmin

    
def groupselectmax(table, key, value, presorted=False, buffersize=None,
                   tempdir=None, cache=True):
    """Group by the `key` field then return the row with the maximum of the
    `value` field within each group. N.B., will only return one row for each
    group, even if multiple rows have the same (maximum) value."""

    return groupselectfirst(sort(table, value, reverse=True), key,
                            presorted=presorted, buffersize=buffersize,
                            tempdir=tempdir, cache=cache)


Table.groupselectmax = groupselectmax


def mergeduplicates(table, key, missing=None, presorted=False, buffersize=None,
                    tempdir=None, cache=True):
    """
    Merge duplicate rows under the given key. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, 2.7],
        ...           ['B', 2, None],
        ...           ['D', 3, 9.4],
        ...           ['B', None, 7.8],
        ...           ['E', None, 42.],
        ...           ['D', 3, 12.3],
        ...           ['A', 2, None]]
        >>> table2 = etl.mergeduplicates(table1, 'foo')
        >>> table2
        +-----+------------------+-----------------------+
        | foo | bar              | baz                   |
        +=====+==================+=======================+
        | 'A' | Conflict({1, 2}) |                   2.7 |
        +-----+------------------+-----------------------+
        | 'B' |                2 |                   7.8 |
        +-----+------------------+-----------------------+
        | 'D' |                3 | Conflict({9.4, 12.3}) |
        +-----+------------------+-----------------------+
        | 'E' | None             |                  42.0 |
        +-----+------------------+-----------------------+

    Missing values are overridden by non-missing values. Conflicting values are
    reported as an instance of the Conflict class (sub-class of frozenset).

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data are sorted, see also the discussion of the
    `buffersize`, `tempdir` and `cache` arguments under the
    :func:`petl.transform.sorts.sort` function.

    See also :func:`petl.transform.dedup.conflicts`.

    """

    return MergeDuplicatesView(table, key, missing=missing, presorted=presorted,
                               buffersize=buffersize, tempdir=tempdir,
                               cache=cache)


Table.mergeduplicates = mergeduplicates


class MergeDuplicatesView(Table):

    def __init__(self, table, key, missing=None, presorted=False,
                 buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.table = table
        else:
            self.table = sort(table, key, buffersize=buffersize,
                              tempdir=tempdir, cache=cache)
        self.key = key
        self.missing = missing

    def __iter__(self):
        return itermergeduplicates(self.table, self.key, self.missing)


def itermergeduplicates(table, key, missing):
    it = iter(table)
    hdr, it = iterpeek(it)
    flds = list(map(text_type, hdr))

    # determine output fields
    if isinstance(key, string_types):
        outhdr = [key]
        keyflds = {key}
    else:
        outhdr = list(key)
        keyflds = set(key)
    valflds = [f for f in flds if f not in keyflds]
    valfldidxs = [flds.index(f) for f in valflds]
    outhdr.extend(valflds)
    yield tuple(outhdr)

    # do the work
    for k, grp in rowgroupby(it, key):
        grp = list(grp)
        if isinstance(key, string_types):
            outrow = [k]
        else:
            outrow = list(k)
        mergedvals = [set(row[i] for row in grp
                          if len(row) > i and row[i] != missing)
                      for i in valfldidxs]
        normedvals = [vals.pop() if len(vals) == 1
                      else missing if len(vals) == 0
                      else Conflict(vals)
                      for vals in mergedvals]
        outrow.extend(normedvals)
        yield tuple(outrow)


def merge(*tables, **kwargs):
    """
    Convenience function to combine multiple tables (via
    :func:`petl.transform.sorts.mergesort`) then combine duplicate rows by
    merging under the given key (via
    :func:`petl.transform.reductions.mergeduplicates`). E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           [1, 'A', True],
        ...           [2, 'B', None],
        ...           [4, 'C', True]]
        >>> table2 = [['bar', 'baz', 'quux'],
        ...           ['A', True, 42.0],
        ...           ['B', False, 79.3],
        ...           ['C', False, 12.4]]
        >>> table3 = etl.merge(table1, table2, key='bar')
        >>> table3
        +-----+-----+-------------------------+------+
        | bar | foo | baz                     | quux |
        +=====+=====+=========================+======+
        | 'A' |   1 | True                    | 42.0 |
        +-----+-----+-------------------------+------+
        | 'B' |   2 | False                   | 79.3 |
        +-----+-----+-------------------------+------+
        | 'C' |   4 | Conflict({False, True}) | 12.4 |
        +-----+-----+-------------------------+------+

    Keyword arguments are the same as for
    :func:`petl.transform.sorts.mergesort`, except `key` is required.

    """

    assert 'key' in kwargs, 'keyword argument "key" is required'
    key = kwargs['key']
    t1 = mergesort(*tables, **kwargs)
    t2 = mergeduplicates(t1, key=key, presorted=True)
    return t2


Table.merge = merge


class Conflict(frozenset):

    def __new__(cls, items):
        s = super(Conflict, cls).__new__(cls, items)
        return s


def fold(table, key, f, value=None, presorted=False, buffersize=None,
         tempdir=None, cache=True):
    """
    Reduce rows recursively via the Python standard :func:`reduce` function.
    E.g.::

        >>> import petl as etl
        >>> table1 = [['id', 'count'],
        ...           [1, 3],
        ...           [1, 5],
        ...           [2, 4],
        ...           [2, 8]]
        >>> import operator
        >>> table2 = etl.fold(table1, 'id', operator.add, 'count',
        ...                   presorted=True)
        >>> table2
        +-----+-------+
        | key | value |
        +=====+=======+
        |   1 |     8 |
        +-----+-------+
        |   2 |    12 |
        +-----+-------+

    See also :func:`petl.transform.reductions.aggregate`,
    :func:`petl.transform.reductions.rowreduce`.

    """

    return FoldView(table, key, f, value=value, presorted=presorted,
                    buffersize=buffersize, tempdir=tempdir, cache=cache)


Table.fold = fold


class FoldView(Table):

    def __init__(self, table, key, f, value=None, presorted=False,
                 buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.table = table
        else:
            self.table = sort(table, key, buffersize=buffersize,
                              tempdir=tempdir, cache=cache)
        self.key = key
        self.f = f
        self.value = value

    def __iter__(self):
        return iterfold(self.table, self.key, self.f, self.value)


def iterfold(table, key, f, value):
    yield ('key', 'value')
    for k, grp in rowgroupby(table, key, value):
        yield k, reduce(f, grp)
