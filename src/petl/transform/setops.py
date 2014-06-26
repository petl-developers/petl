__author__ = 'aliman'


from petl.compat import Counter
from petl.util import header, RowContainer, SortableItem
from petl.transform.sorts import sort
from petl.transform.basics import cut


import logging
logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


def complement(a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Return rows in `a` that are not in `b`. E.g.::

        >>> from petl import complement, look
        >>> look(a)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+

        >>> look(b)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'B' | 2   | False |
        +-----+-----+-------+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+
        | 'C' | 9   | True  |
        +-----+-----+-------+

        >>> aminusb = complement(a, b)
        >>> look(aminusb)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+

        >>> bminusa = complement(b, a)
        >>> look(bminusa)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+

    Note that the field names of each table are ignored - rows are simply compared
    following a lexical sort. See also the :func:`recordcomplement` function.

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the
    :func:`sort` function.

    """

    return ComplementView(a, b, presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)


class ComplementView(RowContainer):

    def __init__(self, a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.a = a
            self.b = b
        else:
            self.a = sort(a, buffersize=buffersize, tempdir=tempdir, cache=cache)
            self.b = sort(b, buffersize=buffersize, tempdir=tempdir, cache=cache)

    def __iter__(self):
        return itercomplement(self.a, self.b)


def itercomplement(ta, tb):
    # coerce rows to tuples to ensure hashable and comparable
    ita = (tuple(row) for row in iter(ta))
    itb = (tuple(row) for row in iter(tb))
    aflds = tuple(str(f) for f in ita.next())
    itb.next() # ignore b fields
    yield aflds

    try:
        a = ita.next()
    except StopIteration:
        debug('a is empty, nothing to yield')
        pass
    else:
        try:
            b = itb.next()
        except StopIteration:
            debug('b is empty, just iterate through a')
            yield a
            for row in ita:
                yield row
        else:
            # we want the elements in a that are not in b
            while True:
                debug('current rows: %r %r', a, b)
                if b is None or SortableItem(a) < SortableItem(b):
                    yield a
                    debug('advance a')
                    try:
                        a = ita.next()
                    except StopIteration:
                        break
                elif a == b:
                    debug('advance both')
                    try:
                        a = ita.next()
                    except StopIteration:
                        break
                    try:
                        b = itb.next()
                    except StopIteration:
                        b = None
                else:
                    debug('advance b')
                    try:
                        b = itb.next()
                    except StopIteration:
                        b = None


def recordcomplement(a, b, buffersize=None, tempdir=None, cache=True):
    """
    Find records in `a` that are not in `b`. E.g.::

        >>> from petl import recordcomplement, look
        >>> look(a)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+

        >>> look(b)
        +-------+-------+-------+
        | 'bar' | 'foo' | 'baz' |
        +=======+=======+=======+
        | 2     | 'B'   | False |
        +-------+-------+-------+
        | 9     | 'A'   | False |
        +-------+-------+-------+
        | 3     | 'B'   | True  |
        +-------+-------+-------+
        | 9     | 'C'   | True  |
        +-------+-------+-------+

        >>> aminusb = recordcomplement(a, b)
        >>> look(aminusb)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+

        >>> bminusa = recordcomplement(b, a)
        >>> look(bminusa)
        +-------+-------+-------+
        | 'bar' | 'foo' | 'baz' |
        +=======+=======+=======+
        | 3     | 'B'   | True  |
        +-------+-------+-------+
        | 9     | 'A'   | False |
        +-------+-------+-------+

    Note that both tables must have the same set of fields, but that the order
    of the fields does not matter. See also the :func:`complement` function.

    See also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the :func:`sort`
    function.

    .. versionadded:: 0.3

    """

    ha = header(a)
    hb = header(b)
    assert set(ha) == set(hb), 'both tables must have the same set of fields'
    # make sure fields are in the same order
    bv = cut(b, *ha)
    return complement(a, bv, buffersize=buffersize, tempdir=tempdir, cache=cache)


def diff(a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Find the difference between rows in two tables. Returns a pair of tables.
    E.g.::

        >>> from petl import diff, look
        >>> look(a)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+

        >>> look(b)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'B' | 2   | False |
        +-----+-----+-------+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+
        | 'C' | 9   | True  |
        +-----+-----+-------+

        >>> added, subtracted = diff(a, b)
        >>> # rows in b not in a
        ... look(added)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+

        >>> # rows in a not in b
        ... look(subtracted)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+

    Convenient shorthand for ``(complement(b, a), complement(a, b))``. See also
    :func:`complement`.

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the
    :func:`sort` function.

    """

    if not presorted:
        a = sort(a)
        b = sort(b)
    added = complement(b, a, presorted=True, buffersize=buffersize, tempdir=tempdir, cache=cache)
    subtracted = complement(a, b, presorted=True, buffersize=buffersize, tempdir=tempdir, cache=cache)
    return added, subtracted


def recorddiff(a, b, buffersize=None, tempdir=None, cache=True):
    """
    Find the difference between records in two tables. E.g.::

        >>> from petl import recorddiff, look
        >>> look(a)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+

        >>> look(b)
        +-------+-------+-------+
        | 'bar' | 'foo' | 'baz' |
        +=======+=======+=======+
        | 2     | 'B'   | False |
        +-------+-------+-------+
        | 9     | 'A'   | False |
        +-------+-------+-------+
        | 3     | 'B'   | True  |
        +-------+-------+-------+
        | 9     | 'C'   | True  |
        +-------+-------+-------+

        >>> added, subtracted = recorddiff(a, b)
        >>> look(added)
        +-------+-------+-------+
        | 'bar' | 'foo' | 'baz' |
        +=======+=======+=======+
        | 3     | 'B'   | True  |
        +-------+-------+-------+
        | 9     | 'A'   | False |
        +-------+-------+-------+

        >>> look(subtracted)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+

    Convenient shorthand for ``(recordcomplement(b, a), recordcomplement(a, b))``.
    See also :func:`recordcomplement`.

    See also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the :func:`sort`
    function.

    .. versionadded:: 0.3

    """

    added = recordcomplement(b, a, buffersize=buffersize, tempdir=tempdir, cache=cache)
    subtracted = recordcomplement(a, b, buffersize=buffersize, tempdir=tempdir, cache=cache)
    return added, subtracted


def intersection(a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Return rows in `a` that are also in `b`. E.g.::

        >>> from petl import intersection, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+

        >>> look(table2)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'B' | 2   | False |
        +-----+-----+-------+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+
        | 'C' | 9   | True  |
        +-----+-----+-------+

        >>> table3 = intersection(table1, table2)
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the
    :func:`sort` function.

    """

    return IntersectionView(a, b, presorted, buffersize)


class IntersectionView(RowContainer):

    def __init__(self, a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.a = a
            self.b = b
        else:
            self.a = sort(a, buffersize=buffersize, tempdir=tempdir, cache=cache)
            self.b = sort(b, buffersize=buffersize, tempdir=tempdir, cache=cache)

    def __iter__(self):
        return iterintersection(self.a, self.b)


def iterintersection(a, b):
    ita = iter(a)
    itb = iter(b)
    aflds = ita.next()
    itb.next() # ignore b fields
    yield tuple(aflds)
    try:
        a = tuple(ita.next())
        b = tuple(itb.next())
        while True:
            if a < b:
                a = tuple(ita.next())
            elif a == b:
                yield a
                a = tuple(ita.next())
                b = tuple(itb.next())
            else:
                b = tuple(itb.next())
    except StopIteration:
        pass


def hashcomplement(a, b):
    """
    Alternative implementation of :func:`complement`, where the complement is executed
    by constructing an in-memory set for all rows found in the right hand table, then
    iterating over rows from the left hand table.

    May be faster and/or more resource efficient where the right table is small
    and the left table is large.

    .. versionadded:: 0.5

    """

    return HashComplementView(a, b)


class HashComplementView(RowContainer):

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __iter__(self):
        return iterhashcomplement(self.a, self.b)


def iterhashcomplement(a, b):
    ita = iter(a)
    aflds = ita.next()
    yield tuple(aflds)
    itb = iter(b)
    itb.next() # discard b fields, assume they are the same

    # n.b., need to account for possibility of duplicate rows
    bcnt = Counter(tuple(row) for row in itb)
    for ar in ita:
        t = tuple(ar)
        if bcnt[t] > 0:
            bcnt[t] -= 1
        else:
            yield t


def hashintersection(a, b):
    """
    Alternative implementation of :func:`intersection`, where the intersection is executed
    by constructing an in-memory set for all rows found in the right hand table, then
    iterating over rows from the left hand table.

    May be faster and/or more resource efficient where the right table is small
    and the left table is large.

    .. versionadded:: 0.5

    """

    return HashIntersectionView(a, b)


class HashIntersectionView(RowContainer):

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __iter__(self):
        return iterhashintersection(self.a, self.b)


def iterhashintersection(a, b):
    ita = iter(a)
    aflds = ita.next()
    yield tuple(aflds)
    itb = iter(b)
    itb.next() # discard b fields, assume they are the same

    # n.b., need to account for possibility of duplicate rows
    bcnt = Counter(tuple(row) for row in itb)
    for ar in ita:
        t = tuple(ar)
        if bcnt[t] > 0:
            yield t
            bcnt[t] -= 1


