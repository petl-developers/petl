from __future__ import absolute_import, print_function, division


from petl.compat import Counter, next
import logging
logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


from petl.comparison import Comparable
from petl.util.base import header, Table
from petl.transform.sorts import sort
from petl.transform.basics import cut


def complement(a, b, presorted=False, buffersize=None, tempdir=None,
               cache=True):
    """
    Return rows in `a` that are not in `b`. E.g.::

        >>> import petl as etl
        >>> a = [['foo', 'bar', 'baz'],
        ...      ['A', 1, True],
        ...      ['C', 7, False],
        ...      ['B', 2, False],
        ...      ['C', 9, True]]
        >>> b = [['x', 'y', 'z'],
        ...      ['B', 2, False],
        ...      ['A', 9, False],
        ...      ['B', 3, True],
        ...      ['C', 9, True]]
        >>> aminusb = etl.complement(a, b)
        >>> aminusb
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'A' |   1 | True  |
        +-----+-----+-------+
        | 'C' |   7 | False |
        +-----+-----+-------+

        >>> bminusa = etl.complement(b, a)
        >>> bminusa
        +-----+---+-------+
        | x   | y | z     |
        +=====+===+=======+
        | 'A' | 9 | False |
        +-----+---+-------+
        | 'B' | 3 | True  |
        +-----+---+-------+

    Note that the field names of each table are ignored - rows are simply
    compared following a lexical sort. See also the
    :func:`petl.transform.setops.recordcomplement` function.

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data are sorted, see also the discussion of the
    `buffersize`, `tempdir` and `cache` arguments under the
    :func:`petl.transform.sorts.sort` function.

    """

    return ComplementView(a, b, presorted=presorted, buffersize=buffersize,
                          tempdir=tempdir, cache=cache)


Table.complement = complement


class ComplementView(Table):

    def __init__(self, a, b, presorted=False, buffersize=None, tempdir=None,
                 cache=True):
        if presorted:
            self.a = a
            self.b = b
        else:
            self.a = sort(a, buffersize=buffersize, tempdir=tempdir,
                          cache=cache)
            self.b = sort(b, buffersize=buffersize, tempdir=tempdir,
                          cache=cache)

    def __iter__(self):
        return itercomplement(self.a, self.b)


def itercomplement(ta, tb):
    # coerce rows to tuples to ensure hashable and comparable
    ita = (tuple(row) for row in iter(ta))
    itb = (tuple(row) for row in iter(tb))
    aflds = tuple(str(f) for f in next(ita))
    next(itb)  # ignore b fields
    yield aflds

    try:
        a = next(ita)
    except StopIteration:
        debug('a is empty, nothing to yield')
        pass
    else:
        try:
            b = next(itb)
        except StopIteration:
            debug('b is empty, just iterate through a')
            yield a
            for row in ita:
                yield row
        else:
            # we want the elements in a that are not in b
            while True:
                debug('current rows: %r %r', a, b)
                if b is None or Comparable(a) < Comparable(b):
                    yield a
                    debug('advance a')
                    try:
                        a = next(ita)
                    except StopIteration:
                        break
                elif a == b:
                    debug('advance both')
                    try:
                        a = next(ita)
                    except StopIteration:
                        break
                    try:
                        b = next(itb)
                    except StopIteration:
                        b = None
                else:
                    debug('advance b')
                    try:
                        b = next(itb)
                    except StopIteration:
                        b = None


def recordcomplement(a, b, buffersize=None, tempdir=None, cache=True):
    """
    Find records in `a` that are not in `b`. E.g.::

        >>> import petl as etl
        >>> a = [['foo', 'bar', 'baz'],
        ...      ['A', 1, True],
        ...      ['C', 7, False],
        ...      ['B', 2, False],
        ...      ['C', 9, True]]
        >>> b = [['bar', 'foo', 'baz'],
        ...      [2, 'B', False],
        ...      [9, 'A', False],
        ...      [3, 'B', True],
        ...      [9, 'C', True]]
        >>> aminusb = etl.recordcomplement(a, b)
        >>> aminusb
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'A' |   1 | True  |
        +-----+-----+-------+
        | 'C' |   7 | False |
        +-----+-----+-------+

        >>> bminusa = etl.recordcomplement(b, a)
        >>> bminusa
        +-----+-----+-------+
        | bar | foo | baz   |
        +=====+=====+=======+
        |   3 | 'B' | True  |
        +-----+-----+-------+
        |   9 | 'A' | False |
        +-----+-----+-------+

    Note that both tables must have the same set of fields, but that the order
    of the fields does not matter. See also the
    :func:`petl.transform.setops.complement` function.

    See also the discussion of the `buffersize`, `tempdir` and `cache` arguments
    under the :func:`petl.transform.sorts.sort` function.

    """

    # TODO possible with only one pass?

    ha = header(a)
    hb = header(b)
    assert set(ha) == set(hb), 'both tables must have the same set of fields'
    # make sure fields are in the same order
    bv = cut(b, *ha)
    return complement(a, bv, buffersize=buffersize, tempdir=tempdir,
                      cache=cache)


Table.recordcomplement = recordcomplement


def diff(a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Find the difference between rows in two tables. Returns a pair of tables.
    E.g.::

        >>> import petl as etl
        >>> a = [['foo', 'bar', 'baz'],
        ...      ['A', 1, True],
        ...      ['C', 7, False],
        ...      ['B', 2, False],
        ...      ['C', 9, True]]
        >>> b = [['x', 'y', 'z'],
        ...      ['B', 2, False],
        ...      ['A', 9, False],
        ...      ['B', 3, True],
        ...      ['C', 9, True]]
        >>> added, subtracted = etl.diff(a, b)
        >>> # rows in b not in a
        ... added
        +-----+---+-------+
        | x   | y | z     |
        +=====+===+=======+
        | 'A' | 9 | False |
        +-----+---+-------+
        | 'B' | 3 | True  |
        +-----+---+-------+

        >>> # rows in a not in b
        ... subtracted
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'A' |   1 | True  |
        +-----+-----+-------+
        | 'C' |   7 | False |
        +-----+-----+-------+

    Convenient shorthand for ``(complement(b, a), complement(a, b))``. See also
    :func:`petl.transform.setops.complement`.

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data are sorted, see also the discussion of the
    `buffersize`, `tempdir` and `cache` arguments under the
    :func:`petl.transform.sorts.sort` function.

    """

    if not presorted:
        a = sort(a)
        b = sort(b)
    added = complement(b, a, presorted=True, buffersize=buffersize,
                       tempdir=tempdir, cache=cache)
    subtracted = complement(a, b, presorted=True, buffersize=buffersize,
                            tempdir=tempdir, cache=cache)
    return added, subtracted


Table.diff = diff


def recorddiff(a, b, buffersize=None, tempdir=None, cache=True):
    """
    Find the difference between records in two tables. E.g.::

        >>> import petl as etl
        >>> a = [['foo', 'bar', 'baz'],
        ...      ['A', 1, True],
        ...      ['C', 7, False],
        ...      ['B', 2, False],
        ...      ['C', 9, True]]
        >>> b = [['bar', 'foo', 'baz'],
        ...      [2, 'B', False],
        ...      [9, 'A', False],
        ...      [3, 'B', True],
        ...      [9, 'C', True]]
        >>> added, subtracted = etl.recorddiff(a, b)
        >>> added
        +-----+-----+-------+
        | bar | foo | baz   |
        +=====+=====+=======+
        |   3 | 'B' | True  |
        +-----+-----+-------+
        |   9 | 'A' | False |
        +-----+-----+-------+

        >>> subtracted
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'A' |   1 | True  |
        +-----+-----+-------+
        | 'C' |   7 | False |
        +-----+-----+-------+

    Convenient shorthand for
    ``(recordcomplement(b, a), recordcomplement(a, b))``. See also
    :func:`petl.transform.setops.recordcomplement`.

    See also the discussion of the `buffersize`, `tempdir` and `cache`
    arguments under the :func:`petl.transform.sorts.sort` function.

    """

    added = recordcomplement(b, a, buffersize=buffersize, tempdir=tempdir,
                             cache=cache)
    subtracted = recordcomplement(a, b, buffersize=buffersize, tempdir=tempdir,
                                  cache=cache)
    return added, subtracted


Table.recorddiff = recorddiff


def intersection(a, b, presorted=False, buffersize=None, tempdir=None,
                 cache=True):
    """
    Return rows in `a` that are also in `b`. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, True],
        ...           ['C', 7, False],
        ...           ['B', 2, False],
        ...           ['C', 9, True]]
        >>> table2 = [['x', 'y', 'z'],
        ...           ['B', 2, False],
        ...           ['A', 9, False],
        ...           ['B', 3, True],
        ...           ['C', 9, True]]
        >>> table3 = etl.intersection(table1, table2)
        >>> table3
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'B' |   2 | False |
        +-----+-----+-------+
        | 'C' |   9 | True  |
        +-----+-----+-------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data are sorted, see also the discussion of the
    `buffersize`, `tempdir` and `cache` arguments under the
    :func:`petl.transform.sorts.sort` function.

    """

    return IntersectionView(a, b, presorted=presorted, buffersize=buffersize,
                            tempdir=tempdir, cache=cache)


Table.intersection = intersection


class IntersectionView(Table):

    def __init__(self, a, b, presorted=False, buffersize=None, tempdir=None,
                 cache=True):
        if presorted:
            self.a = a
            self.b = b
        else:
            self.a = sort(a, buffersize=buffersize, tempdir=tempdir,
                          cache=cache)
            self.b = sort(b, buffersize=buffersize, tempdir=tempdir,
                          cache=cache)

    def __iter__(self):
        return iterintersection(self.a, self.b)


def iterintersection(a, b):
    ita = iter(a)
    itb = iter(b)
    ahdr = next(ita)
    next(itb)  # ignore b header
    yield tuple(ahdr)
    try:
        a = tuple(next(ita))
        b = tuple(next(itb))
        while True:
            if Comparable(a) < Comparable(b):
                a = tuple(next(ita))
            elif a == b:
                yield a
                a = tuple(next(ita))
                b = tuple(next(itb))
            else:
                b = tuple(next(itb))
    except StopIteration:
        pass


def hashcomplement(a, b):
    """
    Alternative implementation of :func:`petl.transform.setops.complement`,
    where the complement is executed by constructing an in-memory set for all
    rows found in the right hand table, then iterating over rows from the
    left hand table.

    May be faster and/or more resource efficient where the right table is small
    and the left table is large.

    """

    return HashComplementView(a, b)


Table.hashcomplement = hashcomplement


class HashComplementView(Table):

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __iter__(self):
        return iterhashcomplement(self.a, self.b)


def iterhashcomplement(a, b):
    ita = iter(a)
    ahdr = next(ita)
    yield tuple(ahdr)
    itb = iter(b)
    next(itb)  # discard b header, assume same as a

    # N.B., need to account for possibility of duplicate rows
    bcnt = Counter(tuple(row) for row in itb)
    for ar in ita:
        t = tuple(ar)
        if bcnt[t] > 0:
            bcnt[t] -= 1
        else:
            yield t


def hashintersection(a, b):
    """
    Alternative implementation of
    :func:`petl.transform.setops.intersection`, where the intersection
    is executed by constructing an in-memory set for all rows found in the
    right hand table, then iterating over rows from the left hand table.

    May be faster and/or more resource efficient where the right table is small
    and the left table is large.

    """

    return HashIntersectionView(a, b)


Table.hashintersection = hashintersection


class HashIntersectionView(Table):

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __iter__(self):
        return iterhashintersection(self.a, self.b)


def iterhashintersection(a, b):
    ita = iter(a)
    ahdr = next(ita)
    yield tuple(ahdr)
    itb = iter(b)
    next(itb)  # discard b header, assume same as a

    # N.B., need to account for possibility of duplicate rows
    bcnt = Counter(tuple(row) for row in itb)
    for ar in ita:
        t = tuple(ar)
        if bcnt[t] > 0:
            yield t
            bcnt[t] -= 1
