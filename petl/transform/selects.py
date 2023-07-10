from __future__ import absolute_import, print_function, division


import operator
from petl.compat import next, string_types, callable, text_type
from petl.comparison import Comparable


from petl.errors import ArgumentError
from petl.util.base import asindices, expr, Table, values, Record


def select(table, *args, **kwargs):
    """
    Select rows meeting a condition. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 4, 9.3],
        ...           ['a', 2, 88.2],
        ...           ['b', 1, 23.3],
        ...           ['c', 8, 42.0],
        ...           ['d', 7, 100.9],
        ...           ['c', 2]]
        >>> # the second positional argument can be a function accepting
        ... # a row
        ... table2 = etl.select(table1,
        ...                     lambda rec: rec.foo == 'a' and rec.baz > 88.1)
        >>> table2
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        | 'a' |   2 | 88.2 |
        +-----+-----+------+

        >>> # the second positional argument can also be an expression
        ... # string, which will be converted to a function using petl.expr()
        ... table3 = etl.select(table1, "{foo} == 'a' and {baz} > 88.1")
        >>> table3
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        | 'a' |   2 | 88.2 |
        +-----+-----+------+

        >>> # the condition can also be applied to a single field
        ... table4 = etl.select(table1, 'foo', lambda v: v == 'a')
        >>> table4
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        | 'a' |   4 |  9.3 |
        +-----+-----+------+
        | 'a' |   2 | 88.2 |
        +-----+-----+------+

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    missing = kwargs.get('missing', None)
    complement = kwargs.get('complement', False)

    if len(args) == 0:
        raise ArgumentError('missing positional argument')
    elif len(args) == 1:
        where = args[0]
        if isinstance(where, string_types):
            where = expr(where)
        else:
            assert callable(where), 'second argument must be string or callable'
        return RowSelectView(table, where, missing=missing,
                             complement=complement)
    else:
        field = args[0]
        where = args[1]
        assert callable(where), 'third argument must be callable'
        return FieldSelectView(table, field, where, complement=complement,
                               missing=missing)


Table.select = select


class RowSelectView(Table):

    def __init__(self, source, where, missing=None, complement=False):
        self.source = source
        self.where = where
        self.missing = missing
        self.complement = complement

    def __iter__(self):
        return iterrowselect(self.source, self.where, self.missing,
                             self.complement)


class FieldSelectView(Table):

    def __init__(self, source, field, where, complement=False, missing=None):
        self.source = source
        self.field = field
        self.where = where
        self.complement = complement
        self.missing = missing

    def __iter__(self):
        return iterfieldselect(self.source, self.field, self.where,
                               self.complement, self.missing)


def iterfieldselect(source, field, where, complement, missing):
    it = iter(source)
    try:
        hdr = next(it)
        yield tuple(hdr)
    except StopIteration:
        hdr = []  # will raise FieldSelectionError below
    indices = asindices(hdr, field)
    getv = operator.itemgetter(*indices)
    for row in it:
        try:
            v = getv(row)
        except IndexError:
            v = missing
        if bool(where(v)) != complement:  # XOR
            yield tuple(row)


def iterrowselect(source, where, missing, complement):
    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        return  # will yield nothing
    flds = list(map(text_type, hdr))
    yield tuple(hdr)
    it = (Record(row, flds, missing=missing) for row in it)
    for row in it:
        if bool(where(row)) != complement:  # XOR
            yield tuple(row)  # need to convert back to tuple?


def rowlenselect(table, n, complement=False):
    """Select rows of length `n`."""

    where = lambda row: len(row) == n
    return select(table, where, complement=complement)


Table.rowlenselect = rowlenselect


def selectop(table, field, value, op, complement=False):
    """Select rows where the function `op` applied to the given field and
    the given value returns `True`."""

    return select(table, field, lambda v: op(v, value),
                  complement=complement)


Table.selectop = selectop


def selecteq(table, field, value, complement=False):
    """Select rows where the given field equals the given value."""

    return selectop(table, field, value, operator.eq, complement=complement)


Table.selecteq = selecteq
Table.eq = selecteq


def selectne(table, field, value, complement=False):
    """Select rows where the given field does not equal the given value."""

    return selectop(table, field, value, operator.ne, complement=complement)


Table.selectne = selectne
Table.ne = selectne


def selectlt(table, field, value, complement=False):
    """Select rows where the given field is less than the given value."""

    value = Comparable(value)
    return selectop(table, field, value, operator.lt, complement=complement)


Table.selectlt = selectlt
Table.lt = selectlt


def selectle(table, field, value, complement=False):
    """Select rows where the given field is less than or equal to the given
    value."""

    value = Comparable(value)
    return selectop(table, field, value, operator.le, complement=complement)


Table.selectle = selectle
Table.le = selectle


def selectgt(table, field, value, complement=False):
    """Select rows where the given field is greater than the given value."""

    value = Comparable(value)
    return selectop(table, field, value, operator.gt, complement=complement)


Table.selectgt = selectgt
Table.gt = selectgt


def selectge(table, field, value, complement=False):
    """Select rows where the given field is greater than or equal to the given
    value."""

    value = Comparable(value)
    return selectop(table, field, value, operator.ge, complement=complement)


Table.selectge = selectge
Table.ge = selectge


def selectcontains(table, field, value, complement=False):
    """Select rows where the given field contains the given value."""

    return selectop(table, field, value, operator.contains,
                    complement=complement)


Table.selectcontains = selectcontains


def selectin(table, field, value, complement=False):
    """Select rows where the given field is a member of the given value."""

    return select(table, field, lambda v: v in value,
                  complement=complement)


Table.selectin = selectin


def selectnotin(table, field, value, complement=False):
    """Select rows where the given field is not a member of the given value."""

    return select(table, field, lambda v: v not in value,
                  complement=complement)


Table.selectnotin = selectnotin


def selectis(table, field, value, complement=False):
    """Select rows where the given field `is` the given value."""

    return selectop(table, field, value, operator.is_, complement=complement)


Table.selectis = selectis


def selectisnot(table, field, value, complement=False):
    """Select rows where the given field `is not` the given value."""

    return selectop(table, field, value, operator.is_not, complement=complement)


Table.selectisnot = selectisnot


def selectisinstance(table, field, value, complement=False):
    """Select rows where the given field is an instance of the given type."""

    return selectop(table, field, value, isinstance, complement=complement)


Table.selectisinstance = selectisinstance


def selectrangeopenleft(table, field, minv, maxv, complement=False):
    """Select rows where the given field is greater than or equal to `minv` and
    less than `maxv`."""

    minv = Comparable(minv)
    maxv = Comparable(maxv)
    return select(table, field, lambda v: minv <= v < maxv,
                  complement=complement)


Table.selectrangeopenleft = selectrangeopenleft


def selectrangeopenright(table, field, minv, maxv, complement=False):
    """Select rows where the given field is greater than `minv` and
    less than or equal to `maxv`."""

    minv = Comparable(minv)
    maxv = Comparable(maxv)
    return select(table, field, lambda v: minv < v <= maxv,
                  complement=complement)


Table.selectrangeopenright = selectrangeopenright


def selectrangeopen(table, field, minv, maxv, complement=False):
    """Select rows where the given field is greater than or equal to `minv` and
    less than or equal to `maxv`."""

    minv = Comparable(minv)
    maxv = Comparable(maxv)
    return select(table, field, lambda v: minv <= v <= maxv,
                  complement=complement)


Table.selectrangeopen = selectrangeopen


def selectrangeclosed(table, field, minv, maxv, complement=False):
    """Select rows where the given field is greater than `minv` and
    less than `maxv`."""

    minv = Comparable(minv)
    maxv = Comparable(maxv)
    return select(table, field, lambda v: minv < Comparable(v) < maxv,
                  complement=complement)


Table.selectrangeclosed = selectrangeclosed


def selecttrue(table, field, complement=False):
    """Select rows where the given field evaluates `True`."""

    return select(table, field, lambda v: bool(v), complement=complement)


Table.selecttrue = selecttrue
Table.true = selecttrue


def selectfalse(table, field, complement=False):
    """Select rows where the given field evaluates `False`."""

    return select(table, field, lambda v: not bool(v),
                  complement=complement)


Table.selectfalse = selectfalse
Table.false = selectfalse


def selectnone(table, field, complement=False):
    """Select rows where the given field is `None`."""

    return select(table, field, lambda v: v is None, complement=complement)


Table.selectnone = selectnone
Table.none = selectnone


def selectnotnone(table, field, complement=False):
    """Select rows where the given field is not `None`."""

    return select(table, field, lambda v: v is not None,
                  complement=complement)


Table.selectnotnone = selectnotnone
Table.notnone = selectnotnone


def selectusingcontext(table, query):
    """
    Select rows based on data in the current row and/or previous and
    next row. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['A', 1],
        ...           ['B', 4],
        ...           ['C', 5],
        ...           ['D', 9]]
        >>> def query(prv, cur, nxt):
        ...     return ((prv is not None and (cur.bar - prv.bar) < 2)
        ...             or (nxt is not None and (nxt.bar - cur.bar) < 2))
        ...
        >>> table2 = etl.selectusingcontext(table1, query)
        >>> table2
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'B' |   4 |
        +-----+-----+
        | 'C' |   5 |
        +-----+-----+

    The `query` function should accept three rows and return a boolean value.

    """

    return SelectUsingContextView(table, query)


Table.selectusingcontext = selectusingcontext


class SelectUsingContextView(Table):

    def __init__(self, table, query):
        self.table = table
        self.query = query

    def __iter__(self):
        return iterselectusingcontext(self.table, self.query)


def iterselectusingcontext(table, query):
    it = iter(table)
    try:
        hdr = tuple(next(it))
    except StopIteration:
        return  # will yield nothing
    flds = list(map(text_type, hdr))
    yield hdr
    it = (Record(row, flds) for row in it)
    prv = None
    cur = next(it)
    for nxt in it:
        if query(prv, cur, nxt):
            yield cur
        prv = cur
        cur = nxt
    # handle last row
    if query(prv, cur, None):
        yield cur


def facet(table, key):
    """
    Return a dictionary mapping field values to tables. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 4, 9.3],
        ...           ['a', 2, 88.2],
        ...           ['b', 1, 23.3],
        ...           ['c', 8, 42.0],
        ...           ['d', 7, 100.9],
        ...           ['c', 2]]
        >>> foo = etl.facet(table1, 'foo')
        >>> sorted(foo.keys())
        ['a', 'b', 'c', 'd']
        >>> foo['a']
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        | 'a' |   4 |  9.3 |
        +-----+-----+------+
        | 'a' |   2 | 88.2 |
        +-----+-----+------+

        >>> foo['c']
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        | 'c' |   8 | 42.0 |
        +-----+-----+------+
        | 'c' |   2 |      |
        +-----+-----+------+

        >>> # works with compound keys too
        >>> table2 = [['foo', 'bar', 'baz'],
        ...           ['a', 1, True],
        ...           ['b', 2, False],
        ...           ['b', 3, True],
        ...           ['b', 3, False]]
        >>> foobar = etl.facet(table2, ('foo', 'bar'))

        >>> sorted(foobar.keys())
        [('a', 1), ('b', 2), ('b', 3)]

        >>> foobar[('b', 3)]
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'b' |   3 | True  |
        +-----+-----+-------+
        | 'b' |   3 | False |
        +-----+-----+-------+

    See also :func:`petl.util.materialise.facetcolumns`.

    """

    fct = dict()
    for v in set(values(table, key)):
        fct[v] = selecteq(table, key, v)
    return fct


Table.facet = facet


def biselect(table, *args, **kwargs):
    """Return two tables, the first containing selected rows, the second
    containing remaining rows. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 4, 9.3],
        ...           ['a', 2, 88.2],
        ...           ['b', 1, 23.3],
        ...           ['c', 8, 42.0],
        ...           ['d', 7, 100.9],
        ...           ['c', 2]]
        >>> table2, table3 = etl.biselect(table1, lambda rec: rec.foo == 'a')
        >>> table2
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        | 'a' |   4 |  9.3 |
        +-----+-----+------+
        | 'a' |   2 | 88.2 |
        +-----+-----+------+
        >>> table3
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'b' |   1 |  23.3 |
        +-----+-----+-------+
        | 'c' |   8 |  42.0 |
        +-----+-----+-------+
        | 'd' |   7 | 100.9 |
        +-----+-----+-------+
        | 'c' |   2 |       |
        +-----+-----+-------+

    .. versionadded:: 1.1.0

    """

    # override complement kwarg
    kwargs['complement'] = False
    t1 = select(table, *args, **kwargs)
    kwargs['complement'] = True
    t2 = select(table, *args, **kwargs)
    return t1, t2


Table.biselect = biselect
