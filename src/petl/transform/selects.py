__author__ = 'aliman'


import operator
import re


from petl.compat import OrderedDict
from petl.util import asindices, expr, RowContainer, hybridrows, values, \
    itervalues, limits


def select(table, *args, **kwargs):
    """
    Select rows meeting a condition. E.g.::

        >>> from petl import select, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 4     | 9.3   |
        +-------+-------+-------+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+
        | 'b'   | 1     | 23.3  |
        +-------+-------+-------+
        | 'c'   | 8     | 42.0  |
        +-------+-------+-------+
        | 'd'   | 7     | 100.9 |
        +-------+-------+-------+
        | 'c'   | 2     |       |
        +-------+-------+-------+

        >>> # the second positional argument can be a function accepting a record
        ... table2 = select(table1, lambda rec: rec[0] == 'a' and rec[1] > 88.1)
        ... # table2 = select(table1, lambda rec: rec['foo'] == 'a' and rec['baz'] > 88.1)
        ... # table2 = select(table1, lambda rec: rec.foo == 'a' and rec.baz > 88.1)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+

        >>> # the second positional argument can also be an expression string, which
        ... # will be converted to a function using expr()
        ... table3 = select(table1, "{foo} == 'a' and {baz} > 88.1")
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+

        >>> # the condition can also be applied to a single field
        ... table4 = select(table1, 'foo', lambda v: v == 'a')
        >>> look(table4)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 4     | 9.3   |
        +-------+-------+-------+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    missing = kwargs.get('missing', None)
    complement = kwargs.get('complement', False)

    if len(args) == 0:
        raise Exception('missing positional argument')
    elif len(args) == 1:
        where = args[0]
        if isinstance(where, basestring):
            where = expr(where)
        else:
            assert callable(where), 'second argument must be string or callable'
        return RowSelectView(table, where, missing=missing, complement=complement)
    else:
        field = args[0]
        where = args[1]
        assert callable(where), 'third argument must be callable'
        return FieldSelectView(table, field, where, complement=complement)


def recordselect(table, where, missing=None, complement=False):
    """
    Select rows matching a condition. The `where` argument should be a function
    accepting a record (row as dictionary of values indexed by field name) as
    argument and returning True or False.

    .. deprecated:: 0.9

    Use :func:`select` instead.

    """

    return rowselect(table, where, missing=missing, complement=complement)


def rowselect(table, where, complement=False):
    """
    Select rows matching a condition. The `where` argument should be a function
    accepting a hybrid row object (supports accessing values either by
    position or by field name) as argument and returning True or False.

    .. deprecated:: 0.10

    Use :func:`select` instead, it supports the same signature.

    """

    return RowSelectView(table, where, complement=complement)


class RowSelectView(RowContainer):

    def __init__(self, source, where, missing=None, complement=False):
        self.source = source
        self.where = where
        self.missing = missing
        self.complement = complement

    def __iter__(self):
        return iterrowselect(self.source, self.where, self.missing, self.complement)


def iterrowselect(source, where, missing, complement):
    it = iter(source)
    flds = it.next()
    yield tuple(flds)
    for row in hybridrows(flds, it, missing): # convert to hybrid row/record
        if where(row) != complement: # XOR
            yield tuple(row) # need to convert back to tuple?


def rowlenselect(table, n, complement=False):
    """
    Select rows of length `n`.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    where = lambda row: len(row) == n
    return rowselect(table, where, complement=complement)


def fieldselect(table, field, where, complement=False):
    """
    Select rows matching a condition. The `where` argument should be a function
    accepting a single data value as argument and returning True or False.

    .. deprecated:: 0.10

    Use :func:`select` instead, it supports the same signature.

    """

    return FieldSelectView(table, field, where, complement=complement)


class FieldSelectView(RowContainer):

    def __init__(self, source, field, where, complement=False):
        self.source = source
        self.field = field
        self.where = where
        self.complement = complement

    def __iter__(self):
        return iterfieldselect(self.source, self.field, self.where, self.complement)


def iterfieldselect(source, field, where, complement):
    it = iter(source)
    flds = it.next()
    yield tuple(flds)
    indices = asindices(flds, field)
    getv = operator.itemgetter(*indices)
    for row in it:
        v = getv(row)
        if where(v) != complement: # XOR
            yield tuple(row)


def selectop(table, field, value, op, complement=False):
    """
    Select rows where the function `op` applied to the given field and the given
    value returns true.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return fieldselect(table, field, lambda v: op(v, value), complement=complement)


def selecteq(table, field, value, complement=False):
    """
    Select rows where the given field equals the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return selectop(table, field, value, operator.eq, complement=complement)


def selectne(table, field, value, complement=False):
    """
    Select rows where the given field does not equal the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return selectop(table, field, value, operator.ne, complement=complement)


def selectlt(table, field, value, complement=False):
    """
    Select rows where the given field is less than the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return selectop(table, field, value, operator.lt, complement=complement)


def selectle(table, field, value, complement=False):
    """
    Select rows where the given field is less than or equal to the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return selectop(table, field, value, operator.le, complement=complement)


def selectgt(table, field, value, complement=False):
    """
    Select rows where the given field is greater than the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return selectop(table, field, value, operator.gt, complement=complement)


def selectge(table, field, value, complement=False):
    """
    Select rows where the given field is greater than or equal to the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return selectop(table, field, value, operator.ge, complement=complement)


def selectcontains(table, field, value, complement=False):
    """
    Select rows where the given field contains the given value.

    .. versionadded:: 0.10

    """

    return selectop(table, field, value, operator.contains, complement=complement)


def selectin(table, field, value, complement=False):
    """
    Select rows where the given field is a member of the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return fieldselect(table, field, lambda v: v in value, complement=complement)


def selectnotin(table, field, value, complement=False):
    """
    Select rows where the given field is not a member of the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return fieldselect(table, field, lambda v: v not in value, complement=complement)


def selectis(table, field, value, complement=False):
    """
    Select rows where the given field `is` the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return selectop(table, field, value, operator.is_, complement=complement)


def selectisnot(table, field, value, complement=False):
    """
    Select rows where the given field `is not` the given value.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return selectop(table, field, value, operator.is_not, complement=complement)


def selectisinstance(table, field, value, complement=False):
    """
    Select rows where the given field is an instance of the given type.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return selectop(table, field, value, isinstance, complement=complement)


def selectrangeopenleft(table, field, minv, maxv, complement=False):
    """
    Select rows where the given field is greater than or equal to `minv` and
    less than `maxv`.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return fieldselect(table, field, lambda v: minv <= v < maxv, complement=complement)


def selectrangeopenright(table, field, minv, maxv, complement=False):
    """
    Select rows where the given field is greater than `minv` and
    less than or equal to `maxv`.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return fieldselect(table, field, lambda v: minv < v <= maxv, complement=complement)


def selectrangeopen(table, field, minv, maxv, complement=False):
    """
    Select rows where the given field is greater than or equal to `minv` and
    less than or equal to `maxv`.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return fieldselect(table, field, lambda v: minv <= v <= maxv, complement=complement)


def selectrangeclosed(table, field, minv, maxv, complement=False):
    """
    Select rows where the given field is greater than `minv` and
    less than `maxv`.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    return fieldselect(table, field, lambda v: minv < v < maxv, complement=complement)


def selectre(table, field, pattern, flags=0, complement=False):
    """
    Select rows where a regular expression search using the given pattern on the
    given field returns a match. E.g.::

        >>> from petl import selectre, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'aa'  | 4     | 9.3   |
        +-------+-------+-------+
        | 'aaa' | 2     | 88.2  |
        +-------+-------+-------+
        | 'b'   | 1     | 23.3  |
        +-------+-------+-------+
        | 'ccc' | 8     | 42.0  |
        +-------+-------+-------+
        | 'bb'  | 7     | 100.9 |
        +-------+-------+-------+
        | 'c'   | 2     |       |
        +-------+-------+-------+

        >>> table2 = selectre(table1, 'foo', '[ab]{2}')
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'aa'  | 4     | 9.3   |
        +-------+-------+-------+
        | 'aaa' | 2     | 88.2  |
        +-------+-------+-------+
        | 'bb'  | 7     | 100.9 |
        +-------+-------+-------+

    See also :func:`re.search`.

    .. versionchanged:: 0.4

    The complement of the selection can be returned (i.e., the query can be
    inverted) by providing `complement=True` as a keyword argument.

    """

    prog = re.compile(pattern, flags)
    test = lambda v: prog.search(v) is not None
    return fieldselect(table, field, test, complement=complement)


def selecttrue(table, field, complement=False):
    """
    Select rows where the given field equals True.

    """

    return fieldselect(table, field, lambda v: bool(v), complement=complement)


def selectfalse(table, field, complement=False):
    """
    Select rows where the given field equals False.

    """

    return fieldselect(table, field, lambda v: not bool(v), complement=complement)


def selectnone(table, field, complement=False):
    """
    Select rows where the given field is None.

    """

    return fieldselect(table, field, lambda v: v is None, complement=complement)


def selectnotnone(table, field, complement=False):
    """
    Select rows where the given field is not None.

    """

    return fieldselect(table, field, lambda v: v is not None, complement=complement)


def selectusingcontext(table, query):
    """
    Select rows based on data in the current row and/or previous and
    next row. E.g.::

        >>> from petl import look, selectusingcontext
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   |     1 |
        +-------+-------+
        | 'B'   |     4 |
        +-------+-------+
        | 'C'   |     5 |
        +-------+-------+
        | 'D'   |     9 |
        +-------+-------+

        >>> def query(prv, cur, nxt):
        ...     return ((prv is not None and (cur.bar - prv.bar) < 2)
        ...             or (nxt is not None and (nxt.bar - cur.bar) < 2))
        ...
        >>> table2 = selectusingcontext(table1, query)
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'B'   |     4 |
        +-------+-------+
        | 'C'   |     5 |
        +-------+-------+

    .. versionadded:: 0.24

    """

    return SelectUsingContextView(table, query)


class SelectUsingContextView(object):

    def __init__(self, table, query):
        self.table = table
        self.query = query

    def __iter__(self):
        return iterselectusingcontext(self.table, self.query)


def iterselectusingcontext(table, query):
    it = iter(table)
    fields = tuple(it.next())
    yield fields
    it = hybridrows(fields, it)
    prv = None
    cur = it.next()
    for nxt in it:
        if query(prv, cur, nxt):
            yield cur
        prv = cur
        cur = nxt
    # handle last row
    if query(prv, cur, None):
        yield cur


def facet(table, field):
    """
    Return a dictionary mapping field values to tables.

    E.g.::

        >>> from petl import facet, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 4     | 9.3   |
        +-------+-------+-------+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+
        | 'b'   | 1     | 23.3  |
        +-------+-------+-------+
        | 'c'   | 8     | 42.0  |
        +-------+-------+-------+
        | 'd'   | 7     | 100.9 |
        +-------+-------+-------+
        | 'c'   | 2     |       |
        +-------+-------+-------+

        >>> foo = facet(table1, 'foo')
        >>> foo.keys()
        ['a', 'c', 'b', 'd']
        >>> look(foo['a'])
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 4     | 9.3   |
        +-------+-------+-------+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+

        >>> look(foo['c'])
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'c'   | 8     | 42.0  |
        +-------+-------+-------+
        | 'c'   | 2     |       |
        +-------+-------+-------+

    See also :func:`facetcolumns`.

    """

    fct = dict()
    for v in set(values(table, field)):
        fct[v] = selecteq(table, field, v)
    return fct


def rangefacet(table, field, width, minv=None, maxv=None,
               presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Return a dictionary mapping ranges to tables. E.g.::

        >>> from petl import rangefacet, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 3     |
        +-------+-------+
        | 'a'   | 7     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'b'   | 1     |
        +-------+-------+
        | 'b'   | 9     |
        +-------+-------+
        | 'c'   | 4     |
        +-------+-------+
        | 'd'   | 3     |
        +-------+-------+

        >>> rf = rangefacet(table1, 'bar', 2)
        >>> rf.keys()
        [(1, 3), (3, 5), (5, 7), (7, 9)]
        >>> look(rf[(1, 3)])
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'b'   | 2     |
        +-------+-------+
        | 'b'   | 1     |
        +-------+-------+

        >>> look(rf[(7, 9)])
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 7     |
        +-------+-------+
        | 'b'   | 9     |
        +-------+-------+

    Note that the last bin includes both edges.

    """

    # determine minimum and maximum values
    if minv is None and maxv is None:
        minv, maxv = limits(table, field)
    elif minv is None:
        minv = min(itervalues(table, field))
    elif max is None:
        maxv = max(itervalues(table, field))

    fct = OrderedDict()
    for binminv in xrange(minv, maxv, width):
        binmaxv = binminv + width
        if binmaxv >= maxv: # final bin
            binmaxv = maxv
            # final bin includes right edge
            fct[(binminv, binmaxv)] = selectrangeopen(table, field, binminv, binmaxv)
        else:
            fct[(binminv, binmaxv)] = selectrangeopenleft(table, field, binminv, binmaxv)

    return fct
