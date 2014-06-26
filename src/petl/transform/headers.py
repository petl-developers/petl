__author__ = 'aliman'


import itertools


from petl.util import RowContainer


def rename(table, *args):
    """
    Replace one or more fields in the table's header row. E.g.::

        >>> from petl import look, rename
        >>> look(table1)
        +-------+-------+
        | 'sex' | 'age' |
        +=======+=======+
        | 'M'   | 12    |
        +-------+-------+
        | 'F'   | 34    |
        +-------+-------+
        | '-'   | 56    |
        +-------+-------+

        >>> # rename a single field
        ... table2 = rename(table1, 'sex', 'gender')
        >>> look(table2)
        +----------+-------+
        | 'gender' | 'age' |
        +==========+=======+
        | 'M'      | 12    |
        +----------+-------+
        | 'F'      | 34    |
        +----------+-------+
        | '-'      | 56    |
        +----------+-------+

        >>> # rename multiple fields by passing a dictionary as the second argument
        ... table3 = rename(table1, {'sex': 'gender', 'age': 'age_years'})
        >>> look(table3)
        +----------+-------------+
        | 'gender' | 'age_years' |
        +==========+=============+
        | 'M'      | 12          |
        +----------+-------------+
        | 'F'      | 34          |
        +----------+-------------+
        | '-'      | 56          |
        +----------+-------------+

        >>> # the returned table object can also be used to modify the field mapping using the suffix notation
        ... table4 = rename(table1)
        >>> table4['sex'] = 'gender'
        >>> table4['age'] = 'age_years'
        >>> look(table4)
        +----------+-------------+
        | 'gender' | 'age_years' |
        +==========+=============+
        | 'M'      | 12          |
        +----------+-------------+
        | 'F'      | 34          |
        +----------+-------------+
        | '-'      | 56          |
        +----------+-------------+

    .. versionchanged:: 0.4

    Function signature changed to support the simple 2 argument form when renaming
    a single field.

    .. versionchanged:: 0.23

    The field to rename can be specified as an index (i.e., integer representing field position).

    """

    return RenameView(table, *args)


class RenameView(RowContainer):

    def __init__(self, table, *args):
        self.source = table
        if len(args) == 0:
            self.spec = dict()
        elif len(args) == 1:
            self.spec = args[0]
        elif len(args) == 2:
            self.spec = {args[0]: args[1]}

    def __iter__(self):
        return iterrename(self.source, self.spec)

    def __setitem__(self, key, value):
        self.spec[key] = value


def iterrename(source, spec):
    it = iter(source)
    spec = spec.copy()  # make sure nobody can change this midstream
    sourceflds = it.next()
    newflds = [spec[f] if f in spec
               else spec[i] if i in spec
               else f
               for i, f in enumerate(sourceflds)]
    yield tuple(newflds)
    for row in it:
        yield tuple(row)


def setheader(table, fields):
    """
    Override fields in the given table. E.g.::

        >>> from petl import setheader, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+

        >>> table2 = setheader(table1, ['foofoo', 'barbar'])
        >>> look(table2)
        +----------+----------+
        | 'foofoo' | 'barbar' |
        +==========+==========+
        | 'a'      | 1        |
        +----------+----------+
        | 'b'      | 2        |
        +----------+----------+

    See also :func:`extendheader`, :func:`pushheader`.

    """

    return SetHeaderView(table, fields)


class SetHeaderView(RowContainer):

    def __init__(self, source, fields):
        self.source = source
        self.fields = fields

    def __iter__(self):
        return itersetheader(self.source, self.fields)


def itersetheader(source, fields):
    it = iter(source)
    it.next() # discard source fields
    yield tuple(fields)
    for row in it:
        yield tuple(row)


def extendheader(table, fields):
    """
    Extend fields in the given table. E.g.::

        >>> from petl import extendheader, look
        >>> look(table1)
        +-------+---+-------+
        | 'foo' |   |       |
        +=======+===+=======+
        | 'a'   | 1 | True  |
        +-------+---+-------+
        | 'b'   | 2 | False |
        +-------+---+-------+

        >>> table2 = extendheader(table1, ['bar', 'baz'])
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 1     | True  |
        +-------+-------+-------+
        | 'b'   | 2     | False |
        +-------+-------+-------+

    See also :func:`setheader`, :func:`pushheader`.
    """

    return ExtendHeaderView(table, fields)


class ExtendHeaderView(RowContainer):

    def __init__(self, source, fields):
        self.source = source
        self.fields = fields

    def __iter__(self):
        return iterextendheader(self.source, self.fields)


def iterextendheader(source, fields):
    it = iter(source)
    srcflds = it.next()
    outflds = list(srcflds)
    outflds.extend(fields)
    yield tuple(outflds)
    for row in it:
        yield tuple(row)


def pushheader(table, fields):
    """
    Push rows down and prepend a header row. E.g.::

        >>> from petl import pushheader, look
        >>> look(table1)
        +-----+---+
        | 'a' | 1 |
        +=====+===+
        | 'b' | 2 |
        +-----+---+

        >>> table2 = pushheader(table1, ['foo', 'bar'])
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+

    Useful, e.g., where data are from a CSV file that has not included a header
    row.

    """

    return PushHeaderView(table, fields)


class PushHeaderView(RowContainer):

    def __init__(self, source, fields):
        self.source = source
        self.fields = fields

    def __iter__(self):
        return iterpushheader(self.source, self.fields)


def iterpushheader(source, fields):
    it = iter(source)
    yield tuple(fields)
    for row in it:
        yield tuple(row)


def skip(table, n):
    """
    Skip `n` rows (including the header row).

    E.g.::

        >>> from petl import skip, look
        >>> look(table1)
        +--------+-------+-------+
        | '#aaa' | 'bbb' | 'ccc' |
        +========+=======+=======+
        | '#mmm' |       |       |
        +--------+-------+-------+
        | 'foo'  | 'bar' |       |
        +--------+-------+-------+
        | 'a'    | 1     |       |
        +--------+-------+-------+
        | 'b'    | 2     |       |
        +--------+-------+-------+

        >>> table2 = skip(table1, 2)
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+

    See also :func:`skipcomments`.

    """

    return SkipView(table, n)


class SkipView(RowContainer):

    def __init__(self, source, n):
        self.source = source
        self.n = n

    def __iter__(self):
        return iterskip(self.source, self.n)


def iterskip(source, n):
    return itertools.islice(source, n, None)


def prefixheader(table, prefix):
    """
    Prefix all fields in the table header.

    .. versionadded:: 0.24

    """

    return PrefixHeaderView(table, prefix)


class PrefixHeaderView(object):

    def __init__(self, table, prefix):
        self.table = table
        self.prefix = prefix

    def __iter__(self):
        it = iter(self.table)
        fields = it.next()
        outfields = tuple((str(self.prefix) + str(f)) for f in fields)
        yield outfields
        for row in it:
            yield row


def suffixheader(table, suffix):
    """
    Suffix all fields in the table header.

    .. versionadded:: 0.24

    """

    return SuffixHeaderView(table, suffix)


class SuffixHeaderView(object):

    def __init__(self, table, suffix):
        self.table = table
        self.suffix = suffix

    def __iter__(self):
        it = iter(self.table)
        fields = it.next()
        outfields = tuple((str(f) + str(self.suffix)) for f in fields)
        yield outfields
        for row in it:
            yield row


