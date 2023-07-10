from __future__ import absolute_import, print_function, division


import itertools
from petl.compat import next, text_type
from petl.errors import FieldSelectionError


from petl.util.base import Table, asindices, rowgetter


def rename(table, *args, **kwargs):
    """
    Replace one or more values in the table's header row. E.g.::

        >>> import petl as etl
        >>> table1 = [['sex', 'age'],
        ...           ['m', 12],
        ...           ['f', 34],
        ...           ['-', 56]]
        >>> # rename a single field
        ... table2 = etl.rename(table1, 'sex', 'gender')
        >>> table2
        +--------+-----+
        | gender | age |
        +========+=====+
        | 'm'    |  12 |
        +--------+-----+
        | 'f'    |  34 |
        +--------+-----+
        | '-'    |  56 |
        +--------+-----+

        >>> # rename multiple fields by passing dictionary as second argument
        ... table3 = etl.rename(table1, {'sex': 'gender', 'age': 'age_years'})
        >>> table3
        +--------+-----------+
        | gender | age_years |
        +========+===========+
        | 'm'    |        12 |
        +--------+-----------+
        | 'f'    |        34 |
        +--------+-----------+
        | '-'    |        56 |
        +--------+-----------+

    The field to rename can be specified as an index (i.e., integer representing
    field position).

    If any nonexistent fields are specified, the default behaviour is to raise
    a `FieldSelectionError`. However, if `strict` keyword argument is `False`, any
    nonexistent fields specified will be silently ignored.
    """

    return RenameView(table, *args, **kwargs)


Table.rename = rename


class RenameView(Table):

    def __init__(self, table, *args, **kwargs):
        self.source = table
        if len(args) == 0:
            self.spec = dict()
        elif len(args) == 1:
            self.spec = args[0]
        elif len(args) == 2:
            self.spec = {args[0]: args[1]}
        self.strict = kwargs.get('strict', True)

    def __iter__(self):
        return iterrename(self.source, self.spec, self.strict)

    def __setitem__(self, key, value):
        self.spec[key] = value


def iterrename(source, spec, strict):
    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    if strict:
        for x in spec:
            if isinstance(x, int):
                if x < 0 or x >= len(hdr):
                    raise FieldSelectionError(x)
            elif x not in flds:
                raise FieldSelectionError(x)
    outhdr = [spec[i] if i in spec
              else spec[f] if f in spec
              else f
              for i, f in enumerate(flds)]
    yield tuple(outhdr)
    for row in it:
        yield tuple(row)


def setheader(table, header):
    """
    Replace header row in the given table. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2]]
        >>> table2 = etl.setheader(table1, ['foofoo', 'barbar'])
        >>> table2
        +--------+--------+
        | foofoo | barbar |
        +========+========+
        | 'a'    |      1 |
        +--------+--------+
        | 'b'    |      2 |
        +--------+--------+

    See also :func:`petl.transform.headers.extendheader`,
    :func:`petl.transform.headers.pushheader`.

    """

    return SetHeaderView(table, header)


Table.setheader = setheader


class SetHeaderView(Table):

    def __init__(self, source, header):
        self.source = source
        self.header = header

    def __iter__(self):
        return itersetheader(self.source, self.header)


def itersetheader(source, header):
    it = iter(source)
    try:
        next(it)  # discard source header
    except StopIteration:
        pass  # no previous header
    yield tuple(header)
    for row in it:
        yield tuple(row)


def extendheader(table, fields):
    """
    Extend header row in the given table. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo'],
        ...           ['a', 1, True],
        ...           ['b', 2, False]]
        >>> table2 = etl.extendheader(table1, ['bar', 'baz'])
        >>> table2
        +-----+-----+-------+
        | foo | bar | baz   |
        +=====+=====+=======+
        | 'a' |   1 | True  |
        +-----+-----+-------+
        | 'b' |   2 | False |
        +-----+-----+-------+

    See also :func:`petl.transform.headers.setheader`,
    :func:`petl.transform.headers.pushheader`.

    """

    return ExtendHeaderView(table, fields)


Table.extendheader = extendheader


class ExtendHeaderView(Table):

    def __init__(self, source, fields):
        self.source = source
        self.fields = fields

    def __iter__(self):
        return iterextendheader(self.source, self.fields)


def iterextendheader(source, fields):
    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    outhdr = list(hdr)
    outhdr.extend(fields)
    yield tuple(outhdr)
    for row in it:
        yield tuple(row)


def pushheader(table, header, *args):
    """
    Push rows down and prepend a header row. E.g.::

        >>> import petl as etl
        >>> table1 = [['a', 1],
        ...           ['b', 2]]
        >>> table2 = etl.pushheader(table1, ['foo', 'bar'])
        >>> table2
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+

    The header row can either be a list or positional arguments.

    """

    return PushHeaderView(table, header, *args)


Table.pushheader = pushheader


class PushHeaderView(Table):

    def __init__(self, source, header, *args):
        self.source = source
        self.args = args
        # if user passes header as a list, just use this and ignore args
        if isinstance(header, (list, tuple)):
            self.header = header
        # otherwise,
        elif len(args) > 0:
            self.header = []
            self.header.append(header)  # first argument is named header
            self.header.extend(args)  # add the other positional arguments
        else:
            assert False, 'bad parameters'

    def __iter__(self):
        return iterpushheader(self.source, self.header)


def iterpushheader(source, header):
    it = iter(source)
    yield tuple(header)
    for row in it:
        yield tuple(row)


def skip(table, n):
    """
    Skip `n` rows, including the header row. E.g.::

        >>> import petl as etl
        >>> table1 = [['#aaa', 'bbb', 'ccc'],
        ...           ['#mmm'],
        ...           ['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2]]
        >>> table2 = etl.skip(table1, 2)
        >>> table2
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+

    See also :func:`petl.transform.basics.skipcomments`.

    """

    return SkipView(table, n)


Table.skip = skip


class SkipView(Table):

    def __init__(self, source, n):
        self.source = source
        self.n = n

    def __iter__(self):
        return iterskip(self.source, self.n)


def iterskip(source, n):
    return itertools.islice(source, n, None)


def prefixheader(table, prefix):
    """Prefix all fields in the table header."""

    return PrefixHeaderView(table, prefix)


Table.prefixheader = prefixheader


class PrefixHeaderView(Table):

    def __init__(self, table, prefix):
        self.table = table
        self.prefix = prefix

    def __iter__(self):
        it = iter(self.table)
        try:
            hdr = next(it)
        except StopIteration:
            return
        outhdr = tuple((text_type(self.prefix) + text_type(f)) for f in hdr)
        yield outhdr
        for row in it:
            yield row


def suffixheader(table, suffix):
    """Suffix all fields in the table header."""

    return SuffixHeaderView(table, suffix)


Table.suffixheader = suffixheader


class SuffixHeaderView(Table):

    def __init__(self, table, suffix):
        self.table = table
        self.suffix = suffix

    def __iter__(self):
        it = iter(self.table)
        try:
            hdr = next(it)
        except StopIteration:
            return
        outhdr = tuple((text_type(f) + text_type(self.suffix)) for f in hdr)
        yield outhdr
        for row in it:
            yield row


def sortheader(table, reverse=False, missing=None):
    """Re-order columns so the header is sorted.

    .. versionadded:: 1.1.0

    """

    return SortHeaderView(table, reverse, missing)


Table.sortheader = sortheader


class SortHeaderView(Table):

    def __init__(self, table, reverse, missing):
        self.table = table
        self.reverse = reverse
        self.missing = missing

    def __iter__(self):
        it = iter(self.table)
        try:
            hdr = next(it)
        except StopIteration:
            return
        shdr = sorted(hdr)
        indices = asindices(hdr, shdr)
        transform = rowgetter(*indices)

        # yield the transformed header
        yield tuple(shdr)

        # construct the transformed data
        missing = self.missing
        for row in it:
            try:
                yield transform(row)
            except IndexError:
                # row is short, let's be kind and fill in any missing fields
                yield tuple(row[i] if i < len(row) else missing
                            for i in indices)
