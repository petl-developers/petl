from __future__ import absolute_import, print_function, division


import itertools
from petl.compat import next


from petl.util.base import Table


def rename(table, *args):
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

        >>> # rename multiple fields by passing a dictionary as the second argument
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

    """

    return RenameView(table, *args)


Table.rename = rename


class RenameView(Table):

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
    sourceflds = next(it)
    newflds = [spec[f] if f in spec
               else spec[i] if i in spec
               else f
               for i, f in enumerate(sourceflds)]
    yield tuple(newflds)
    for row in it:
        yield tuple(row)


def setheader(table, fields):
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

    return SetHeaderView(table, fields)


Table.setheader = setheader


class SetHeaderView(Table):

    def __init__(self, source, fields):
        self.source = source
        self.fields = fields

    def __iter__(self):
        return itersetheader(self.source, self.fields)


def itersetheader(source, fields):
    it = iter(source)
    next(it)  # discard source fields
    yield tuple(fields)
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
    srcflds = next(it)
    outflds = list(srcflds)
    outflds.extend(fields)
    yield tuple(outflds)
    for row in it:
        yield tuple(row)


def pushheader(table, fields, *args):
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

    return PushHeaderView(table, fields, *args)


Table.pushheader = pushheader


class PushHeaderView(Table):

    def __init__(self, source, fields, *args):
        self.source = source
        self.args = args
        # if user passes fields as a list, just use this and ignore args
        if isinstance(fields, (list, tuple)):
            self.fields = fields
        # otherwise,
        elif len(args) > 0:
            self.fields = []
            self.fields.append(fields)  # first argument is named fields
            for arg in args:
                self.fields.append(arg)  # add the other positional arguments
        else:
            assert False, 'bad parameters'

    def __iter__(self):
        return iterpushheader(self.source, self.fields)


def iterpushheader(source, fields):
    it = iter(source)
    yield tuple(fields)
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
        fields = next(it)
        outfields = tuple((str(self.prefix) + str(f)) for f in fields)
        yield outfields
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
        fields = next(it)
        outfields = tuple((str(f) + str(self.suffix)) for f in fields)
        yield outfields
        for row in it:
            yield row
