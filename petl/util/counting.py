from __future__ import absolute_import, print_function, division


from collections import Counter
from petl.compat import string_types, maketrans


from petl.util.base import values, Table, data, wrap


def nrows(table):
    """
    Count the number of data rows in a table. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> etl.nrows(table)
        2

    """

    return sum(1 for _ in data(table))


Table.nrows = nrows


def valuecount(table, field, value, missing=None):
    """
    Count the number of occurrences of `value` under the given field. Returns
    the absolute count and relative frequency as a pair. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['b', 7]]
        >>> etl.valuecount(table, 'foo', 'b')
        (2, 0.6666666666666666)

    The `field` argument can be a single field name or index (starting from
    zero) or a tuple of field names and/or indexes.

    """

    total = 0
    vs = 0
    for v in values(table, field, missing=missing):
        total += 1
        if v == value:
            vs += 1
    return vs, float(vs)/total


Table.valuecount = valuecount


def valuecounter(table, *field, **kwargs):
    """
    Find distinct values for the given field and count the number of
    occurrences. Returns a :class:`dict` mapping values to counts. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'],
        ...          ['a', True],
        ...          ['b'],
        ...          ['b', True],
        ...          ['c', False]]
        >>> etl.valuecounter(table, 'foo')
        Counter({'b': 2, 'a': 1, 'c': 1})

    The `field` argument can be a single field name or index (starting from
    zero) or a tuple of field names and/or indexes.

    """

    missing = kwargs.get('missing', None)
    counter = Counter()
    for v in values(table, field, missing=missing):
        try:
            counter[v] += 1
        except IndexError:
            pass  # short row
    return counter


Table.valuecounter = valuecounter


def valuecounts(table, *field, **kwargs):
    """
    Find distinct values for the given field and count the number and relative
    frequency of occurrences. Returns a table mapping values to counts, with
    most common values first. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['a', True, 0.12],
        ...          ['a', True, 0.17],
        ...          ['b', False, 0.34],
        ...          ['b', False, 0.44],
        ...          ['b']]
        >>> etl.valuecounts(table, 'foo')
        +-----+-------+-----------+
        | foo | count | frequency |
        +=====+=======+===========+
        | 'b' |     3 |       0.6 |
        +-----+-------+-----------+
        | 'a' |     2 |       0.4 |
        +-----+-------+-----------+

        >>> etl.valuecounts(table, 'foo', 'bar')
        +-----+-------+-------+-----------+
        | foo | bar   | count | frequency |
        +=====+=======+=======+===========+
        | 'a' | True  |     2 |       0.4 |
        +-----+-------+-------+-----------+
        | 'b' | False |     2 |       0.4 |
        +-----+-------+-------+-----------+
        | 'b' | None  |     1 |       0.2 |
        +-----+-------+-------+-----------+

    If rows are short, the value of the keyword argument `missing` is counted.

    Multiple fields can be given as positional arguments. If multiple fields are
    given, these are treated as a compound key.

    """

    return ValueCountsView(table, field, **kwargs)


Table.valuecounts = valuecounts


class ValueCountsView(Table):

    def __init__(self, table, field, missing=None):
        self.table = table
        self.field = field
        self.missing = missing

    def __iter__(self):

        # construct output header
        if isinstance(self.field, (tuple, list)):
            outhdr = tuple(self.field) + ('count', 'frequency')
        else:
            outhdr = (self.field, 'count', 'frequency')
        yield outhdr

        # count values
        counter = valuecounter(self.table, *self.field, missing=self.missing)
        counts = counter.most_common()  # sort descending
        total = sum(c[1] for c in counts)

        if len(self.field) > 1:
            for c in counts:
                yield tuple(c[0]) + (c[1], float(c[1])/total)
        else:
            for c in counts:
                yield (c[0], c[1], float(c[1])/total)


def parsecounter(table, field, parsers=(('int', int), ('float', float))):
    """
    Count the number of `str` or `unicode` values under the given fields that
    can be parsed as ints, floats or via custom parser functions. Return a
    pair of `Counter` objects, the first mapping parser names to the number of
    strings successfully parsed, the second mapping parser names to the
    number of errors. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 'aaa', 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', '3.7', 9.0],
        ...          ['E', 42]]
        >>> counter, errors = etl.parsecounter(table, 'bar')
        >>> counter
        Counter({'float': 3, 'int': 2})
        >>> errors
        Counter({'int': 2, 'float': 1})

    The `field` argument can be a field name or index (starting from zero).

    """

    if isinstance(parsers, (list, tuple)):
        parsers = dict(parsers)
    counter, errors = Counter(), Counter()
    # need to initialise
    for n in parsers.keys():
        counter[n] = 0
        errors[n] = 0
    for v in values(table, field):
        if isinstance(v, string_types):
            for name, parser in parsers.items():
                try:
                    parser(v)
                except:
                    errors[name] += 1
                else:
                    counter[name] += 1
    return counter, errors


Table.parsecounter = parsecounter


def parsecounts(table, field, parsers=(('int', int), ('float', float))):
    """
    Count the number of `str` or `unicode` values that can be parsed as ints,
    floats or via custom parser functions. Return a table mapping parser names
    to the number of values successfully parsed and the number of errors. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 'aaa', 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', '3.7', 9.0],
        ...          ['E', 42]]
        >>> etl.parsecounts(table, 'bar')
        +---------+-------+--------+
        | type    | count | errors |
        +=========+=======+========+
        | 'float' |     3 |      1 |
        +---------+-------+--------+
        | 'int'   |     2 |      2 |
        +---------+-------+--------+

    The `field` argument can be a field name or index (starting from zero).

    """

    return ParseCountsView(table, field, parsers=parsers)


Table.parsecounts = parsecounts


class ParseCountsView(Table):

    def __init__(self, table, field, parsers=(('int', int), ('float', float))):
        self.table = table
        self.field = field
        if isinstance(parsers, (list, tuple)):
            parsers = dict(parsers)
        self.parsers = parsers

    def __iter__(self):
        counter, errors = parsecounter(self.table, self.field, self.parsers)
        yield ('type', 'count', 'errors')
        for (item, n) in counter.most_common():
            yield (item, n, errors[item])


def typecounter(table, field):
    """
    Count the number of values found for each Python type.

        >>> import petl as etl
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', u'xyz', 9.0],
        ...          ['E', 42]]
        >>> etl.typecounter(table, 'foo')
        Counter({'str': 5})
        >>> etl.typecounter(table, 'bar')
        Counter({'str': 3, 'int': 2})
        >>> etl.typecounter(table, 'baz')
        Counter({'str': 2, 'int': 1, 'float': 1, 'NoneType': 1})

    The `field` argument can be a field name or index (starting from zero).

    """

    counter = Counter()
    for v in values(table, field):
        try:
            counter[v.__class__.__name__] += 1
        except IndexError:
            pass  # ignore short rows
    return counter


Table.typecounter = typecounter


def typecounts(table, field):
    """
    Count the number of values found for each Python type and return a table
    mapping class names to counts and frequencies. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar', 'baz'],
        ...          [b'A', 1, 2],
        ...          [b'B', '2', b'3.4'],
        ...          ['B', '3', '7.8', True],
        ...          ['D', u'xyz', 9.0],
        ...          ['E', 42]]
        >>> etl.typecounts(table, 'foo')
        +---------+-------+-----------+
        | type    | count | frequency |
        +=========+=======+===========+
        | 'str'   |     3 |       0.6 |
        +---------+-------+-----------+
        | 'bytes' |     2 |       0.4 |
        +---------+-------+-----------+

        >>> etl.typecounts(table, 'bar')
        +-------+-------+-----------+
        | type  | count | frequency |
        +=======+=======+===========+
        | 'str' |     3 |       0.6 |
        +-------+-------+-----------+
        | 'int' |     2 |       0.4 |
        +-------+-------+-----------+

        >>> etl.typecounts(table, 'baz')
        +------------+-------+-----------+
        | type       | count | frequency |
        +============+=======+===========+
        | 'int'      |     1 |       0.2 |
        +------------+-------+-----------+
        | 'bytes'    |     1 |       0.2 |
        +------------+-------+-----------+
        | 'str'      |     1 |       0.2 |
        +------------+-------+-----------+
        | 'float'    |     1 |       0.2 |
        +------------+-------+-----------+
        | 'NoneType' |     1 |       0.2 |
        +------------+-------+-----------+

    The `field` argument can be a field name or index (starting from zero).

    """

    return TypeCountsView(table, field)


Table.typecounts = typecounts


class TypeCountsView(Table):

    def __init__(self, table, field):
        self.table = table
        self.field = field

    def __iter__(self):
        counter = typecounter(self.table, self.field)
        yield ('type', 'count', 'frequency')
        counts = counter.most_common()
        total = sum(c[1] for c in counts)
        for c in counts:
            yield (c[0], c[1], float(c[1])/total)


def stringpatterncounter(table, field):
    """
    Profile string patterns in the given field, returning a :class:`dict`
    mapping patterns to counts.

    """

    trans = maketrans(
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789',
        'AAAAAAAAAAAAAAAAAAAAAAAAAAaaaaaaaaaaaaaaaaaaaaaaaaaa9999999999'
    )
    counter = Counter()
    for v in values(table, field):
        p = str(v).translate(trans)
        counter[p] += 1
    return counter


Table.stringpatterncounter = stringpatterncounter


def stringpatterns(table, field):
    """
    Profile string patterns in the given field, returning a table of patterns,
    counts and frequencies. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'],
        ...          ['Mr. Foo', '123-1254'],
        ...          ['Mrs. Bar', '234-1123'],
        ...          ['Mr. Spo', '123-1254'],
        ...          [u'Mr. Baz', u'321 1434'],
        ...          [u'Mrs. Baz', u'321 1434'],
        ...          ['Mr. Quux', '123-1254-XX']]
        >>> etl.stringpatterns(table, 'foo')
        +------------+-------+---------------------+
        | pattern    | count | frequency           |
        +============+=======+=====================+
        | 'Aa. Aaa'  |     3 |                 0.5 |
        +------------+-------+---------------------+
        | 'Aaa. Aaa' |     2 |  0.3333333333333333 |
        +------------+-------+---------------------+
        | 'Aa. Aaaa' |     1 | 0.16666666666666666 |
        +------------+-------+---------------------+

        >>> etl.stringpatterns(table, 'bar')
        +---------------+-------+---------------------+
        | pattern       | count | frequency           |
        +===============+=======+=====================+
        | '999-9999'    |     3 |                 0.5 |
        +---------------+-------+---------------------+
        | '999 9999'    |     2 |  0.3333333333333333 |
        +---------------+-------+---------------------+
        | '999-9999-AA' |     1 | 0.16666666666666666 |
        +---------------+-------+---------------------+

    """

    counter = stringpatterncounter(table, field)
    output = [('pattern', 'count', 'frequency')]
    counter = counter.most_common()
    total = sum(c[1] for c in counter)
    cnts = [(c[0], c[1], float(c[1])/total) for c in counter]
    output.extend(cnts)
    return wrap(output)


Table.stringpatterns = stringpatterns


def rowlengths(table):
    """
    Report on row lengths found in the table. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', '2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', 'xyz', 9.0],
        ...          ['E', None],
        ...          ['F', 9]]
        >>> etl.rowlengths(table)
        +--------+-------+
        | length | count |
        +========+=======+
        |      3 |     3 |
        +--------+-------+
        |      2 |     2 |
        +--------+-------+
        |      4 |     1 |
        +--------+-------+

    Useful for finding potential problems in data files.

    """

    counter = Counter()
    for row in data(table):
        counter[len(row)] += 1
    output = [('length', 'count')]
    output.extend(counter.most_common())
    return wrap(output)


Table.rowlengths = rowlengths
