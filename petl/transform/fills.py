from __future__ import absolute_import, print_function, division


from petl.compat import next


from petl.util.base import Table, asindices


def filldown(table, *fields, **kwargs):
    """
    Replace missing values with non-missing values from the row above. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           [1, 'a', None],
        ...           [1, None, .23],
        ...           [1, 'b', None],
        ...           [2, None, None],
        ...           [2, None, .56],
        ...           [2, 'c', None],
        ...           [None, 'c', .72]]
        >>> table2 = etl.filldown(table1)
        >>> table2.lookall()
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        |   1 | 'a' | None |
        +-----+-----+------+
        |   1 | 'a' | 0.23 |
        +-----+-----+------+
        |   1 | 'b' | 0.23 |
        +-----+-----+------+
        |   2 | 'b' | 0.23 |
        +-----+-----+------+
        |   2 | 'b' | 0.56 |
        +-----+-----+------+
        |   2 | 'c' | 0.56 |
        +-----+-----+------+
        |   2 | 'c' | 0.72 |
        +-----+-----+------+

        >>> table3 = etl.filldown(table1, 'bar')
        >>> table3.lookall()
        +------+-----+------+
        | foo  | bar | baz  |
        +======+=====+======+
        |    1 | 'a' | None |
        +------+-----+------+
        |    1 | 'a' | 0.23 |
        +------+-----+------+
        |    1 | 'b' | None |
        +------+-----+------+
        |    2 | 'b' | None |
        +------+-----+------+
        |    2 | 'b' | 0.56 |
        +------+-----+------+
        |    2 | 'c' | None |
        +------+-----+------+
        | None | 'c' | 0.72 |
        +------+-----+------+

        >>> table4 = etl.filldown(table1, 'bar', 'baz')
        >>> table4.lookall()
        +------+-----+------+
        | foo  | bar | baz  |
        +======+=====+======+
        |    1 | 'a' | None |
        +------+-----+------+
        |    1 | 'a' | 0.23 |
        +------+-----+------+
        |    1 | 'b' | 0.23 |
        +------+-----+------+
        |    2 | 'b' | 0.23 |
        +------+-----+------+
        |    2 | 'b' | 0.56 |
        +------+-----+------+
        |    2 | 'c' | 0.56 |
        +------+-----+------+
        | None | 'c' | 0.72 |
        +------+-----+------+

    Use the `missing` keyword argument to control which value is treated as
    missing (`None` by default).

    """

    return FillDownView(table, fields, **kwargs)


Table.filldown = filldown


class FillDownView(Table):

    def __init__(self, table, fields, missing=None):
        self.table = table
        self.fields = fields
        self.missing = missing

    def __iter__(self):
        return iterfilldown(self.table, self.fields, self.missing)


def iterfilldown(table, fillfields, missing):
    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        return
    yield tuple(hdr)
    if not fillfields:  # fill down all fields
        fillfields = hdr
    fillindices = asindices(hdr, fillfields)
    fill = list(next(it))  # fill values
    yield tuple(fill)
    for row in it:
        outrow = list(row)
        for idx in fillindices:
            if row[idx] == missing:
                outrow[idx] = fill[idx]  # fill down
            else:
                fill[idx] = row[idx]  # new fill value
        yield tuple(outrow)


def fillright(table, missing=None):
    """
    Replace missing values with preceding non-missing values. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           [1, 'a', None],
        ...           [1, None, .23],
        ...           [1, 'b', None],
        ...           [2, None, None],
        ...           [2, None, .56],
        ...           [2, 'c', None],
        ...           [None, 'c', .72]]
        >>> table2 = etl.fillright(table1)
        >>> table2.lookall()
        +------+-----+------+
        | foo  | bar | baz  |
        +======+=====+======+
        |    1 | 'a' | 'a'  |
        +------+-----+------+
        |    1 |   1 | 0.23 |
        +------+-----+------+
        |    1 | 'b' | 'b'  |
        +------+-----+------+
        |    2 |   2 |    2 |
        +------+-----+------+
        |    2 |   2 | 0.56 |
        +------+-----+------+
        |    2 | 'c' | 'c'  |
        +------+-----+------+
        | None | 'c' | 0.72 |
        +------+-----+------+

    Use the `missing` keyword argument to control which value is treated as
    missing (`None` by default).

    """

    return FillRightView(table, missing=missing)


Table.fillright = fillright


class FillRightView(Table):

    def __init__(self, table, missing=None):
        self.table = table
        self.missing = missing

    def __iter__(self):
        return iterfillright(self.table, self.missing)


def iterfillright(table, missing):
    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        return
    yield tuple(hdr)
    for row in it:
        outrow = list(row)
        for i, _ in enumerate(outrow):
            if i > 0 and outrow[i] == missing and outrow[i-1] != missing:
                outrow[i] = outrow[i-1]
        yield tuple(outrow)


def fillleft(table, missing=None):
    """
    Replace missing values with following non-missing values. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           [1, 'a', None],
        ...           [1, None, .23],
        ...           [1, 'b', None],
        ...           [2, None, None],
        ...           [2, None, .56],
        ...           [2, 'c', None],
        ...           [None, 'c', .72]]
        >>> table2 = etl.fillleft(table1)
        >>> table2.lookall()
        +-----+------+------+
        | foo | bar  | baz  |
        +=====+======+======+
        |   1 | 'a'  | None |
        +-----+------+------+
        |   1 | 0.23 | 0.23 |
        +-----+------+------+
        |   1 | 'b'  | None |
        +-----+------+------+
        |   2 | None | None |
        +-----+------+------+
        |   2 | 0.56 | 0.56 |
        +-----+------+------+
        |   2 | 'c'  | None |
        +-----+------+------+
        | 'c' | 'c'  | 0.72 |
        +-----+------+------+

    Use the `missing` keyword argument to control which value is treated as
    missing (`None` by default).

    """

    return FillLeftView(table, missing=missing)


Table.fillleft = fillleft


class FillLeftView(Table):

    def __init__(self, table, missing=None):
        self.table = table
        self.missing = missing

    def __iter__(self):
        return iterfillleft(self.table, self.missing)


def iterfillleft(table, missing):
    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        return
    yield tuple(hdr)
    for row in it:
        outrow = list(reversed(row))
        for i, _ in enumerate(outrow):
            if i > 0 and outrow[i] == missing and outrow[i-1] != missing:
                outrow[i] = outrow[i-1]
        yield tuple(reversed(outrow))
