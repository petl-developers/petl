__author__ = 'aliman'


from petl.util import RowContainer, asindices


def filldown(table, *fields, **kwargs):
    """
    Replace missing values with non-missing values from the row above.
    E.g.::

        >>> from petl import filldown, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | None  | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | None  | None  |
        +-------+-------+-------+
        | 2     | None  | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+

        >>> table2 = filldown(table1)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | 'a'   | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | 0.23  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.23  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | 0.72  |
        +-------+-------+-------+

        >>> table3 = filldown(table1, 'bar')
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | 'a'   | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+

        >>> table4 = filldown(table1, 'bar', 'baz')
        >>> look(table4)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | 'a'   | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | 0.23  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.23  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | 0.56  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+

    .. versionadded:: 0.11

    """

    return FillDownView(table, fields, **kwargs)


class FillDownView(RowContainer):

    def __init__(self, table, fields, missing=None):
        self.table = table
        self.fields = fields
        self.missing = missing

    def __iter__(self):
        return iterfilldown(self.table, self.fields, self.missing)


def iterfilldown(table, fillfields, missing):
    it = iter(table)
    fields = it.next()
    yield tuple(fields)
    if not fillfields: # fill down all fields
        fillfields = fields
    fillindices = asindices(fields, fillfields)
    fill = list(it.next()) # fill values
    yield tuple(fill)
    for row in it:
        outrow = list(row)
        for idx in fillindices:
            if row[idx] == missing:
                outrow[idx] = fill[idx] # fill down
            else:
                fill[idx] = row[idx] # new fill value
        yield tuple(outrow)


def fillright(table, missing=None):
    """
    Replace missing values with preceding non-missing values. E.g.::

        >>> from petl import fillright, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | None  | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | None  | None  |
        +-------+-------+-------+
        | 2     | None  | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+

        >>> table2 = fillright(table1)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | 'a'   |
        +-------+-------+-------+
        | 1     | 1     | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | 'b'   |
        +-------+-------+-------+
        | 2     | 2     | 2     |
        +-------+-------+-------+
        | 2     | 2     | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | 'c'   |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+

    .. versionadded:: 0.11

    """

    return FillRightView(table, missing=missing)


class FillRightView(RowContainer):

    def __init__(self, table, missing=None):
        self.table = table
        self.missing = missing

    def __iter__(self):
        return iterfillright(self.table, self.missing)


def iterfillright(table, missing):
    it = iter(table)
    fields = it.next()
    yield tuple(fields)
    for row in it:
        outrow = list(row)
        for i, _ in enumerate(outrow):
            if i > 0 and outrow[i] == missing and outrow[i-1] != missing:
                outrow[i] = outrow[i-1]
        yield tuple(outrow)


def fillleft(table, missing=None):
    """
    Replace missing values with following non-missing values. E.g.::

        >>> from petl import fillleft, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | None  | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | None  | None  |
        +-------+-------+-------+
        | None  | None  | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+

        >>> table2 = fillleft(table1)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | 0.23  | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | None  | None  |
        +-------+-------+-------+
        | 0.56  | 0.56  | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | 'c'   | 'c'   | 0.72  |
        +-------+-------+-------+

    .. versionadded:: 0.11

    """

    return FillLeftView(table, missing=missing)


class FillLeftView(RowContainer):

    def __init__(self, table, missing=None):
        self.table = table
        self.missing = missing

    def __iter__(self):
        return iterfillleft(self.table, self.missing)


def iterfillleft(table, missing):
    it = iter(table)
    fields = it.next()
    yield tuple(fields)
    for row in it:
        outrow = list(reversed(row))
        for i, _ in enumerate(outrow):
            if i > 0 and outrow[i] == missing and outrow[i-1] != missing:
                outrow[i] = outrow[i-1]
        yield tuple(reversed(outrow))


