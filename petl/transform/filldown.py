from __future__ import absolute_import, print_function, division
from petl.util.base import Table
from petl.errors import FieldSelectionError


def filldown(table, *fields):
    """Propagate the last observed non-``None`` value downward within one or
    more fields.  Rows where the field already has a value are left unchanged.
    If a field's very first data value is ``None`` it stays ``None`` until a
    non-``None`` value is encountered.

    For example::

        >>> import petl as etl
        >>> table1 = [['site', 'reading'],
        ...           ['A',    1.2],
        ...           [None,   1.5],
        ...           [None,   2.0],
        ...           ['B',    0.9],
        ...           [None,   1.1]]
        >>> table2 = etl.filldown(table1, 'site')
        >>> table2
        +------+---------+
        | site | reading |
        +======+=========+
        | 'A'  |     1.2 |
        +------+---------+
        | 'A'  |     1.5 |
        +------+---------+
        | 'A'  |     2.0 |
        +------+---------+
        | 'B'  |     0.9 |
        +------+---------+
        | 'B'  |     1.1 |
        +------+---------+

    Multiple fields can be filled in a single call::

        >>> table3 = [['a', 'b'],
        ...           [1,   'x'],
        ...           [None, None],
        ...           [2,   None]]
        >>> etl.filldown(table3, 'a', 'b')

    .. versionadded:: 1.8.0

    """
    return FillDownView(table, fields)


class FillDownView(Table):
    """View returned by :func:`filldown`."""

    def __init__(self, table, fields):
        self.table = table
        self.fields = fields

    def __iter__(self):
        it = iter(self.table)
        header = next(it)
        yield header

        # resolve the index of each requested field
        header_list = list(header)
        try:
            indices = [header_list.index(f) for f in self.fields]
        except ValueError as e:
            raise FieldSelectionError(str(e))

        # last seen non-None value for each tracked field
        last_seen = {idx: None for idx in indices}

        for row in it:
            row = list(row)
            for idx in indices:
                if row[idx] is None:
                    row[idx] = last_seen[idx]  # fill from above
                else:
                    last_seen[idx] = row[idx]  # update running value
            yield tuple(row)