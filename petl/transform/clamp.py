from __future__ import absolute_import, print_function, division
from petl.util.base import Table


def clampvalues(table, field, low, high):
    """Clamp numeric values in a field so they never fall below `low` or
    exceed `high`. Values already within the range are left unchanged.
    Non-numeric values (e.g. ``None``) are passed through as-is.

    For example::

        >>> import petl as etl
        >>> table1 = [['id', 'score'],
        ...           [1, -5],
        ...           [2, 42],
        ...           [3, 105],
        ...           [4, None]]
        >>> table2 = etl.clampvalues(table1, 'score', 0, 100)
        >>> table2
        +----+-------+
        | id | score |
        +====+=======+
        |  1 |     0 |
        +----+-------+
        |  2 |    42 |
        +----+-------+
        |  3 |   100 |
        +----+-------+
        |  4 |  None |
        +----+-------+

    The `low` and `high` parameters are both inclusive bounds. Raises
    ``ValueError`` if ``low > high``.

    .. versionadded:: 1.8.0

    """
    return ClampValuesView(table, field, low, high)


class ClampValuesView(Table):
    """View returned by :func:`clampvalues`."""

    def __init__(self, table, field, low, high):
        if low > high:
            raise ValueError(
                'low (%r) must not be greater than high (%r)' % (low, high)
            )
        self.table = table
        self.field = field
        self.low = low
        self.high = high

    def __iter__(self):
        it = iter(self.table)
        header = next(it)
        yield header  # pass header row through unchanged

        # find the index of the target field
        try:
            idx = list(header).index(self.field)
        except ValueError:
            raise ValueError('field %r not found in header %r' % (self.field, header))

        low, high = self.low, self.high
        for row in it:
            row = list(row)
            val = row[idx]
            if val is not None:
                try:
                    if val < low:
                        row[idx] = low
                    elif val > high:
                        row[idx] = high
                except TypeError:
                    pass  # non-comparable type: leave value unchanged
            yield tuple(row)
