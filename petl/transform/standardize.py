from __future__ import absolute_import, print_function, division


import math


from petl.compat import integer_types
from petl.util.base import Table, asindices


def standardize(table, fields, newfields=None, ddof=0):
    """
    Standardize numeric values under one or more fields to have a mean of 0
    and a standard deviation of 1. E.g.::

        >>> import petl as etl
        >>> table1 = [['id', 'score'],
        ...           [1, 10],
        ...           [2, 20],
        ...           [3, 30]]
        >>> table2 = etl.standardize(table1, 'score')
        >>> table2.values('score').list()
        [-1.224744871391589, 0.0, 1.224744871391589]

    The `fields` argument can select one field or a list or tuple of fields.
    Field names and indexes are both supported. By default, standardized
    values replace the selected fields. Use `newfields` to append the values
    under new field names instead. The number of `newfields` must match the
    number of selected fields.

    Use `ddof` to set the delta degrees of freedom used when calculating the
    standard deviation. The default, 0, uses the population standard
    deviation; use 1 for the sample standard deviation.

    Non-numeric values, including ``None`` and booleans, are passed through
    unchanged. Constant fields are standardized to 0.0.

    Note that the source table is materialized before values are returned.

    """

    return StandardizeView(table, fields, newfields=newfields, ddof=ddof)


Table.standardize = standardize


class StandardizeView(Table):

    def __init__(self, source, fields, newfields=None, ddof=0):
        self.source = source
        self.fields = fields
        self.newfields = newfields
        self.ddof = ddof

    def __iter__(self):
        return iterstandardize(self.source, self.fields, self.newfields,
                               self.ddof)


def iterstandardize(source, fields, newfields, ddof):
    if not isinstance(ddof, integer_types) or ddof < 0:
        raise ValueError('ddof must be a non-negative integer')

    rows = list(iter(source))
    if not rows:
        return

    hdr = rows[0]
    data = rows[1:]
    indices = asindices(hdr, fields)

    if newfields is not None:
        newfields = _as_list(newfields)
        if len(newfields) != len(indices):
            raise ValueError('newfields must be the same length as fields')
        outhdr = tuple(hdr) + tuple(newfields)
    else:
        outhdr = tuple(hdr)

    stats = []
    for index in indices:
        values = [float(row[index]) for row in data
                  if _is_number(row[index])]
        stats.append(_mean_std(values, ddof))

    yield outhdr

    for row in data:
        outrow = list(row)
        standardized = []
        for index, (mean, std) in zip(indices, stats):
            value = row[index]
            if _is_number(value):
                if std == 0 or math.isnan(std):
                    value = 0.0
                else:
                    value = (float(value) - mean) / std
            standardized.append(value)

        if newfields is None:
            for index, value in zip(indices, standardized):
                outrow[index] = value
        else:
            outrow.extend(standardized)
        yield tuple(outrow)


def _as_list(value):
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _is_number(value):
    return (isinstance(value, integer_types + (float,)) and
            not isinstance(value, bool))


def _mean_std(values, ddof):
    if not values:
        return 0.0, 0.0
    divisor = len(values) - ddof
    if divisor <= 0:
        raise ValueError('ddof must be less than the number of numeric values')
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / divisor
    return mean, math.sqrt(variance)
