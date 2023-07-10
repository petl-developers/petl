from __future__ import absolute_import, print_function, division


import itertools
from petl.compat import next, text_type


from petl.errors import ArgumentError
from petl.util.base import Table


def unpack(table, field, newfields=None, include_original=False, missing=None):
    """
    Unpack data values that are lists or tuples. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           [1, ['a', 'b']],
        ...           [2, ['c', 'd']],
        ...           [3, ['e', 'f']]]
        >>> table2 = etl.unpack(table1, 'bar', ['baz', 'quux'])
        >>> table2
        +-----+-----+------+
        | foo | baz | quux |
        +=====+=====+======+
        |   1 | 'a' | 'b'  |
        +-----+-----+------+
        |   2 | 'c' | 'd'  |
        +-----+-----+------+
        |   3 | 'e' | 'f'  |
        +-----+-----+------+

    This function will attempt to unpack exactly the number of values as
    given by the number of new fields specified. If there are more values
    than new fields, remaining values will not be unpacked. If there are less
    values than new fields, `missing` values will be added.

    See also :func:`petl.transform.unpacks.unpackdict`.

    """

    return UnpackView(table, field, newfields=newfields,
                      include_original=include_original, missing=missing)


Table.unpack = unpack


class UnpackView(Table):

    def __init__(self, source, field, newfields=None, include_original=False,
                 missing=None):
        self.source = source
        self.field = field
        self.newfields = newfields
        self.include_original = include_original
        self.missing = missing

    def __iter__(self):
        return iterunpack(self.source, self.field, self.newfields,
                          self.include_original, self.missing)


def iterunpack(source, field, newfields, include_original, missing):
    it = iter(source)

    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    if field in flds:
        field_index = flds.index(field)
    elif isinstance(field, int) and field < len(flds):
        field_index = field
        field = flds[field_index]
    else:
        raise ArgumentError('field invalid: must be either field name or index')

    # determine output fields
    outhdr = list(flds)
    if not include_original:
        outhdr.remove(field)
    if isinstance(newfields, (list, tuple)):
        outhdr.extend(newfields)
        nunpack = len(newfields)
    elif isinstance(newfields, int):
        nunpack = newfields
        newfields = [text_type(field) + text_type(i+1) for i in range(newfields)]
        outhdr.extend(newfields)
    elif newfields is None:
        nunpack = 0
    else:
        raise ArgumentError('newfields argument must be list or tuple of field '
                            'names, or int (number of values to unpack)')
    yield tuple(outhdr)

    # construct the output data
    for row in it:
        value = row[field_index]
        if include_original:
            out_row = list(row)
        else:
            out_row = [v for i, v in enumerate(row) if i != field_index]
        nvals = len(value)
        if nunpack > 0:
            if nvals >= nunpack:
                newvals = value[:nunpack]
            else:
                newvals = list(value) + ([missing] * (nunpack - nvals))
            out_row.extend(newvals)
        yield tuple(out_row)


def unpackdict(table, field, keys=None, includeoriginal=False,
               samplesize=1000, missing=None):
    """
    Unpack dictionary values into separate fields. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           [1, {'baz': 'a', 'quux': 'b'}],
        ...           [2, {'baz': 'c', 'quux': 'd'}],
        ...           [3, {'baz': 'e', 'quux': 'f'}]]
        >>> table2 = etl.unpackdict(table1, 'bar')
        >>> table2
        +-----+-----+------+
        | foo | baz | quux |
        +=====+=====+======+
        |   1 | 'a' | 'b'  |
        +-----+-----+------+
        |   2 | 'c' | 'd'  |
        +-----+-----+------+
        |   3 | 'e' | 'f'  |
        +-----+-----+------+

    See also :func:`petl.transform.unpacks.unpack`.

    """

    return UnpackDictView(table, field, keys=keys,
                          includeoriginal=includeoriginal,
                          samplesize=samplesize, missing=missing)


Table.unpackdict = unpackdict


class UnpackDictView(Table):

    def __init__(self, table, field, keys=None, includeoriginal=False,
                 samplesize=1000, missing=None):
        self.table = table
        self.field = field
        self.keys = keys
        self.includeoriginal = includeoriginal
        self.samplesize = samplesize
        self.missing = missing

    def __iter__(self):
        return iterunpackdict(self.table, self.field, self.keys,
                              self.includeoriginal, self.samplesize,
                              self.missing)


def iterunpackdict(table, field, keys, includeoriginal, samplesize, missing):

    # set up
    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    fidx = flds.index(field)
    outhdr = list(flds)
    if not includeoriginal:
        del outhdr[fidx]

    # are keys specified?
    if not keys:
        # need to sample to find keys
        sample = list(itertools.islice(it, samplesize))
        keys = set()
        for row in sample:
            try:
                keys |= set(row[fidx].keys())
            except AttributeError:
                pass
        it = itertools.chain(sample, it)
        keys = sorted(keys)
    outhdr.extend(keys)
    yield tuple(outhdr)

    # generate the data rows
    for row in it:
        outrow = list(row)
        if not includeoriginal:
            del outrow[fidx]
        for key in keys:
            try:
                outrow.append(row[fidx][key])
            except (IndexError, KeyError, TypeError):
                outrow.append(missing)
        yield tuple(outrow)
