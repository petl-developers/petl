__author__ = 'aliman'


import itertools


from petl.util import RowContainer


def unpack(table, field, newfields=None, include_original=False, missing=None):
    """
    Unpack data values that are lists or tuples. E.g.::

        >>> from petl import unpack, look
        >>> look(table1)
        +-------+------------+
        | 'foo' | 'bar'      |
        +=======+============+
        | 1     | ['a', 'b'] |
        +-------+------------+
        | 2     | ['c', 'd'] |
        +-------+------------+
        | 3     | ['e', 'f'] |
        +-------+------------+

        >>> table2 = unpack(table1, 'bar', ['baz', 'quux'])
        >>> look(table2)
        +-------+-------+--------+
        | 'foo' | 'baz' | 'quux' |
        +=======+=======+========+
        | 1     | 'a'   | 'b'    |
        +-------+-------+--------+
        | 2     | 'c'   | 'd'    |
        +-------+-------+--------+
        | 3     | 'e'   | 'f'    |
        +-------+-------+--------+

        >>> table3 = unpack(table1, 'bar', 2)
        >>> look(table3)
        +-------+--------+--------+
        | 'foo' | 'bar1' | 'bar2' |
        +=======+========+========+
        | 1     | 'a'    | 'b'    |
        +-------+--------+--------+
        | 2     | 'c'    | 'd'    |
        +-------+--------+--------+
        | 3     | 'e'    | 'f'    |
        +-------+--------+--------+


    See also :func:`unpackdict`.

    .. versionchanged:: 0.23

    This function will attempt to unpack exactly the number of values as
    given by the number of new fields specified. If
    there are more values than new fields, remaining values will not be
    unpacked. If there are less values than new
    fields, missing values will be added.

    """

    return UnpackView(table, field, newfields=newfields,
                      include_original=include_original, missing=missing)


class UnpackView(RowContainer):

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

    flds = it.next()
    if field in flds:
        field_index = flds.index(field)
    elif isinstance(field, int) and field < len(flds):
        field_index = field
        field = flds[field_index]
    else:
        raise Exception('field invalid: must be either field name or index')

    # determine output fields
    out_flds = list(flds)
    if not include_original:
        out_flds.remove(field)
    if isinstance(newfields, (list, tuple)):
        out_flds.extend(newfields)
        nunpack = len(newfields)
    elif isinstance(newfields, int):
        nunpack = newfields
        newfields = [str(field) + str(i+1) for i in range(newfields)]
        out_flds.extend(newfields)
    elif newfields is None:
        nunpack = 0
    else:
        raise Exception('newfields argument must be list or tuple of field names, or int (number of values to unpack)')
    yield tuple(out_flds)

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

        >>> from petl import unpackdict, look
        >>> look(table1)
        +-------+---------------------------+
        | 'foo' | 'bar'                     |
        +=======+===========================+
        | 1     | {'quux': 'b', 'baz': 'a'} |
        +-------+---------------------------+
        | 2     | {'quux': 'd', 'baz': 'c'} |
        +-------+---------------------------+
        | 3     | {'quux': 'f', 'baz': 'e'} |
        +-------+---------------------------+

        >>> table2 = unpackdict(table1, 'bar')
        >>> look(table2)
        +-------+-------+--------+
        | 'foo' | 'baz' | 'quux' |
        +=======+=======+========+
        | 1     | 'a'   | 'b'    |
        +-------+-------+--------+
        | 2     | 'c'   | 'd'    |
        +-------+-------+--------+
        | 3     | 'e'   | 'f'    |
        +-------+-------+--------+

    .. versionadded:: 0.10

    """

    return UnpackDictView(table, field, keys=keys,
                          includeoriginal=includeoriginal,
                          samplesize=samplesize, missing=missing)


class UnpackDictView(RowContainer):

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
    fields = it.next()
    fidx = fields.index(field)
    outfields = list(fields)
    if not includeoriginal:
        del outfields[fidx]

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
    outfields.extend(keys)
    yield tuple(outfields)

    # generate the data rows
    for row in it:
        outrow = list(row)
        if not includeoriginal:
            del outrow[fidx]
        for key in keys:
            try:
                outrow.append(row[fidx][key])
            except:
                outrow.append(missing)
        yield tuple(outrow)


