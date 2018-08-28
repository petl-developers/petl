# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


from petl.compat import next, string_types
from petl.util.base import iterpeek, ValuesView, Table
from petl.util.materialise import columns


def infer_dtype(table):
    import numpy as np
    # get numpy to infer dtype
    it = iter(table)
    hdr = next(it)
    flds = list(map(str, hdr))
    rows = tuple(it)
    dtype = np.rec.array(rows).dtype
    dtype.names = flds
    return dtype


def construct_dtype(flds, peek, dtype):
    import numpy as np

    if dtype is None:
        dtype = infer_dtype(peek)

    elif isinstance(dtype, string_types):
        # insert field names from source table
        typestrings = [s.strip() for s in dtype.split(',')]
        dtype = [(f, t) for f, t in zip(flds, typestrings)]

    elif (isinstance(dtype, dict) and
          ('names' not in dtype or 'formats' not in dtype)):
        # allow for partial specification of dtype
        cols = columns(peek)
        newdtype = {'names': [], 'formats': []}
        for f in flds:
            newdtype['names'].append(f)
            if f in dtype and isinstance(dtype[f], tuple):
                # assume fully specified
                newdtype['formats'].append(dtype[f][0])
            elif f not in dtype:
                # not specified at all
                a = np.array(cols[f])
                newdtype['formats'].append(a.dtype)
            else:
                # assume directly specified, just need to add offset
                newdtype['formats'].append(dtype[f])
        dtype = newdtype

    return dtype


def toarray(table, dtype=None, count=-1, sample=1000):
    """
    Load data from the given `table` into a
    `numpy <http://www.numpy.org/>`_ structured array. E.g.::

        >>> import petl as etl
        >>> table = [('foo', 'bar', 'baz'),
        ...          ('apples', 1, 2.5),
        ...          ('oranges', 3, 4.4),
        ...          ('pears', 7, .1)]
        >>> a = etl.toarray(table)
        >>> a
        array([('apples', 1, 2.5), ('oranges', 3, 4.4), ('pears', 7, 0.1)],
              dtype=(numpy.record, [('foo', '<U7'), ('bar', '<i8'), ('baz', '<f8')]))
        >>> # the dtype can be specified as a string
        ... a = etl.toarray(table, dtype='a4, i2, f4')
        >>> a
        array([(b'appl', 1, 2.5), (b'oran', 3, 4.4), (b'pear', 7, 0.1)],
              dtype=[('foo', 'S4'), ('bar', '<i2'), ('baz', '<f4')])
        >>> # the dtype can also be partially specified
        ... a = etl.toarray(table, dtype={'foo': 'a4'})
        >>> a
        array([(b'appl', 1, 2.5), (b'oran', 3, 4.4), (b'pear', 7, 0.1)],
              dtype=[('foo', 'S4'), ('bar', '<i8'), ('baz', '<f8')])

    If the dtype is not completely specified, `sample` rows will be
    examined to infer an appropriate dtype.

    """

    import numpy as np
    it = iter(table)
    peek, it = iterpeek(it, sample)
    hdr = next(it)
    flds = list(map(str, hdr))
    dtype = construct_dtype(flds, peek, dtype)

    # numpy is fussy about having tuples, need to make sure
    it = (tuple(row) for row in it)
    sa = np.fromiter(it, dtype=dtype, count=count)

    return sa


Table.toarray = toarray


def torecarray(*args, **kwargs):
    """
    Convenient shorthand for ``toarray(*args, **kwargs).view(np.recarray)``.

    """

    import numpy as np
    return toarray(*args, **kwargs).view(np.recarray)


Table.torecarray = torecarray


def fromarray(a):
    """
    Extract a table from a `numpy <http://www.numpy.org/>`_ structured array,
    e.g.::

        >>> import petl as etl
        >>> import numpy as np
        >>> a = np.array([('apples', 1, 2.5),
        ...               ('oranges', 3, 4.4),
        ...               ('pears', 7, 0.1)],
        ...              dtype='U8, i4,f4')
        >>> table = etl.fromarray(a)
        >>> table
        +-----------+----+-----+
        | f0        | f1 | f2  |
        +===========+====+=====+
        | 'apples'  | 1  | 2.5 |
        +-----------+----+-----+
        | 'oranges' | 3  | 4.4 |
        +-----------+----+-----+
        | 'pears'   | 7  | 0.1 |
        +-----------+----+-----+

    """

    return ArrayView(a)


class ArrayView(Table):

    def __init__(self, a):
        self.a = a

    def __iter__(self):
        yield tuple(self.a.dtype.names)
        for row in self.a:
            yield tuple(row)


def valuestoarray(vals, dtype=None, count=-1, sample=1000):
    """
    Load values from a table column into a `numpy <http://www.numpy.org/>`_
    array, e.g.::

        >>> import petl as etl
        >>> table = [('foo', 'bar', 'baz'),
        ...          ('apples', 1, 2.5),
        ...          ('oranges', 3, 4.4),
        ...          ('pears', 7, .1)]
        >>> table = etl.wrap(table)
        >>> table.values('bar').array()
        array([1, 3, 7])
        >>> # specify dtype
        ... table.values('bar').array(dtype='i4')
        array([1, 3, 7], dtype=int32)

    """

    import numpy as np
    it = iter(vals)
    if dtype is None:
        peek, it = iterpeek(it, sample)
        dtype = np.array(peek).dtype
    a = np.fromiter(it, dtype=dtype, count=count)
    return a


ValuesView.toarray = valuestoarray
ValuesView.array = valuestoarray
