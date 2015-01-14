"""
A module providing convenience functionality for moving to/from numpy structured
arrays.

"""


from petl.compat import next, string_types
from petl.util.base import iterpeek, ValuesView, Table
from petl.util.materialise import columns


def guessdtype(table):
    import numpy as np
    # get numpy to infer dtype
    it = iter(table)
    hdr = next(it)
    flds = list(map(str, hdr))
    rows = tuple(it)
    dtype = np.rec.array(rows).dtype
    dtype.names = flds
    return dtype


def toarray(table, dtype=None, count=-1, sample=1000):
    """
    Load data from the given `table` into a
    `numpy <http://www.numpy.org/>`_ structured array. E.g.::

        TODO
        
    If no datatype is specified, `sample` rows will be examined to infer an
    appropriate datatype for each field.
        
    The datatype can be specified as a string, e.g.:

        TODO

    The datatype can also be partially specified, in which case datatypes will
    be inferred for other fields, e.g.:
    
        TODO
    
    """
    
    import numpy as np
    it = iter(table)
    peek, it = iterpeek(it, sample)
    hdr = next(it)
    flds = list(map(str, hdr))

    if dtype is None:
        dtype = guessdtype(peek)

    elif isinstance(dtype, string_types):
        # insert field names from source table
        typestrings = [s.strip() for s in dtype.split(',')]
        dtype = [(f, t) for f, t in zip(flds, typestrings)]

    elif (isinstance(dtype, dict)
          and ('names' not in dtype or 'formats' not in dtype)):
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

    else:
        pass  # leave dtype as-is

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
    Extract a table from a numpy structured array, e.g.::

        TODO
    
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
    import numpy as np
    it = iter(vals)
    if dtype is None:
        peek, it = iterpeek(it, sample)
        dtype = np.array(peek).dtype
    a = np.fromiter(it, dtype=dtype, count=count)
    return a


ValuesView.array = valuestoarray
