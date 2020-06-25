# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division
import itertools


from petl.compat import string_types, text_type
from petl.util.base import Table, iterpeek
from petl.io.numpy import construct_dtype


def frombcolz(source, expression=None, outcols=None, limit=None, skip=0):
    """Extract a table from a bcolz ctable, e.g.::

        >>> import petl as etl
        >>>
        >>> def example_from_bcolz():
        ...     import bcolz
        ...     cols = [
        ...         ['apples', 'oranges', 'pears'],
        ...         [1, 3, 7],
        ...         [2.5, 4.4, .1]
        ...     ]
        ...     names = ('foo', 'bar', 'baz')
        ...     ctbl = bcolz.ctable(cols, names=names)
        ...     return etl.frombcolz(ctbl)
        >>>
        >>> example_from_bcolz() # doctest: +SKIP
        +-----------+-----+-----+
        | foo       | bar | baz |
        +===========+=====+=====+
        | 'apples'  |   1 | 2.5 |
        +-----------+-----+-----+
        | 'oranges' |   3 | 4.4 |
        +-----------+-----+-----+
        | 'pears'   |   7 | 0.1 |
        +-----------+-----+-----+

    If `expression` is provided it will be executed by bcolz and only
    matching rows returned, e.g.::

        >>> tbl2 = etl.frombcolz(ctbl, expression='bar > 1') # doctest: +SKIP
        >>> tbl2 # doctest: +SKIP
        +-----------+-----+-----+
        | foo       | bar | baz |
        +===========+=====+=====+
        | 'oranges' |   3 | 4.4 |
        +-----------+-----+-----+
        | 'pears'   |   7 | 0.1 |
        +-----------+-----+-----+

    .. versionadded:: 1.1.0

    """

    return BcolzView(source, expression=expression, outcols=outcols,
                     limit=limit, skip=skip)


class BcolzView(Table):

    def __init__(self, source, expression=None, outcols=None, limit=None,
                 skip=0):
        self.source = source
        self.expression = expression
        self.outcols = outcols
        self.limit = limit
        self.skip = skip

    def __iter__(self):

        # obtain ctable
        if isinstance(self.source, string_types):
            import bcolz
            ctbl = bcolz.open(self.source, mode='r')
        else:
            # assume bcolz ctable
            ctbl = self.source

        # obtain header
        if self.outcols is None:
            header = tuple(ctbl.names)
        else:
            header = tuple(self.outcols)
            assert all(h in ctbl.names for h in header), 'invalid outcols'
        yield header

        # obtain iterator
        if self.expression is None:
            it = ctbl.iter(outcols=self.outcols, skip=self.skip,
                           limit=self.limit)
        else:
            it = ctbl.where(self.expression, outcols=self.outcols, skip=self.skip,
                           limit=self.limit)

        for row in it:
            yield row


def tobcolz(table, dtype=None, sample=1000, **kwargs):
    """Load data into a bcolz ctable, e.g.::

        >>> import petl as etl
        >>>
        >>> def example_to_bcolz():
        ...     table = [('foo', 'bar', 'baz'),
        ...              ('apples', 1, 2.5),
        ...              ('oranges', 3, 4.4),
        ...              ('pears', 7, .1)]
        ...     return etl.tobcolz(table)
        >>> 
        >>> ctbl = example_to_bcolz() # doctest: +SKIP
        >>> ctbl # doctest: +SKIP
        ctable((3,), [('foo', '<U7'), ('bar', '<i8'), ('baz', '<f8')])
          nbytes: 132; cbytes: 1023.98 KB; ratio: 0.00
          cparams := cparams(clevel=5, shuffle=1, cname='lz4', quantize=0)
        [('apples', 1, 2.5) ('oranges', 3, 4.4) ('pears', 7, 0.1)]
        >>> ctbl.names # doctest: +SKIP
        ['foo', 'bar', 'baz']
        >>> ctbl['foo'] # doctest: +SKIP
        carray((3,), <U7)
          nbytes := 84; cbytes := 511.98 KB; ratio: 0.00
          cparams := cparams(clevel=5, shuffle=1, cname='lz4', quantize=0)
          chunklen := 18724; chunksize: 524272; blocksize: 0
        ['apples' 'oranges' 'pears']

    Other keyword arguments are passed through to the ctable constructor.

    .. versionadded:: 1.1.0

    """

    import bcolz
    import numpy as np

    it = iter(table)
    peek, it = iterpeek(it, sample)
    hdr = next(it)
    # numpy is fussy about having tuples, need to make sure
    it = (tuple(row) for row in it)
    flds = list(map(text_type, hdr))
    dtype = construct_dtype(flds, peek, dtype)

    # create ctable
    kwargs.setdefault('expectedlen', 1000000)
    kwargs.setdefault('mode', 'w')
    ctbl = bcolz.ctable(np.array([], dtype=dtype), **kwargs)

    # fill chunk-wise
    chunklen = sum(ctbl.cols[name].chunklen
                   for name in ctbl.names) // len(ctbl.names)
    while True:
        data = list(itertools.islice(it, chunklen))
        data = np.array(data, dtype=dtype)
        ctbl.append(data)
        if len(data) < chunklen:
            break

    ctbl.flush()
    return ctbl


def appendbcolz(table, obj, check_names=True):
    """Append data into a bcolz ctable. The `obj` argument can be either an
    existing ctable or the name of a directory were an on-disk ctable is
    stored.

    .. versionadded:: 1.1.0

    """

    import bcolz
    import numpy as np

    if isinstance(obj, string_types):
        ctbl = bcolz.open(obj, mode='a')
    else:
        assert hasattr(obj, 'append') and hasattr(obj, 'names'), \
            'expected rootdir or ctable, found %r' % obj
        ctbl = obj

    # setup
    dtype = ctbl.dtype
    it = iter(table)
    hdr = next(it)
    flds = list(map(text_type, hdr))

    # check names match
    if check_names:
        assert tuple(flds) == tuple(ctbl.names), 'column names do not match'

    # fill chunk-wise
    chunklen = sum(ctbl.cols[name].chunklen
                   for name in ctbl.names) // len(ctbl.names)
    while True:
        data = list(itertools.islice(it, chunklen))
        data = np.array(data, dtype=dtype)
        ctbl.append(data)
        if len(data) < chunklen:
            break

    ctbl.flush()
    return ctbl
