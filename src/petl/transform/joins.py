__author__ = 'aliman'


import itertools
import operator


from petl.util import RowContainer, asindices, rowgetter, rowgroupby, header,\
    data
from petl.transform.sorts import sort
from petl.transform.basics import cut, cutout
from petl.transform.dedup import distinct


def natural_key(left, right):
    # determine key field or fields
    lflds = header(left)
    rflds = header(right)
    key = [f for f in lflds if f in rflds]
    assert len(key) > 0, 'no fields in common'
    if len(key) == 1:
        key = key[0]  # deal with singletons
    return key


def keys_from_args(left, right, key, lkey, rkey):

    if key is lkey is rkey is None:
        # no keys specified, attempt natural join
        lkey = rkey = natural_key(left, right)
    elif key is not None and lkey is rkey is None:
        # common key specified
        lkey = rkey = key
    elif key is None and lkey is not None and rkey is not None:
        # left and right keys specified
        pass
    else:
        raise Exception(
            'bad key arguments: either specify key, or specify both lkey and '
            'rkey, or provide no key/lkey/rkey arguments at all (natural join)'
        )
    return lkey, rkey


def join(left, right, key=None, lkey=None, rkey=None, presorted=False,
         buffersize=None, tempdir=None, cache=True, lprefix=None, rprefix=None):
    """
    Perform an equi-join on the given tables. E.g.::

        >>> from petl import join, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+

        >>> look(table2)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 3    | 'square'  |
        +------+-----------+
        | 4    | 'ellipse' |
        +------+-----------+

        >>> table3 = join(table1, table2, key='id')
        >>> look(table3)
        +------+----------+----------+
        | 'id' | 'colour' | 'shape'  |
        +======+==========+==========+
        | 1    | 'blue'   | 'circle' |
        +------+----------+----------+
        | 3    | 'purple' | 'square' |
        +------+----------+----------+

        >>>  # if no key is given, a natural join is tried
        ... table4 = join(table1, table2)
        >>> look(table4)
        +------+----------+----------+
        | 'id' | 'colour' | 'shape'  |
        +======+==========+==========+
        | 1    | 'blue'   | 'circle' |
        +------+----------+----------+
        | 3    | 'purple' | 'square' |
        +------+----------+----------+

        >>>  # note behaviour if the key is not unique in either or both tables
        ... look(table5)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 1    | 'red'    |
        +------+----------+
        | 2    | 'purple' |
        +------+----------+

        >>> look(table6)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 1    | 'square'  |
        +------+-----------+
        | 2    | 'ellipse' |
        +------+-----------+

        >>> table7 = join(table5, table6, key='id')
        >>> look(table7)
        +------+----------+-----------+
        | 'id' | 'colour' | 'shape'   |
        +======+==========+===========+
        | 1    | 'blue'   | 'circle'  |
        +------+----------+-----------+
        | 1    | 'blue'   | 'square'  |
        +------+----------+-----------+
        | 1    | 'red'    | 'circle'  |
        +------+----------+-----------+
        | 1    | 'red'    | 'square'  |
        +------+----------+-----------+
        | 2    | 'purple' | 'ellipse' |
        +------+----------+-----------+

        >>>  # compound keys are supported
        ... look(table8)
        +------+--------+----------+
        | 'id' | 'time' | 'height' |
        +======+========+==========+
        | 1    | 1      | 12.3     |
        +------+--------+----------+
        | 1    | 2      | 34.5     |
        +------+--------+----------+
        | 2    | 1      | 56.7     |
        +------+--------+----------+

        >>> look(table9)
        +------+--------+----------+
        | 'id' | 'time' | 'weight' |
        +======+========+==========+
        | 1    | 2      | 4.5      |
        +------+--------+----------+
        | 2    | 1      | 6.7      |
        +------+--------+----------+
        | 2    | 2      | 8.9      |
        +------+--------+----------+

        >>> table10 = join(table8, table9, key=['id', 'time'])
        >>> look(table10)
        +------+--------+----------+----------+
        | 'id' | 'time' | 'height' | 'weight' |
        +======+========+==========+==========+
        | 1    | 2      | 34.5     | 4.5      |
        +------+--------+----------+----------+
        | 2    | 1      | 56.7     | 6.7      |
        +------+--------+----------+----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and
    `cache` arguments under the
    :func:`sort` function.

    .. versionchanged:: 0.24

    Added support for left and right tables with different key fields via the
    `lkey` and `rkey` arguments.

    """

    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return JoinView(left, right, lkey=lkey, rkey=rkey,
                    presorted=presorted, buffersize=buffersize, tempdir=tempdir,
                    cache=cache, lprefix=lprefix, rprefix=rprefix)


class JoinView(RowContainer):

    def __init__(self, left, right, lkey, rkey,
                 presorted=False, leftouter=False, rightouter=False,
                 missing=None, buffersize=None, tempdir=None, cache=True,
                 lprefix=None, rprefix=None):
        self.lkey = lkey
        self.rkey = rkey
        if presorted:
            self.left = left
            self.right = right
        else:
            self.left = sort(left, lkey, buffersize=buffersize,
                             tempdir=tempdir, cache=cache)
            self.right = sort(right, rkey, buffersize=buffersize,
                              tempdir=tempdir, cache=cache)
        self.leftouter = leftouter
        self.rightouter = rightouter
        self.missing = missing
        self.lprefix = lprefix
        self.rprefix = rprefix

    def __iter__(self):
        return iterjoin(self.left, self.right, self.lkey, self.rkey,
                        leftouter=self.leftouter, rightouter=self.rightouter,
                        missing=self.missing, lprefix=self.lprefix,
                        rprefix=self.rprefix)


def leftjoin(left, right, key=None, lkey=None, rkey=None, missing=None,
             presorted=False, buffersize=None, tempdir=None, cache=True,
             lprefix=None, rprefix=None):
    """
    Perform a left outer join on the given tables. E.g.::

        >>> from petl import leftjoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+

        >>> look(table2)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 3    | 'square'  |
        +------+-----------+
        | 4    | 'ellipse' |
        +------+-----------+

        >>> table3 = leftjoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+----------+
        | 'id' | 'colour' | 'shape'  |
        +======+==========+==========+
        | 1    | 'blue'   | 'circle' |
        +------+----------+----------+
        | 2    | 'red'    | None     |
        +------+----------+----------+
        | 3    | 'purple' | 'square' |
        +------+----------+----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and
    `cache` arguments under the
    :func:`sort` function.

    .. versionchanged:: 0.24

    Added support for left and right tables with different key fields via the
    `lkey` and `rkey` arguments.

    """

    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return JoinView(left, right, lkey=lkey, rkey=rkey,
                    presorted=presorted, leftouter=True, rightouter=False,
                    missing=missing, buffersize=buffersize, tempdir=tempdir,
                    cache=cache, lprefix=lprefix, rprefix=rprefix)


def rightjoin(left, right, key=None, lkey=None, rkey=None, missing=None,
              presorted=False, buffersize=None, tempdir=None, cache=True,
              lprefix=None, rprefix=None):
    """
    Perform a right outer join on the given tables. E.g.::

        >>> from petl import rightjoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+

        >>> look(table2)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 3    | 'square'  |
        +------+-----------+
        | 4    | 'ellipse' |
        +------+-----------+

        >>> table3 = rightjoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+-----------+
        | 'id' | 'colour' | 'shape'   |
        +======+==========+===========+
        | 1    | 'blue'   | 'circle'  |
        +------+----------+-----------+
        | 3    | 'purple' | 'square'  |
        +------+----------+-----------+
        | 4    | None     | 'ellipse' |
        +------+----------+-----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and
    `cache` arguments under the
    :func:`sort` function.

    .. versionchanged:: 0.24

    Added support for left and right tables with different key fields via the
    `lkey` and `rkey` arguments.

    """

    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return JoinView(left, right, lkey=lkey, rkey=rkey,
                    presorted=presorted, leftouter=False, rightouter=True,
                    missing=missing, buffersize=buffersize,
                    tempdir=tempdir, cache=cache, lprefix=lprefix,
                    rprefix=rprefix)


def outerjoin(left, right, key=None, lkey=None, rkey=None, missing=None,
              presorted=False, buffersize=None, tempdir=None, cache=True,
              lprefix=None, rprefix=None):
    """
    Perform a full outer join on the given tables. E.g.::

        >>> from petl import outerjoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+

        >>> look(table2)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 3    | 'square'  |
        +------+-----------+
        | 4    | 'ellipse' |
        +------+-----------+

        >>> table3 = outerjoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+-----------+
        | 'id' | 'colour' | 'shape'   |
        +======+==========+===========+
        | 1    | 'blue'   | 'circle'  |
        +------+----------+-----------+
        | 2    | 'red'    | None      |
        +------+----------+-----------+
        | 3    | 'purple' | 'square'  |
        +------+----------+-----------+
        | 4    | None     | 'ellipse' |
        +------+----------+-----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and
    `cache` arguments under the
    :func:`sort` function.

    .. versionchanged:: 0.24

    Added support for left and right tables with different key fields via the
    `lkey` and `rkey` arguments.

    """

    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return JoinView(left, right, lkey=lkey, rkey=rkey,
                    presorted=presorted, leftouter=True, rightouter=True,
                    missing=missing, buffersize=buffersize, tempdir=tempdir,
                    cache=cache, lprefix=lprefix, rprefix=rprefix)


def iterjoin(left, right, lkey, rkey, leftouter=False, rightouter=False,
             missing=None, lprefix=None, rprefix=None):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()

    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, lkey)
    rkind = asindices(rflds, rkey)

    # construct functions to extract key values from both tables
    lgetk = operator.itemgetter(*lkind)
    rgetk = operator.itemgetter(*rkind)

    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)

    # determine the output fields
    if lprefix is None:
        outflds = list(lflds)
    else:
        outflds = [(str(lprefix) + str(f))
                   for f in lflds]
    if rprefix is None:
        outflds.extend(rgetv(rflds))
    else:
        outflds.extend([(str(rprefix) + str(f)) for f in rgetv(rflds)])
    yield tuple(outflds)

    # define a function to join two groups of rows
    def joinrows(_lrowgrp, _rrowgrp):
        if _rrowgrp is None:
            for lrow in _lrowgrp:
                outrow = list(lrow)  # start with the left row
                # extend with missing values in place of the right row
                outrow.extend([missing] * len(rvind))
                yield tuple(outrow)
        elif _lrowgrp is None:
            for rrow in _rrowgrp:
                # start with missing values in place of the left row
                outrow = [missing] * len(lflds)
                # set key values
                for li, ri in zip(lkind, rkind):
                    outrow[li] = rrow[ri]
                # extend with non-key values from the right row
                outrow.extend(rgetv(rrow))
                yield tuple(outrow)
        else:
            _rrowgrp = list(_rrowgrp)  # may need to iterate more than once
            for lrow in _lrowgrp:
                for rrow in _rrowgrp:
                    # start with the left row
                    outrow = list(lrow)
                    # extend with non-key values from the right row
                    outrow.extend(rgetv(rrow))
                    yield tuple(outrow)

    # construct group iterators for both tables
    lgit = itertools.groupby(lit, key=lgetk)
    rgit = itertools.groupby(rit, key=rgetk)

    # loop until *either* of the iterators is exhausted
    lkval, rkval = None, None  # initialise here to handle empty tables
    try:

        # pick off initial row groups
        lkval, lrowgrp = lgit.next()
        rkval, rrowgrp = rgit.next()

        while True:
            if lkval < rkval:
                if leftouter:
                    for row in joinrows(lrowgrp, None):
                        yield tuple(row)
                # advance left
                lkval, lrowgrp = lgit.next()
            elif lkval > rkval:
                if rightouter:
                    for row in joinrows(None, rrowgrp):
                        yield tuple(row)
                # advance right
                rkval, rrowgrp = rgit.next()
            else:
                for row in joinrows(lrowgrp, rrowgrp):
                    yield tuple(row)
                # advance both
                lkval, lrowgrp = lgit.next()
                rkval, rrowgrp = rgit.next()

    except StopIteration:
        pass

    # make sure any left rows remaining are yielded
    if leftouter:
        if lkval > rkval:
            # yield anything that got left hanging
            for row in joinrows(lrowgrp, None):
                yield tuple(row)
        # yield the rest
        for lkval, lrowgrp in lgit:
            for row in joinrows(lrowgrp, None):
                yield tuple(row)

    # make sure any right rows remaining are yielded
    if rightouter:
        if lkval < rkval:
            # yield anything that got left hanging
            for row in joinrows(None, rrowgrp):
                yield tuple(row)
        # yield the rest
        for rkval, rrowgrp in rgit:
            for row in joinrows(None, rrowgrp):
                yield tuple(row)


def crossjoin(*tables, **kwargs):
    """
    Form the cartesian product of the given tables. E.g.::

        >>> from petl import crossjoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+

        >>> look(table2)
        +------+----------+
        | 'id' | 'shape'  |
        +======+==========+
        | 1    | 'circle' |
        +------+----------+
        | 3    | 'square' |
        +------+----------+

        >>> table3 = crossjoin(table1, table2)
        >>> look(table3)
        +------+----------+------+----------+
        | 'id' | 'colour' | 'id' | 'shape'  |
        +======+==========+======+==========+
        | 1    | 'blue'   | 1    | 'circle' |
        +------+----------+------+----------+
        | 1    | 'blue'   | 3    | 'square' |
        +------+----------+------+----------+
        | 2    | 'red'    | 1    | 'circle' |
        +------+----------+------+----------+
        | 2    | 'red'    | 3    | 'square' |
        +------+----------+------+----------+

    See also :func:`join`, :func:`leftjoin`, :func:`rightjoint`, :func:`outerjoin`.

    """

    return CrossJoinView(*tables, **kwargs)


class CrossJoinView(RowContainer):

    def __init__(self, *sources, **kwargs):
        self.sources = sources
        self.prefix = kwargs.get('prefix', False)

    def __iter__(self):
        return itercrossjoin(self.sources, self.prefix)


def itercrossjoin(sources, prefix):

    # construct fields
    outflds = list()
    for i, s in enumerate(sources):
        if prefix:
            # use one-based numbering
            outflds.extend([str(i+1) + '_' + str(f) for f in header(s)])
        else:
            outflds.extend(header(s))
    yield tuple(outflds)

    datasrcs = [data(src) for src in sources]
    for prod in itertools.product(*datasrcs):
        outrow = list()
        for row in prod:
            outrow.extend(row)
        yield tuple(outrow)


def antijoin(left, right, key=None, lkey=None, rkey=None, presorted=False,
             buffersize=None, tempdir=None, cache=True):
    """
    Return rows from the `left` table where the key value does not occur in the
    `right` table. E.g.::

        >>> from petl import antijoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 0    | 'black'  |
        +------+----------+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 4    | 'yellow' |
        +------+----------+
        | 5    | 'white'  |
        +------+----------+

        >>> look(table2)
        +------+----------+
        | 'id' | 'shape'  |
        +======+==========+
        | 1    | 'circle' |
        +------+----------+
        | 3    | 'square' |
        +------+----------+

        >>> table3 = antijoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 0    | 'black'  |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 4    | 'yellow' |
        +------+----------+
        | 5    | 'white'  |
        +------+----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the
    :func:`sort` function.

    .. versionchanged:: 0.24

    Added support for left and right tables with different key fields via the
    `lkey` and `rkey` arguments.

    """

    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return AntiJoinView(left, right, lkey, rkey, presorted, buffersize)


class AntiJoinView(RowContainer):

    def __init__(self, left, right, lkey, rkey, presorted=False,
                 buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.left = left
            self.right = right
        else:
            self.left = sort(left, lkey, buffersize=buffersize,
                             tempdir=tempdir, cache=cache)
            self.right = sort(right, rkey, buffersize=buffersize,
                              tempdir=tempdir, cache=cache)
        self.lkey = lkey
        self.rkey = rkey

    def __iter__(self):
        return iterantijoin(self.left, self.right, self.lkey, self.rkey)


def iterantijoin(left, right, lkey, rkey):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    yield tuple(lflds)

    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, lkey)
    rkind = asindices(rflds, rkey)

    # construct functions to extract key values from both tables
    lgetk = operator.itemgetter(*lkind)
    rgetk = operator.itemgetter(*rkind)

    # construct group iterators for both tables
    lgit = itertools.groupby(lit, key=lgetk)
    rgit = itertools.groupby(rit, key=rgetk)

    # loop until *either* of the iterators is exhausted
    lkval, rkval = None, None  # initialise here to handle empty tables
    try:

        # pick off initial row groups
        lkval, lrowgrp = lgit.next()
        rkval, _ = rgit.next()

        while True:
            if lkval < rkval:
                for row in lrowgrp:
                    yield tuple(row)
                # advance left
                lkval, lrowgrp = lgit.next()
            elif lkval > rkval:
                # advance right
                rkval, _ = rgit.next()
            else:
                # advance both
                lkval, lrowgrp = lgit.next()
                rkval, _ = rgit.next()

    except StopIteration:
        pass

    # any left over?
    if lkval > rkval:
        # yield anything that got left hanging
        for row in lrowgrp:
            yield tuple(row)
    # and the rest...
    for lkval, lrowgrp in lgit:
        for row in lrowgrp:
            yield tuple(row)


def lookupjoin(left, right, key=None, lkey=None, rkey=None, missing=None,
               presorted=False, buffersize=None, tempdir=None, cache=True,
               lprefix=None, rprefix=None):
    """
    Perform a left join, but where the key is not unique in the right-hand
    table, arbitrarily choose the first row and ignore others. E.g.::

        >>> from petl import lookupjoin, look
        >>> look(table1)
        +------+----------+--------+
        | 'id' | 'color'  | 'cost' |
        +======+==========+========+
        | 1    | 'blue'   | 12     |
        +------+----------+--------+
        | 2    | 'red'    | 8      |
        +------+----------+--------+
        | 3    | 'purple' | 4      |
        +------+----------+--------+

        >>> look(table2)
        +------+-----------+---------+
        | 'id' | 'shape'   | 'size'  |
        +======+===========+=========+
        | 1    | 'circle'  | 'big'   |
        +------+-----------+---------+
        | 1    | 'circle'  | 'small' |
        +------+-----------+---------+
        | 2    | 'square'  | 'tiny'  |
        +------+-----------+---------+
        | 2    | 'square'  | 'big'   |
        +------+-----------+---------+
        | 3    | 'ellipse' | 'small' |
        +------+-----------+---------+
        | 3    | 'ellipse' | 'tiny'  |
        +------+-----------+---------+

        >>> table3 = lookupjoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+--------+-----------+---------+
        | 'id' | 'color'  | 'cost' | 'shape'   | 'size'  |
        +======+==========+========+===========+=========+
        | 1    | 'blue'   | 12     | 'circle'  | 'big'   |
        +------+----------+--------+-----------+---------+
        | 2    | 'red'    | 8      | 'square'  | 'tiny'  |
        +------+----------+--------+-----------+---------+
        | 3    | 'purple' | 4      | 'ellipse' | 'small' |
        +------+----------+--------+-----------+---------+

    See also :func:`leftjoin`.

    .. versionadded:: 0.11

    .. versionchanged:: 0.24

    Added support for left and right tables with different key fields via the
    `lkey` and `rkey` arguments.

    """

    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return LookupJoinView(left, right, lkey, rkey, presorted=presorted,
                          missing=missing, buffersize=buffersize,
                          tempdir=tempdir, cache=cache,
                          lprefix=lprefix, rprefix=rprefix)


class LookupJoinView(RowContainer):

    def __init__(self, left, right, lkey, rkey, presorted=False, missing=None,
                 buffersize=None, tempdir=None, cache=True,
                 lprefix=None, rprefix=None):
        if presorted:
            self.left = left
            self.right = right
        else:
            self.left = sort(left, lkey, buffersize=buffersize,
                             tempdir=tempdir, cache=cache)
            self.right = sort(right, rkey, buffersize=buffersize,
                              tempdir=tempdir, cache=cache)
        self.lkey = lkey
        self.rkey = rkey
        self.missing = missing
        self.lprefix = lprefix
        self.rprefix = rprefix

    def __iter__(self):
        return iterlookupjoin(self.left, self.right, self.lkey, self.rkey,
                              missing=self.missing, lprefix=self.lprefix,
                              rprefix=self.rprefix)


def iterlookupjoin(left, right, lkey, rkey, missing=None, lprefix=None,
                   rprefix=None):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()

    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, lkey)
    rkind = asindices(rflds, rkey)

    # construct functions to extract key values from both tables
    lgetk = operator.itemgetter(*lkind)
    rgetk = operator.itemgetter(*rkind)

    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)

    # determine the output fields
    if lprefix is None:
        outflds = list(lflds)
    else:
        outflds = [(str(lprefix) + str(f))
                   for f in lflds]
    if rprefix is None:
        outflds.extend(rgetv(rflds))
    else:
        outflds.extend([(str(rprefix) + str(f)) for f in rgetv(rflds)])
    yield tuple(outflds)

    # define a function to join two groups of rows
    def joinrows(lrowgrp, rrowgrp):
        if rrowgrp is None:
            for lrow in lrowgrp:
                outrow = list(lrow)  # start with the left row
                # extend with missing values in place of the right row
                outrow.extend([missing] * len(rvind))
                yield tuple(outrow)
        else:
            rrow = iter(rrowgrp).next()  # pick first arbitrarily
            for lrow in lrowgrp:
                # start with the left row
                outrow = list(lrow)
                # extend with non-key values from the right row
                outrow.extend(rgetv(rrow))
                yield tuple(outrow)

    # construct group iterators for both tables
    lgit = itertools.groupby(lit, key=lgetk)
    rgit = itertools.groupby(rit, key=rgetk)

    # loop until *either* of the iterators is exhausted
    lkval, rkval = None, None  # initialise here to handle empty tables
    try:

        # pick off initial row groups
        lkval, lrowgrp = lgit.next()
        rkval, rrowgrp = rgit.next()

        while True:
            if lkval < rkval:
                for row in joinrows(lrowgrp, None):
                    yield tuple(row)
                # advance left
                lkval, lrowgrp = lgit.next()
            elif lkval > rkval:
                # advance right
                rkval, rrowgrp = rgit.next()
            else:
                for row in joinrows(lrowgrp, rrowgrp):
                    yield tuple(row)
                # advance both
                lkval, lrowgrp = lgit.next()
                rkval, rrowgrp = rgit.next()

    except StopIteration:
        pass

    # make sure any left rows remaining are yielded
    if lkval > rkval:
        # yield anything that got left hanging
        for row in joinrows(lrowgrp, None):
            yield tuple(row)
    # yield the rest
    for lkval, lrowgrp in lgit:
        for row in joinrows(lrowgrp, None):
            yield tuple(row)


def unjoin(table, value, key=None, autoincrement=(1, 1), presorted=False,
           buffersize=None, tempdir=None, cache=True):
    """
    Split a table into two tables by reversing an inner join.

    E.g., if the join key is present in the table::

        >>> from petl import look, unjoin
        >>> look(table1)
        +-------+-------+----------+
        | 'foo' | 'bar' | 'baz'    |
        +=======+=======+==========+
        | 'A'   | 1     | 'apple'  |
        +-------+-------+----------+
        | 'B'   | 1     | 'apple'  |
        +-------+-------+----------+
        | 'C'   | 2     | 'orange' |
        +-------+-------+----------+

        >>> table2, table3 = unjoin(table1, 'baz', key='bar')
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 1     |
        +-------+-------+
        | 'B'   | 1     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+

        >>> look(table3)
        +-------+----------+
        | 'bar' | 'baz'    |
        +=======+==========+
        | 1     | 'apple'  |
        +-------+----------+
        | 2     | 'orange' |
        +-------+----------+

    An integer join key can also be reconstructed, e.g.::

        >>> look(table4)
        +-------+----------+
        | 'foo' | 'bar'    |
        +=======+==========+
        | 'A'   | 'apple'  |
        +-------+----------+
        | 'B'   | 'apple'  |
        +-------+----------+
        | 'C'   | 'orange' |
        +-------+----------+

        >>> table5, table6 = unjoin(table4, 'bar')
        >>> look(table5)
        +-------+----------+
        | 'foo' | 'bar_id' |
        +=======+==========+
        | 'A'   | 1        |
        +-------+----------+
        | 'B'   | 1        |
        +-------+----------+
        | 'C'   | 2        |
        +-------+----------+

        >>> look(table6)
        +------+----------+
        | 'id' | 'bar'    |
        +======+==========+
        | 1    | 'apple'  |
        +------+----------+
        | 2    | 'orange' |
        +------+----------+

    .. versionadded:: 0.12

    """

    if key is None:
        # first sort the table by the value field
        if presorted:
            tbl_sorted = table
        else:
            tbl_sorted = sort(table, value, buffersize=buffersize,
                              tempdir=tempdir, cache=cache)
        # on the left, return the original table but with the value field
        # replaced by an incrementing integer
        left = ConvertToIncrementingCounterView(tbl_sorted, value,
                                                autoincrement)
        # on the right, return a new table with distinct values from the
        # given field
        right = EnumerateDistinctView(tbl_sorted, value, autoincrement)
    else:
        # on the left, return distinct rows from the original table
        # with the value field cut out
        left = distinct(cutout(table, value))
        # on the right, return distinct rows from the original table
        # with all fields but the key and value cut out
        right = distinct(cut(table, key, value))
    return left, right


class ConvertToIncrementingCounterView(RowContainer):

    def __init__(self, tbl, value, autoincrement):
        self.table = tbl
        self.value = value
        self.autoincrement = autoincrement

    def __iter__(self):
        it = iter(self.table)
        fields = it.next()
        table = itertools.chain([fields], it)
        value = self.value
        vidx = fields.index(value)
        outflds = list(fields)
        outflds[vidx] = '%s_id' % value
        yield tuple(outflds)
        offset, multiplier = self.autoincrement
        for n, (_, group) in enumerate(rowgroupby(table, value)):
            for row in group:
                outrow = list(row)
                outrow[vidx] = (n * multiplier) + offset
                yield tuple(outrow)


class EnumerateDistinctView(RowContainer):

    def __init__(self, tbl, value, autoincrement):
        self.table = tbl
        self.value = value
        self.autoincrement = autoincrement

    def __iter__(self):
        offset, multiplier = self.autoincrement
        yield ('id', self.value)
        for n, (v, _) in enumerate(rowgroupby(self.table, self.value)):
            yield ((n * multiplier) + offset, v)



