from __future__ import absolute_import, division, print_function

import operator

from petl.compat import next, text_type
from petl.transform.basics import stack
from petl.transform.joins import keys_from_args
from petl.util.base import Table, asindices, iterpeek, rowgetter
from petl.util.lookups import lookup, lookupone


def hashjoin(left, right, key=None, lkey=None, rkey=None, cache=True,
             lprefix=None, rprefix=None, missing=None):
    """Alternative implementation of :func:`petl.transform.joins.join`, where
    the join is executed by constructing an in-memory lookup for the right
    hand table, then iterating over rows from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    By default data from right hand table is cached to improve performance
    (only available when `key` is given).

    Left and right tables with different key fields can be handled via the
    `lkey` and `rkey` arguments.

    .. versionchanged:: 1.7.16
        To ensure correct results for tables with uneven rows, tables will be
        squared up and rows will be filled with the value if `missing` keyword
        argument before joining to ensure correct results.

    """
    
    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return HashJoinView(left, right, lkey=lkey, rkey=rkey, cache=cache,
                        lprefix=lprefix, rprefix=rprefix, missing=missing)


Table.hashjoin = hashjoin


class HashJoinView(Table):
    
    def __init__(self, left, right, lkey, rkey, cache=True, lprefix=None,
                 rprefix=None, missing=None):
        self.left = stack(left, missing=missing)
        self.right = stack(right, missing=missing)
        self.lkey = lkey
        self.rkey = rkey
        self.cache = cache
        self.rlookup = None
        self.lprefix = lprefix
        self.rprefix = rprefix
        
    def __iter__(self):
        if not self.cache or self.rlookup is None:
            self.rlookup = lookup(self.right, self.rkey)
        return iterhashjoin(self.left, self.right, self.lkey, self.rkey,
                            self.rlookup, self.lprefix, self.rprefix)
    

def iterhashjoin(left, right, lkey, rkey, rlookup, lprefix, rprefix):
    lit = iter(left)
    rit = iter(right)

    lhdr = next(lit)
    rhdr = next(rit)
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lhdr, lkey)
    rkind = asindices(rhdr, rkey)
    
    # construct functions to extract key values from left table
    lgetk = operator.itemgetter(*lkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rhdr)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    if lprefix is None:
        outhdr = list(lhdr)
    else:
        outhdr = [(text_type(lprefix) + text_type(f))
                  for f in lhdr]
    if rprefix is None:
        outhdr.extend(rgetv(rhdr))
    else:
        outhdr.extend([(text_type(rprefix) + text_type(f)) for f in rgetv(rhdr)])
    yield tuple(outhdr)

    # define a function to join rows
    def joinrows(_lrow, _rrows):
        for rrow in _rrows:
            # start with the left row
            _outrow = list(_lrow)
            # extend with non-key values from the right row
            _outrow.extend(rgetv(rrow))
            yield tuple(_outrow)

    for lrow in lit:
        k = lgetk(lrow)
        if k in rlookup:
            rrows = rlookup[k]
            for outrow in joinrows(lrow, rrows):
                yield outrow
        
        
def hashleftjoin(left, right, key=None, lkey=None, rkey=None, missing=None,
                 cache=True, lprefix=None, rprefix=None):
    """Alternative implementation of :func:`petl.transform.joins.leftjoin`,
    where the join is executed by constructing an in-memory lookup for the
    right hand table, then iterating over rows from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    By default data from right hand table is cached to improve performance
    (only available when `key` is given).

    Left and right tables with different key fields can be handled via the
    `lkey` and `rkey` arguments.

    .. versionchanged:: 1.7.16
        To ensure correct results for tables with uneven rows, tables will be
        squared up before joining to ensure correct results.

    """

    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return HashLeftJoinView(left, right, lkey, rkey, missing=missing,
                            cache=cache, lprefix=lprefix, rprefix=rprefix)


Table.hashleftjoin = hashleftjoin


class HashLeftJoinView(Table):
    
    def __init__(self, left, right, lkey, rkey, missing=None, cache=True,
                 lprefix=None, rprefix=None):
        self.left = stack(left, missing=missing)
        self.right = stack(right, missing=missing)
        self.lkey = lkey
        self.rkey = rkey
        self.missing = missing
        self.cache = cache
        self.rlookup = None
        self.lprefix = lprefix
        self.rprefix = rprefix

    def __iter__(self):
        if not self.cache or self.rlookup is None:
            self.rlookup = lookup(self.right, self.rkey)
        return iterhashleftjoin(self.left, self.right, self.lkey, self.rkey,
                                self.missing, self.rlookup, self.lprefix,
                                self.rprefix)
    

def iterhashleftjoin(left, right, lkey, rkey, missing, rlookup, lprefix,
                     rprefix):
    lit = iter(left)
    rit = iter(right)

    lhdr = next(lit)
    rhdr = next(rit)
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lhdr, lkey)
    rkind = asindices(rhdr, rkey)
    
    # construct functions to extract key values from left table
    lgetk = operator.itemgetter(*lkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rhdr)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    if lprefix is None:
        outhdr = list(lhdr)
    else:
        outhdr = [(text_type(lprefix) + text_type(f))
                  for f in lhdr]
    if rprefix is None:
        outhdr.extend(rgetv(rhdr))
    else:
        outhdr.extend([(text_type(rprefix) + text_type(f)) for f in rgetv(rhdr)])
    yield tuple(outhdr)

    # define a function to join rows
    def joinrows(_lrow, _rrows):
        for rrow in _rrows:
            # start with the left row
            _outrow = list(_lrow)
            # extend with non-key values from the right row
            _outrow.extend(rgetv(rrow))
            yield tuple(_outrow)

    for lrow in lit:
        k = lgetk(lrow)
        if k in rlookup:
            rrows = rlookup[k]
            for outrow in joinrows(lrow, rrows):
                yield outrow
        else:
            outrow = list(lrow)  # start with the left row
            # extend with missing values in place of the right row
            outrow.extend([missing] * len(rvind))
            yield tuple(outrow)
        
        
def hashrightjoin(left, right, key=None, lkey=None, rkey=None, missing=None,
                  cache=True, lprefix=None, rprefix=None):
    """Alternative implementation of :func:`petl.transform.joins.rightjoin`,
    where the join is executed by constructing an in-memory lookup for the
    left hand table, then iterating over rows from the right hand table.
    
    May be faster and/or more resource efficient where the left table is small
    and the right table is large.
    
    By default data from right hand table is cached to improve performance
    (only available when `key` is given).

    Left and right tables with different key fields can be handled via the
    `lkey` and `rkey` arguments.

    .. versionchanged:: 1.7.16
        To ensure correct results for tables with uneven rows, tables will be
        squared up before joining to ensure correct results.

    """

    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return HashRightJoinView(left, right, lkey, rkey, missing=missing,
                             cache=cache, lprefix=lprefix, rprefix=rprefix)


Table.hashrightjoin = hashrightjoin


class HashRightJoinView(Table):
    
    def __init__(self, left, right, lkey, rkey, missing=None, cache=True,
                 lprefix=None, rprefix=None):
        self.left = stack(left, missing=missing)
        self.right = stack(right, missing=missing)
        self.lkey = lkey
        self.rkey = rkey
        self.missing = missing
        self.cache = cache
        self.llookup = None
        self.lprefix = lprefix
        self.rprefix = rprefix

    def __iter__(self):
        if not self.cache or self.llookup is None:
            self.llookup = lookup(self.left, self.lkey)
        return iterhashrightjoin(self.left, self.right, self.lkey, self.rkey,
                                 self.missing, self.llookup, self.lprefix,
                                 self.rprefix)
    

def iterhashrightjoin(left, right, lkey, rkey, missing, llookup, lprefix,
                      rprefix):
    lit = iter(left)
    rit = iter(right)

    lhdr = next(lit)
    rhdr = next(rit)
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lhdr, lkey)
    rkind = asindices(rhdr, rkey)
    
    # construct functions to extract key values from left table
    rgetk = operator.itemgetter(*rkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rhdr)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    if lprefix is None:
        outhdr = list(lhdr)
    else:
        outhdr = [(text_type(lprefix) + text_type(f))
                  for f in lhdr]
    if rprefix is None:
        outhdr.extend(rgetv(rhdr))
    else:
        outhdr.extend([(text_type(rprefix) + text_type(f))
                       for f in rgetv(rhdr)])
    yield tuple(outhdr)

    # define a function to join rows
    def joinrows(_rrow, _lrows):
        for lrow in _lrows:
            # start with the left row
            _outrow = list(lrow)
            # extend with non-key values from the right row
            _outrow.extend(rgetv(_rrow))
            yield tuple(_outrow)

    for rrow in rit:
        k = rgetk(rrow)
        if k in llookup:
            lrows = llookup[k]
            for outrow in joinrows(rrow, lrows):
                yield outrow
        else:
            # start with missing values in place of the left row
            outrow = [missing] * len(lhdr)
            # set key values
            for li, ri in zip(lkind, rkind):
                outrow[li] = rrow[ri]
            # extend with non-key values from the right row  
            outrow.extend(rgetv(rrow))
            yield tuple(outrow)
        
        
def hashantijoin(left, right, key=None, lkey=None, rkey=None):
    """Alternative implementation of :func:`petl.transform.joins.antijoin`,
    where the join is executed by constructing an in-memory set for all keys
    found in the right hand table, then iterating over rows from the left
    hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    Left and right tables with different key fields can be handled via the
    `lkey` and `rkey` arguments.

    """
    
    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return HashAntiJoinView(left, right, lkey, rkey)


Table.hashantijoin = hashantijoin


class HashAntiJoinView(Table):
    
    def __init__(self, left, right, lkey, rkey):
        self.left = left
        self.right = right
        self.lkey = lkey
        self.rkey = rkey

    def __iter__(self):
        return iterhashantijoin(self.left, self.right, self.lkey, self.rkey)
    
    
def iterhashantijoin(left, right, lkey, rkey):
    lit = iter(left)
    rit = iter(right)

    lhdr = next(lit)
    rhdr = next(rit)
    yield tuple(lhdr)

    # determine indices of the key fields in left and right tables
    lkind = asindices(lhdr, lkey)
    rkind = asindices(rhdr, rkey)
    
    # construct functions to extract key values from both tables
    lgetk = operator.itemgetter(*lkind)
    rgetk = operator.itemgetter(*rkind)
    
    rkeys = set()
    for rrow in rit:
        rk = rgetk(rrow)
        rkeys.add(rk)
        
    for lrow in lit:
        lk = lgetk(lrow)
        if lk not in rkeys:
            yield tuple(lrow)


def hashlookupjoin(left, right, key=None, lkey=None, rkey=None, missing=None,
                   lprefix=None, rprefix=None):
    """Alternative implementation of :func:`petl.transform.joins.lookupjoin`,
    where the join is executed by constructing an in-memory lookup for the
    right hand table, then iterating over rows from the left hand table.

    May be faster and/or more resource efficient where the right table is small
    and the left table is large.

    Left and right tables with different key fields can be handled via the
    `lkey` and `rkey` arguments.

    .. versionchanged:: 1.7.16
        To ensure correct results for tables with uneven rows, tables will be
        squared up before joining to ensure correct results.

    """

    lkey, rkey = keys_from_args(left, right, key, lkey, rkey)
    return HashLookupJoinView(left, right, lkey, rkey, missing=missing,
                              lprefix=lprefix, rprefix=rprefix)


Table.hashlookupjoin = hashlookupjoin


class HashLookupJoinView(Table):

    def __init__(self, left, right, lkey, rkey, missing=None, lprefix=None,
                 rprefix=None):
        self.left = stack(left, missing=missing)
        self.right = stack(right, missing=missing)
        self.lkey = lkey
        self.rkey = rkey
        self.missing = missing
        self.lprefix = lprefix
        self.rprefix = rprefix

    def __iter__(self):
        return iterhashlookupjoin(self.left, self.right, self.lkey, self.rkey,
                                  self.missing, self.lprefix, self.rprefix)


def iterhashlookupjoin(left, right, lkey, rkey, missing, lprefix, rprefix):
    lit = iter(left)
    lhdr = next(lit)

    rhdr, rit = iterpeek(right)  # need the whole lot to pass to lookup
    rlookup = lookupone(rit, rkey, strict=False)

    # determine indices of the key fields in left and right tables
    lkind = asindices(lhdr, lkey)
    rkind = asindices(rhdr, rkey)

    # construct functions to extract key values from left table
    lgetk = operator.itemgetter(*lkind)

    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rhdr)) if i not in rkind]
    rgetv = rowgetter(*rvind)

    # determine the output fields
    if lprefix is None:
        outhdr = list(lhdr)
    else:
        outhdr = [(text_type(lprefix) + text_type(f))
                  for f in lhdr]
    if rprefix is None:
        outhdr.extend(rgetv(rhdr))
    else:
        outhdr.extend([(text_type(rprefix) + text_type(f))
                       for f in rgetv(rhdr)])
    yield tuple(outhdr)

    # define a function to join rows
    def joinrows(_lrow, _rrow):
        # start with the left row
        _outrow = list(_lrow)
        # extend with non-key values from the right row
        _outrow.extend(rgetv(_rrow))
        return tuple(_outrow)

    for lrow in lit:
        k = lgetk(lrow)
        if k in rlookup:
            rrow = rlookup[k]
            yield joinrows(lrow, rrow)
        else:
            outrow = list(lrow)  # start with the left row
            # extend with missing values in place of the right row
            outrow.extend([missing] * len(rvind))
            yield tuple(outrow)
