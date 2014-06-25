__author__ = 'aliman'


import collections
import operator


from petl.util import RowContainer, lookup, asindices, rowgetter, iterpeek
from petl.transform.joins import natural_key


def hashjoin(left, right, key=None, cache=True, lprefix=None, rprefix=None):
    """
    Alternative implementation of :func:`join`, where the join is executed
    by constructing an in-memory lookup for the right hand table, then iterating over rows 
    from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.5
    
    .. versionchanged:: 0.16
    
    Added support for caching data from right hand table (only available when
    `key` is given).

    """
    
    if key is None:
        key = natural_key(left, right)
    return HashJoinView(left, right, key, cache=cache, lprefix=lprefix,
                        rprefix=rprefix)


class HashJoinView(RowContainer):
    
    def __init__(self, left, right, key, cache=True, lprefix=None,
                 rprefix=None):
        self.left = left
        self.right = right
        self.key = key
        self.cache = True
        self.rlookup = None
        self.lprefix = lprefix
        self.rprefix = rprefix
        
    def __iter__(self):
        if not self.cache or self.rlookup is None:
            self.rlookup = lookup(self.right, self.key)
        return iterhashjoin(self.left, self.right, self.key, self.rlookup,
                            self.lprefix, self.rprefix)
    

def iterhashjoin(left, right, key, rlookup, lprefix, rprefix):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from left table
    lgetk = operator.itemgetter(*lkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    if lprefix is None:
        outflds = list(lflds)
    else:
        outflds = [f if f == key else (str(lprefix) + str(f))
                   for f in lflds]
    if rprefix is None:
        outflds.extend(rgetv(rflds))
    else:
        outflds.extend([f if f == key else (str(rprefix) + str(f))
                        for f in rgetv(rflds)])
    yield tuple(outflds)

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
        
        
def hashleftjoin(left, right, key=None, missing=None, cache=True, lprefix=None,
                 rprefix=None):
    """
    Alternative implementation of :func:`leftjoin`, where the join is executed
    by constructing an in-memory lookup for the right hand table, then iterating over rows 
    from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.5

    .. versionchanged:: 0.16
    
    Added support for caching data from right hand table (only available when
    `key` is given).

    """

    if key is None:
        key = natural_key(left, right)
    return HashLeftJoinView(left, right, key, missing=missing, cache=cache,
                            lprefix=lprefix, rprefix=rprefix)


class HashLeftJoinView(RowContainer):
    
    def __init__(self, left, right, key, missing=None, cache=True, lprefix=None,
                 rprefix=None):
        self.left = left
        self.right = right
        self.key = key
        self.missing = missing
        self.cache = cache
        self.rlookup = None
        self.lprefix = lprefix
        self.rprefix = rprefix

    def __iter__(self):
        if not self.cache or self.rlookup is None:
            self.rlookup = lookup(self.right, self.key)
        return iterhashleftjoin(self.left, self.right, self.key, self.missing,
                                self.rlookup, self.lprefix, self.rprefix)
    

def iterhashleftjoin(left, right, key, missing, rlookup, lprefix, rprefix):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from left table
    lgetk = operator.itemgetter(*lkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    if lprefix is None:
        outflds = list(lflds)
    else:
        outflds = [f if f == key else (str(lprefix) + str(f))
                   for f in lflds]
    if rprefix is None:
        outflds.extend(rgetv(rflds))
    else:
        outflds.extend([f if f == key else (str(rprefix) + str(f))
                        for f in rgetv(rflds)])
    yield tuple(outflds)

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
            outrow = list(lrow) # start with the left row
            # extend with missing values in place of the right row
            outrow.extend([missing] * len(rvind))
            yield tuple(outrow)
        
        
def hashrightjoin(left, right, key=None, missing=None, cache=True, lprefix=None,
                  rprefix=None):
    """
    Alternative implementation of :func:`rightjoin`, where the join is executed
    by constructing an in-memory lookup for the left hand table, then iterating over rows 
    from the right hand table.
    
    May be faster and/or more resource efficient where the left table is small
    and the right table is large.
    
    .. versionadded:: 0.5

    .. versionchanged:: 0.16
    
    Added support for caching data from left hand table (only available when
    `key` is given).

    """

    if key is None:
        key = natural_key(left, right)
    return HashRightJoinView(left, right, key, missing=missing, cache=cache,
                             lprefix=lprefix, rprefix=rprefix)


class HashRightJoinView(RowContainer):
    
    def __init__(self, left, right, key, missing=None, cache=True, lprefix=None,
                 rprefix=None):
        self.left = left
        self.right = right
        self.key = key
        self.missing = missing
        self.cache = cache
        self.llookup = None
        self.lprefix = lprefix
        self.rprefix = rprefix

    def __iter__(self):
        if not self.cache or self.llookup is None:
            self.llookup = lookup(self.left, self.key)
        return iterhashrightjoin(self.left, self.right, self.key, self.missing,
                                 self.llookup, self.lprefix, self.rprefix)
    

def iterhashrightjoin(left, right, key, missing, llookup, lprefix, rprefix):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from left table
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
        outflds = [f if f == key else (str(lprefix) + str(f))
                   for f in lflds]
    if rprefix is None:
        outflds.extend(rgetv(rflds))
    else:
        outflds.extend([f if f == key else (str(rprefix) + str(f))
                        for f in rgetv(rflds)])
    yield tuple(outflds)

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
            outrow = [missing] * len(lflds)
            # set key values
            for li, ri in zip(lkind, rkind):
                outrow[li] = rrow[ri]
            # extend with non-key values from the right row  
            outrow.extend(rgetv(rrow))
            yield tuple(outrow)
        
        
def hashantijoin(left, right, key=None):
    """
    Alternative implementation of :func:`antijoin`, where the join is executed
    by constructing an in-memory set for all keys found in the right hand table, then 
    iterating over rows from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.5

    """
    
    if key is None:
        key = natural_key(left, right)
    return HashAntiJoinView(left, right, key)


class HashAntiJoinView(RowContainer):
    
    def __init__(self, left, right, key):
        self.left = left
        self.right = right
        self.key = key
        
    def __iter__(self):
        return iterhashantijoin(self.left, self.right, self.key)
    
    
def iterhashantijoin(left, right, key):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    yield tuple(lflds)

    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
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


def hashlookupjoin(left, right, key=None, missing=None, lprefix=None,
                   rprefix=None):
    """
    Alternative implementation of :func:`lookupjoin`, where the join is executed
    by constructing an in-memory lookup for the right hand table, then iterating
    over rows from the left hand table.

    May be faster and/or more resource efficient where the right table is small
    and the left table is large.

    .. versionadded:: 0.11

    """

    if key is None:
        key = natural_key(left, right)
    return HashLookupJoinView(left, right, key, missing=missing,
                              lprefix=lprefix, rprefix=rprefix)


class HashLookupJoinView(RowContainer):

    def __init__(self, left, right, key, missing=None, lprefix=None,
                 rprefix=None):
        self.left = left
        self.right = right
        self.key = key
        self.missing = missing
        self.lprefix = lprefix
        self.rprefix = rprefix

    def __iter__(self):
        return iterhashlookupjoin(self.left, self.right, self.key, self.missing,
                                  self.lprefix, self.rprefix)


def iterhashlookupjoin(left, right, key, missing, lprefix, rprefix):
    lit = iter(left)
    lflds = lit.next()

    rflds, rit = iterpeek(right)  # need the whole lot to pass to lookup
    from petl.util import lookupone
    rlookup = lookupone(rit, key, strict=False)

    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)

    # construct functions to extract key values from left table
    lgetk = operator.itemgetter(*lkind)

    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)

    # determine the output fields
    if lprefix is None:
        outflds = list(lflds)
    else:
        outflds = [f if f == key else (str(lprefix) + str(f))
                   for f in lflds]
    if rprefix is None:
        outflds.extend(rgetv(rflds))
    else:
        outflds.extend([f if f == key else (str(rprefix) + str(f))
                        for f in rgetv(rflds)])
    yield tuple(outflds)

    # define a function to join rows
    def joinrows(lrow, rrow):
        # start with the left row
        outrow = list(lrow)
        # extend with non-key values from the right row
        outrow.extend(rgetv(rrow))
        return tuple(outrow)

    for lrow in lit:
        k = lgetk(lrow)
        if k in rlookup:
            rrow = rlookup[k]
            yield joinrows(lrow, rrow)
        else:
            outrow = list(lrow) # start with the left row
            # extend with missing values in place of the right row
            outrow.extend([missing] * len(rvind))
            yield tuple(outrow)





