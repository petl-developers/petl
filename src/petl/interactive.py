"""
As the root :mod:`petl` module but with optimisations for use in an interactive
session.

"""


from itertools import islice
import sys
from petl.util import valueset


petl = sys.modules['petl']
thismodule = sys.modules[__name__]


cachesize = 100
debug = False
representation = petl.look


class ContainerCache(object):
    
    def __init__(self, inner):
        self._inner = inner
        self._cache = []
        self._tag = None
        
    def __iter__(self):
        try:
            tag = self._inner.cachetag()
        except:
            # cannot cache for some reason, just pass through
            if debug: print repr(self._inner) + ' :: uncacheable'
            return iter(self._inner)
        else:
            if self._tag is None or self._tag != tag:
                # _tag is not fresh
                if debug: print repr(self._inner) + ' :: stale, clearing cache'
                self._tag = tag
                self._cache = list() # reset cache
            return self._iterwithcache()
            
    def _iterwithcache(self):
        if debug: print repr(self._inner) + ' :: serving from cache, cache size ' + str(len(self._cache))
        for row in self._cache:
            yield row
        if debug: print repr(self._inner) + ' :: cache exhausted, serving from inner iterator'    
        it = iter(self._inner)
        for row in islice(it, len(self._cache), None):
            # maybe there's more room in the cache?
            if len(self._cache) < cachesize:
                self._cache.append(row)
            yield row
        
    def __repr__(self):
        if representation is not None:
            return repr(representation(self))
        else:
            return object.__repr__(self)
            
    def __getitem__(self, item):
        return self._inner[item]
    
    def __getattr__(self, attr):
        return getattr(self._inner, attr)
    
    def __setitem__(self, item, value):
        self._inner[item] = value
    
    def __setattr__(self, attr, value):
        if attr in {'_inner', '_cache', '_tag'}:
            object.__setattr__(self, attr, value)
        else:
            setattr(self._inner, attr, value)
    
    
def wrap(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        if hasattr(_innerresult, 'cachetag') and hasattr(_innerresult, '__iter__'):
            return ContainerCache(_innerresult)
        else:
            return _innerresult
    return wrapper

        
for n, c in petl.__dict__.items():
    if callable(c):
        setattr(thismodule, n, wrap(c))
    else:
        setattr(thismodule, n, c)
        
        
# need to manually override for facet, because it returns a dict 
def facet(table, field):
    fct = dict()
    for v in valueset(table, field):
        fct[v] = getattr(thismodule, 'selecteq')(table, field, v)
    return fct


