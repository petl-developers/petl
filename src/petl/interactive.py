"""
TODO doc me

"""


from itertools import islice, chain
import sys


petl = sys.modules['petl']
thismodule = sys.modules[__name__]


cachesize = 100
debug = False
representation = petl.look


class IterCache(object):
    
    def __init__(self, inner):
        self._inner = inner
        self._cache = []
        self._tag = None
        self._complete = False
        
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
                it = iter(self._inner)
                self._cache = list() # reset cache
            # serve from _cache
            return self._iterwithcache()
            
    def __repr__(self):
        if representation is not None:
            return repr(representation(self))
        else:
            return object.__repr__(self)
            
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
        
    def __getitem__(self, item):
        return self._inner[item]
    
    def __getattr__(self, attr):
        return getattr(self._inner, attr)
    
    def __setitem__(self, item, value):
        self._inner[item] = value
    
    def __setattr__(self, attr, value):
        if attr in {'_inner', '_cache', '_tag', '_complete'}:
            object.__setattr__(self, attr, value)
        else:
            setattr(self._inner, attr, value)
    
    
def wrap(f):
    def wrapper(*args, **kwargs):
        result = f(*args, **kwargs)
        if hasattr(result, 'cachetag') and hasattr(result, '__iter__'):
            return IterCache(result)
        else:
            return result
    return wrapper

        
for n, c in petl.__dict__.items():
    if callable(c):
        setattr(thismodule, n, wrap(c))
    else:
        setattr(thismodule, n, c)