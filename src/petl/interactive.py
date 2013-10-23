"""
As the root :mod:`petl` module but with optimisations for use in an interactive
session.

"""


from itertools import islice
import sys
from petl.util import valueset, RowContainer
import petl.fluent
from petl.io import tohtml, StringSource
import logging
logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


petl = sys.modules['petl']
thismodule = sys.modules[__name__]


cachesize = 10000
representation = petl.look


class InteractiveWrapper(petl.fluent.FluentWrapper):
    
    def __init__(self, inner=None):
        super(InteractiveWrapper, self).__init__(inner)
        object.__setattr__(self, '_cache', [])
        object.__setattr__(self, '_cachecomplete', False)
        
    def clearcache(self):
        object.__setattr__(self, '_cache', []) # reset cache
        object.__setattr__(self, '_cachecomplete', False)
        
    def __iter__(self):
        debug('serving from cache, cache size %s', len(self._cache))

        # serve whatever is in the cache first
        for row in self._cache:
            yield row
            
        if not self._cachecomplete:
            
            # serve the remainder from the inner iterator
            debug('cache exhausted, serving from inner iterator')
            it = iter(self._inner)
            for row in islice(it, len(self._cache), None):
                # maybe there's more room in the cache?
                if len(self._cache) < cachesize:
                    self._cache.append(row)
                yield row
                
            # does the cache contain a complete copy of the inner table?
            if len(self._cache) < cachesize:
                debug('cache is complete')
                object.__setattr__(self, '_cachecomplete', True)
        
    def __repr__(self):
        if InteractiveWrapper.repr_index_header:
            indexed_header = ['%s|%s' % (i, f) for (i, f) in enumerate(petl.util.header(self))]
            target = petl.transform.setheader(self, indexed_header)
        else:
            target = self
        if representation is not None:
            return repr(representation(target))
        else:
            return object.__repr__(target)
        
    def _repr_html_(self):
        if InteractiveWrapper.repr_index_header:
            indexed_header = ['%s|%s' % (i, f) for (i, f) in enumerate(petl.util.header(self))]
            target = petl.transform.setheader(self, indexed_header)
        else:
            target = self
        buf = StringSource()
        tohtml(target, buf)
        return buf.getvalue()


# set True to display field indices
InteractiveWrapper.repr_index_header = False


def wrap(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        if isinstance(_innerresult, RowContainer):
            return InteractiveWrapper(_innerresult)
        else:
            return _innerresult
    wrapper.__name__ = f.__name__
    wrapper.__doc__ = f.__doc__
    return wrapper

        
# import and wrap all functions from root petl module
for n, c in petl.__dict__.items():
    if callable(c):
        setattr(thismodule, n, wrap(c))
    else:
        setattr(thismodule, n, c)
        
        
# add module functions as methods on the wrapper class
# TODO add only those methods that expect to have row container as first argument
for n, c in thismodule.__dict__.items():
    if callable(c):
        if n.startswith('from') or n in petl.fluent.STATICMETHODS: # avoids having to import anything other than "etl"
            setattr(InteractiveWrapper, n, staticmethod(c))
        else:
            setattr(InteractiveWrapper, n, c) 
            
            
# special case to act like static method if no inner
def _catmethod(self, *args, **kwargs):
    if self._inner is None:
        return InteractiveWrapper(petl.cat(*args, **kwargs))
    else:
        return InteractiveWrapper(petl.cat(self, *args, **kwargs))
setattr(InteractiveWrapper, 'cat', _catmethod)        

        
# need to manually override for facet, because it returns a dict 
def facet(table, field):
    fct = dict()
    for v in valueset(table, field):
        fct[v] = getattr(thismodule, 'selecteq')(table, field, v)
    return fct


# need to manually override for diff(), because it returns a tuple 
def diff(*args, **kwargs):
    a, b = petl.diff(*args, **kwargs)
    return InteractiveWrapper(a), InteractiveWrapper(b)


# short alias to wrap explicitly
etl = InteractiveWrapper    

    