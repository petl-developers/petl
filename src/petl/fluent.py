"""
As the root :mod:`petl` module but with modifications to allow for fluent style
usage.

.. versionadded:: 0.6

"""


import sys
from functools import partial
from petl.util import valueset


petl = sys.modules['petl']
thismodule = sys.modules[__name__]


class FluentWrapper(object):
    
    def __init__(self, inner):
        self._inner = inner
        
    def __getattr__(self, attr):
        if hasattr(self._inner, attr):
            return getattr(self._inner, attr)
        elif hasattr(thismodule, attr) and callable(getattr(thismodule, attr)):
            f = getattr(thismodule, attr)
            return partial(f, self._inner)
        else:
            return getattr(self._inner, attr) # should raise appropriate attribute error
    
    def __setattr__(self, attr, value):
        if attr in {'_inner'}:
            object.__setattr__(self, attr, value)
        else:
            setattr(self._inner, attr, value)
        setattr(self._inner, attr, value)
        
    def __getitem__(self, item):
        return self._inner[item]
    
    def __setitem__(self, item, value):
        self._inner[item] = value

    def __str__(self):
        return str(self._inner)

    def __repr__(self):
        return repr(self._inner)


def wrap(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        if not callable(_innerresult): # make an exception for functions
            return FluentWrapper(_innerresult)
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




