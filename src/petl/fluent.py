"""
As the root :mod:`petl` module but with modifications to allow for fluent style
usage.

.. versionadded:: 0.6

"""


from __future__ import absolute_import, print_function, division


import sys
import inspect
from petl.util import valueset, RowContainer


petl = sys.modules['petl']
thismodule = sys.modules[__name__]


class FluentWrapper(RowContainer):
    
    def __init__(self, inner=None):
        object.__setattr__(self, '_inner', inner)

    def __iter__(self):
        return iter(self._inner)

    def __getattr__(self, attr):
        # pass through
        return getattr(self._inner, attr) 
    
    def __setattr__(self, attr, value):
        # pass through
        setattr(self._inner, attr, value)
        
#    def __getitem__(self, item):
#        # pass through
#        return self._inner[item]
#    
#    def __setitem__(self, item, value):
#        # pass through
#        self._inner[item] = value

    def __str__(self):
        return str(self._inner)

    def __repr__(self):
        return repr(self._inner)


def _wrap_result(inner_result):
    if isinstance(inner_result, RowContainer):
        return FluentWrapper(inner_result)
    else:
        return inner_result


def _wrap_function(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        if isinstance(_innerresult, tuple):
            return tuple(_wrap_result(item) for item in _innerresult)
        else:
            return _wrap_result(_innerresult)
    wrapper.__name__ = f.__name__
    wrapper.__doc__ = f.__doc__
    return wrapper

        
# import and wrap all functions from root petl module
for n, c in petl.__dict__.items():
    if inspect.isfunction(c):
        setattr(thismodule, n, _wrap_function(c))
    else:
        setattr(thismodule, n, c)


STATICMETHODS = ['dummytable', 'randomtable', 'dateparser', 'timeparser',
                 'datetimeparser', 'boolparser', 'parsenumber', 'expr',
                 'strjoin', 'heapqmergesorted', 'shortlistmergesorted',
                 'nthword']


# add module functions as methods on the wrapper class
# N.B., add only those functions that expect to have row container (i.e., table)
# as first argument
# i.e., only those were it makes sense for them to be methods
for n, c in thismodule.__dict__.items():
    if inspect.isfunction(c):
        if n.startswith('from') or n in STATICMETHODS: 
            setattr(FluentWrapper, n, staticmethod(c))
        else:
            setattr(FluentWrapper, n, c) 


# special case to act like static method if no inner
def _catmethod(self, *args, **kwargs):
    if self._inner is None:
        return FluentWrapper(petl.cat(*args, **kwargs))
    else:
        return FluentWrapper(petl.cat(self, *args, **kwargs))
setattr(FluentWrapper, 'cat', _catmethod)        

        
# need to manually override because it returns a dict 
def facet(table, field):
    fct = dict()
    for v in valueset(table, field):
        fct[v] = getattr(thismodule, 'selecteq')(table, field, v)
    return fct


# need to manually override because it returns a tuple 
def diff(*args, **kwargs):
    a, b = petl.diff(*args, **kwargs)
    return FluentWrapper(a), FluentWrapper(b)


# need to manually override because it returns a tuple 
def unjoin(*args, **kwargs):
    a, b = petl.unjoin(*args, **kwargs)
    return FluentWrapper(a), FluentWrapper(b)


# shorthand alias for wrapping tables
wrap = FluentWrapper    

