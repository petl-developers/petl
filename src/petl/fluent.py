"""
As the root :mod:`petl` module but with modifications to allow for fluent style
usage.

.. versionadded:: 0.6

"""


from __future__ import absolute_import, print_function, division, \
    unicode_literals


import sys
import inspect
from .util import RowContainer


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
        
    def __str__(self):
        return str(self._inner)

    def __repr__(self):
        return 'FluentWrapper(' + repr(self._inner) + ')'


def _wrap_function(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        if isinstance(_innerresult, RowContainer): 
            return FluentWrapper(_innerresult)
        else:
            return _innerresult
    wrapper.__name__ = f.__name__
    wrapper.__doc__ = f.__doc__
    return wrapper


def _wrap_function_tuple(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        assert isinstance(_innerresult, tuple)
        return tuple(FluentWrapper(x) for x in _innerresult)
    wrapper.__name__ = f.__name__
    wrapper.__doc__ = f.__doc__
    return wrapper


def _wrap_function_dict(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        assert isinstance(_innerresult, dict)
        for k, v in _innerresult.items():
            _innerresult[k] = FluentWrapper(v)
        return _innerresult
    wrapper.__name__ = f.__name__
    wrapper.__doc__ = f.__doc__
    return wrapper


# these functions return a tuple of RowContainers and need to be wrapped accordingly
WRAP_TUPLE = 'diff', 'unjoin'


# these functions return a dict of RowContainers and need to be wrapped accordingly
WRAP_DICT = 'facet'


# import and wrap all functions from root petl module
for n, c in petl.__dict__.items():
    if inspect.isfunction(c):
        if n in WRAP_TUPLE:
            setattr(thismodule, n, _wrap_function_tuple(c))
        elif n in WRAP_DICT:
            setattr(thismodule, n, _wrap_function_dict(c))
        else:
            setattr(thismodule, n, _wrap_function(c))
    else:
        setattr(thismodule, n, c)


NONMETHODS = ['dummytable', 'randomtable', 'dateparser', 'timeparser',
              'datetimeparser', 'boolparser', 'parsenumber', 'expr',
              'strjoin', 'heapqmergesorted', 'shortlistmergesorted',
              'nthword']


# add module functions as methods on the wrapper class
# N.B., add only those functions that expect to have row container (i.e., table)
# as first argument
# i.e., only those were it makes sense for them to be methods
for n, c in thismodule.__dict__.items():
    if inspect.isfunction(c):
        if n.startswith('from') or n in NONMETHODS:
            pass
        else:
            setattr(FluentWrapper, n, c) 


# shorthand alias for wrapping tables
wrap = FluentWrapper    
