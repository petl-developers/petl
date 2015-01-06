"""The module :mod:`petl.interactive` provides all of the functions
present in the root :mod:`petl` module, but with a couple of
optimisations for use within an interactive session.

The main optimisation is that some caching is done by default,
such that the first 100 rows of any table are cached in
memory the first time they are requested. This usually provides a
better experience when building up a transformation pipeline one step
at a time, where you are examining the outputs of each intermediate
step as its written via :func:`look` or :func:`see`. I.e., as each new
step is added and the output examined, as long as less than 100 rows
are requested, only that new step will actually be executed, and none
of the upstream transformations will be repeated, because the outputs
from previous steps will have been cached.

The default cache size can be changed by setting
``petl.interactive.cachesize`` to an integer value.

Also, by default, the :func:`look` function is used to generate a
representation of tables. So you don't need to type, e.g., ``>>>
look(mytable)``, you can just type ``>>> mytable``. The default
representation function can be changed by setting
``petl.interactive.representation``, e.g.,
``petl.interactive.representation = petl.see``, or
``petl.interactive.representation = None`` to disable this behaviour.

If used within an IPython notebook, tables will automatically be formatted as
HTML.

Finally, this module extends :mod:`petl.fluent` so you can use the
fluent style if you wish, e.g.::

    >>> import petl.interactive as etl
    >>> l = [['foo', 'bar'], ['a', 1], ['b', 3]]
    >>> table1 = etl.wrap(l)
    >>> table1.look()
    +-------+-------+
    | 'foo' | 'bar' |
    +=======+=======+
    | 'a'   |     1 |
    +-------+-------+
    | 'b'   |     3 |
    +-------+-------+

    >>> table1.cut('foo').look()
    +-------+
    | 'foo' |
    +=======+
    | 'a'   |
    +-------+
    | 'b'   |
    +-------+

    >>> table1.tocsv('test.csv')
    >>> etl.fromcsv('test.csv').look()
    +-------+-------+
    | 'foo' | 'bar' |
    +=======+=======+
    | 'a'   | '1'   |
    +-------+-------+
    | 'b'   | '3'   |
    +-------+-------+

"""


from __future__ import absolute_import, print_function, division, \
    unicode_literals


from itertools import islice
import sys
import inspect
from .compat import text_type
import logging
logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


from petl.util import RowContainer
import petl.fluent
from petl.io import StringSource


petl = sys.modules['petl']
thismodule = sys.modules[__name__]


cachesize = 100
representation = petl.look


# set True to display field indices
repr_index_header = False


# set to str or repr for different behaviour
repr_html_value = text_type


# default limit for html table representation
repr_html_limit = 5


def repr_html(tbl, limit=None, index_header=None, representation=text_type,
              caption=None, encoding='utf-8'):

    # add column indices to header?
    if index_header is None:
        index_header = repr_index_header  # use default
    if index_header:
        indexed_header = [u'%s|%s' % (i, f)
                          for (i, f) in enumerate(petl.util.header(tbl))]
        target = petl.transform.setheader(tbl, indexed_header)
    else:
        target = tbl

    # limit number of rows output?
    # N.B., limit is max number of data rows (not including header)
    if limit is None:
        # use default
        limit = repr_html_limit

    overflow = False
    if limit > 0:
        # try reading one more than the limit, to see if there are more rows
        target = list(islice(target, 0, limit+2))
        if len(target) > limit+1:
            overflow = True
            target = target[:-1]
    else:
        # render the entire table
        pass

    # write to html string
    buf = StringSource()
    if encoding:
        petl.io.touhtml(target, buf, caption=caption, encoding=encoding)
        s = buf.getvalue()
        if overflow:
            s += b'<p><strong>...</strong></p>'

    else:
        petl.io.tohtml(target, buf, representation=representation,
                       caption=caption)
        s = buf.getvalue()
        if overflow:
            s += '<p><strong>...</strong></p>'

    return s


class InteractiveWrapper(petl.fluent.FluentWrapper):
    
    def __init__(self, inner=None):
        super(InteractiveWrapper, self).__init__(inner)
        object.__setattr__(self, '_cache', [])
        object.__setattr__(self, '_cachecomplete', False)
        
    def clearcache(self):
        object.__setattr__(self, '_cache', [])  # reset cache
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
        if repr_index_header:
            indexed_header = ['%s|%s' % (i, f)
                              for (i, f) in enumerate(petl.util.header(self))]
            target = petl.transform.setheader(self, indexed_header)
        else:
            target = self
        if representation is not None:
            return repr(representation(target))
        else:
            return object.__repr__(target)
        
    def _repr_html_(self):
        return repr_html(self,
                         index_header=repr_index_header,
                         representation=repr_html_value)


def _wrap_function(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        if isinstance(_innerresult, RowContainer):
            return InteractiveWrapper(_innerresult)
        else:
            return _innerresult
    wrapper.__name__ = f.__name__
    wrapper.__doc__ = f.__doc__
    return wrapper


def _wrap_function_tuple(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        assert isinstance(_innerresult, tuple)
        return tuple(InteractiveWrapper(x) for x in _innerresult)
    wrapper.__name__ = f.__name__
    wrapper.__doc__ = f.__doc__
    return wrapper


def _wrap_function_dict(f):
    def wrapper(*args, **kwargs):
        _innerresult = f(*args, **kwargs)
        assert isinstance(_innerresult, dict)
        for k, v in _innerresult.items():
            _innerresult[k] = InteractiveWrapper(v)
        return _innerresult
    wrapper.__name__ = f.__name__
    wrapper.__doc__ = f.__doc__
    return wrapper

        
# import and wrap all functions from root petl module
for n, c in petl.__dict__.items():
    if inspect.isfunction(c):
        if n in petl.fluent.WRAP_TUPLE:
            setattr(thismodule, n, _wrap_function_tuple(c))
        elif n in petl.fluent.WRAP_DICT:
            setattr(thismodule, n, _wrap_function_dict(c))
        else:
            setattr(thismodule, n, _wrap_function(c))

        
# add module functions as methods on the wrapper class
for n, c in thismodule.__dict__.items():
    if inspect.isfunction(c):
        if n.startswith('from') or n in petl.fluent.NONMETHODS:
            pass
        else:
            setattr(InteractiveWrapper, n, c) 


# shorthand alias for wrapping tables
wrap = InteractiveWrapper
