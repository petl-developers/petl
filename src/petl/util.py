"""
Utility functions.

"""


from itertools import islice, groupby, chain, count
from collections import defaultdict, namedtuple
from operator import itemgetter
import re
from string import maketrans
import random
import time
import datetime
from functools import partial
from itertools import izip_longest
import heapq
import sys
import operator
from math import ceil
import logging
logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


from petl.base import IterContainer


# Python 2.6 compatibility
try:
    from collections import Counter, OrderedDict
except ImportError:
    from .compat import count, Counter, OrderedDict


SINGLETONS = set([None, False, True])
SAFE_TYPES = set([complex, float, int, long, str, unicode])


class RowContainer(IterContainer):
    
    def __getitem__(self, item):
        if isinstance(item, basestring):
            return ValuesContainer(self, item)
        else:
            return super(RowContainer, self).__getitem__(item)


def header(table):
    """
    Return the header row for the given table. E.g.::
    
        >>> from petl import header
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> header(table)
        ['foo', 'bar']
    
    See also :func:`fieldnames`.
        
    """
    
    it = iter(table)
    return it.next()


def fieldnames(table):
    """
    Return the string values of all fields for the given table. If the fields
    are strings, then this function is equivalent to :func:`header`, i.e.::
    
        >>> from petl import header, fieldnames
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> header(table)
        ['foo', 'bar']
        >>> fieldnames(table)
        ['foo', 'bar']
        >>> header(table) == fieldnames(table)
        True
    
    Allows for custom field objects, e.g.::

        >>> class CustomField(object):
        ...     def __init__(self, id, description):
        ...         self.id = id
        ...         self.description = description
        ...     def __str__(self):
        ...         return self.id
        ...     def __repr__(self):
        ...         return 'CustomField(%r, %r)' % (self.id, self.description)
        ... 
        >>> table = [[CustomField('foo', 'Get some foo.'), CustomField('bar', 'A lot of bar.')], 
        ...          ['a', 1], 
        ...          ['b', 2]]
        >>> header(table)
        [CustomField('foo', 'Get some foo.'), CustomField('bar', 'A lot of bar.')]
        >>> fieldnames(table)    
        ['foo', 'bar']

    """
    
    return [str(f) for f in header(table)]

    
def iterdata(table, *sliceargs):
    """
    Return an iterator over the data rows for the given table. E.g.::
    
        >>> from petl import data
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> it = iterdata(table)
        >>> it.next()
        ['a', 1]
        >>> it.next()
        ['b', 2]
        
    .. versionchanged:: 0.3
    
    Positional arguments can be used to slice the data rows. The `sliceargs` are 
    passed to :func:`itertools.islice`.
    
    .. versionchanged:: 0.10
    
    Renamed from "data".
    
    """

    it = islice(table, 1, None) # skip header row
    if sliceargs:
        it = islice(it, *sliceargs)
    return it
    

def data(table, *sliceargs):
    """
    Return a container supporting iteration over data rows in a given 
    table. I.e., like :func:`iterdata` only a container is returned so you 
    can iterate over it multiple times.
    
    .. versionchanged:: 0.10 

    Now returns a container, previously returned an iterator. See also 
    :func:`iterdata`.
    """
    
    return DataContainer(table, *sliceargs)


class DataContainer(RowContainer):
    
    def __init__(self, table, *sliceargs):
        self.table = table
        self.sliceargs = sliceargs
        
    def __iter__(self):
        return iterdata(self.table, *self.sliceargs) 
        
           
def dataslice(table, *args):
    """
    .. deprecated:: 0.3
    
    Use :func:`data` instead, it supports slice arguments.
    
    """
    
    return islice(data(table), *args)

    
def iterdicts(table, *sliceargs, **kwargs):
    """
    Return an iterator over the data in the table, yielding each row as a 
    dictionary of values indexed by field name. E.g.::
    
        >>> from petl import dicts
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> it = dicts(table)
        >>> it.next()
        {'foo': 'a', 'bar': 1}
        >>> it.next()
        {'foo': 'b', 'bar': 2}
        >>> it.next()
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration
        
    Short rows are padded, e.g.::
    
        >>> table = [['foo', 'bar'], ['a', 1], ['b']]
        >>> it = dicts(table)
        >>> it.next()
        {'foo': 'a', 'bar': 1}
        >>> it.next()
        {'foo': 'b', 'bar': None}
        >>> it.next()
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration
        
    .. versionadded:: 0.15

    """
    
    if 'missing' in kwargs:
        missing = kwargs['missing']
    else:
        missing = None
    it = iter(table)
    flds = it.next()
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        yield asdict(flds, row, missing)
        
        
class DictsContainer(IterContainer):

    def __init__(self, table, *sliceargs, **kwargs):
        self.table = table
        self.sliceargs = sliceargs
        self.kwargs = kwargs
        
    def __iter__(self):
        return iterdicts(self.table, *self.sliceargs, **self.kwargs)
    
    def __repr__(self):
        vreprs = map(repr, islice(self, 11))
        r = '\n'.join(vreprs[:10])
        if len(vreprs) > 10:
            r += '\n...'
        return r


def dicts(table, *sliceargs, **kwargs):
    """
    Return a container supporting iteration over rows as dicts. I.e., like 
    :func:`iterdicts` only a container is returned so you can iterate over it 
    multiple times.
    
    .. versionadded:: 0.15

    """
    
    return DictsContainer(table, *sliceargs, **kwargs)
        
    
def asdict(flds, row, missing=None):
    names = [str(f) for f in flds]
    try:
        # list comprehension should be faster
        items = [(names[i], row[i]) for i in range(len(names))]
    except IndexError:
        # short row, fall back to slower for loop
        items = list()
        for i, f in enumerate(names):
            try:
                v = row[i]
            except IndexError:
                v = missing
            items.append((f, v))
    return dict(items)
    

def asnamedtuple(nt, row, missing=None):
    try:
        return nt(*row)
    except TypeError:
        # row may be long or short
        # expected number of fields
        ne = len(nt._fields)
        # actual number of values
        na = len(row)
        if ne > na:
            # pad short rows
            padded = tuple(row) + (missing,) * (ne-na)
            return nt(*padded)
        elif ne < na:
            # truncate long rows
            return nt(*row[:ne])
        else:
            raise
        

def namedtuples(table, *sliceargs, **kwargs):
    """
    View the table as a container of named tuples. I.e., like 
    :func:`iternamedtuples` only a container is returned so you can iterate over 
    it multiple times.
    
    .. versionadded:: 0.15
    
    """

    return NamedTuplesContainer(table, *sliceargs, **kwargs)


class NamedTuplesContainer(IterContainer):

    def __init__(self, table, *sliceargs, **kwargs):
        self.table = table
        self.sliceargs = sliceargs
        self.kwargs = kwargs
        
    def __iter__(self):
        return iternamedtuples(self.table, *self.sliceargs, **self.kwargs)
    
    def __repr__(self):
        vreprs = map(repr, islice(self, 11))
        r = '\n'.join(vreprs[:10])
        if len(vreprs) > 10:
            r += '\n...'
        return r


def iternamedtuples(table, *sliceargs, **kwargs):
    """
    Return an iterator over the data in the table, yielding each row as a 
    named tuple.
    
    .. versionadded:: 0.15
    
    """
    
    if 'missing' in kwargs:
        missing = kwargs['missing']
    else:
        missing = None
    if 'name' in kwargs:
        name = kwargs['name']
    else:
        name = 'row'
    it = iter(table)
    flds = it.next()
    nt = namedtuple(name, tuple(flds))
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        yield asnamedtuple(nt, row, missing)
    

def nrows(table):
    """
    Count the number of data rows in a table. E.g.::
    
        >>> from petl import nrows
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> nrows(table)
        2
        
    .. versionchanged:: 0.10
    
    Renamed from 'rowcount' to 'nrows'.
    
    """
    
    return sum(1 for _ in iterdata(table))
    
    
rowcount = nrows # backwards compatibility

    
def look(table, *sliceargs, **kwargs):
    """
    Format a portion of the table as text for inspection in an interactive
    session. E.g.::
    
        >>> from petl import look
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+

    Any irregularities in the length of header and/or data rows will appear as
    blank cells, e.g.::
    
        >>> table = [['foo', 'bar'], ['a'], ['b', 2, True]]
        >>> look(table)
        +-------+-------+------+
        | 'foo' | 'bar' |      |
        +=======+=======+======+
        | 'a'   |       |      |
        +-------+-------+------+
        | 'b'   | 2     | True |
        +-------+-------+------+
        
    .. versionchanged:: 0.3
    
    Positional arguments can be used to slice the data rows. The `sliceargs` are 
    passed to :func:`itertools.islice`.
    
    .. versionchanged:: 0.8
    
    The properties `n` and `p` can be used to look at the next and previous rows
    respectively. I.e., try ``>>> look(table)`` then ``>>> _.n`` then ``>>> _.p``. 

    .. versionchanged:: 0.13
    
    Three alternative presentation styles are available: 'grid', 'simple' and 
    'minimal', where 'grid' is the default. A different style can be specified
    using the `style` keyword argument, e.g.::
    
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> look(table, style='simple')
        =====  =====
        'foo'  'bar'
        =====  =====
        'a'        1
        'b'        2
        =====  =====
        
        >>> look(table, style='minimal')
        'foo'  'bar'
        'a'        1
        'b'        2
        
    The default style can also be changed, e.g.::
        
        >>> look.default_style = 'simple'
        >>> look(table)
        =====  =====
        'foo'  'bar'
        =====  =====
        'a'        1
        'b'        2
        =====  =====
        
        >>> look.default_style = 'grid'
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   |     1 |
        +-------+-------+
        | 'b'   |     2 |
        +-------+-------+    
    
    See also :func:`lookall` and :func:`see`.
    
    """
    
    return Look(table, *sliceargs, **kwargs)


def lookall(table, **kwargs):
    """
    Format the entire table as text for inspection in an interactive session.
    
    N.B., this will load the entire table into memory.
    """
    
    return look(table, 0, None, **kwargs)
    
    
class Look(object):
    
    def __init__(self, table, *sliceargs, **kwargs):
        self.table = table
        if not sliceargs:
            self.sliceargs = (10,)
        else:
            self.sliceargs = sliceargs
        if 'vrepr' in kwargs:
            self.vrepr = kwargs['vrepr']
        else:
            self.vrepr = repr
        if 'style' in kwargs:
            self.style = kwargs['style']
        else:
            self.style = look.default_style
        
    @property
    def n(self):
        if not self.sliceargs:
            sliceargs = (10,)
        elif len(self.sliceargs) == 1:
            stop = self.sliceargs[0]
            sliceargs = (stop, 2*stop)
        elif len(self.sliceargs) == 2:
            start = self.sliceargs[0]
            stop = self.sliceargs[1]
            page = stop - start
            sliceargs = (stop, stop + page)
        else:
            start = self.sliceargs[0]
            stop = self.sliceargs[1]
            page = stop - start
            step = self.sliceargs[2]
            sliceargs = (stop, stop + page, step)
        return Look(self.table, *sliceargs)
    
    @property
    def p(self):
        if not self.sliceargs:
            sliceargs = (10,)
        elif len(self.sliceargs) == 1:
            # already at the start, do nothing
            sliceargs = self.sliceargs
        elif len(self.sliceargs) == 2:
            start = self.sliceargs[0]
            stop = self.sliceargs[1]
            page = stop - start
            if start - page < 0:
                sliceargs = (0, page)
            else:
                sliceargs = (start - page, start)
        else:
            start = self.sliceargs[0]
            stop = self.sliceargs[1]
            page = stop - start
            step = self.sliceargs[2]
            if start - page < 0:
                sliceargs = (0, page, step)
            else:
                sliceargs = (start - page, start, step)
        return Look(self.table, *sliceargs)
    
    def __repr__(self):
        if self.style == 'simple':
            return format_table_simple(self.table, self.vrepr, self.sliceargs)
        elif self.style == 'minimal':
            return format_table_minimal(self.table, self.vrepr, self.sliceargs)
        else:
            return format_table_grid(self.table, self.vrepr, self.sliceargs)
    
    
    def __str__(self):
        return repr(self)


look.default_style = 'grid'
    
    
def format_table_grid(table, vrepr, sliceargs):
    it = iter(table)
    
    # fields representation
    flds = it.next()
    fldsrepr = [vrepr(f) for f in flds]
    
    # rows representations
    rows = list(islice(it, *sliceargs))
    rowsrepr = [[vrepr(v) for v in row] for row in rows]
    
    # find maximum row length - may be uneven
    rowlens = [len(flds)]
    rowlens.extend([len(row) for row in rows])
    maxrowlen = max(rowlens)
    
    # pad short fields and rows
    if len(flds) < maxrowlen:
        fldsrepr.extend([u''] * (maxrowlen - len(flds)))
    for valsrepr in rowsrepr:
        if len(valsrepr) < maxrowlen:
            valsrepr.extend([u''] * (maxrowlen - len(valsrepr)))
    
    # find longest representations so we know how wide to make cells
    colwidths = [0] * maxrowlen # initialise to 0
    for i, fr in enumerate(fldsrepr):
        colwidths[i] = len(fr)
    for valsrepr in rowsrepr:
        for i, vr in enumerate(valsrepr):
            if len(vr) > colwidths[i]:
                colwidths[i] = len(vr)
                
    # construct a line separator
    sep = u'+'
    for w in colwidths:
        sep += u'-' * (w + 2)
        sep += u'+'
    sep += u'\n'
    
    # construct a header separator
    hedsep = u'+'
    for w in colwidths:
        hedsep += u'=' * (w + 2)
        hedsep += u'+'
    hedsep += u'\n'
    
    # construct a line for the header row
    fldsline = u'|'
    for i, w in enumerate(colwidths):
        f = fldsrepr[i]
        fldsline += u' ' + f
        fldsline += u' ' * (w - len(f)) # padding
        fldsline += u' |'
    fldsline += u'\n'
    
    # construct a line for each data row
    rowlines = list()
    for vals, valsrepr in zip(rows, rowsrepr):
        rowline = u'|'
        for i, w in enumerate(colwidths):
            vr = valsrepr[i]
            if i < len(vals) and isinstance(vals[i], (int, long, float)):
                # left pad numbers
                rowline += u' ' * (w + 1 - len(vr)) # padding
                rowline += vr + u' |'
            else:      
                # right pad everything else
                rowline += u' ' + vr
                rowline += u' ' * (w - len(vr)) # padding
                rowline += u' |'
        rowline += u'\n'
        rowlines.append(rowline)
        
    # put it all together
    output = sep + fldsline + hedsep
    for line in rowlines:
        output += line + sep
    
    return output


def format_table_simple(table, vrepr, sliceargs):
    it = iter(table)
    
    # fields representation
    flds = it.next()
    fldsrepr = [vrepr(f) for f in flds]
    
    # rows representations
    rows = list(islice(it, *sliceargs))
    rowsrepr = [[vrepr(v) for v in row] for row in rows]
    
    # find maximum row length - may be uneven
    rowlens = [len(flds)]
    rowlens.extend([len(row) for row in rows])
    maxrowlen = max(rowlens)
    
    # pad short fields and rows
    if len(flds) < maxrowlen:
        fldsrepr.extend([u''] * (maxrowlen - len(flds)))
    for valsrepr in rowsrepr:
        if len(valsrepr) < maxrowlen:
            valsrepr.extend([u''] * (maxrowlen - len(valsrepr)))
    
    # find longest representations so we know how wide to make cells
    colwidths = [0] * maxrowlen # initialise to 0
    for i, fr in enumerate(fldsrepr):
        colwidths[i] = len(fr)
    for valsrepr in rowsrepr:
        for i, vr in enumerate(valsrepr):
            if len(vr) > colwidths[i]:
                colwidths[i] = len(vr)
                
    # construct a header separator
    hedsep = u'  '.join(u'=' * w for w in colwidths)
    hedsep += u'\n'
    
    # construct a line for the header row
    fldsline = u'  '.join(f.ljust(w) for f, w in zip(fldsrepr, colwidths))
    fldsline += u'\n'
    
    # construct a line for each data row
    rowlines = list()
    for vals, valsrepr in zip(rows, rowsrepr):
        rowline = u''
        for i, w in enumerate(colwidths):
            vr = valsrepr[i]
            if i < len(vals) and isinstance(vals[i], (int, long, float)):
                # left pad numbers
                rowline += vr.rjust(w)
            else:      
                # right pad everything else
                rowline += vr.ljust(w)
            if i < len(colwidths) - 1:
                rowline += '  '
        rowline += u'\n'
        rowlines.append(rowline)
        
    # put it all together
    output = hedsep + fldsline + hedsep
    for line in rowlines:
        output += line
    output += hedsep
    
    return output
        
        
def format_table_minimal(table, vrepr, sliceargs):
    it = iter(table)
    
    # fields representation
    flds = it.next()
    fldsrepr = [vrepr(f) for f in flds]
    
    # rows representations
    rows = list(islice(it, *sliceargs))
    rowsrepr = [[vrepr(v) for v in row] for row in rows]
    
    # find maximum row length - may be uneven
    rowlens = [len(flds)]
    rowlens.extend([len(row) for row in rows])
    maxrowlen = max(rowlens)
    
    # pad short fields and rows
    if len(flds) < maxrowlen:
        fldsrepr.extend([u''] * (maxrowlen - len(flds)))
    for valsrepr in rowsrepr:
        if len(valsrepr) < maxrowlen:
            valsrepr.extend([u''] * (maxrowlen - len(valsrepr)))
    
    # find longest representations so we know how wide to make cells
    colwidths = [0] * maxrowlen # initialise to 0
    for i, fr in enumerate(fldsrepr):
        colwidths[i] = len(fr)
    for valsrepr in rowsrepr:
        for i, vr in enumerate(valsrepr):
            if len(vr) > colwidths[i]:
                colwidths[i] = len(vr)
                
    # construct a line for the header row
    fldsline = u'  '.join(f.ljust(w) for f, w in zip(fldsrepr, colwidths))
    fldsline += u'\n'
    
    # construct a line for each data row
    rowlines = list()
    for vals, valsrepr in zip(rows, rowsrepr):
        rowline = u''
        for i, w in enumerate(colwidths):
            vr = valsrepr[i]
            if i < len(vals) and isinstance(vals[i], (int, long, float)):
                # left pad numbers
                rowline += vr.rjust(w)
            else:      
                # right pad everything else
                rowline += vr.ljust(w)
            if i < len(colwidths) - 1:
                rowline += '  '
        rowline += u'\n'
        rowlines.append(rowline)
        
    # put it all together
    output = fldsline
    for line in rowlines:
        output += line
    
    return output
        
        
def lookstr(table, *sliceargs):
    """
    Like :func:`look` but use str() rather than repr() for cell
    contents.

    .. versionadded:: 0.10
    
    """
    
    return Look(table, *sliceargs, vrepr=str)


def lookallstr(table):
    """
    Like :func:`lookall` but use str() rather than repr() for cell
    contents.

    .. versionadded:: 0.10
    
    """
    
    return lookstr(table, 0, None)
        
        
def see(table, *sliceargs):
    """
    Format a portion of a table as text in a column-oriented layout for 
    inspection in an interactive session. E.g.::
    
        >>> from petl import see
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> see(table)
        'foo': 'a', 'b'
        'bar': 1, 2

    Useful for tables with a larger number of fields.
    
    .. versionchanged:: 0.3
    
    Positional arguments can be used to slice the data rows. The `sliceargs` are 
    passed to :func:`itertools.islice`.

    """

    return See(table, *sliceargs)


class See(object):
    
    def __init__(self, table, *sliceargs):
        self.table = table
        if not sliceargs:
            self.sliceargs = (10,)
        else:
            self.sliceargs = sliceargs
        
    def __repr__(self):    
        it = iter(self.table)
        flds = it.next()
        cols = defaultdict(list)
        for row in islice(it, *self.sliceargs):
            for i, f in enumerate(flds):
                try:
                    cols[str(f)].append(repr(row[i]))
                except IndexError:
                    cols[str(f)].append('')
        output = u''
        for f in flds:
            output += u'%r: %s\n' % (f, u', '.join(cols[str(f)]))
        return output
        
    
def itervalues(table, field, *sliceargs, **kwargs):
    """
    Return an iterator over values in a given field or fields. E.g.::
    
        >>> from petl import itervalues
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> foo = itervalues(table, 'foo')
        >>> foo.next()
        'a'
        >>> foo.next()
        'b'
        >>> foo.next()
        'b'
        >>> foo.next()
        'c'
        >>> foo.next()
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration

    The `field` argument can be a single field name or index (starting from zero)
    or a tuple of field names and/or indexes.    

    If rows are uneven, the value of the keyword argument `missing` is returned.
        
    More than one field can be selected, e.g.::
    
        >>> table = [['foo', 'bar', 'baz'],
        ...          [1, 'a', True],
        ...          [2, 'bb', True],
        ...          [3, 'd', False]]
        >>> foobaz = itervalues(table, ('foo', 'baz'))
        >>> foobaz.next()
        (1, True)
        >>> foobaz.next()
        (2, True)
        >>> foobaz.next()
        (3, False)
        >>> foobaz.next()
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration

    .. versionchanged:: 0.3
    
    Positional arguments can be used to slice the data rows. The `sliceargs` are 
    passed to :func:`itertools.islice`.
    
    .. versionchanged:: 0.7 
    
    In previous releases this function was known as 'values'. Also in this release
    the behaviour with short rows is changed. Now for any value missing due to a 
    short row, ``None`` is returned by default, or whatever is given by the
    `missing` keyword argument.

    """
    
    if 'missing' in kwargs:
        missing = kwargs['missing']
    else:
        missing = None
    it = iter(table)
    srcflds = it.next()
    indices = asindices(srcflds, field)
    assert len(indices) > 0, 'no field selected'
    getvalue = itemgetter(*indices)
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        try:
            value = getvalue(row)
            yield value
        except IndexError:
            yield missing
    
    
def values(table, field, *sliceargs, **kwargs):
    """
    Return a container supporting iteration over values in a given field or 
    fields. I.e., like :func:`itervalues` only a container is returned so you 
    can iterate over it multiple times.
    
    .. versionchanged:: 0.7 

    Now returns a container, previously returned an iterator. See also 
    :func:`itervalues`.
    
    """
    
    return ValuesContainer(table, field, *sliceargs, **kwargs)
    
    
class ValuesContainer(IterContainer):

    def __init__(self, table, field, *sliceargs, **kwargs):
        self.table = table
        self.field = field
        self.sliceargs = sliceargs
        self.kwargs = kwargs
        
    def __iter__(self):
        return itervalues(self.table, self.field, *self.sliceargs, **self.kwargs)
    
    def __repr__(self):
        vreprs = map(repr, islice(self, 11))
        r = ', '.join(vreprs[:10])
        if len(vreprs) > 10:
            r += ', ...'
        return r
    
        
def valueset(table, field, missing=None):
    """
    .. deprecated:: 0.3
    
    Use ``set(values(table, *fields))`` instead, see also :func:`values`.
        
    """

    return set(itervalues(table, field, missing=missing))


def valuecount(table, field, value, missing=None):
    """
    Count the number of occurrences of `value` under the given field. Returns
    the absolute count and relative frequency as a pair. E.g.::
    
        >>> from petl import valuecount
        >>> table = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 7))
        >>> n, f = valuecount(table, 'foo', 'b')
        >>> n
        2
        >>> f
        0.6666666666666666

    The `field` argument can be a single field name or index (starting from zero)
    or a tuple of field names and/or indexes.    

    """
    
    if isinstance(field, (list, tuple)):
        it = itervalues(table, *field, missing=missing)
    else:
        it = itervalues(table, field, missing=missing)
    total = 0
    vs = 0
    for v in it:
        total += 1
        if v == value:
            vs += 1
    return vs, float(vs)/total
    
    
def valuecounter(table, field, missing=None):
    """
    Find distinct values for the given field and count the number of 
    occurrences. Returns a :class:`dict` mapping values to counts. E.g.::

        >>> from petl import valuecounter
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> c = valuecounter(table, 'foo')
        >>> c['a']
        1
        >>> c['b']
        2
        >>> c['c']
        1
        >>> c
        Counter({'b': 2, 'a': 1, 'c': 1})
    
    The `field` argument can be a single field name or index (starting from zero)
    or a tuple of field names and/or indexes.    

    """

    counter = Counter()
    for v in itervalues(table, field, missing=missing):
        try:
            counter[v] += 1
        except IndexError:
            pass # short row
    return counter
            

def valuecounts(table, *fields, **kwargs):    
    """
    Find distinct values for the given field and count the number and relative
    frequency of occurrences. Returns a table mapping values to counts, with most common 
    values first. E.g.::

        >>> from petl import valuecounts, look
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> look(valuecounts(table, 'foo'))
        +---------+---------+-------------+
        | 'value' | 'count' | 'frequency' |
        +=========+=========+=============+
        | 'b'     | 2       | 0.5         |
        +---------+---------+-------------+
        | 'a'     | 1       | 0.25        |
        +---------+---------+-------------+
        | 'c'     | 1       | 0.25        |
        +---------+---------+-------------+
        
        >>> look(valuecounts(table, 'bar'))
        +---------+---------+--------------------+
        | 'value' | 'count' | 'frequency'        |
        +=========+=========+====================+
        | True    | 2       | 0.6666666666666666 |
        +---------+---------+--------------------+
        | False   | 1       | 0.3333333333333333 |
        +---------+---------+--------------------+
            
    If more than one field is given, a report of value counts for each field
    is given, e.g.::
    
        >>> look(valuecounts(table, 'foo', 'bar'))
        +---------+---------+---------+-------------+
        | 'field' | 'value' | 'count' | 'frequency' |
        +=========+=========+=========+=============+
        | 'foo'   | 'b'     |       2 |         0.5 |
        +---------+---------+---------+-------------+
        | 'foo'   | 'a'     |       1 |        0.25 |
        +---------+---------+---------+-------------+
        | 'foo'   | 'c'     |       1 |        0.25 |
        +---------+---------+---------+-------------+
        | 'bar'   |    True |       2 |         0.5 |
        +---------+---------+---------+-------------+
        | 'bar'   | None    |       1 |        0.25 |
        +---------+---------+---------+-------------+
        | 'bar'   |   False |       1 |        0.25 |
        +---------+---------+---------+-------------+
        
    If rows are short, the value of the keyword argument `missing` is counted.
    
    """
    
    if len(fields) == 1:
        return ValueCountsView(table, fields[0], **kwargs)
    else:
        return MultiValueCountsView(table, fields, **kwargs)


class ValueCountsView(RowContainer):
    
    def __init__(self, table, field, missing=None):
        self.table = table
        self.field = field
        self.missing = missing
        
    def __iter__(self):
        counter = valuecounter(self.table, self.field, missing=self.missing)
        yield ('value', 'count', 'frequency')
        counts = counter.most_common()
        total = sum(c[1] for c in counts)
        for c in counts:
            yield (c[0], c[1], float(c[1])/total)

        
class MultiValueCountsView(RowContainer):
    
    def __init__(self, table, fields, missing=None):
        self.table = table
        self.fields = fields
        self.missing = missing
        
    def __iter__(self):
        
        counters = dict()
        it = iter(self.table)
        fields = it.next()
        if self.fields:
            self.countfields = self.fields
        else:
            self.countfields = fields
        for f in self.countfields:
            counters[f] = Counter()
        for row in it:
            for f, v in izip_longest(fields, row, fillvalue=self.missing):
                if f != self.missing and f in self.countfields:
                    counters[f][v] += 1
                
        yield ('field', 'value', 'count', 'frequency')
        for f in self.countfields:
            counts = counters[f].most_common()
            total = sum(c[1] for c in counts)
            for c in counts:
                yield (f, c[0], c[1], float(c[1])/total)

        
def columns(table, missing=None):
    """
    Construct a :class:`dict` mapping field names to lists of values. E.g.::
    
        >>> from petl import columns
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> cols = columns(table)
        >>> cols['foo']
        ['a', 'b', 'b']
        >>> cols['bar']    
        [1, 2, 3]

    See also :func:`facetcolumns`.
    
    """
    
    cols = dict()
    it = iter(table)
    fields = [str(f) for f in it.next()]
    for f in fields:
        cols[f] = list()
    for row in it:
        for f, v in izip_longest(fields, row, fillvalue=missing):
            if f in cols:
                cols[f].append(v)
    return cols


def facetcolumns(table, key, missing=None):
    """
    Like :func:`columns` but stratified by values of the given key field. E.g.::
    
        >>> from petl import facetcolumns
        >>> table = [['foo', 'bar', 'baz'], 
        ...          ['a', 1, True], 
        ...          ['b', 2, True], 
        ...          ['b', 3]]
        >>> fc = facetcolumns(table, 'foo')
        >>> fc['a']
        {'baz': [True], 'foo': ['a'], 'bar': [1]}
        >>> fc['b']
        {'baz': [True, None], 'foo': ['b', 'b'], 'bar': [2, 3]}
        >>> fc['c']
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        KeyError: 'c'

    .. versionadded:: 0.8
    
    """
    
    fct = dict()
    it = iter(table)
    fields = [str(f) for f in it.next()]
    indices = asindices(fields, key)
    assert len(indices) > 0, 'no key field selected'
    getkey = itemgetter(*indices)
    
    for row in it:
        kv = getkey(row)
        if kv not in fct:
            cols = dict()
            for f in fields:
                cols[f] = list()
            fct[kv] = cols
        else:
            cols = fct[kv]
        for f, v in izip_longest(fields, row, fillvalue=missing):
            if f in cols:
                cols[f].append(v)
        
    return fct
    
    
def isunique(table, field):
    """
    Return True if there are no duplicate values for the given field(s), otherwise
    False. E.g.::

        >>> from petl import isunique
        >>> table = [['foo', 'bar'], ['a', 1], ['b'], ['b', 2], ['c', 3, True]]
        >>> isunique(table, 'foo')
        False
        >>> isunique(table, 'bar')
        True
    
    The `field` argument can be a single field name or index (starting from zero)
    or a tuple of field names and/or indexes.    

    .. versionchanged:: 0.10
    
    Renamed from "unique". See also :func:`petl.unique`.
    
    """    

    vals = set()
    for v in itervalues(table, field):
        if v in vals:
            return False
        else:
            vals.add(v)
    return True
       
        
# TODO handle short rows in lookup, lookupone, dictlookup, dictlookupone?


def lookup(table, keyspec, valuespec=None, dictionary=None):
    """
    Load a dictionary with data from the given table. E.g.::
    
        >>> from petl import lookup
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookup(table, 'foo', 'bar')
        >>> lkp['a']
        [1]
        >>> lkp['b']
        [2, 3]

    If no `valuespec` argument is given, defaults to the whole
    row (as a tuple), e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookup(table, 'foo')
        >>> lkp['a']
        [('a', 1)]
        >>> lkp['b']
        [('b', 2), ('b', 3)]

    Compound keys are supported, e.g.::
    
        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True],
        ...       ['b', 3, False]]
        >>> lkp = lookup(t2, ('foo', 'bar'), 'baz')
        >>> lkp[('a', 1)]
        [True]
        >>> lkp[('b', 2)]
        [False]
        >>> lkp[('b', 3)]
        [True, False]

    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::
    
        >>> import shelve
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = shelve.open('mylookup.dat')
        >>> lkp = lookup(table, 'foo', 'bar', lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24) 
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mylookup.dat')
        >>> lkp['a']
        [1]
        >>> lkp['b']
        [2, 3]

    """
    
    if dictionary is None:
        dictionary = dict()
        
    it = iter(table)
    flds = it.next()
    if valuespec is None:
        valuespec = flds # default valuespec is complete row
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    valueindices = asindices(flds, valuespec)
    assert len(valueindices) > 0, 'no valuespec selected'
    getkey = itemgetter(*keyindices)
    getvalue = itemgetter(*valueindices)
    for row in it:
        k = getkey(row)
        v = getvalue(row)
        if k in dictionary:
            # work properly with shelve
            l = dictionary[k]
            l.append(v)
            dictionary[k] = l
        else:
            dictionary[k] = [v]
    return dictionary
    
    
def lookupone(table, keyspec, valuespec=None, dictionary=None, strict=False):
    """
    Load a dictionary with data from the given table, assuming there is
    at most one value for each key. E.g.::
    
        >>> from petl import lookupone
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = lookupone(table, 'foo', 'bar')
        >>> lkp['a']
        1
        >>> lkp['b']
        2
        >>> lkp['c']
        2
        
    If the specified key is not unique and strict=False (default),
    the first value wins, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookupone(table, 'foo', 'bar', strict=False)
        >>> lkp['a']
        1
        >>> lkp['b']
        2
        
    If the specified key is not unique and strict=True, will raise 
    DuplicateKeyError, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookupone(table, 'foo', strict=True)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 451, in lookupone
        petl.util.DuplicateKeyError
        
    Compound keys are supported, e.g.::
    
        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True]]
        >>> lkp = lookupone(t2, ('foo', 'bar'), 'baz')
        >>> lkp[('a', 1)]
        True
        >>> lkp[('b', 2)]
        False
        >>> lkp[('b', 3)]
        True
    
    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::
    
        >>> from petl import lookupone
        >>> import shelve
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = shelve.open('mylookupone.dat')
        >>> lkp = lookupone(table, 'foo', 'bar', dictionary=lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24) 
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mylookupone.dat')
        >>> lkp['a']
        1
        >>> lkp['b']
        2
        >>> lkp['c']
        2

    .. versionchanged:: 0.11
    
    Changed so that strict=False is default and first value wins.
    
    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = it.next()
    if valuespec is None:
        valuespec = flds
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    valueindices = asindices(flds, valuespec)
    assert len(valueindices) > 0, 'no valuespec selected'
    getkey = itemgetter(*keyindices)
    getvalue = itemgetter(*valueindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError
        elif k not in dictionary:
            v = getvalue(row)
            dictionary[k] = v
    return dictionary
    
    
def dictlookup(table, keyspec, dictionary=None):
    """
    Load a dictionary with data from the given table, mapping to dicts. E.g.::
    
        >>> from petl import dictlookup
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = dictlookup(table, 'foo')
        >>> lkp['a']
        [{'foo': 'a', 'bar': 1}]
        >>> lkp['b']
        [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]

    Compound keys are supported, e.g.::
    
        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True],
        ...       ['b', 3, False]]
        >>> lkp = dictlookup(t2, ('foo', 'bar'))
        >>> lkp[('a', 1)]
        [{'baz': True, 'foo': 'a', 'bar': 1}]
        >>> lkp[('b', 2)]
        [{'baz': False, 'foo': 'b', 'bar': 2}]
        >>> lkp[('b', 3)]
        [{'baz': True, 'foo': 'b', 'bar': 3}, {'baz': False, 'foo': 'b', 'bar': 3}]
    
    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::

        >>> import shelve
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = shelve.open('mydictlookup.dat')
        >>> lkp = dictlookup(table, 'foo', dictionary=lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24) 
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mydictlookup.dat')
        >>> lkp['a']
        [{'foo': 'a', 'bar': 1}]
        >>> lkp['b']
        [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]

    .. versionchanged:: 0.15
    
    Renamed from `recordlookup`.
    
    """
    
    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = it.next()
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        rec = asdict(flds, row)
        if k in dictionary:
            # work properly with shelve
            l = dictionary[k]
            l.append(rec)
            dictionary[k] = l
        else:
            dictionary[k] = [rec]
    return dictionary
    
    
def recordlookup(table, keyspec, dictionary=None):
    """
    Load a dictionary with data from the given table, mapping to record objects.

    .. versionadded:: 0.17

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = it.next()
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        rec = Record(row, flds)
        if k in dictionary:
            # work properly with shelve
            l = dictionary[k]
            l.append(rec)
            dictionary[k] = l
        else:
            dictionary[k] = [rec]
    return dictionary

        
def dictlookupone(table, keyspec, dictionary=None, strict=False):
    """
    Load a dictionary with data from the given table, mapping to dicts,
    assuming there is at most one row for each key. E.g.::
    
        >>> from petl import dictlookupone
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = dictlookupone(table, 'foo')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}
        >>> lkp['c']
        {'foo': 'c', 'bar': 2}
        
    If the specified key is not unique and strict=False (default), 
    the first dict wins, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = dictlookupone(table, 'foo')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}
        
    If the specified key is not unique and strict=True, will raise 
    DuplicateKeyError, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = dictlookupone(table, 'foo', strict=True)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 451, in lookupone
        petl.util.DuplicateKeyError
        
    Compound keys are supported, e.g.::
    
        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True]]
        >>> lkp = dictlookupone(t2, ('foo', 'bar'), strict=False)
        >>> lkp[('a', 1)]
        {'baz': True, 'foo': 'a', 'bar': 1}
        >>> lkp[('b', 2)]
        {'baz': False, 'foo': 'b', 'bar': 2}
        >>> lkp[('b', 3)]
        {'baz': True, 'foo': 'b', 'bar': 3}
    
    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::

        >>> import shelve
        >>> lkp = shelve.open('mydictlookupone.dat')
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = dictlookupone(table, 'foo', dictionary=lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24) 
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mydictlookupone.dat')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}
        >>> lkp['c']
        {'foo': 'c', 'bar': 2}

    .. versionchanged:: 0.11
    
    Changed so that strict=False is default and first value wins.
    
    .. versionchanged:: 0.15
    
    Renamed from `recordlookupone`.
    
    """    

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = it.next()
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError
        elif k not in dictionary:
            d = asdict(flds, row)
            dictionary[k] = d
    return dictionary


def recordlookupone(table, keyspec, dictionary=None, strict=False):
    """
    Load a dictionary with data from the given table, mapping to record objects,
    assuming there is at most one row for each key.

    .. versionchanged:: 0.17

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = it.next()
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError
        elif k not in dictionary:
            d = Record(row, flds)
            dictionary[k] = d
    return dictionary


class DuplicateKeyError(Exception):
    pass


def asindices(flds, spec):
    """
    TODO doc me
    
    """

    names = [str(f) for f in flds]
    indices = list()
    if isinstance(spec, basestring):
        spec = (spec,)
    if isinstance(spec, int):
        spec = (spec,)
    for s in spec:
        # spec could be a field name
        if s in names:
            indices.append(names.index(s))
        # or spec could be a field index
        elif isinstance(s, int) and s < len(names):
            indices.append(s) # index fields from 0
        else:
            raise FieldSelectionError(s)
    return indices
        
        
class FieldSelectionError(Exception):
    """
    TODO doc me
    
    """
    
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return 'selection is not a field or valid field index: %s' % self.value
    
    
def rowitemgetter(fields, spec):
    indices = asindices(fields, spec)
    getter = itemgetter(*indices)
    return getter

    
def rowgetter(*indices):
    """
    TODO doc me
    
    """
    
    # guard condition
    assert len(indices) > 0, 'indices is empty'

    # if only one index, we cannot use itemgetter, because we want a singleton 
    # sequence to be returned, but itemgetter with a single argument returns the 
    # value itself, so let's define a function
    if len(indices) == 1:
        index = indices[0]
        return lambda row: (row[index],) # note comma - singleton tuple!
    # if more than one index, use itemgetter, it should be the most efficient
    else:
        return itemgetter(*indices)
    
    
def rowlengths(table):
    """
    Report on row lengths found in the table. E.g.::
    
        >>> from petl import look, rowlengths
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', '2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', 'xyz', 9.0],
        ...          ['E', None],
        ...          ['F', 9]]
        >>> look(rowlengths(table))
        +----------+---------+
        | 'length' | 'count' |
        +==========+=========+
        | 3        | 3       |
        +----------+---------+
        | 2        | 2       |
        +----------+---------+
        | 4        | 1       |
        +----------+---------+

    Useful for finding potential problems in data files.
    
    """

    it = data(table)
    counter = Counter()
    for row in it:
        counter[len(row)] += 1
    output = [('length', 'count')]
    output.extend(counter.most_common())
    return output


def typecounter(table, field):    
    """
    Count the number of values found for each Python type. E.g.::

        >>> from petl import typecounter
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', u'xyz', 9.0],
        ...          ['E', 42]]
        >>> typecounter(table, 'foo')
        Counter({'str': 4, 'unicode': 1})
        >>> typecounter(table, 'bar')
        Counter({'unicode': 3, 'int': 2})
        >>> typecounter(table, 'baz')
        Counter({'int': 1, 'float': 1, 'unicode': 1, 'str': 1})

    The `field` argument can be a field name or index (starting from zero).    
    
    """
    
    counter = Counter()
    for v in itervalues(table, field):
        try:
            counter[v.__class__.__name__] += 1
        except IndexError:
            pass # ignore short rows
    return counter


def typecounts(table, field, **kwargs):    
    """
    Count the number of values found for each Python type and return a table
    mapping class names to counts and frequencies. E.g.::

        >>> from petl import look, typecounts
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', u'xyz', 9.0],
        ...          ['E', 42]]
        >>> look(typecounts(table, 'foo'))
        +-----------+---------+-------------+
        | 'type'    | 'count' | 'frequency' |
        +===========+=========+=============+
        | 'str'     | 4       | 0.8         |
        +-----------+---------+-------------+
        | 'unicode' | 1       | 0.2         |
        +-----------+---------+-------------+
        
        >>> look(typecounts(table, 'bar'))
        +-----------+---------+-------------+
        | 'type'    | 'count' | 'frequency' |
        +===========+=========+=============+
        | 'unicode' | 3       | 0.6         |
        +-----------+---------+-------------+
        | 'int'     | 2       | 0.4         |
        +-----------+---------+-------------+
        
        >>> look(typecounts(table, 'baz'))
        +-----------+---------+-------------+
        | 'type'    | 'count' | 'frequency' |
        +===========+=========+=============+
        | 'int'     | 1       | 0.25        |
        +-----------+---------+-------------+
        | 'float'   | 1       | 0.25        |
        +-----------+---------+-------------+
        | 'unicode' | 1       | 0.25        |
        +-----------+---------+-------------+
        | 'str'     | 1       | 0.25        |
        +-----------+---------+-------------+
        
    The `field` argument can be a field name or index (starting from zero).
    
    .. versionchanged:: 0.6
    
    Added frequency.    
 
    """
    
    return TypeCountsView(table, field, **kwargs)


class TypeCountsView(RowContainer):
    
    def __init__(self, table, field):
        self.table = table
        self.field = field
        
    def __iter__(self):
        counter = typecounter(self.table, self.field)
        yield ('type', 'count', 'frequency')
        counts = counter.most_common()
        total = sum(c[1] for c in counts)
        for c in counts:
            yield (c[0], c[1], float(c[1])/total)


def typeset(table, field):
    """
    Return a set containing all Python types found for values in the given field.
    E.g.::
    
        >>> from petl import typeset
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, '2'],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', '7.8', True],
        ...          ['D', u'xyz', 9.0],
        ...          ['E', 42]]
        >>> typeset(table, 'foo') 
        set([<type 'str'>, <type 'unicode'>])
        >>> typeset(table, 'bar') 
        set([<type 'int'>, <type 'unicode'>])
        >>> typeset(table, 'baz') 
        set([<type 'float'>, <type 'str'>])
    
    The `field` argument can be a field name or index (starting from zero).    

    """

    s = set()
    for v in itervalues(table, field):
        try:
            s.add(v.__class__)
        except IndexError:
            pass # ignore short rows
    return s
    

def parsecounter(table, field, parsers={'int': int, 'float': float}):    
    """
    Count the number of `str` or `unicode` values under the given fields that can 
    be parsed as ints, floats or via custom parser functions. Return a pair of 
    `Counter` objects, the first mapping parser names to the number of strings 
    successfully parsed, the second mapping parser names to the number of errors. 
    E.g.::
    
        >>> from petl import parsecounter
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 'aaa', 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', '3.7', 9.0],
        ...          ['E', 42]]
        >>> counter, errors = parsecounter(table, 'bar')
        >>> counter
        Counter({'float': 3, 'int': 2})
        >>> errors
        Counter({'int': 2, 'float': 1})
        
    The `field` argument can be a field name or index (starting from zero).    

    """
    
    counter, errors = Counter(), Counter()
    # need to initialise
    for n in parsers.keys():
        counter[n] = 0
        errors[n] = 0
    for v in itervalues(table, field):
        if isinstance(v, basestring):
            for name, parser in parsers.items():
                try:
                    parser(v)
                except:
                    errors[name] += 1
                else:
                    counter[name] += 1
    return counter, errors


def parsecounts(table, field, parsers={'int': int, 'float': float}):    
    """
    Count the number of `str` or `unicode` values that can be parsed as ints, 
    floats or via custom parser functions. Return a table mapping parser names
    to the number of values successfully parsed and the number of errors. E.g.::
    
        >>> from petl import look, parsecounts
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 'aaa', 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', '3.7', 9.0],
        ...          ['E', 42]]
        >>> look(parsecounts(table, 'bar'))
        +---------+---------+----------+
        | 'type'  | 'count' | 'errors' |
        +=========+=========+==========+
        | 'float' | 3       | 1        |
        +---------+---------+----------+
        | 'int'   | 2       | 2        |
        +---------+---------+----------+

    The `field` argument can be a field name or index (starting from zero).    

    """
    
    return ParseCountsView(table, field, parsers=parsers)


class ParseCountsView(RowContainer):
    
    def __init__(self, table, field, parsers={'int': int, 'float': float}):
        self.table = table
        self.field = field
        self.parsers = parsers
        
    def __iter__(self):
        counter, errors = parsecounter(self.table, self.field, self.parsers)
        yield ('type', 'count', 'errors')
        for (item, count) in counter.most_common():
            yield (item, count, errors[item])


def datetimeparser(fmt, strict=True):
    """
    Return a function to parse strings as :class:`datetime.datetime` objects using a given format.
    E.g.::

        >>> from petl import datetimeparser
        >>> isodatetime = datetimeparser('%Y-%m-%dT%H:%M:%S')
        >>> isodatetime('2002-12-25T00:00:00')
        datetime.datetime(2002, 12, 25, 0, 0)
        >>> isodatetime('2002-12-25T00:00:99')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1018, in parser
            return datetime.strptime(value.strip(), format)
          File "/usr/lib/python2.7/_strptime.py", line 328, in _strptime
            data_string[found.end():])
        ValueError: unconverted data remains: 9

    Can be used with :func:`parsecounts`, e.g.::
    
        >>> from petl import look, parsecounts, datetimeparser
        >>> table = [['when', 'who'],
        ...          ['2002-12-25T00:00:00', 'Alex'],
        ...          ['2004-09-12T01:10:11', 'Gloria'],
        ...          ['2002-13-25T00:00:00', 'Marty'],
        ...          ['2002-02-30T07:09:00', 'Melman']]
        >>> parsers={'datetime': datetimeparser('%Y-%m-%dT%H:%M:%S')}
        >>> look(parsecounts(table, 'when', parsers))
        +------------+---------+----------+
        | 'type'     | 'count' | 'errors' |
        +============+=========+==========+
        | 'datetime' | 2       | 2        |
        +------------+---------+----------+
        
    .. versionchanged:: 0.6
    
    Added `strict` keyword argument. If ``strict=False`` then if an error occurs
    when parsing, the original value will be returned as-is, and no error will
    be raised. Allows for, e.g., incremental parsing of mixed format fields.
    
    """
    
    def parser(value):
        try:
            return datetime.datetime.strptime(value.strip(), fmt)
        except:
            if strict:
                raise
            else:
                return value
    return parser
    

def dateparser(fmt, strict=True):
    """
    Return a function to parse strings as :class:`datetime.date` objects using a given format.
    E.g.::
    
        >>> from petl import dateparser
        >>> isodate = dateparser('%Y-%m-%d')
        >>> isodate('2002-12-25')
        datetime.date(2002, 12, 25)
        >>> isodate('2002-02-30')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1032, in parser
            return parser
          File "/usr/lib/python2.7/_strptime.py", line 440, in _strptime
            datetime_date(year, 1, 1).toordinal() + 1
        ValueError: day is out of range for month


    Can be used with :func:`parsecounts`, e.g.::
    
        >>> from petl import look, parsecounts, dateparser
        >>> table = [['when', 'who'],
        ...          ['2002-12-25', 'Alex'],
        ...          ['2004-09-12', 'Gloria'],
        ...          ['2002-13-25', 'Marty'],
        ...          ['2002-02-30', 'Melman']]
        >>> parsers={'date': dateparser('%Y-%m-%d')}
        >>> look(parsecounts(table, 'when', parsers))
        +--------+---------+----------+
        | 'type' | 'count' | 'errors' |
        +========+=========+==========+
        | 'date' | 2       | 2        |
        +--------+---------+----------+
        
    .. versionchanged:: 0.6
    
    Added `strict` keyword argument. If ``strict=False`` then if an error occurs
    when parsing, the original value will be returned as-is, and no error will
    be raised. Allows for, e.g., incremental parsing of mixed format fields.
    
    """
    
    def parser(value):
        try:
            return datetime.datetime.strptime(value.strip(), fmt).date()
        except:
            if strict:
                raise
            else:
                return value
    return parser
    

def timeparser(fmt, strict=True):
    """
    Return a function to parse strings as :class:`datetime.time` objects using a given format.
    E.g.::
    
        >>> from petl import timeparser
        >>> isotime = timeparser('%H:%M:%S')
        >>> isotime('00:00:00')
        datetime.time(0, 0)
        >>> isotime('13:00:00')
        datetime.time(13, 0)
        >>> isotime('12:00:99')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1046, in parser
            
          File "/usr/lib/python2.7/_strptime.py", line 328, in _strptime
            data_string[found.end():])
        ValueError: unconverted data remains: 9
        >>> isotime('25:00:00')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1046, in parser
            
          File "/usr/lib/python2.7/_strptime.py", line 325, in _strptime
            (data_string, format))
        ValueError: time data '25:00:00' does not match format '%H:%M:%S'

    Can be used with :func:`parsecounts`, e.g.::
    
        >>> from petl import look, parsecounts, timeparser
        >>> table = [['when', 'who'],
        ...          ['00:00:00', 'Alex'],
        ...          ['12:02:45', 'Gloria'],
        ...          ['25:01:01', 'Marty'],
        ...          ['09:70:00', 'Melman']]
        >>> parsers={'time': timeparser('%H:%M:%S')}
        >>> look(parsecounts(table, 'when', parsers))
        +--------+---------+----------+
        | 'type' | 'count' | 'errors' |
        +========+=========+==========+
        | 'time' | 2       | 2        |
        +--------+---------+----------+
    
    .. versionchanged:: 0.6
    
    Added `strict` keyword argument. If ``strict=False`` then if an error occurs
    when parsing, the original value will be returned as-is, and no error will
    be raised. Allows for, e.g., incremental parsing of mixed format fields.
    
    """
    
    def parser(value):
        try:
            return datetime.datetime.strptime(value.strip(), fmt).time()
        except:
            if strict:
                raise
            else:
                return value
    return parser
    

def boolparser(true_strings=['true', 't', 'yes', 'y', '1'], 
               false_strings=['false', 'f', 'no', 'n', '0'],
               case_sensitive=False, 
               strict=True):
    """
    Return a function to parse strings as :class:`bool` objects using a given set of
    string representations for `True` and `False`.
    E.g.::

        >>> from petl import boolparser    
        >>> mybool = boolparser(true_strings=['yes', 'y'], false_strings=['no', 'n'])
        >>> mybool('y')
        True
        >>> mybool('Y')
        True
        >>> mybool('yes')
        True
        >>> mybool('No')
        False
        >>> mybool('nO')
        False
        >>> mybool('true')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1175, in parser
            raise ValueError('value is not one of recognised boolean strings: %r' % value)
        ValueError: value is not one of recognised boolean strings: 'true'
        >>> mybool('foo')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1175, in parser
            raise ValueError('value is not one of recognised boolean strings: %r' % value)
        ValueError: value is not one of recognised boolean strings: 'foo'
    
    Can be used with :func:`parsecounts`, e.g.::
    
        >>> from petl import look, parsecounts, boolparser
        >>> table = [['who', 'vote'],
        ...          ['Alex', 'yes'],
        ...          ['Gloria', 'N'],
        ...          ['Marty', 'hmmm'],
        ...          ['Melman', 'nope']]
        >>> mybool = boolparser(true_strings=['yes', 'y'], false_strings=['no', 'n'])
        >>> parsers = {'bool': mybool}
        >>> look(parsecounts(table, 'vote', parsers))
        +--------+---------+----------+
        | 'type' | 'count' | 'errors' |
        +========+=========+==========+
        | 'bool' | 2       | 2        |
        +--------+---------+----------+

    .. versionchanged:: 0.6
    
    Added `strict` keyword argument. If ``strict=False`` then if an error occurs
    when parsing, the original value will be returned as-is, and no error will
    be raised. Allows for, e.g., incremental parsing of mixed format fields.
    
    """
    
    if not case_sensitive:
        true_strings = [s.lower() for s in true_strings]
        false_strings = [s.lower() for s in false_strings]
    def parser(value):
        value = value.strip()
        if not case_sensitive:
            value = value.lower()
        if value in true_strings:
            return True
        elif value in false_strings:
            return False
        elif strict:
            raise ValueError('value is not one of recognised boolean strings: %r' % value)
        else:
            return value
    return parser
    

def limits(table, field):
    """
    Find minimum and maximum values under the given field. E.g.::
    
        >>> from petl import limits
        >>> t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> minv, maxv = limits(t1, 'bar')
        >>> minv
        1
        >>> maxv
        3
    
    The `field` argument can be a field name or index (starting from zero).    

    """
    
    vals = itervalues(table, field)
    try:
        minv = maxv = vals.next()
    except StopIteration:
        return None, None
    else:
        for v in vals:
            if v < minv:
                minv = v
            if v > maxv:
                maxv = v
        return minv, maxv


def stats(table, field):
    """
    Calculate basic descriptive statistics on a given field. E.g.::
    
        >>> from petl import stats
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', '2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', 'xyz', 9.0],
        ...          ['E', None]]
        >>> stats(table, 'bar')    
        {'count': 3, 'errors': 2, 'min': 1.0, 'max': 3.0, 'sum': 6.0, 'mean': 2.0}
        
    The `field` argument can be a field name or index (starting from zero).    

    """
    
    output = {'min': None, 
              'max': None,
              'sum': None, 
              'mean': None, 
              'count': 0, 
              'errors': 0}
    for v in itervalues(table, field):
        try:
            v = float(v)
        except:
            output['errors'] += 1
        else:
            if output['min'] is None or v < output['min']:
                output['min'] = v
            if output['max'] is None or v > output['max']:
                output['max'] = v
            if output['sum'] is None:
                output['sum'] = v
            else:
                output['sum'] += v
            output['count'] += 1
    if output['count'] > 0:
        output['mean'] = output['sum'] / output['count']
    return output
        

def expr(s):
    """
    Construct a function operating on a record (i.e., a dictionary representation
    of a data row, indexed by field name).
    
    The expression string is converted into a lambda function by prepending
    the string with ``'lambda rec: '``, then replacing anything enclosed in 
    curly braces (e.g., ``"{foo}"``) with a lookup on the record (e.g., 
    ``"rec['foo']"``), then finally calling :func:`eval`.
    
    So, e.g., the expression string ``"{foo} * {bar}"`` is converted to the 
    function ``lambda rec: rec['foo'] * rec['bar']``
    
    """
    
    prog = re.compile('\{([^}]+)\}')
    def repl(matchobj):
        return "rec['%s']" % matchobj.group(1)
    return eval("lambda rec: " + prog.sub(repl, s))
    
    
def strjoin(s):
    """
    Return a function to join sequences using `s` as the separator.
    
    """
    
    return lambda l: s.join(map(str, l))


def parsenumber(v, strict=False):
    """
    Attempt to parse the value as a number, trying :func:`int`, :func:`long`,
    :func:`float` and :func:`complex` in that order. If all fail, return the
    value as-is.
    
    .. versionadded:: 0.4
    
    .. versionchanged:: 0.7 Set ``strict=True`` to get an exception if parsing fails.
    
    """
    
    try:
        return int(v)
    except:
        pass
    try:
        return long(v)
    except:
        pass
    try:
        return float(v)
    except:
        pass
    try:
        return complex(v)
    except:
        if strict:
            raise
    return v


def stringpatterncounter(table, field):
    """
    Profile string patterns in the given field, returning a :class:`dict` 
    mapping patterns to counts. 

    .. versionadded:: 0.5

    """
    
    trans = maketrans('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', 
                      'AAAAAAAAAAAAAAAAAAAAAAAAAAaaaaaaaaaaaaaaaaaaaaaaaaaa9999999999')
    counter = Counter()
    for v in itervalues(table, field):
        p = str(v).translate(trans)
        counter[p] += 1
    return counter


def stringpatterns(table, field):
    """
    Profile string patterns in the given field, returning a table of patterns,
    counts and frequencies. E.g.::

        >>> from petl import stringpatterns, look    
        >>> table = [['foo', 'bar'],
        ...          ['Mr. Foo', '123-1254'],
        ...          ['Mrs. Bar', '234-1123'],
        ...          ['Mr. Spo', '123-1254'],
        ...          [u'Mr. Baz', u'321 1434'],
        ...          [u'Mrs. Baz', u'321 1434'],
        ...          ['Mr. Quux', '123-1254-XX']]
        >>> foopats = stringpatterns(table, 'foo')
        >>> look(foopats)
        +------------+---------+---------------------+
        | 'pattern'  | 'count' | 'frequency'         |
        +============+=========+=====================+
        | 'Aa. Aaa'  | 3       | 0.5                 |
        +------------+---------+---------------------+
        | 'Aaa. Aaa' | 2       | 0.3333333333333333  |
        +------------+---------+---------------------+
        | 'Aa. Aaaa' | 1       | 0.16666666666666666 |
        +------------+---------+---------------------+
        
        >>> barpats = stringpatterns(table, 'bar')
        >>> look(barpats)
        +---------------+---------+---------------------+
        | 'pattern'     | 'count' | 'frequency'         |
        +===============+=========+=====================+
        | '999-9999'    | 3       | 0.5                 |
        +---------------+---------+---------------------+
        | '999 9999'    | 2       | 0.3333333333333333  |
        +---------------+---------+---------------------+
        | '999-9999-AA' | 1       | 0.16666666666666666 |
        +---------------+---------+---------------------+

    .. versionadded:: 0.5

    """
    
    counter = stringpatterncounter(table, field)
    output = [('pattern', 'count', 'frequency')]
    counter = counter.most_common()
    total = sum(c[1] for c in counter)
    counts = [(c[0], c[1], float(c[1])/total) for c in counter]
    output.extend(counts)
    return output


def randomtable(numflds=5, numrows=100, wait=0):
    """
    Construct a table with random numerical data. Use `numflds` and `numrows` to
    specify the number of fields and rows respectively. Set `wait` to a float
    greater than zero to simulate a delay on each row generation (number of 
    seconds per row). E.g.::
    
        >>> from petl import randomtable, look
        >>> t = randomtable(5, 10000)
        >>> look(t)
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 'f0'                | 'f1'                | 'f2'                | 'f3'                 | 'f4'                 |
        +=====================+=====================+=====================+======================+======================+
        | 0.37981479583619415 | 0.5651754962690851  | 0.5219839418441516  | 0.400507081757018    | 0.18772722969580335  |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.8523718373108918  | 0.9728988775985702  | 0.539819811070272   | 0.5253127991162814   | 0.032332586052070345 |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.15767415808765595 | 0.8723372406647985  | 0.8116271113050197  | 0.19606663402788693  | 0.02917384287810021  |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.29027126477145737 | 0.9458013821235983  | 0.0558711583090582  | 0.8388382491420909   | 0.533855533396786    |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.7299727877963395  | 0.7293822340944851  | 0.953624640847381   | 0.7161554959575555   | 0.8681001821667421   |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.7057077618876934  | 0.5222733323906424  | 0.26527912571554013 | 0.41069309093677264  | 0.7062831671289698   |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.9447075997744453  | 0.3980291877822444  | 0.5748113148854611  | 0.037655670603881974 | 0.30826709590498524  |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.21559911346698513 | 0.8353039675591192  | 0.5558847892537019  | 0.8561403358605812   | 0.01109608253313421  |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.27334411287843097 | 0.10064946027523636 | 0.7476185996637322  | 0.26201984851765325  | 0.6303996377010502   |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.8348722928576766  | 0.40319578510057763 | 0.3658094978577834  | 0.9829576880714145   | 0.6170025401631835   |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
    
    Note that the data are generated on the fly and are not stored in memory,
    so this function can be used to simulate very large tables.
    
    .. versionadded:: 0.6
    
    See also :func:`dummytable`.
    
    """
    
    return RandomTable(numflds, numrows, wait=wait)
    
    
class RandomTable(RowContainer):
    
    def __init__(self, numflds=5, numrows=100, wait=0):
        self.numflds = numflds
        self.numrows = numrows
        self.wait = wait
        self.seed = datetime.datetime.now()
        
    def __iter__(self):

        nf = self.numflds
        nr = self.numrows
        seed = self.seed
        
        # N.B., we want this to be stable, i.e., same data each time
        random.seed(seed)
        
        # construct fields
        flds = ['f%s' % n for n in range(nf)]
        yield tuple(flds)

        # construct data rows
        for _ in xrange(nr):
            # artificial delay
            if self.wait:
                time.sleep(self.wait)
            yield tuple(random.random() for n in range(nf))
            
    def reseed(self):
        self.seed = datetime.datetime.now()
                
        
def dummytable(numrows=100, 
               fields=[('foo', partial(random.randint, 0, 100)), 
                       ('bar', partial(random.choice, ['apples', 'pears', 'bananas', 'oranges'])), 
                       ('baz', random.random)], 
               wait=0):
    """
    Construct a table with dummy data. Use `numrows` to specify the number of 
    rows. Set `wait` to a float greater than zero to simulate a delay on each 
    row generation (number of seconds per row). E.g.::
    
        >>> from petl import dummytable, look
        >>> t1 = dummytable(10000)
        >>> look(t1)
        +-------+-----------+----------------------+
        | 'foo' | 'bar'     | 'baz'                |
        +=======+===========+======================+
        | 98    | 'oranges' | 0.017443519200384117 |
        +-------+-----------+----------------------+
        | 85    | 'pears'   | 0.6126183086894914   |
        +-------+-----------+----------------------+
        | 43    | 'apples'  | 0.8354915052285888   |
        +-------+-----------+----------------------+
        | 32    | 'pears'   | 0.9612740566307508   |
        +-------+-----------+----------------------+
        | 35    | 'bananas' | 0.4845179128370132   |
        +-------+-----------+----------------------+
        | 16    | 'pears'   | 0.150174888085586    |
        +-------+-----------+----------------------+
        | 98    | 'bananas' | 0.22592589109877748  |
        +-------+-----------+----------------------+
        | 82    | 'bananas' | 0.4887849296756226   |
        +-------+-----------+----------------------+
        | 75    | 'apples'  | 0.8414305202212253   |
        +-------+-----------+----------------------+
        | 78    | 'bananas' | 0.025845900016858714 |
        +-------+-----------+----------------------+
    
    Note that the data are generated on the fly and are not stored in memory,
    so this function can be used to simulate very large tables.
    
    Data generation functions can be specified via the `fields` keyword argument,
    or set on the table via the suffix notation, e.g.::
    
        >>> import random
        >>> from functools import partial
        >>> t2 = dummytable(10000, fields=[('foo', random.random), ('bar', partial(random.randint, 0, 500))])
        >>> t2['baz'] = partial(random.choice, ['chocolate', 'strawberry', 'vanilla'])
        >>> look(t2)
        +---------------------+-------+--------------+
        | 'foo'               | 'bar' | 'baz'        |
        +=====================+=======+==============+
        | 0.04595169186388326 | 370   | 'strawberry' |
        +---------------------+-------+--------------+
        | 0.29252999472988905 | 90    | 'chocolate'  |
        +---------------------+-------+--------------+
        | 0.7939324498894116  | 146   | 'chocolate'  |
        +---------------------+-------+--------------+
        | 0.4964898678468417  | 123   | 'chocolate'  |
        +---------------------+-------+--------------+
        | 0.26250784199548494 | 327   | 'strawberry' |
        +---------------------+-------+--------------+
        | 0.748470693146964   | 275   | 'strawberry' |
        +---------------------+-------+--------------+
        | 0.8995553034254133  | 151   | 'strawberry' |
        +---------------------+-------+--------------+
        | 0.26331484411715367 | 211   | 'chocolate'  |
        +---------------------+-------+--------------+
        | 0.4740252948218193  | 364   | 'vanilla'    |
        +---------------------+-------+--------------+
        | 0.166428545780258   | 59    | 'vanilla'    |
        +---------------------+-------+--------------+
        
    .. versionchanged:: 0.6
    
    Now supports different field types, e.g., non-numeric. Previous functionality
    is available as :func:`randomtable`.
        
    """
    
    return DummyTable(numrows=numrows, fields=fields, wait=wait)


class DummyTable(RowContainer):
    
    def __init__(self, numrows=100, fields=None, wait=0):
        self.numrows = numrows
        self.wait = wait
        if fields is None:
            self.fields = OrderedDict()
        else:
            self.fields = OrderedDict(fields)
        self.seed = datetime.datetime.now()

    def __setitem__(self, item, value):
        self.fields[str(item)] = value
            
    def __iter__(self):
        nr = self.numrows
        seed = self.seed
        fields = self.fields.copy()
        
        # N.B., we want this to be stable, i.e., same data each time
        random.seed(seed)
        
        # construct header row
        header = tuple(str(f) for f in fields.keys())
        yield header

        # construct data rows
        for _ in xrange(nr):
            # artificial delay
            if self.wait:
                time.sleep(self.wait)
            yield tuple(fields[f]() for f in fields)
            
    def reseed(self):
        self.seed = datetime.datetime.now()
        

def diffheaders(t1, t2):
    """
    Return the difference between the headers of the two tables as a pair of
    sets. E.g.::

        >>> from petl import diffheaders    
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 1, .3]]
        >>> table2 = [['baz', 'bar', 'quux'],
        ...           ['a', 1, .3]]
        >>> add, sub = diffheaders(table1, table2)
        >>> add
        set(['quux'])
        >>> sub
        set(['foo'])

    .. versionadded:: 0.6
    
    """
    
    t1h = set(header(t1))
    t2h = set(header(t2))
    return t2h - t1h, t1h - t2h


def diffvalues(t1, t2, f):
    """
    Return the difference between the values under the given field in the two
    tables, e.g.::
    
        >>> from petl import diffvalues
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 3]]
        >>> table2 = [['bar', 'foo'],
        ...           [1, 'a'],
        ...           [3, 'c']]
        >>> add, sub = diffvalues(table1, table2, 'foo')
        >>> add
        set(['c'])
        >>> sub
        set(['b'])

    .. versionadded:: 0.6
    
    """

#from petl import diffvalues
#table1 = [['foo', 'bar'],
#          ['a', 1],
#          ['b', 3]]
#table2 = [['bar', 'foo'],
#          [1, 'a'],
#          [3, 'c']]
#add, sub = diffvalues(table1, table2, 'foo')
#add
#sub
    
    t1v = set(itervalues(t1, f))
    t2v = set(itervalues(t2, f))
    return t2v - t1v, t1v - t2v


Keyed = namedtuple('Keyed', ['key', 'obj'])
    
    
def heapqmergesorted(key=None, *iterables):            
    """
    Return a single iterator over the given iterables, sorted by the given `key`
    function, assuming the input iterables are already sorted by the same function. 
    (I.e., the merge part of a general merge sort.) Uses :func:`heapq.merge` for
    the underlying implementation. See also :func:`shortlistmergesorted`.
    
    .. versionadded:: 0.9
        
    """
    
    if key is None:
        keyed_iterables = iterables
        for element in heapq.merge(*keyed_iterables):
            yield element
    else:
        keyed_iterables = [(Keyed(key(obj), obj) for obj in iterable) for iterable in iterables]
        for element in heapq.merge(*keyed_iterables):
            yield element.obj


def shortlistmergesorted(key=None, reverse=False, *iterables):
    """
    Return a single iterator over the given iterables, sorted by the given `key`
    function, assuming the input iterables are already sorted by the same function. 
    (I.e., the merge part of a general merge sort.) Uses :func:`min` (or :func:`max` 
    if ``reverse=True``) for the underlying implementation. See also 
    :func:`heapqmergesorted`.
    
    .. versionadded:: 0.9
        
    """
    
    if reverse:
        op = max
    else:
        op = min
    if key is not None:
        opkwargs = {'key': key}
    else:
        opkwargs = dict()
    # populate initial shortlist
    # (remember some iterables might be empty)
    iterators = list()
    shortlist = list()
    for iterable in iterables:
        it = iter(iterable)
        try:
            first = it.next()
            iterators.append(it)
            shortlist.append(first)
        except StopIteration:
            pass
    # do the mergesort
    while iterators:
        nxt = op(shortlist, **opkwargs)
        yield nxt
        nextidx = shortlist.index(nxt)
        try:
            shortlist[nextidx] = iterators[nextidx].next()
        except StopIteration:
            del shortlist[nextidx]
            del iterators[nextidx]
        
    
class Record(tuple):
    
    def __new__(cls, row, flds, missing=None):
        t = super(HybridRow, cls).__new__(cls, row)
        return t
    
    def __init__(self, row, flds, missing=None):
        self.flds = flds
        self.missing = missing
        
    def __getitem__(self, f):
        if isinstance(f, int):
            return super(HybridRow, self).__getitem__(f)
        elif f in self.flds:
            try:
                return super(HybridRow, self).__getitem__(self.flds.index(f))
            except IndexError: # handle short rows
                return self.missing
        else:
            raise Exception('item ' + str(f) + ' not in fields ' + str(self.flds))

    def __getattr__(self, f):
        if f in self.flds:
            try:
                return super(HybridRow, self).__getitem__(self.flds.index(f))
            except IndexError: # handle short rows
                return self.missing
        else:
            raise Exception('item ' + str(f) + ' not in fields ' + str(self.flds))


# backwards compatibility
HybridRow = Record


def iterrecords(table, *sliceargs, **kwargs):
    """
    Return an iterator over the data in the table, where rows support value 
    access by index or field name. See also :func:`iterdicts`.
    
    .. versionchanged:: 0.15 
    
    Previously returned dicts, now returns hybrid objects which behave like 
    tuples/dicts/namedtuples. 

    """
    
    if 'missing' in kwargs:
        missing = kwargs['missing']
    else:
        missing = None
    it = iter(table)
    flds = it.next()
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        yield Record(row, flds, missing=missing)
        
        
class RecordsContainer(IterContainer):

    def __init__(self, table, *sliceargs, **kwargs):
        self.table = table
        self.sliceargs = sliceargs
        self.kwargs = kwargs
        
    def __iter__(self):
        return iterrecords(self.table, *self.sliceargs, **self.kwargs)
    
    def __repr__(self):
        vreprs = map(repr, islice(self, 11))
        r = '\n'.join(vreprs[:10])
        if len(vreprs) > 10:
            r += '\n...'
        return r


def records(table, *sliceargs, **kwargs):
    """
    Return a container supporting iteration over rows as records. I.e., like 
    :func:`iterrecords` only a container is returned so you can iterate over it 
    multiple times. See also :func:`dicts`.
    
    .. versionchanged:: 0.15
    
    Previously returned dicts, now returns hybrid objects which behave like 
    tuples/dicts/namedtuples. 

    """
    
    return RecordsContainer(table, *sliceargs, **kwargs)
    
    
# retain for backwards compatibility
def hybridrows(flds, it, missing=None):
    return (HybridRow(row, flds, missing) for row in it)
    
    
def progress(table, batchsize=1000, prefix="", out=sys.stderr):
    """
    Report progress on rows passing through. E.g.::
    
        >>> from petl import dummytable, progress, tocsv
        >>> d = dummytable(100500)
        >>> p = progress(d, 10000)
        >>> tocsv(p, 'output.csv')
        10000 rows in 0.57s (17574 rows/second); batch in 0.57s (17574 rows/second)
        20000 rows in 1.13s (17723 rows/second); batch in 0.56s (17876 rows/second)
        30000 rows in 1.69s (17732 rows/second); batch in 0.56s (17749 rows/second)
        40000 rows in 2.27s (17652 rows/second); batch in 0.57s (17418 rows/second)
        50000 rows in 2.83s (17679 rows/second); batch in 0.56s (17784 rows/second)
        60000 rows in 3.39s (17694 rows/second); batch in 0.56s (17769 rows/second)
        70000 rows in 3.96s (17671 rows/second); batch in 0.57s (17534 rows/second)
        80000 rows in 4.53s (17677 rows/second); batch in 0.56s (17720 rows/second)
        90000 rows in 5.09s (17681 rows/second); batch in 0.56s (17715 rows/second)
        100000 rows in 5.66s (17675 rows/second); batch in 0.57s (17625 rows/second)
        100500 rows in 5.69s (17674 rows/second)
    
    See also :func:`clock`.
    
    .. versionadded:: 0.10
    
    """
    
    return ProgressView(table, batchsize, prefix, out)


class ProgressView(RowContainer):
    
    def __init__(self, wrapped, batchsize, prefix, out):
        self.wrapped = wrapped
        self.batchsize = batchsize
        self.prefix = prefix
        self.out = out
        
    def __iter__(self):
        start = time.time()
        batchstart = start
        for n, r in enumerate(self.wrapped):
            if n % self.batchsize == 0 and n > 0:
                batchend = time.time()
                batchtime = batchend - batchstart
                elapsedtime = batchend - start
                rate = int(n / elapsedtime)
                batchrate = int(self.batchsize / batchtime)
                v = (n, elapsedtime, rate, batchtime, batchrate)
                message = self.prefix + '%s rows in %.2fs (%s row/s); batch in %.2fs (%s row/s)' % v
                print >>self.out, message
                batchstart = batchend
            yield r
        end = time.time()
        elapsedtime = end - start
        rate = int(n / elapsedtime)    
        v = (n, elapsedtime, rate)
        message = self.prefix + '%s rows in %.2fs (%s row/s)' % v
        print >>self.out, message
            

def clock(table):
    """
    Time how long is spent retrieving rows from the wrapped container. Enables
    diagnosis of which steps in a pipeline are taking the most time. E.g.::

        >>> from petl import dummytable, clock, convert, progress, tocsv
        >>> t1 = dummytable(100000)
        >>> c1 = clock(t1)
        >>> t2 = convert(c1, 'foo', lambda v: v**2)
        >>> c2 = clock(t2)
        >>> p = progress(c2, 10000)
        >>> tocsv(p, 'dummy.csv')
        10000 rows in 1.17s (8559 rows/second); batch in 1.17s (8559 rows/second)
        20000 rows in 2.34s (8548 rows/second); batch in 1.17s (8537 rows/second)
        30000 rows in 3.51s (8547 rows/second); batch in 1.17s (8546 rows/second)
        40000 rows in 4.68s (8541 rows/second); batch in 1.17s (8522 rows/second)
        50000 rows in 5.89s (8483 rows/second); batch in 1.21s (8261 rows/second)
        60000 rows in 7.30s (8221 rows/second); batch in 1.40s (7121 rows/second)
        70000 rows in 8.59s (8144 rows/second); batch in 1.30s (7711 rows/second)
        80000 rows in 9.78s (8182 rows/second); batch in 1.18s (8459 rows/second)
        90000 rows in 10.98s (8193 rows/second); batch in 1.21s (8279 rows/second)
        100000 rows in 12.30s (8132 rows/second); batch in 1.31s (7619 rows/second)
        100000 rows in 12.30s (8132 rows/second)
        >>> # time consumed retrieving rows from t1
        ... c1.time
        5.4099999999999895
        >>> # time consumed retrieving rows from t2
        ... c2.time
        8.740000000000006
        >>> # actual time consumed by the convert step
        ... c2.time - c1.time 
        3.330000000000016
    
    See also :func:`progress`.
    
    .. versionadded:: 0.10
    
    """
    
    return ClockView(table)


class ClockView(RowContainer):
    
    def __init__(self, wrapped):
        self.wrapped = wrapped
        
    def __iter__(self):
        self.time = 0
        it = iter(self.wrapped)
        while True:
            before = time.clock()
            row = it.next()
            after = time.clock()
            self.time += (after - before)
            yield row


def isordered(table, key=None, reverse=False, strict=False):
    """
    Return True if the table is ordered (i.e., sorted) by the given key. E.g.::
    
        >>> from petl import isordered, look
        >>> look(table)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 1     | True  |
        +-------+-------+-------+
        | 'b'   | 3     | True  |
        +-------+-------+-------+
        | 'b'   | 2     |       |
        +-------+-------+-------+
        
        >>> isordered(table, key='foo')
        True
        >>> isordered(table, key='foo', strict=True)
        False
        >>> isordered(table, key='foo', reverse=True)
        False

    .. versionadded:: 0.10
    
    """

    # determine the operator to use when comparing rows
    if reverse and strict:
        op = operator.lt
    elif reverse and not strict:
        op = operator.le
    elif strict:
        op = operator.gt
    else:
        op = operator.ge
        
    it = iter(table)
    fieldnames = [str(f) for f in it.next()]
    if key is None:
        prev = it.next()
        for curr in it:
            if not op(curr, prev):
                return False
            prev = curr
    else:
        getkey = itemgetter(*asindices(fieldnames, key))
        prev = it.next()
        prevkey = getkey(prev)
        for curr in it:
            currkey = getkey(curr)
            if not op(currkey, prevkey):
                return False
            prevkey = currkey
    return True
    
    
def rowgroupby(table, key, value=None):
    """
    Convenient adapter for :func:`itertools.groupby`. E.g.::

        >>> from petl import rowgroupby, look
        >>> look(table)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 1     | True  |
        +-------+-------+-------+
        | 'b'   | 3     | True  |
        +-------+-------+-------+
        | 'b'   | 2     |       |
        +-------+-------+-------+
        
        >>> # group entire rows
        ... for key, group in rowgroupby(table, 'foo'):
        ...     print key, list(group)
        ... 
        a [('a', 1, True)]
        b [('b', 3, True), ('b', 2)]
        >>> # group specific values
        ... for key, group in rowgroupby(table, 'foo', 'bar'):
        ...     print key, list(group)
        ... 
        a [1]
        b [3, 2]

    N.B., assumes the input table is already sorted by the given key.
    
    .. versionadded:: 0.10
    
    """
    
    it = iter(table)
    fields = it.next()
    
    # wrap rows 
    it = hybridrows(fields, it)
        
    # determine key function
    if callable(key):
        getkey = key
    else:
        kindices = asindices(fields, key)
        getkey = itemgetter(*kindices)
    
    # determine value function
    if value is None:
        return groupby(it, key=getkey)
    else:
        if callable(value):
            getval = value
        else:
            vindices = asindices(fields, value)
            getval = itemgetter(*vindices)
        return ((k, (getval(v) for v in vals)) for k, vals in groupby(it, key=getkey))


def iterpeek(it, n=1):
    it = iter(it) # make sure it's an iterator
    if n == 1:
        peek = it.next()
        return peek, chain([peek], it)
    else:
        peek = list(islice(it, n))
        return peek, chain(peek, it)
    

def rowgroupbybin(table, key, width, value=None, minv=None, maxv=None):
    """
    Group rows into bins of a given width.
    
    """

    it = iter(table)
    fields = it.next()
    
    # wrap rows 
    it = hybridrows(fields, it)

    # determine key function
    if callable(key):
        getkey = key
    else:
        kindices = asindices(fields, key)
        getkey = itemgetter(*kindices)
    
    # determine value function
    if value is None:
        getval = lambda v: v # identity function - i.e., whole row
    else:
        if callable(value):
            getval = value
        else:
            vindices = asindices(fields, value)
            getval = itemgetter(*vindices)
            
    # use a different algorithm if minv and maxv are specified - fixed bins
    if minv is not None and maxv is not None:
        numbins = int(ceil((maxv - minv) / width))
        keyv = None
        for n in xrange(0, numbins):
            binminv = minv + n*width
            binmaxv = binminv + width
            if binmaxv >= maxv: # final bin
                binmaxv = maxv # truncate final bin to specified maximum
            binnedvals = []
            try:
                while keyv < binminv: # advance until we're within the bin's range
                    row = it.next()
                    keyv = getkey(row)
                while binminv <= keyv < binmaxv: # within the bin
                    binnedvals.append(getval(row))
                    row = it.next()
                    keyv = getkey(row)
                while keyv == binmaxv == maxv: # possible floating point precision bug here?
                    binnedvals.append(getval(row)) # last bin is open if maxv is specified
                    row = it.next()
                    keyv = getkey(row)
            except StopIteration:
                pass
            yield (binminv, binmaxv), binnedvals

    else:
        
        # initialise minimum
        try:
            row = it.next()
        except StopIteration:
            pass
        else:
            keyv = getkey(row)
            if minv is None:
                minv = keyv # initialise minimum to first key value found
        
            # N.B., we need to account for two possible scenarios
            # (1) maxv is not specified, so keep making bins until we run out of rows
            # (2) maxv is specified, so iterate over bins up to maxv
            try:
        
                for binminv in count(minv, width):
                    binmaxv = binminv + width
                    if maxv is not None and binmaxv >= maxv: # final bin
                        binmaxv = maxv # truncate final bin to specified maximum
                    binnedvals = []
                    while keyv < binminv: # advance until we're within the bin's range
                        row = it.next()
                        keyv = getkey(row)
                    while binminv <= keyv < binmaxv: # within the bin
                        binnedvals.append(getval(row))
                        row = it.next()
                        keyv = getkey(row)
                    while maxv is not None and keyv == binmaxv == maxv: # possible floating point precision bug here?
                        binnedvals.append(getval(row)) # last bin is open if maxv is specified
                        row = it.next()
                        keyv = getkey(row)
                    yield (binminv, binmaxv), binnedvals
                    if maxv is not None and binmaxv == maxv: # possible floating point precision bug here?
                        break
            except StopIteration:
                # don't forget to handle the last bin
                yield (binminv, binmaxv), binnedvals
        

        
def nthword(n, sep=None):
    """
    Construct a function to return the nth word in a string. E.g.::

        >>> from petl import nthword
        >>> s = 'foo bar'
        >>> f = nthword(0)
        >>> f(s)
        'foo'
        >>> g = nthword(1)
        >>> g(s)
        'bar'
    
    .. versionadded:: 0.10
    
    """

    return lambda s: s.split(sep)[n] 


class SortableItem(object):
    """
    Wrapper to allow comparison with :const:`None` for objects
    which support only comparison with same-type objects.

    For example, the date and time objects from the standard library
    cannot be compared with `None`.

        >>> from datetime import datetime
        >>> from petl.util import SortableItem
        >>> dateobj = datetime(2012, 11, 10)
        >>> SortableItem(42) is 42
        True
        >>> SortableItem(None) is None
        True
        >>> SortableItem(dateobj) is dateobj
        False
        >>> SortableItem(dateobj) > None
        True
        >>> dateobj > None
        Traceback (most recent call last):
        ...
        TypeError: can't compare datetime.datetime to NoneType


    .. versionadded:: 0.11

    """
    __slots__ = ['obj']
    def __new__(cls, obj):
        if obj in SINGLETONS or obj.__class__ in SAFE_TYPES:
            return obj
        if isinstance(obj, (list, tuple)):
            return tuple(cls(o) for o in obj)
        return object.__new__(cls)
    def __init__(self, obj):
        self.obj = obj
    def __eq__(self, other):
        if isinstance(other, SortableItem):
            return self.obj == other.obj
        return self.obj == other
    def __lt__(self, other):
        if isinstance(other, SortableItem):
            other = other.obj
        if other is None:
            return False
        return self.obj < other
    def __le__(self, other):
        return self < other or self == other
    def __gt__(self, other):
        return not (self < other or self == other)
    def __ge__(self, other):
        return not (self < other)
SAFE_TYPES.add(SortableItem)


def sortable_itemgetter(*items):
    """
    Derivate of :func:`itertools.itemgetter` which can be safely
    used as key for sort functions.

    .. versionadded:: 0.11

    """
    ig = itemgetter(*items)
    if len(items) == 1:
        def g(obj):
            return SortableItem(ig(obj))
    else:
        def g(obj):
            return tuple(SortableItem(item) for item in ig(obj))
    return g


def listoflists(tbl):
    return [list(row) for row in tbl]

lol = listoflists

def tupleoftuples(tbl):
    return tuple(tuple(row) for row in tbl)

tot = tupleoftuples

def listoftuples(tbl):
    return [tuple(row) for row in tbl]

lot = listoftuples

def tupleoflists(tbl):
    return tuple(list(row) for row in tbl)

tol = tupleoflists


def cache(table, n=10000):
    """
    Wrap the table with a cache that caches up to `n` rows as they are initially
    requested via iteration.
    
    .. versionadded:: 0.16
    
    """
    
    return CacheContainer(table, n=n)


class CacheContainer(RowContainer):
    
    def __init__(self, inner, n=10000):
        self._inner = inner
        self._n = n
        self._cache = list()
        self._cachecomplete = False
        
    def clearcache(self):
        self._cache = list()
        self._cachecomplete = False
        
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
                if len(self._cache) < self._n:
                    self._cache.append(row)
                yield row
                
            # does the cache contain a complete copy of the inner table?
            if len(self._cache) < self._n:
                debug('cache is complete')
                object.__setattr__(self, '_cachecomplete', True)
        
