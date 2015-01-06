"""
Base classes.

"""


from __future__ import absolute_import, print_function, division, \
    unicode_literals


import re
from itertools import islice, chain, cycle, product,\
    permutations, combinations, takewhile, dropwhile, \
    starmap, groupby, tee
import operator
from collections import namedtuple
from petl.compat import imap, izip, izip_longest, ifilter, ifilterfalse, \
    Counter, OrderedDict, compress, combinations_with_replacement, reduce, \
    next, string_types


from petl.errors import FieldSelectionError
from petl.comparison import comparable_itemgetter


class IterContainer(object):
    
    def __contains__(self, item):
        for o in self:
            if o == item:
                return True
        return False
        
    def __len__(self):
        return sum(1 for _ in self)
        
    def __getitem__(self, item):
        if isinstance(item, int):
            try:
                return next(islice(self, item, item+1))
            except StopIteration:
                raise IndexError('index out of range')
        elif isinstance(item, slice):
            return islice(self, item.start, item.stop, item.step)

    def index(self, item):
        for i, o in enumerate(self):
            if o == item:
                return i
        raise ValueError('%s is not in container' % item)
    
    def min(self, **kwargs):
        return min(self, **kwargs)
    
    def max(self, **kwargs):
        return max(self, **kwargs)
    
    def len(self):
        return len(self)
    
    def set(self):
        return set(self)
    
    def frozenset(self):
        return frozenset(self)
    
    def list(self):
        # avoid iterating twice
        l = list()
        for i in iter(self):
            l.append(i)
        return l

    def tuple(self):
        # avoid iterating twice
        return tuple(self.list())

    def dict(self, **kwargs):
        return dict(self, **kwargs)
    
    def enumerate(self, start=0):
        return enumerate(self, start)
    
    def filter(self, function):
        return filter(function, self)

    def map(self, function):
        return map(function, self)

    def reduce(self, function, **kwargs):
        return reduce(function, self, **kwargs)

    def sum(self, *args, **kwargs):
        return sum(self, *args, **kwargs)
    
    def all(self):
        return all(self)
    
    def any(self):
        return any(self)
    
    def apply(self, function):
        for item in self:
            function(item)
            
    def counter(self):
        return Counter(self)
    
    def ordereddict(self):
        return OrderedDict(self)
    
    def cycle(self):
        return cycle(self)
    
    def chain(self, *others):
        return chain(self, *others)
    
    def dropwhile(self, predicate):
        return dropwhile(predicate, self)

    def takewhile(self, predicate):
        return takewhile(predicate, self)

    def ifilter(self, predicate):
        return ifilter(predicate, self)

    def ifilterfalse(self, predicate):
        return ifilterfalse(predicate, self)

    def imap(self, function):
        return imap(function, self)

    def starmap(self, function):
        return starmap(function, self)

    def islice(self, *args):
        return islice(self, *args)
    
    def compress(self, selectors):
        return compress(self, selectors)
    
    def groupby(self, *args, **kwargs):
        return groupby(self, *args, **kwargs)
    
    def tee(self, *args, **kwargs):
        return tee(self, *args, **kwargs)
    
    def permutations(self, *args, **kwargs):
        return permutations(self, *args, **kwargs)
    
    def combinations(self, *args, **kwargs):
        return combinations(self, *args, **kwargs)
    
    def combinations_with_replacement(self, *args, **kwargs):
        return combinations_with_replacement(self, *args, **kwargs)
    
    def izip(self, *args, **kwargs):
        return izip(self, *args, **kwargs)
    
    def izip_longest(self, *args, **kwargs):
        return izip_longest(self, *args, **kwargs)
    
    def product(self, *args, **kwargs):
        return product(self, *args, **kwargs)
    
    def __add__(self, other):
        return chain(self, other)
    
    def __iadd__(self, other):
        return chain(self, other)


def values(table, *field, **kwargs):
    """TODO

    """

    return ValuesContainer(table, *field, **kwargs)


class ValuesContainer(IterContainer):

    def __init__(self, table, *field, **kwargs):
        self.table = table
        self.field = field
        self.kwargs = kwargs

    def __iter__(self):
        return itervalues(self.table, *self.field, **self.kwargs)

    def __repr__(self):
        vreprs = list(map(repr, islice(self, 6)))
        r = ', '.join(vreprs[:5])
        if len(vreprs) > 5:
            r += ', ...'
        return r


def itervalues(table, *field, **kwargs):

    missing = kwargs.get('missing', None)
    it = iter(table)
    srcflds = next(it)

    # deal with field arg in a backwards-compatible way
    if len(field) == 1:
        field = field[0]

    indices = asindices(srcflds, field)
    assert len(indices) > 0, 'no field selected'
    getvalue = operator.itemgetter(*indices)
    for row in it:
        try:
            value = getvalue(row)
            yield value
        except IndexError:
            if len(indices) > 1:
                # try one at a time
                value = list()
                for i in indices:
                    if i < len(row):
                        value.append(row[i])
                    else:
                        value.append(missing)
                yield tuple(value)
            else:
                yield missing


class RowContainer(IterContainer):

    def __getitem__(self, item):
        if isinstance(item, string_types):
            return ValuesContainer(self, item)
        else:
            return super(RowContainer, self).__getitem__(item)


def asindices(flds, spec):
    """Convert the given field `spec` into a list of field indices."""

    names = [str(f) for f in flds]
    indices = list()
    if isinstance(spec, string_types):
        spec = (spec,)
    if isinstance(spec, int):
        spec = (spec,)
    for s in spec:
        # spec could be a field name
        if s in names:
            indices.append(names.index(s))
        # or spec could be a field index
        elif isinstance(s, int) and s < len(names):
            indices.append(s)  # index fields from 0
        else:
            raise FieldSelectionError(s)
    return indices


def rowitemgetter(fields, spec):
    indices = asindices(fields, spec)
    getter = comparable_itemgetter(*indices)
    return getter


def rowgetter(*indices):

    # guard condition
    assert len(indices) > 0, 'indices is empty'

    # if only one index, we cannot use itemgetter, because we want a singleton
    # sequence to be returned, but itemgetter with a single argument returns the
    # value itself, so let's define a function
    if len(indices) == 1:
        index = indices[0]
        return lambda row: (row[index],)  # note comma - singleton tuple!
    # if more than one index, use itemgetter, it should be the most efficient
    else:
        return operator.itemgetter(*indices)


def header(table):
    """TODO

    """

    it = iter(table)
    return tuple(next(it))


def fieldnames(table):
    """TODO

    """

    return [str(f) for f in header(table)]


def data(table, *sliceargs):
    """TODO

    """

    return DataContainer(table, *sliceargs)


class DataContainer(RowContainer):

    def __init__(self, table, *sliceargs):
        self.table = table
        self.sliceargs = sliceargs

    def __iter__(self):
        return iterdata(self.table, *self.sliceargs)


def iterdata(table, *sliceargs):

    it = islice(table, 1, None)  # skip header row
    if sliceargs:
        it = islice(it, *sliceargs)
    return it


def dicts(table, *sliceargs, **kwargs):
    """TODO

    """

    return DictsContainer(table, *sliceargs, **kwargs)


class DictsContainer(IterContainer):

    def __init__(self, table, *sliceargs, **kwargs):
        self.table = table
        self.sliceargs = sliceargs
        self.kwargs = kwargs

    def __iter__(self):
        return iterdicts(self.table, *self.sliceargs, **self.kwargs)

    def __repr__(self):
        vreprs = list(map(repr, islice(self, 6)))
        r = '\n'.join(vreprs[:5])
        if len(vreprs) > 5:
            r += '\n...'
        return r


def iterdicts(table, *sliceargs, **kwargs):

    missing = kwargs.get('missing', None)
    it = iter(table)
    flds = next(it)
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        yield asdict(flds, row, missing)


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


def namedtuples(table, *sliceargs, **kwargs):
    """TODO

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
        vreprs = list(map(repr, islice(self, 6)))
        r = '\n'.join(vreprs[:5])
        if len(vreprs) > 5:
            r += '\n...'
        return r


def iternamedtuples(table, *sliceargs, **kwargs):

    missing = kwargs.get('missing', None)
    name = kwargs.get('name', 'row')
    it = iter(table)
    flds = next(it)
    nt = namedtuple(name, tuple(flds))
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        yield asnamedtuple(nt, row, missing)


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


class Record(tuple):

    def __new__(cls, row, flds, missing=None):
        t = super(Record, cls).__new__(cls, row)
        return t

    def __init__(self, row, flds, missing=None):
        self.flds = flds
        self.missing = missing

    def __getitem__(self, f):
        if isinstance(f, int):
            idx = f
        elif f in self.flds:
            idx = self.flds.index(f)
        else:
            raise Exception('item ' + repr(f) +
                            ' not in fields ' + repr(self.flds))
        try:
            return super(Record, self).__getitem__(idx)
        except IndexError:  # handle short rows
            return self.missing

    def __getattr__(self, f):
        if f in self.flds:
            try:
                return super(Record, self).__getitem__(self.flds.index(f))
            except IndexError:  # handle short rows
                return self.missing
        else:
            raise Exception('item ' + repr(f) +
                            ' not in fields ' + repr(self.flds))


def records(table, *sliceargs, **kwargs):
    """TODO

    """

    return RecordsContainer(table, *sliceargs, **kwargs)


class RecordsContainer(IterContainer):

    def __init__(self, table, *sliceargs, **kwargs):
        self.table = table
        self.sliceargs = sliceargs
        self.kwargs = kwargs

    def __iter__(self):
        return iterrecords(self.table, *self.sliceargs, **self.kwargs)

    def __repr__(self):
        vreprs = list(map(repr, islice(self, 6)))
        r = '\n'.join(vreprs[:5])
        if len(vreprs) > 5:
            r += '\n...'
        return r


def iterrecords(table, *sliceargs, **kwargs):
    """TODO

    """

    missing = kwargs.get('missing', None)
    it = iter(table)
    flds = next(it)
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        yield Record(row, flds, missing=missing)


def nrows(table):
    """Count the number of data rows in a table. E.g.::

        >>> from petl import nrows
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> nrows(table)
        2

    """

    return sum(1 for _ in iterdata(table))


def expr(s):
    """Construct a function operating on a record (i.e., a dictionary
    representation of a data row, indexed by field name).

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


def rowgroupby(table, key, value=None):
    """Convenient adapter for :func:`itertools.groupby`. E.g.::

        >>> TODO

    N.B., assumes the input table is already sorted by the given key.

    """

    it = iter(table)
    fields = next(it)
    # wrap rows as records
    it = (Record(row, fields) for row in it)

    # determine key function
    if callable(key):
        getkey = key
        native_key = True
    else:
        kindices = asindices(fields, key)
        getkey = comparable_itemgetter(*kindices)
        native_key = False

    git = groupby(it, key=getkey)
    if value is None:
        if native_key:
            return git
        else:
            return ((k.inner, vals) for (k, vals) in git)
    else:
        if callable(value):
            getval = value
        else:
            vindices = asindices(fields, value)
            getval = operator.itemgetter(*vindices)
        if native_key:
            return ((k, (getval(v) for v in vals))
                    for (k, vals) in git)
        else:
            return ((k.inner, (getval(v) for v in vals))
                    for (k, vals) in git)


def iterpeek(it, n=1):
    it = iter(it)  # make sure it's an iterator
    if n == 1:
        peek = next(it)
        return peek, chain([peek], it)
    else:
        peek = list(islice(it, n))
        return peek, chain(peek, it)


def empty():
    """Return an empty table. Can be useful when building up a table from a set
    of columns, e.g.::

        >>> from petl import empty, addcolumn, look
        >>> table1 = addcolumn(empty(), 'foo', ['A', 'B'])
        >>> table2 = addcolumn(table1, 'bar', [1, 2])
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   |     1 |
        +-------+-------+
        | 'B'   |     2 |
        +-------+-------+

    """

    return EmptyContainer()


class EmptyContainer(RowContainer):

    def __iter__(self):
        # empty header row
        yield tuple()
