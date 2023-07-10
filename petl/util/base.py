from __future__ import absolute_import, print_function, division


import re
from itertools import islice, chain, cycle, product,\
    permutations, combinations, takewhile, dropwhile, \
    starmap, groupby, tee
import operator
from collections import Counter, namedtuple, OrderedDict
from itertools import compress, combinations_with_replacement
from petl.compat import imap, izip, izip_longest, ifilter, ifilterfalse, \
    reduce, next, string_types, text_type


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

    def __iter__(self):
        raise NotImplementedError

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
        return list(iter(self))

    def tuple(self):
        # avoid iterating twice
        return tuple(iter(self))

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


class Table(IterContainer):

    def __getitem__(self, item):
        if isinstance(item, string_types):
            return ValuesView(self, item)
        else:
            return super(Table, self).__getitem__(item)


def values(table, *field, **kwargs):
    """
    Return a container supporting iteration over values in a given field or
    fields. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', True],
        ...           ['b'],
        ...           ['b', True],
        ...           ['c', False]]
        >>> foo = etl.values(table1, 'foo')
        >>> foo
        foo: 'a', 'b', 'b', 'c'
        >>> list(foo)
        ['a', 'b', 'b', 'c']
        >>> bar = etl.values(table1, 'bar')
        >>> bar
        bar: True, None, True, False
        >>> list(bar)
        [True, None, True, False]
        >>> # values from multiple fields
        ... table2 = [['foo', 'bar', 'baz'],
        ...           [1, 'a', True],
        ...           [2, 'bb', True],
        ...           [3, 'd', False]]
        >>> foobaz = etl.values(table2, 'foo', 'baz')
        >>> foobaz
        ('foo', 'baz'): (1, True), (2, True), (3, False)
        >>> list(foobaz)
        [(1, True), (2, True), (3, False)]

    The field argument can be a single field name or index (starting from
    zero) or a tuple of field names and/or indexes. Multiple fields can also be
    provided as positional arguments.

    If rows are uneven, the value of the keyword argument `missing` is returned.

    """

    return ValuesView(table, *field, **kwargs)


Table.values = values


class ValuesView(IterContainer):

    def __init__(self, table, *field, **kwargs):
        self.table = table
        # deal with field arg in a backwards-compatible way
        if len(field) == 1:
            field = field[0]
        self.field = field
        self.kwargs = kwargs

    def __iter__(self):
        return itervalues(self.table, self.field, **self.kwargs)

    def __repr__(self):
        vreprs = list(map(repr, islice(self, 6)))
        r = text_type(self.field) + ': '
        r += ', '.join(vreprs[:5])
        if len(vreprs) > 5:
            r += ', ...'
        return r


def itervalues(table, field, **kwargs):

    missing = kwargs.get('missing', None)
    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []

    indices = asindices(hdr, field)
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


class TableWrapper(Table):

    def __init__(self, inner):
        self.inner = inner

    def __iter__(self):
        return iter(self.inner)


wrap = TableWrapper


def asindices(hdr, spec):
    """Convert the given field `spec` into a list of field indices."""

    flds = list(map(text_type, hdr))
    indices = list()
    if not isinstance(spec, (list, tuple)):
        spec = (spec,)
    for s in spec:
        # spec could be a field index (takes priority)
        if isinstance(s, int) and s < len(hdr):
            indices.append(s)  # index fields from 0
        # spec could be a field
        elif s in flds:
            idx = flds.index(s)
            indices.append(idx)
            flds[idx] = None  # replace with None to mark as used
        else:
            raise FieldSelectionError(s)
    return indices


def rowitemgetter(hdr, spec):
    indices = asindices(hdr, spec)
    getter = comparable_itemgetter(*indices)
    return getter


def rowgetter(*indices):
    if len(indices) == 0:
        return lambda row: tuple()
    elif len(indices) == 1:
        # if only one index, we cannot use itemgetter, because we want a
        # singleton sequence to be returned, but itemgetter with a single
        # argument returns the value itself, so let's define a function
        index = indices[0]
        return lambda row: (row[index],)  # note comma - singleton tuple
    # if more than one index, use itemgetter, it should be the most efficient
    else:
        return operator.itemgetter(*indices)


def header(table):
    """
    Return the header row for the given table. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> etl.header(table)
        ('foo', 'bar')

    Note that the header row will always be returned as a tuple, regardless
    of what the underlying data are.

    """

    it = iter(table)
    return tuple(next(it))


Table.header = header


def fieldnames(table):
    """
    Return the string values of the header row. If the header row
    contains only strings, then this function is equivalent to header(), i.e.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> etl.fieldnames(table)
        ('foo', 'bar')
        >>> etl.header(table)
        ('foo', 'bar')

    """

    return tuple(text_type(f) for f in header(table))


Table.fieldnames = fieldnames


def data(table, *sliceargs):
    """
    Return a container supporting iteration over data rows in a given table
    (i.e., without the header). E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> d = etl.data(table)
        >>> list(d)
        [['a', 1], ['b', 2]]

    Positional arguments can be used to slice the data rows. The sliceargs
    are passed to :func:`itertools.islice`.

    """

    return DataView(table, *sliceargs)


Table.data = data


class DataView(Table):

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
    """
    Return a container supporting iteration over rows as dicts. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> d = etl.dicts(table)
        >>> d
        {'foo': 'a', 'bar': 1}
        {'foo': 'b', 'bar': 2}
        >>> list(d)
        [{'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': 2}]

    Short rows are padded with the value of the `missing` keyword argument.

    """

    return DictsView(table, *sliceargs, **kwargs)


Table.dicts = dicts


class DictsView(IterContainer):

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
    try:
        hdr = next(it)
    except StopIteration:
        return
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        yield asdict(hdr, row, missing)


def asdict(hdr, row, missing=None):
    flds = [text_type(f) for f in hdr]
    try:
        # list comprehension should be faster
        items = [(flds[i], row[i]) for i in range(len(flds))]
    except IndexError:
        # short row, fall back to slower for loop
        items = list()
        for i, f in enumerate(flds):
            try:
                v = row[i]
            except IndexError:
                v = missing
            items.append((f, v))
    return dict(items)


def namedtuples(table, *sliceargs, **kwargs):
    """
    View the table as a container of named tuples. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> d = etl.namedtuples(table)
        >>> d
        row(foo='a', bar=1)
        row(foo='b', bar=2)
        >>> list(d)
        [row(foo='a', bar=1), row(foo='b', bar=2)]

    Short rows are padded with the value of the `missing` keyword argument.

    The `name` keyword argument can be given to override the name of the
    named tuple class (defaults to 'row').

    """

    return NamedTuplesView(table, *sliceargs, **kwargs)


Table.namedtuples = namedtuples


class NamedTuplesView(IterContainer):

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
    try:
        hdr = next(it)
    except StopIteration:
        return
    flds = list(map(text_type, hdr))
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
            raise KeyError('item ' + repr(f) +
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
            raise AttributeError('item ' + repr(f) +
                                ' not in fields ' + repr(self.flds))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


def records(table, *sliceargs, **kwargs):
    """
    Return a container supporting iteration over rows as records, where a
    record is a hybrid object supporting all possible ways of accessing values.
    E.g.::


        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> d = etl.records(table)
        >>> d
        ('a', 1)
        ('b', 2)
        >>> list(d)
        [('a', 1), ('b', 2)]
        >>> [r[0] for r in d]
        ['a', 'b']
        >>> [r['foo'] for r in d]
        ['a', 'b']
        >>> [r.foo for r in d]
        ['a', 'b']

    Short rows are padded with the value of the `missing` keyword argument.

    """

    return RecordsView(table, *sliceargs, **kwargs)


Table.records = records


class RecordsView(IterContainer):

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
    missing = kwargs.get('missing', None)
    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        return
    flds = list(map(text_type, hdr))
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        yield Record(row, flds, missing=missing)


def expr(s):
    """
    Construct a function operating on a table record.

    The expression string is converted into a lambda function by prepending
    the string with ``'lambda rec: '``, then replacing anything enclosed in
    curly braces (e.g., ``"{foo}"``) with a lookup on the record (e.g.,
    ``"rec['foo']"``), then finally calling :func:`eval`.

    So, e.g., the expression string ``"{foo} * {bar}"`` is converted to the
    function ``lambda rec: rec['foo'] * rec['bar']``

    """

    prog = re.compile(r'\{([^}]+)\}')

    def repl(matchobj):
        return "rec['%s']" % matchobj.group(1)

    return eval("lambda rec: " + prog.sub(repl, s))


def rowgroupby(table, key, value=None):
    """Convenient adapter for :func:`itertools.groupby`. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 1, True],
        ...           ['b', 3, True],
        ...           ['b', 2]]
        >>> # group entire rows
        ... for key, group in etl.rowgroupby(table1, 'foo'):
        ...     print(key, list(group))
        ...
        a [('a', 1, True)]
        b [('b', 3, True), ('b', 2)]
        >>> # group specific values
        ... for key, group in etl.rowgroupby(table1, 'foo', 'bar'):
        ...     print(key, list(group))
        ...
        a [1]
        b [3, 2]

    N.B., assumes the input table is already sorted by the given key.

    """

    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    # wrap rows as records
    it = (Record(row, flds) for row in it)

    # determine key function
    if callable(key):
        getkey = key
        native_key = True
    else:
        kindices = asindices(hdr, key)
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
            vindices = asindices(hdr, value)
            getval = operator.itemgetter(*vindices)
        if native_key:
            return ((k, (getval(v) for v in vals))
                    for (k, vals) in git)
        else:
            return ((k.inner, (getval(v) for v in vals))
                    for (k, vals) in git)


Table.rowgroupby = rowgroupby


def iterpeek(it, n=1):
    it = iter(it)  # make sure it's an iterator
    if n == 1:
        peek = next(it)
        return peek, chain([peek], it)
    else:
        peek = list(islice(it, n))
        return peek, chain(peek, it)


def empty():
    """
    Return an empty table. Can be useful when building up a table from a set
    of columns, e.g.::

        >>> import petl as etl
        >>> table = (
        ...     etl
        ...     .empty()
        ...     .addcolumn('foo', ['A', 'B'])
        ...     .addcolumn('bar', [1, 2])
        ... )
        >>> table
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'A' |   1 |
        +-----+-----+
        | 'B' |   2 |
        +-----+-----+

    """

    return EmptyTable()


class EmptyTable(Table):

    def __iter__(self):
        # empty header row
        yield tuple()
