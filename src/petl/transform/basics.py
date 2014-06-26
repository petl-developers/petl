"""
Functions for transforming tables.

"""

from itertools import islice, chain, izip_longest, izip
from collections import deque


from petl.util import asindices, rowgetter, valueset, limits, itervalues, \
    hybridrows, OrderedDict, RowContainer, count


from petl.transform.selects import selecteq, selectrangeopenleft, \
    selectrangeopen


import logging
logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


def cut(table, *args, **kwargs):
    """
    Choose and/or re-order columns. E.g.::

        >>> from petl import look, cut    
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | 2.7   |
        +-------+-------+-------+
        | 'B'   | 2     | 3.4   |
        +-------+-------+-------+
        | 'B'   | 3     | 7.8   |
        +-------+-------+-------+
        | 'D'   | 42    | 9.0   |
        +-------+-------+-------+
        | 'E'   | 12    |       |
        +-------+-------+-------+
        
        >>> table2 = cut(table1, 'foo', 'baz')
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'baz' |
        +=======+=======+
        | 'A'   | 2.7   |
        +-------+-------+
        | 'B'   | 3.4   |
        +-------+-------+
        | 'B'   | 7.8   |
        +-------+-------+
        | 'D'   | 9.0   |
        +-------+-------+
        | 'E'   | None  |
        +-------+-------+
        
        >>> # fields can also be specified by index, starting from zero
        ... table3 = cut(table1, 0, 2)
        >>> look(table3)
        +-------+-------+
        | 'foo' | 'baz' |
        +=======+=======+
        | 'A'   | 2.7   |
        +-------+-------+
        | 'B'   | 3.4   |
        +-------+-------+
        | 'B'   | 7.8   |
        +-------+-------+
        | 'D'   | 9.0   |
        +-------+-------+
        | 'E'   | None  |
        +-------+-------+
        
        >>> # field names and indices can be mixed
        ... table4 = cut(table1, 'bar', 0)
        >>> look(table4)
        +-------+-------+
        | 'bar' | 'foo' |
        +=======+=======+
        | 1     | 'A'   |
        +-------+-------+
        | 2     | 'B'   |
        +-------+-------+
        | 3     | 'B'   |
        +-------+-------+
        | 42    | 'D'   |
        +-------+-------+
        | 12    | 'E'   |
        +-------+-------+
        
        >>> # select a range of fields
        ... table5 = cut(table1, *range(0, 2))
        >>> look(table5)    
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 1     |
        +-------+-------+
        | 'B'   | 2     |
        +-------+-------+
        | 'B'   | 3     |
        +-------+-------+
        | 'D'   | 42    |
        +-------+-------+
        | 'E'   | 12    |
        +-------+-------+

    Note that any short rows will be padded with `None` values (or whatever is
    provided via the `missing` keyword argument).
    
    See also :func:`cutout`.
    
    """

    # support passing a single list or tuple of fields
    if len(args) == 1 and isinstance(args[0], (list, tuple)):
        args = args[0]
            
    return CutView(table, args, **kwargs)


class CutView(RowContainer):
    
    def __init__(self, source, spec, missing=None):
        self.source = source
        self.spec = spec
        self.missing = missing
        
    def __iter__(self):
        return itercut(self.source, self.spec, self.missing)
    
        
def itercut(source, spec, missing=None):
    it = iter(source)
    spec = tuple(spec)  # make sure no-one can change midstream
    
    # convert field selection into field indices
    flds = it.next()
    indices = asindices(flds, spec)

    # define a function to transform each row in the source data 
    # according to the field selection
    transform = rowgetter(*indices)
    
    # yield the transformed field names
    yield transform(flds)
    
    # construct the transformed data
    for row in it:
        try:
            yield transform(row) 
        except IndexError:
            # row is short, let's be kind and fill in any missing fields
            yield tuple(row[i] if i < len(row) else missing for i in indices)

    
def cutout(table, *args, **kwargs):
    """
    Remove fields. E.g.::

        >>> from petl import cutout, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | 2.7   |
        +-------+-------+-------+
        | 'B'   | 2     | 3.4   |
        +-------+-------+-------+
        | 'B'   | 3     | 7.8   |
        +-------+-------+-------+
        | 'D'   | 42    | 9.0   |
        +-------+-------+-------+
        | 'E'   | 12    |       |
        +-------+-------+-------+
        
        >>> table2 = cutout(table1, 'bar')
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'baz' |
        +=======+=======+
        | 'A'   | 2.7   |
        +-------+-------+
        | 'B'   | 3.4   |
        +-------+-------+
        | 'B'   | 7.8   |
        +-------+-------+
        | 'D'   | 9.0   |
        +-------+-------+
        | 'E'   | None  |
        +-------+-------+
        
    See also :func:`cut`.
    
    .. versionadded:: 0.3
    
    """

    return CutOutView(table, args, **kwargs)


class CutOutView(RowContainer):
    
    def __init__(self, source, spec, missing=None):
        self.source = source
        self.spec = spec
        self.missing = missing
        
    def __iter__(self):
        return itercutout(self.source, self.spec, self.missing)
    
        
def itercutout(source, spec, missing=None):
    it = iter(source)
    spec = tuple(spec) # make sure no-one can change midstream
    
    # convert field selection into field indices
    flds = it.next()
    indicesout = asindices(flds, spec)
    indices = [i for i in range(len(flds)) if i not in indicesout]
    
    # define a function to transform each row in the source data 
    # according to the field selection
    transform = rowgetter(*indices)
    
    # yield the transformed field names
    yield transform(flds)
    
    # construct the transformed data
    for row in it:
        try:
            yield transform(row) 
        except IndexError:
            # row is short, let's be kind and fill in any missing fields
            yield tuple(row[i] if i < len(row) else missing for i in indices)

    
def cat(*tables, **kwargs):
    """
    Concatenate data from two or more tables. E.g.::
    
        >>> from petl import look, cat
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 1     | 'A'   |
        +-------+-------+
        | 2     | 'B'   |
        +-------+-------+
        
        >>> look(table2)
        +-------+-------+
        | 'bar' | 'baz' |
        +=======+=======+
        | 'C'   | True  |
        +-------+-------+
        | 'D'   | False |
        +-------+-------+
        
        >>> table3 = cat(table1, table2)
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'A'   | None  |
        +-------+-------+-------+
        | 2     | 'B'   | None  |
        +-------+-------+-------+
        | None  | 'C'   | True  |
        +-------+-------+-------+
        | None  | 'D'   | False |
        +-------+-------+-------+
        
        >>> # can also be used to square up a single table with uneven rows
        ... look(table4)
        +-------+-------+--------+------+
        | 'foo' | 'bar' | 'baz'  |      |
        +=======+=======+========+======+
        | 'A'   | 1     | 2      |      |
        +-------+-------+--------+------+
        | 'B'   | '2'   | '3.4'  |      |
        +-------+-------+--------+------+
        | u'B'  | u'3'  | u'7.8' | True |
        +-------+-------+--------+------+
        | 'D'   | 'xyz' | 9.0    |      |
        +-------+-------+--------+------+
        | 'E'   | None  |        |      |
        +-------+-------+--------+------+
        
        >>> look(cat(table4))
        +-------+-------+--------+
        | 'foo' | 'bar' | 'baz'  |
        +=======+=======+========+
        | 'A'   | 1     | 2      |
        +-------+-------+--------+
        | 'B'   | '2'   | '3.4'  |
        +-------+-------+--------+
        | u'B'  | u'3'  | u'7.8' |
        +-------+-------+--------+
        | 'D'   | 'xyz' | 9.0    |
        +-------+-------+--------+
        | 'E'   | None  | None   |
        +-------+-------+--------+
        
        >>> # use the header keyword argument to specify a fixed set of fields 
        ... look(table5)
        +-------+-------+
        | 'bar' | 'foo' |
        +=======+=======+
        | 'A'   | 1     |
        +-------+-------+
        | 'B'   | 2     |
        +-------+-------+
        
        >>> table6 = cat(table5, header=['A', 'foo', 'B', 'bar', 'C'])
        >>> look(table6)
        +------+-------+------+-------+------+
        | 'A'  | 'foo' | 'B'  | 'bar' | 'C'  |
        +======+=======+======+=======+======+
        | None | 1     | None | 'A'   | None |
        +------+-------+------+-------+------+
        | None | 2     | None | 'B'   | None |
        +------+-------+------+-------+------+
        
        >>> # using the header keyword argument with two input tables
        ... look(table7)
        +-------+-------+
        | 'bar' | 'foo' |
        +=======+=======+
        | 'A'   | 1     |
        +-------+-------+
        | 'B'   | 2     |
        +-------+-------+
        
        >>> look(table8)
        +-------+-------+
        | 'bar' | 'baz' |
        +=======+=======+
        | 'C'   | True  |
        +-------+-------+
        | 'D'   | False |
        +-------+-------+
        
        >>> table9 = cat(table7, table8, header=['A', 'foo', 'B', 'bar', 'C'])
        >>> look(table9)
        +------+-------+------+-------+------+
        | 'A'  | 'foo' | 'B'  | 'bar' | 'C'  |
        +======+=======+======+=======+======+
        | None | 1     | None | 'A'   | None |
        +------+-------+------+-------+------+
        | None | 2     | None | 'B'   | None |
        +------+-------+------+-------+------+
        | None | None  | None | 'C'   | None |
        +------+-------+------+-------+------+
        | None | None  | None | 'D'   | None |
        +------+-------+------+-------+------+    
    
    Note that the tables do not need to share exactly the same fields, any 
    missing fields will be padded with `None` or whatever is provided via the 
    `missing` keyword argument. 

    .. versionchanged:: 0.5
    
    By default, the fields for the output table will be determined as the 
    union of all fields found in the input tables. Use the `header` keyword 
    argument to override this behaviour and specify a fixed set of fields for 
    the output table. 
    
    """
    
    return CatView(tables, **kwargs)
    
    
class CatView(RowContainer):
    
    def __init__(self, sources, missing=None, header=None):
        self.sources = sources
        self.missing = missing
        if header is not None:
            header = tuple(header) # ensure hashable
        self.header = header

    def __iter__(self):
        return itercat(self.sources, self.missing, self.header)
    

def itercat(sources, missing, header):
    its = [iter(t) for t in sources]
    source_flds_lists = [it.next() for it in its]

    if header is None:
        # determine output fields by gathering all fields found in the sources
        outflds = list()
        for flds in source_flds_lists:
            for f in flds:
                if f not in outflds:
                    # add any new fields as we find them
                    outflds.append(f)
    else:
        # predetermined output fields
        outflds = header
    yield tuple(outflds)

    # output data rows
    for source_index, it in enumerate(its):

        flds = source_flds_lists[source_index]
        
        # now construct and yield the data rows
        for row in it:
            try:
                # should be quickest to do this way
                yield tuple(row[flds.index(f)] if f in flds else missing for f in outflds)
            except IndexError:
                # handle short rows
                outrow = [missing] * len(outflds)
                for i, f in enumerate(flds):
                    try:
                        outrow[outflds.index(f)] = row[i]
                    except IndexError:
                        pass # be relaxed about short rows
                yield tuple(outrow)


def addfield(table, field, value=None, index=None):
    """
    Add a field with a fixed or calculated value. E.g.::
    
        >>> from petl import addfield, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'M'   | 12    |
        +-------+-------+
        | 'F'   | 34    |
        +-------+-------+
        | '-'   | 56    |
        +-------+-------+
        
        >>> # using a fixed value
        ... table2 = addfield(table1, 'baz', 42)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'M'   | 12    | 42    |
        +-------+-------+-------+
        | 'F'   | 34    | 42    |
        +-------+-------+-------+
        | '-'   | 56    | 42    |
        +-------+-------+-------+
        
        >>> # calculating the value
        ... table2 = addfield(table1, 'baz', lambda rec: rec['bar'] * 2)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'M'   | 12    | 24    |
        +-------+-------+-------+
        | 'F'   | 34    | 68    |
        +-------+-------+-------+
        | '-'   | 56    | 112   |
        +-------+-------+-------+
        
        >>> # an expression string can also be used via expr
        ... from petl import expr
        >>> table3 = addfield(table1, 'baz', expr('{bar} * 2'))
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'M'   | 12    | 24    |
        +-------+-------+-------+
        | 'F'   | 34    | 68    |
        +-------+-------+-------+
        | '-'   | 56    | 112   |
        +-------+-------+-------+
        
    .. versionchanged:: 0.10
    
    Renamed 'extend' to 'addfield'.
    
    """

    return AddFieldView(table, field, value=value, index=index)


class AddFieldView(RowContainer):
    
    def __init__(self, source, field, value=None, index=None):
        self.source = source
        self.field = field
        self.value = value
        self.index = index
        
    def __iter__(self):
        return iteraddfield(self.source, self.field, self.value, self.index)
    

def iteraddfield(source, field, value, index):
    it = iter(source)
    flds = it.next()
    
    # determine index of new field
    if index is None:
        index = len(flds)
        
    # construct output fields
    outflds = list(flds)    
    outflds.insert(index, field)
    yield tuple(outflds)

    # hybridise rows if using calculated value
    if callable(value):
        for row in hybridrows(flds, it):
            outrow = list(row)
            v = value(row)
            outrow.insert(index, v)
            yield tuple(outrow)
    else:
        for row in it:
            outrow = list(row)
            outrow.insert(index, value)
            yield tuple(outrow)
        
    
def rowslice(table, *sliceargs):
    """
    Choose a subsequence of data rows. E.g.::
    
        >>> from petl import rowslice, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 5     |
        +-------+-------+
        | 'd'   | 7     |
        +-------+-------+
        | 'f'   | 42    |
        +-------+-------+
        
        >>> table2 = rowslice(table1, 2)
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        
        >>> table3 = rowslice(table1, 1, 4)
        >>> look(table3)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 5     |
        +-------+-------+
        | 'd'   | 7     |
        +-------+-------+
        
        >>> table4 = rowslice(table1, 0, 5, 2)
        >>> look(table4)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'c'   | 5     |
        +-------+-------+
        | 'f'   | 42    |
        +-------+-------+
        
    .. versionchanged:: 0.3
    
    Positional arguments can be used to slice the data rows. The `sliceargs` are 
    passed to :func:`itertools.islice`.

    """

    return RowSliceView(table, *sliceargs)


class RowSliceView(RowContainer):
    
    def __init__(self, source, *sliceargs):
        self.source = source
        if not sliceargs:
            self.sliceargs = (None,)
        else:
            self.sliceargs = sliceargs
        
    def __iter__(self):
        return iterrowslice(self.source, self.sliceargs)


def iterrowslice(source, sliceargs):    
    it = iter(source)
    yield tuple(it.next()) # fields
    for row in islice(it, *sliceargs):
        yield tuple(row)


def head(table, n=5):
    """
    Choose the first n data rows. E.g.::

        >>> from petl import head, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 5     |
        +-------+-------+
        | 'd'   | 7     |
        +-------+-------+
        | 'f'   | 42    |
        +-------+-------+
        | 'f'   | 3     |
        +-------+-------+
        | 'h'   | 90    |
        +-------+-------+
        
        >>> table2 = head(table1, 4)
        >>> look(table2)    
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 5     |
        +-------+-------+
        | 'd'   | 7     |
        +-------+-------+

    Syntactic sugar, equivalent to ``rowslice(table, n)``.
    
    """

    return rowslice(table, n)

        
def tail(table, n=5):
    """
    Choose the last n data rows. 
    
    E.g.::

        >>> from petl import tail, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 5     |
        +-------+-------+
        | 'd'   | 7     |
        +-------+-------+
        | 'f'   | 42    |
        +-------+-------+
        | 'f'   | 3     |
        +-------+-------+
        | 'h'   | 90    |
        +-------+-------+
        | 'k'   | 12    |
        +-------+-------+
        | 'l'   | 77    |
        +-------+-------+
        | 'q'   | 2     |
        +-------+-------+
        
        >>> table2 = tail(table1, 4)
        >>> look(table2)    
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'h'   | 90    |
        +-------+-------+
        | 'k'   | 12    |
        +-------+-------+
        | 'l'   | 77    |
        +-------+-------+
        | 'q'   | 2     |
        +-------+-------+
        
    See also :func:`head`, :func:`rowslice`.

    """

    return TailView(table, n)


class TailView(RowContainer):
    
    def __init__(self, source, n):
        self.source = source
        self.n = n
        
    def __iter__(self):
        return itertail(self.source, self.n)


def itertail(source, n):
    it = iter(source)
    yield tuple(it.next()) # fields
    cache = deque()
    for row in it:
        cache.append(row)
        if len(cache) > n:
            cache.popleft()
    for row in cache:
        yield tuple(row)


def skipcomments(table, prefix):
    """
    Skip any row where the first value is a string and starts with 
    `prefix`. E.g.::
    
        >>> from petl import skipcomments, look
        >>> look(table1)
        +---------+-------+-------+
        | '##aaa' | 'bbb' | 'ccc' |
        +=========+=======+=======+
        | '##mmm' |       |       |
        +---------+-------+-------+
        | '#foo'  | 'bar' |       |
        +---------+-------+-------+
        | '##nnn' | 1     |       |
        +---------+-------+-------+
        | 'a'     | 1     |       |
        +---------+-------+-------+
        | 'b'     | 2     |       |
        +---------+-------+-------+
        
        >>> table2 = skipcomments(table1, '##')
        >>> look(table2)
        +--------+-------+
        | '#foo' | 'bar' |
        +========+=======+
        | 'a'    | 1     |
        +--------+-------+
        | 'b'    | 2     |
        +--------+-------+
        
    .. versionadded:: 0.4

    """ 

    return SkipCommentsView(table, prefix)


class SkipCommentsView(RowContainer):
    
    def __init__(self, source, prefix):
        self.source = source
        self.prefix = prefix
        
    def __iter__(self):
        return iterskipcomments(self.source, self.prefix)   


def iterskipcomments(source, prefix):
    return (row for row in source if len(row) > 0 and not(isinstance(row[0], basestring) and row[0].startswith(prefix)))


def movefield(table, field, index):
    """
    Move a field to a new position.

    .. versionadded:: 0.24

    """

    return MoveFieldView(table, field, index)


class MoveFieldView(object):

    def __init__(self, table, field, index, missing=None):
        self.table = table
        self.field = field
        self.index = index
        self.missing = missing

    def __iter__(self):
        it = iter(self.table)

        # determine output fields
        fields = list(it.next())
        newfields = [f for f in fields if f != self.field]
        newfields.insert(self.index, self.field)
        yield tuple(newfields)

        # define a function to transform each row in the source data
        # according to the field selection
        indices = asindices(fields, newfields)
        transform = rowgetter(*indices)

        # construct the transformed data
        for row in it:
            try:
                yield transform(row)
            except IndexError:
                # row is short, let's be kind and fill in any missing fields
                yield tuple(row[i] if i < len(row) else self.missing for i in indices)


def annex(*tables, **kwargs):
    """
    Join two or more tables by row order. E.g.::

        >>> from petl import annex, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 9     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+
        
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'baz' |
        +=======+=======+
        | 'B'   | 3     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        
        >>> table3 = annex(table1, table2)
        >>> look(table3)    
        +-------+-------+-------+-------+
        | 'foo' | 'bar' | 'foo' | 'baz' |
        +=======+=======+=======+=======+
        | 'A'   | 9     | 'B'   | 3     |
        +-------+-------+-------+-------+
        | 'C'   | 2     | 'D'   | 10    |
        +-------+-------+-------+-------+
        | 'F'   | 1     | None  | None  |
        +-------+-------+-------+-------+

    .. versionadded:: 0.10
    
    """
    
    return AnnexView(tables, **kwargs)


class AnnexView(RowContainer):
    
    def __init__(self, tables, missing=None):
        self.tables = tables
        self.missing = missing
        
    def __iter__(self):
        return iterannex(self.tables, self.missing)
    

def iterannex(tables, missing):
    iters = [iter(t) for t in tables]
    headers = [it.next() for it in iters]
    outfields = tuple(chain(*headers))  
    yield outfields
    for rows in izip_longest(*iters):
        outrow = list()
        for i, row in enumerate(rows):
            lh = len(headers[i])
            if row is None: # handle uneven length tables
                row = [missing] * len(headers[i])
            else:
                lr = len(row)
                if lr < lh: # handle short rows
                    row = list(row)
                    row.extend([missing] * (lh-lr))
                elif lr > lh: # handle long rows
                    row = row[:lh]
            outrow.extend(row)
        yield tuple(outrow)
          
    
def addrownumbers(table, start=1, step=1):
    """
    Add a field of row numbers. E.g.::

        >>> from petl import addrownumbers, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 9     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+
        
        >>> table2 = addrownumbers(table1)
        >>> look(table2)
        +-------+-------+-------+
        | 'row' | 'foo' | 'bar' |
        +=======+=======+=======+
        | 1     | 'A'   | 9     |
        +-------+-------+-------+
        | 2     | 'C'   | 2     |
        +-------+-------+-------+
        | 3     | 'F'   | 1     |
        +-------+-------+-------+

    .. versionadded:: 0.10
    
    """
    
    return AddRowNumbersView(table, start, step)


class AddRowNumbersView(RowContainer):
    
    def __init__(self, table, start=1, step=1):
        self.table = table
        self.start = start
        self.step = step

    def __iter__(self):
        return iteraddrownumbers(self.table, self.start, self.step)
    

def iteraddrownumbers(table, start, step):
    it = iter(table)
    flds = it.next()
    outflds = ['row']
    outflds.extend(flds)
    yield tuple(outflds)
    for row, n in izip(it, count(start, step)):
        outrow = [n]
        outrow.extend(row)
        yield tuple(outrow)
        

def addcolumn(table, field, col, index=None, missing=None):
    """
    Add a column of data to the table. E.g.::
    
        >>> from petl import addcolumn, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 1     |
        +-------+-------+
        | 'B'   | 2     |
        +-------+-------+
        
        >>> col = [True, False]
        >>> table2 = addcolumn(table1, 'baz', col)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
    
    .. versionadded:: 0.10
    
    """
    
    return AddColumnView(table, field, col, index=index, missing=missing)


class AddColumnView(RowContainer):
    
    def __init__(self, table, field, col, index=None, missing=None):
        self._table = table
        self._field = field
        self._col = col
        self._index = index
        self._missing = missing
        
    def __iter__(self):
        return iteraddcolumn(self._table, self._field, self._col, 
                             self._index, self._missing)
    
    
def iteraddcolumn(table, field, col, index, missing):
    it = iter(table)
    fields = [str(f) for f in it.next()]
    
    # determine position of new column
    if index is None:
        index = len(fields)
    
    # construct output header
    outflds = list(fields)
    outflds.insert(index, field)
    yield tuple(outflds)
    
    # construct output data
    for row, val in izip_longest(it, col, fillvalue=missing):
        # run out of rows?
        if row == missing:
            row = [missing] * len(fields)
        outrow = list(row)
        outrow.insert(index, val)
        yield tuple(outrow)
        
        
class TransformError(Exception):
    pass


def addfieldusingcontext(table, field, query):
    """
    Like :func:`addfield` but the `query` function is passed the previous,
    current and next rows, so values may be calculated based on data in adjacent
    rows.

        >>> from petl import look, addfieldusingcontext
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   |     1 |
        +-------+-------+
        | 'B'   |     4 |
        +-------+-------+
        | 'C'   |     5 |
        +-------+-------+
        | 'D'   |     9 |
        +-------+-------+

        >>> def upstream(prv, cur, nxt):
        ...     if prv is None:
        ...         return None
        ...     else:
        ...         return cur.bar - prv.bar
        ...
        >>> def downstream(prv, cur, nxt):
        ...     if nxt is None:
        ...         return None
        ...     else:
        ...         return nxt.bar - cur.bar
        ...
        >>> table2 = addfieldusingcontext(table1, 'baz', upstream)
        >>> table3 = addfieldusingcontext(table2, 'quux', downstream)
        >>> look(table3)
        +-------+-------+-------+--------+
        | 'foo' | 'bar' | 'baz' | 'quux' |
        +=======+=======+=======+========+
        | 'A'   |     1 | None  |      3 |
        +-------+-------+-------+--------+
        | 'B'   |     4 |     3 |      1 |
        +-------+-------+-------+--------+
        | 'C'   |     5 |     1 |      4 |
        +-------+-------+-------+--------+
        | 'D'   |     9 |     4 | None   |
        +-------+-------+-------+--------+

    .. versionadded:: 0.24

    """

    return AddFieldUsingContextView(table, field, query)


class AddFieldUsingContextView(object):

    def __init__(self, table, field, query):
        self.table = table
        self.field = field
        self.query = query

    def __iter__(self):
        return iteraddfieldusingcontext(self.table, self.field, self.query)


def iteraddfieldusingcontext(table, field, query):
    it = iter(table)
    fields = tuple(it.next())
    yield fields + (field,)
    it = hybridrows(fields, it)
    prv = None
    cur = it.next()
    for nxt in it:
        v = query(prv, cur, nxt)
        yield tuple(cur) + (v,)
        prv = cur
        cur = nxt
    # handle last row
    v = query(prv, cur, None)
    yield tuple(cur) + (v,)

