"""
Functions for transforming tables.

"""

from itertools import islice, groupby, product, chain, izip_longest, izip
from collections import deque, defaultdict
from operator import itemgetter
import cPickle as pickle
from tempfile import NamedTemporaryFile
import operator
import re
from math import ceil


from petl.util import asindices, rowgetter, asdict,\
    expr, valueset, header, data, limits, itervalues, parsenumber, lookup,\
    values, shortlistmergesorted, heapqmergesorted, hybridrows, rowgroupby,\
    iterpeek, count, Counter, OrderedDict, RowContainer, SortableItem, sortable_itemgetter, FieldSelectionError, \
    rowgroupbybin, rowitemgetter


import logging
logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


def rename(table, *args):
    """
    Replace one or more fields in the table's header row. E.g.::
        
        >>> from petl import look, rename
        >>> look(table1)
        +-------+-------+
        | 'sex' | 'age' |
        +=======+=======+
        | 'M'   | 12    |
        +-------+-------+
        | 'F'   | 34    |
        +-------+-------+
        | '-'   | 56    |
        +-------+-------+
        
        >>> # rename a single field
        ... table2 = rename(table1, 'sex', 'gender')
        >>> look(table2)
        +----------+-------+
        | 'gender' | 'age' |
        +==========+=======+
        | 'M'      | 12    |
        +----------+-------+
        | 'F'      | 34    |
        +----------+-------+
        | '-'      | 56    |
        +----------+-------+
        
        >>> # rename multiple fields by passing a dictionary as the second argument
        ... table3 = rename(table1, {'sex': 'gender', 'age': 'age_years'})
        >>> look(table3)
        +----------+-------------+
        | 'gender' | 'age_years' |
        +==========+=============+
        | 'M'      | 12          |
        +----------+-------------+
        | 'F'      | 34          |
        +----------+-------------+
        | '-'      | 56          |
        +----------+-------------+
        
        >>> # the returned table object can also be used to modify the field mapping using the suffix notation
        ... table4 = rename(table1)
        >>> table4['sex'] = 'gender'
        >>> table4['age'] = 'age_years'
        >>> look(table4)
        +----------+-------------+
        | 'gender' | 'age_years' |
        +==========+=============+
        | 'M'      | 12          |
        +----------+-------------+
        | 'F'      | 34          |
        +----------+-------------+
        | '-'      | 56          |
        +----------+-------------+
        
    .. versionchanged:: 0.4
    
    Function signature changed to support the simple 2 argument form when renaming
    a single field.

    """
    
    return RenameView(table, *args)


class RenameView(RowContainer):
    
    def __init__(self, table, *args):
        self.source = table
        if len(args) == 0:
            self.spec = dict()
        elif len(args) == 1:
            self.spec = args[0]
        elif len(args) == 2:
            self.spec = {args[0]: args[1]}
        
    def __iter__(self):
        return iterrename(self.source, self.spec)
    
    def __setitem__(self, key, value):
        self.spec[key] = value
        
    
def iterrename(source, spec):
    it = iter(source)
    spec = spec.copy() # make sure nobody can change this midstream
    sourceflds = it.next()
    newflds = [spec[f] if f in spec else f for f in sourceflds]
    yield tuple(newflds)
    for row in it:
        yield tuple(row)
        
        
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
    spec = tuple(spec) # make sure no-one can change midstream
    
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


def convert(table, *args, **kwargs):
    """
    Transform values under one or more fields via arbitrary functions, method
    invocations or dictionary translations. E.g.::
    
        >>> from petl import convert, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | '2.4' | 12    |
        +-------+-------+-------+
        | 'B'   | '5.7' | 34    |
        +-------+-------+-------+
        | 'C'   | '1.2' | 56    |
        +-------+-------+-------+
        
        >>> # using the built-in float function:
        ... table2 = convert(table1, 'bar', float)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 2.4   | 12    |
        +-------+-------+-------+
        | 'B'   | 5.7   | 34    |
        +-------+-------+-------+
        | 'C'   | 1.2   | 56    |
        +-------+-------+-------+
        
        >>> # using a lambda function::
        ... table3 = convert(table1, 'baz', lambda v: v*2)
        >>> look(table3)    
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | '2.4' | 24    |
        +-------+-------+-------+
        | 'B'   | '5.7' | 68    |
        +-------+-------+-------+
        | 'C'   | '1.2' | 112   |
        +-------+-------+-------+
        
        >>> # a method of the data value can also be invoked by passing the method name
        ... table4 = convert(table1, 'foo', 'lower')
        >>> look(table4)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | '2.4' | 12    |
        +-------+-------+-------+
        | 'b'   | '5.7' | 34    |
        +-------+-------+-------+
        | 'c'   | '1.2' | 56    |
        +-------+-------+-------+
        
        >>> # arguments to the method invocation can also be given
        ... table5 = convert(table1, 'foo', 'replace', 'A', 'AA')
        >>> look(table5)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'AA'  | '2.4' | 12    |
        +-------+-------+-------+
        | 'B'   | '5.7' | 34    |
        +-------+-------+-------+
        | 'C'   | '1.2' | 56    |
        +-------+-------+-------+
        
        >>> # values can also be translated via a dictionary
        ... table7 = convert(table1, 'foo', {'A': 'Z', 'B': 'Y'})
        >>> look(table7)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'Z'   | '2.4' | 12    |
        +-------+-------+-------+
        | 'Y'   | '5.7' | 34    |
        +-------+-------+-------+
        | 'C'   | '1.2' | 56    |
        +-------+-------+-------+
        
        >>> # the same conversion can be applied to multiple fields
        ... table8 = convert(table1, ('foo', 'bar', 'baz'), unicode)
        >>> look(table8)
        +-------+--------+-------+
        | 'foo' | 'bar'  | 'baz' |
        +=======+========+=======+
        | u'A'  | u'2.4' | u'12' |
        +-------+--------+-------+
        | u'B'  | u'5.7' | u'34' |
        +-------+--------+-------+
        | u'C'  | u'1.2' | u'56' |
        +-------+--------+-------+
        
        >>> # multiple conversions can be specified at the same time
        ... table9 = convert(table1, {'foo': 'lower', 'bar': float, 'baz': lambda v: v*2})
        >>> look(table9)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2.4   | 24    |
        +-------+-------+-------+
        | 'b'   | 5.7   | 68    |
        +-------+-------+-------+
        | 'c'   | 1.2   | 112   |
        +-------+-------+-------+
        
        >>> # ...or alternatively via a list
        ... table10 = convert(table1, ['lower', float, lambda v: v*2])
        >>> look(table10)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2.4   | 24    |
        +-------+-------+-------+
        | 'b'   | 5.7   | 68    |
        +-------+-------+-------+
        | 'c'   | 1.2   | 112   |
        +-------+-------+-------+
        
        >>> # ...or alternatively via suffix notation on the returned table object
        ... table11 = convert(table1)
        >>> table11['foo'] = 'lower'
        >>> table11['bar'] = float
        >>> table11['baz'] = lambda v: v*2
        >>> look(table11)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2.4   | 24    |
        +-------+-------+-------+
        | 'b'   | 5.7   | 68    |
        +-------+-------+-------+
        | 'c'   | 1.2   | 112   |
        +-------+-------+-------+

    Note that either field names or indexes can be given.
    
    .. versionchanged:: 0.11
    
    Now supports multiple field conversions.
    
    """
    
    if len(args) == 0:
        converters = None # no conversion specified, can be set afterwards via suffix notation
    elif len(args) == 1:
        converters = args[0]
    elif len(args) > 1:
        converters = dict()
        # assume first arg is field name or spec
        field = args[0]
        if len(args) == 2:
            conv = args[1]
        else:
            conv = args[1:]
        if isinstance(field, (list, tuple)): # allow for multiple fields
            for f in field:
                converters[f] = conv
        else:
            converters[field] = conv
    return FieldConvertView(table, converters, **kwargs)

    
def convertall(table, *args, **kwargs):
    """
    Convenience function to convert all fields in the table using a common 
    function or mapping. See also :func:`convert`.
    
    .. versionadded:: 0.4
    
    """
    
    # TODO don't read the data twice!
    
    return convert(table, header(table), *args, **kwargs)


def replaceall(table, a, b):
    """
    Convenience function to replace all instances of `a` with `b` under all 
    fields. See also :func:`convertall`.
     
    .. versionadded:: 0.5

    """
    
    return convertall(table, {a: b})
    

def convertnumbers(table):
    """
    Convenience function to convert all field values to numbers where possible. 
    E.g.::

        >>> from petl import convertnumbers, look
        >>> look(table1)
        +-------+-------+--------+--------+
        | 'foo' | 'bar' | 'baz'  | 'quux' |
        +=======+=======+========+========+
        | '1'   | '3.0' | '9+3j' | 'aaa'  |
        +-------+-------+--------+--------+
        | '2'   | '1.3' | '7+2j' | None   |
        +-------+-------+--------+--------+
        
        >>> table2 = convertnumbers(table1)
        >>> look(table2)
        +-------+-------+--------+--------+
        | 'foo' | 'bar' | 'baz'  | 'quux' |
        +=======+=======+========+========+
        | 1     | 3.0   | (9+3j) | 'aaa'  |
        +-------+-------+--------+--------+
        | 2     | 1.3   | (7+2j) | None   |
        +-------+-------+--------+--------+
    
    .. versionadded:: 0.4
    
    """
    
    return convertall(table, parsenumber)


def fieldconvert(table, converters=None, failonerror=False, errorvalue=None):
    """
    Transform values in one or more fields via functions or method invocations. 

    .. deprecated:: 0.11
    
    Use :func:`convert` instead.
    
    """

    return FieldConvertView(table, converters, failonerror, errorvalue)


class FieldConvertView(RowContainer):
    
    def __init__(self, source, converters=None, failonerror=False, errorvalue=None):
        self.source = source
        if converters is None:
            self.converters = dict()
        elif isinstance(converters, dict):
            self.converters = converters
        elif isinstance(converters, (tuple, list)):
            self.converters = dict([(i, v) for i, v in enumerate(converters)])
        else:
            raise Exception('unexpected converters: %r' % converters)
        self.failonerror = failonerror
        self.errorvalue = errorvalue
        
    def __iter__(self):
        return iterfieldconvert(self.source, self.converters, self.failonerror, self.errorvalue)
    
    def __setitem__(self, key, value):
        self.converters[key] = value
        
    
def iterfieldconvert(source, converters, failonerror, errorvalue):

    # grab the fields in the source table
    it = iter(source)
    flds = it.next()
    yield tuple(flds) # these are not modified

    norm_converters = dict()
    # normalise converters
    for k, c in converters.items():
        # turn field names into row indices
        if isinstance(k, basestring):
            try:
                k = flds.index(k)
            except ValueError: # not in list
                raise FieldSelectionError(k)
        assert isinstance(k, int), 'expected integer, found %r' % k
        # is converter a function?
        if callable(c):
            norm_converters[k] = c
        # is converter a method name?
        elif isinstance(c, basestring):
            norm_converters[k] = methodcaller(c)
        # is converter a method name with arguments?
        elif isinstance(c, (tuple, list)) and isinstance(c[0], basestring):
            methnm = c[0]
            methargs = c[1:]
            norm_converters[k] = methodcaller(methnm, *methargs)
        # is converter a dictionary
        elif isinstance(c, dict):
            norm_converters[k] = dictconverter(c)
        elif c is None:
            pass # ignore
        else:
            raise Exception('unexpected converter specification on field %r: %r' % (k, c))
    
    
    # define a function to transform a value
    def transform_value(i, v):
        if i not in norm_converters:
            # no converter defined on this field, return value as-is
            return v
        else:
            try:
                return norm_converters[i](v)
            except:
                if failonerror:
                    raise
                else:
                    return errorvalue

    # construct the data rows
    for row in it:
        yield tuple(transform_value(i, v) for i, v in enumerate(row))

            
def methodcaller(nm, *args):
    return lambda v: getattr(v, nm)(*args)


def dictconverter(d):
    def conv(v):
        if v in d:
            return d[v]
        else:
            return v
    return conv


def replace(table, field, a, b):
    """
    Convenience function to replace all occurrences of `a` with `b` under the 
    given field. See also :func:`convert`.
    
    .. versionadded:: 0.5
    
    """
    
    return convert(table, field, {a: b})


def sub(table, field, pattern, repl, count=0, flags=0):
    """
    Convenience function to convert values under the given field using a 
    regular expression substitution. See also :func:`re.sub`.
    
    .. versionadded:: 0.5
    
    .. versionchanged:: 0.10
    
    Renamed 'resub' to 'sub'.

    """
    
    prog = re.compile(pattern, flags)
    conv = lambda v: prog.sub(repl, v, count=count)
    return convert(table, field, conv)


resub = sub # backwards compatibility

    
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


def head(table, n=10):
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

        
def tail(table, n=10):
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


def sort(table, key=None, reverse=False, buffersize=None, tempdir=None, cache=True):
    """
    Sort the table. Field names or indices (from zero) can be used to specify 
    the key. E.g.::
    
        >>> from petl import sort, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'C'   | 2     |
        +-------+-------+
        | 'A'   | 9     |
        +-------+-------+
        | 'A'   | 6     |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        
        >>> table2 = sort(table1, 'foo')
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 9     |
        +-------+-------+
        | 'A'   | 6     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+
        
        >>> # sorting by compound key is supported
        ... table3 = sort(table1, key=['foo', 'bar'])
        >>> look(table3)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 6     |
        +-------+-------+
        | 'A'   | 9     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+
        
        >>> # if no key is specified, the default is a lexical sort
        ... table4 = sort(table1)
        >>> look(table4)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 6     |
        +-------+-------+
        | 'A'   | 9     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+

    The `buffersize` argument should be an `int` or `None`.
    
    If the number of rows in the table is less than `buffersize`, the table
    will be sorted in memory. Otherwise, the table is sorted in chunks of
    no more than `buffersize` rows, each chunk is written to a temporary file, 
    and then a merge sort is performed on the temporary files.
    
    If `buffersize` is `None`, the value of `petl.transform.defaultbuffersize` 
    will be used. By default this is set to 100000 rows, but can be changed, e.g.::
    
        >>> import petl.transform
        >>> petl.transform.defaultbuffersize = 500000
        
    If `petl.transform.defaultbuffersize` is set to `None`, this forces all
    sorting to be done entirely in memory.
    
    .. versionchanged:: 0.16
    
    By default the results of the sort will be cached, and so a second pass over
    the sorted table will yield rows from the cache and will not repeat the
    sort operation. To turn off caching, set the `cache` argument to `False`.

    """
    
    return SortView(table, key=key, reverse=reverse, buffersize=buffersize,
                    tempdir=tempdir, cache=cache)
    

def iterchunk(f):
    # reopen so iterators from file cache are independent
    with open(f.name, 'rb') as f:
        try:
            while True:
                yield pickle.load(f)
        except EOFError:
            pass

# non-independent version of iteration from file cache which doesn't depend
# on named temporary files
#def iterchunk(f):
#    debug('seek(0): %r', f)
#    f.seek(0)
#    try:
#        while True:
#            yield pickle.load(f)
#    except EOFError:
#        pass


def _mergesorted(key=None, reverse=False, *iterables):

    # N.B., I've used heapq for normal merge sort and shortlist merge sort for reverse
    # merge sort because I've assumed that heapq.merge is faster and so is preferable
    # but it doesn't support reverse sorting so the shortlist merge sort has to
    # be used for reverse sorting. Some casual profiling suggests there isn't much
    # between the two in terms of speed, but might be worth profiling more carefully
    
    if reverse:
        return shortlistmergesorted(key, True, *iterables)
    else:
        return heapqmergesorted(key, *iterables)


defaultbuffersize = 100000
    
    
class SortView(RowContainer):
    
    def __init__(self, source, key=None, reverse=False, buffersize=None,
                 tempdir=None, cache=True):
        self.source = source
        self.key = key
        self.reverse = reverse
        if buffersize is None:
            self.buffersize = defaultbuffersize
        else:
            self.buffersize = buffersize
        self.tempdir = tempdir
        self.cache = cache
        self._fldcache = None
        self._memcache = None
        self._filecache = None
        self._getkey = None
    
    def clearcache(self):
        self._clearcache()
        
    def _clearcache(self):
        self._fldcache = None
        self._memcache = None
        self._filecache = None
        self._getkey = None
        
    def __iter__(self):
        source = self.source
        key = self.key
        reverse = self.reverse
        if self.cache and self._memcache is not None:
            return self._iterfrommemcache()
        elif self.cache and self._filecache is not None:
            return self._iterfromfilecache()
        else:
            return self._iternocache(source, key, reverse)
        
    def _iterfrommemcache(self):
        debug('iterate from mem cache')
        yield tuple(self._fldcache)
        for row in self._memcache:
            yield tuple(row)
            
    def _iterfromfilecache(self):
        debug('iterate from file cache: %r', [f for f in self._filecache])
        yield tuple(self._fldcache)
        chunkiters = [iterchunk(f) for f in self._filecache]
        for row in _mergesorted(self._getkey, self.reverse, *chunkiters):
            yield tuple(row)
        
    def _iternocache(self, source, key, reverse):
        debug('iterate without cache')
        self._clearcache()
        it = iter(source)

        flds = it.next()
        yield tuple(flds)
        
        if key is not None:
            # convert field selection into field indices
            indices = asindices(flds, key)
        else:
            indices = range(len(flds))
        # now use field indices to construct a _getkey function
        # N.B., this will probably raise an exception on short rows
        getkey = sortable_itemgetter(*indices)
        
        # initialise the first chunk
        rows = list(islice(it, 0, self.buffersize))
        rows.sort(key=getkey, reverse=reverse)
        
        # have we exhausted the source iterator?
        if self.buffersize is None or len(rows) < self.buffersize:

            if self.cache:
                debug('caching mem')
                self._fldcache = flds
                self._memcache = rows
                self._getkey = getkey # actually not needed to iterate from memcache
    
            for row in rows:
                yield tuple(row)
                
        else:

            chunkfiles = []  
            
            while rows:
            
                # dump the chunk
                f = NamedTemporaryFile(dir=self.tempdir)
                for row in rows:
                    pickle.dump(row, f, protocol=-1)
                f.flush()
                # N.B., do not close the file! Closing will delete
                # the file, and we might want to keep it around
                # if it can be cached. We'll let garbage collection
                # deal with this, i.e., when no references to the 
                # chunk files exist any more, garbage collection
                # should be an implicit close, which will cause file
                # deletion.
                chunkfiles.append(f)
                
                # grab the next chunk
                rows = list(islice(it, 0, self.buffersize))
                rows.sort(key=getkey, reverse=reverse)

            if self.cache:
                debug('caching files %r', chunkfiles)
                self._fldcache = flds
                self._filecache = chunkfiles
                self._getkey = getkey

            chunkiters = [iterchunk(f) for f in chunkfiles]
            for row in _mergesorted(getkey, reverse, *chunkiters):
                yield tuple(row)

    
def melt(table, key=None, variables=None, variablefield='variable', valuefield='value'):
    """
    Reshape a table, melting fields into data. E.g.::

        >>> from petl import melt, look
        >>> look(table1)
        +------+----------+-------+
        | 'id' | 'gender' | 'age' |
        +======+==========+=======+
        | 1    | 'F'      | 12    |
        +------+----------+-------+
        | 2    | 'M'      | 17    |
        +------+----------+-------+
        | 3    | 'M'      | 16    |
        +------+----------+-------+
        
        >>> table2 = melt(table1, 'id')
        >>> look(table2)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | 1    | 'gender'   | 'F'     |
        +------+------------+---------+
        | 1    | 'age'      | 12      |
        +------+------------+---------+
        | 2    | 'gender'   | 'M'     |
        +------+------------+---------+
        | 2    | 'age'      | 17      |
        +------+------------+---------+
        | 3    | 'gender'   | 'M'     |
        +------+------------+---------+
        | 3    | 'age'      | 16      |
        +------+------------+---------+
        
        >>> # compound keys are supported
        ... look(table3)
        +------+--------+----------+----------+
        | 'id' | 'time' | 'height' | 'weight' |
        +======+========+==========+==========+
        | 1    | 11     | 66.4     | 12.2     |
        +------+--------+----------+----------+
        | 2    | 16     | 53.2     | 17.3     |
        +------+--------+----------+----------+
        | 3    | 12     | 34.5     | 9.4      |
        +------+--------+----------+----------+
        
        >>> table4 = melt(table3, key=['id', 'time'])
        >>> look(table4)
        +------+--------+------------+---------+
        | 'id' | 'time' | 'variable' | 'value' |
        +======+========+============+=========+
        | 1    | 11     | 'height'   | 66.4    |
        +------+--------+------------+---------+
        | 1    | 11     | 'weight'   | 12.2    |
        +------+--------+------------+---------+
        | 2    | 16     | 'height'   | 53.2    |
        +------+--------+------------+---------+
        | 2    | 16     | 'weight'   | 17.3    |
        +------+--------+------------+---------+
        | 3    | 12     | 'height'   | 34.5    |
        +------+--------+------------+---------+
        | 3    | 12     | 'weight'   | 9.4     |
        +------+--------+------------+---------+
        
        >>> # a subset of variable fields can be selected
        ... table5 = melt(table3, key=['id', 'time'], variables=['height'])    
        >>> look(table5)
        +------+--------+------------+---------+
        | 'id' | 'time' | 'variable' | 'value' |
        +======+========+============+=========+
        | 1    | 11     | 'height'   | 66.4    |
        +------+--------+------------+---------+
        | 2    | 16     | 'height'   | 53.2    |
        +------+--------+------------+---------+
        | 3    | 12     | 'height'   | 34.5    |
        +------+--------+------------+---------+

    See also :func:`recast`.
    
    """
    
    return MeltView(table, key=key, variables=variables, 
                    variablefield=variablefield, 
                    valuefield=valuefield)
    
    
class MeltView(RowContainer):
    
    def __init__(self, source, key=None, variables=None, 
                 variablefield='variable', valuefield='value'):
        self.source = source
        self.key = key
        self.variables = variables
        self.variablefield = variablefield
        self.valuefield = valuefield
        
    def __iter__(self):
        return itermelt(self.source, self.key, self.variables, 
                        self.variablefield, self.valuefield)
    

def itermelt(source, key, variables, variablefield, valuefield):
    it = iter(source)
    
    # normalise some stuff
    flds = it.next()
    if isinstance(key, basestring):
        key = (key,) # normalise to a tuple
    if isinstance(variables, basestring):
        # shouldn't expect this, but ... ?
        variables = (variables,) # normalise to a tuple
    if not key:
        # assume key is fields not in variables
        key = [f for f in flds if f not in variables]
    if not variables:
        # assume variables are fields not in key
        variables = [f for f in flds if f not in key]
    
    # determine the output fields
    out_flds = list(key)
    out_flds.append(variablefield)
    out_flds.append(valuefield)
    yield tuple(out_flds)
    
    key_indices = [flds.index(k) for k in key]
    getkey = rowgetter(*key_indices)
    variables_indices = [flds.index(v) for v in variables]
    
    # construct the output data
    for row in it:
        k = getkey(row)
        for v, i in zip(variables, variables_indices):
            o = list(k) # populate with key values initially
            o.append(v) # add variable
            o.append(row[i]) # add value
            yield tuple(o)
            

def recast(table, key=None, variablefield='variable', valuefield='value', 
           samplesize=1000, reducers=None, missing=None):
    """
    Recast molten data. E.g.::
    
        >>> from petl import recast, look
        >>> look(table1)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | 3    | 'age'      | 16      |
        +------+------------+---------+
        | 1    | 'gender'   | 'F'     |
        +------+------------+---------+
        | 2    | 'gender'   | 'M'     |
        +------+------------+---------+
        | 2    | 'age'      | 17      |
        +------+------------+---------+
        | 1    | 'age'      | 12      |
        +------+------------+---------+
        | 3    | 'gender'   | 'M'     |
        +------+------------+---------+
        
        >>> table2 = recast(table1)
        >>> look(table2)
        +------+-------+----------+
        | 'id' | 'age' | 'gender' |
        +======+=======+==========+
        | 1    | 12    | 'F'      |
        +------+-------+----------+
        | 2    | 17    | 'M'      |
        +------+-------+----------+
        | 3    | 16    | 'M'      |
        +------+-------+----------+
        
        >>> # specifying variable and value fields
        ... look(table3)
        +------+----------+--------+
        | 'id' | 'vars'   | 'vals' |
        +======+==========+========+
        | 3    | 'age'    | 16     |
        +------+----------+--------+
        | 1    | 'gender' | 'F'    |
        +------+----------+--------+
        | 2    | 'gender' | 'M'    |
        +------+----------+--------+
        | 2    | 'age'    | 17     |
        +------+----------+--------+
        | 1    | 'age'    | 12     |
        +------+----------+--------+
        | 3    | 'gender' | 'M'    |
        +------+----------+--------+
        
        >>> table4 = recast(table3, variablefield='vars', valuefield='vals')
        >>> look(table4)
        +------+-------+----------+
        | 'id' | 'age' | 'gender' |
        +======+=======+==========+
        | 1    | 12    | 'F'      |
        +------+-------+----------+
        | 2    | 17    | 'M'      |
        +------+-------+----------+
        | 3    | 16    | 'M'      |
        +------+-------+----------+
        
        >>> # if there are multiple values for each key/variable pair, and no reducers
        ... # function is provided, then all values will be listed
        ... look(table6)
        +------+--------+------------+---------+
        | 'id' | 'time' | 'variable' | 'value' |
        +======+========+============+=========+
        | 1    | 11     | 'weight'   | 66.4    |
        +------+--------+------------+---------+
        | 1    | 14     | 'weight'   | 55.2    |
        +------+--------+------------+---------+
        | 2    | 12     | 'weight'   | 53.2    |
        +------+--------+------------+---------+
        | 2    | 16     | 'weight'   | 43.3    |
        +------+--------+------------+---------+
        | 3    | 12     | 'weight'   | 34.5    |
        +------+--------+------------+---------+
        | 3    | 17     | 'weight'   | 49.4    |
        +------+--------+------------+---------+
        
        >>> table7 = recast(table6, key='id')
        >>> look(table7)
        +------+--------------+
        | 'id' | 'weight'     |
        +======+==============+
        | 1    | [66.4, 55.2] |
        +------+--------------+
        | 2    | [53.2, 43.3] |
        +------+--------------+
        | 3    | [34.5, 49.4] |
        +------+--------------+
        
        >>> # multiple values can be reduced via an aggregation function
        ... def mean(values):
        ...     return float(sum(values)) / len(values)
        ... 
        >>> table8 = recast(table6, key='id', reducers={'weight': mean})
        >>> look(table8)    
        +------+--------------------+
        | 'id' | 'weight'           |
        +======+====================+
        | 1    | 60.800000000000004 |
        +------+--------------------+
        | 2    | 48.25              |
        +------+--------------------+
        | 3    | 41.95              |
        +------+--------------------+
        
        >>> # missing values are padded with whatever is provided via the missing 
        ... # keyword argument (None by default)
        ... look(table9)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | 1    | 'gender'   | 'F'     |
        +------+------------+---------+
        | 2    | 'age'      | 17      |
        +------+------------+---------+
        | 1    | 'age'      | 12      |
        +------+------------+---------+
        | 3    | 'gender'   | 'M'     |
        +------+------------+---------+
        
        >>> table10 = recast(table9, key='id')
        >>> look(table10)
        +------+-------+----------+
        | 'id' | 'age' | 'gender' |
        +======+=======+==========+
        | 1    | 12    | 'F'      |
        +------+-------+----------+
        | 2    | 17    | None     |
        +------+-------+----------+
        | 3    | None  | 'M'      |
        +------+-------+----------+

    Note that the table is scanned once to discover variables, then a second
    time to reshape the data and recast variables as fields. How many rows are
    scanned in the first pass is determined by the `samplesize` argument.
    
    See also :func:`melt`.
    
    """
    
    return RecastView(table, key=key, variablefield=variablefield, 
                      valuefield=valuefield, samplesize=samplesize, 
                      reducers=reducers, missing=missing)
    

class RecastView(RowContainer):
    
    def __init__(self, source, key=None, variablefield='variable', 
                 valuefield='value', samplesize=1000, reducers=None, 
                 missing=None):
        self.source = source
        self.key = key
        self.variablefield = variablefield
        self.valuefield = valuefield
        self.samplesize = samplesize
        if reducers is None:
            self.reducers = dict()
        else:
            self.reducers = reducers
        self.missing = missing
        
    def __iter__(self):
        return iterrecast(self.source, self.key, self.variablefield, 
                          self.valuefield, self.samplesize, self.reducers,
                          self.missing)


def iterrecast(source, key, variablefield, valuefield, 
               samplesize, reducers, missing):        
    #
    # TODO implementing this by making two passes through the data is a bit
    # ugly, and could be costly if there are several upstream transformations
    # that would need to be re-executed each pass - better to make one pass,
    # caching the rows sampled to discover variables to be recast as fields?
    #
    
    
    it = iter(source)
    fields = it.next()
    
    # normalise some stuff
    keyfields = key
    variablefields = variablefield # N.B., could be more than one
    if isinstance(keyfields, basestring):
        keyfields = (keyfields,)
    if isinstance(variablefields, basestring):
        variablefields = (variablefields,)
    if not keyfields:
        # assume keyfields is fields not in variables
        keyfields = [f for f in fields if f not in variablefields and f != valuefield]
    if not variablefields:
        # assume variables are fields not in keyfields
        variablefields = [f for f in fields if f not in keyfields and f != valuefield]
    
    # sanity checks
    assert valuefield in fields, 'invalid value field: %s' % valuefield
    assert valuefield not in keyfields, 'value field cannot be keyfields'
    assert valuefield not in variablefields, 'value field cannot be variable field'
    for f in keyfields:
        assert f in fields, 'invalid keyfields field: %s' % f
    for f in variablefields:
        assert f in fields, 'invalid variable field: %s' % f

    # we'll need these later
    valueindex = fields.index(valuefield)
    keyindices = [fields.index(f) for f in keyfields]
    variableindices = [fields.index(f) for f in variablefields]
    
    # determine the actual variable names to be cast as fields
    if isinstance(variablefields, dict):
        # user supplied dictionary
        variables = variablefields
    else:
        variables = defaultdict(set)
        # sample the data to discover variables to be cast as fields
        for row in islice(it, 0, samplesize):
            for i, f in zip(variableindices, variablefields):
                variables[f].add(row[i])
        for f in variables:
            variables[f] = sorted(variables[f]) # turn from sets to sorted lists

    # finished the first pass
        
    # determine the output fields
    outfields = list(keyfields)
    for f in variablefields:
        outfields.extend(variables[f])
    yield tuple(outfields)
    
    # output data
    
    source = sort(source, key=keyfields)
    it = islice(source, 1, None) # skip header row
    getsortablekey = sortable_itemgetter(*keyindices)
    getactualkey = itemgetter(*keyindices)
    
    # process sorted data in newfields
    groups = groupby(it, key=getsortablekey)
    for _, group in groups:
        group = list(group) # may need to iterate over the group more than once
        # N.B., key returned by groupby may be wrapped as SortableItem, we want
        # to output the actual key value, get it from the first row in the group
        key_value = getactualkey(group[0])
        if len(keyfields) > 1:
            out_row = list(key_value)
        else:
            out_row = [key_value]
        for f, i in zip(variablefields, variableindices):
            for variable in variables[f]:
                # collect all values for the current variable
                values = [r[valueindex] for r in group if r[i] == variable]
                if len(values) == 0:
                    value = missing
                elif len(values) == 1:
                    value = values[0]
                else:
                    if variable in reducers:
                        redu = reducers[variable]
                    else:
                        redu = list # list all values
                    value = redu(values)
                out_row.append(value)
        yield tuple(out_row)
                
            
def duplicates(table, key=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Select rows with duplicate values under a given key (or duplicate
    rows where no key is given). E.g.::

        >>> from petl import duplicates, look    
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | 2.0   |
        +-------+-------+-------+
        | 'B'   | 2     | 3.4   |
        +-------+-------+-------+
        | 'D'   | 6     | 9.3   |
        +-------+-------+-------+
        | 'B'   | 3     | 7.8   |
        +-------+-------+-------+
        | 'B'   | 2     | 12.3  |
        +-------+-------+-------+
        | 'E'   | None  | 1.3   |
        +-------+-------+-------+
        | 'D'   | 4     | 14.5  |
        +-------+-------+-------+
        
        >>> table2 = duplicates(table1, 'foo')
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'B'   | 2     | 3.4   |
        +-------+-------+-------+
        | 'B'   | 3     | 7.8   |
        +-------+-------+-------+
        | 'B'   | 2     | 12.3  |
        +-------+-------+-------+
        | 'D'   | 6     | 9.3   |
        +-------+-------+-------+
        | 'D'   | 4     | 14.5  |
        +-------+-------+-------+
        
        >>> # compound keys are supported
        ... table3 = duplicates(table1, key=['foo', 'bar'])
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'B'   | 2     | 3.4   |
        +-------+-------+-------+
        | 'B'   | 2     | 12.3  |
        +-------+-------+-------+
        
    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    See also :func:`unique` and :func:`distinct`.
    
    """

    return DuplicatesView(table, key=key, presorted=presorted, 
                          buffersize=buffersize, tempdir=tempdir, cache=cache)


class DuplicatesView(RowContainer):
    
    def __init__(self, source, key=None, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key # TODO property
        
    def __iter__(self):
        return iterduplicates(self.source, self.key)


def iterduplicates(source, key):
    # assume source is sorted
    # first need to sort the data
    it = iter(source)

    flds = it.next()
    yield tuple(flds)

    # convert field selection into field indices
    if key is None:
        indices = range(len(flds))
    else:
        indices = asindices(flds, key)
        
    # now use field indices to construct a _getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    previous = None
    previous_yielded = False
    
    for row in it:
        if previous is None:
            previous = row
        else:
            kprev = getkey(previous)
            kcurr = getkey(row)
            if kprev == kcurr:
                if not previous_yielded:
                    yield tuple(previous)
                    previous_yielded = True
                yield tuple(row)
            else:
                # reset
                previous_yielded = False
            previous = row
    
    
def unique(table, key=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Select rows with unique values under a given key (or unique rows
    if no key is given). E.g.::

        >>> from petl import unique, look
        >>> look(table1)
        +-------+-------+--------+
        | 'foo' | 'bar' | 'baz'  |
        +=======+=======+========+
        | 'A'   | 1     | 2      |
        +-------+-------+--------+
        | 'B'   | '2'   | '3.4'  |
        +-------+-------+--------+
        | 'D'   | 'xyz' | 9.0    |
        +-------+-------+--------+
        | 'B'   | u'3'  | u'7.8' |
        +-------+-------+--------+
        | 'B'   | '2'   | 42     |
        +-------+-------+--------+
        | 'E'   | None  | None   |
        +-------+-------+--------+
        | 'D'   | 4     | 12.3   |
        +-------+-------+--------+
        | 'F'   | 7     | 2.3    |
        +-------+-------+--------+
        
        >>> table2 = unique(table1, 'foo')
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | 2     |
        +-------+-------+-------+
        | 'E'   | None  | None  |
        +-------+-------+-------+
        | 'F'   | 7     | 2.3   |
        +-------+-------+-------+
        
    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.

    .. versionadded:: 0.10

    See also :func:`duplicates` and :func:`distinct`.
    
    """

    return UniqueView(table, key=key, presorted=presorted, 
                      buffersize=buffersize, tempdir=tempdir, cache=cache)


class UniqueView(RowContainer):
    
    def __init__(self, source, key=None, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key # TODO property
        
    def __iter__(self):
        return iterunique(self.source, self.key)


def iterunique(source, key):
    # assume source is sorted
    # first need to sort the data
    it = iter(source)

    flds = it.next()
    yield tuple(flds)

    # convert field selection into field indices
    if key is None:
        indices = range(len(flds))
    else:
        indices = asindices(flds, key)
        
    # now use field indices to construct a _getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    prev = it.next()
    prev_key = getkey(prev)
    prev_comp_ne = True
    
    for curr in it:
        curr_key = getkey(curr)
        curr_comp_ne = (curr_key != prev_key)
        if prev_comp_ne and curr_comp_ne:
            yield tuple(prev)
        prev = curr
        prev_key = curr_key
        prev_comp_ne = curr_comp_ne
        
    # last one?
    if prev_comp_ne:
        yield prev
    
    
def conflicts(table, key, missing=None, include=None, exclude=None, 
              presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Select rows with the same key value but differing in some other field. E.g.::

        >>> from petl import conflicts, look    
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | 2.7   |
        +-------+-------+-------+
        | 'B'   | 2     | None  |
        +-------+-------+-------+
        | 'D'   | 3     | 9.4   |
        +-------+-------+-------+
        | 'B'   | None  | 7.8   |
        +-------+-------+-------+
        | 'E'   | None  |       |
        +-------+-------+-------+
        | 'D'   | 3     | 12.3  |
        +-------+-------+-------+
        | 'A'   | 2     | None  |
        +-------+-------+-------+
        
        >>> table2 = conflicts(table1, 'foo')
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | 2.7   |
        +-------+-------+-------+
        | 'A'   | 2     | None  |
        +-------+-------+-------+
        | 'D'   | 3     | 9.4   |
        +-------+-------+-------+
        | 'D'   | 3     | 12.3  |
        +-------+-------+-------+
        
    Missing values are not considered conflicts. By default, `None` is treated
    as the missing value, this can be changed via the `missing` keyword 
    argument.

    One or more fields can be ignored when determining conflicts by providing
    the `exclude` keyword argument. Alternatively, fields to use when determining
    conflicts can be specified explicitly with the `include` keyword argument. 

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    .. versionchanged:: 0.8
    
    Added the `include` and `exclude` keyword arguments. The `exclude` keyword 
    argument replaces the `ignore` keyword argument in previous versions.
    
    """
    
    return ConflictsView(table, key, missing=missing, exclude=exclude, include=include,
                         presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)


class ConflictsView(RowContainer):
    
    def __init__(self, source, key, missing=None, exclude=None, include=None, 
                 presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key
        self.missing = missing
        self.exclude = exclude
        self.include = include
        
    def __iter__(self):
        return iterconflicts(self.source, self.key, self.missing, self.exclude, 
                             self.include)
    
    
def iterconflicts(source, key, missing, exclude, include):

    # normalise arguments
    if isinstance(exclude, basestring):
        exclude = (exclude,)
    if isinstance(include, basestring):
        include = (include,)

    # exclude overrides include
    if include and exclude:
        include = None
        
    it = iter(source)
    flds = it.next()
    yield tuple(flds)

    # convert field selection into field indices
    indices = asindices(flds, key)
                    
    # now use field indices to construct a _getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    previous = None
    previous_yielded = False
    
    for row in it:
        if previous is None:
            previous = row
        else:
            kprev = getkey(previous)
            kcurr = getkey(row)
            if kprev == kcurr:
                # is there a conflict?
                conflict = False
                for x, y, f in zip(previous, row, flds):
                    if (exclude and f not in exclude) or (include and f in include) or (not exclude and not include):
                        if missing not in (x, y) and x != y:
                            conflict = True
                            break
                if conflict:
                    if not previous_yielded:
                        yield tuple(previous)
                        previous_yielded = True
                    yield tuple(row)
            else:
                # reset
                previous_yielded = False
            previous = row
    

def complement(a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Return rows in `a` that are not in `b`. E.g.::
    
        >>> from petl import complement, look
        >>> look(a)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+
        
        >>> look(b)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'B' | 2   | False |
        +-----+-----+-------+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+
        | 'C' | 9   | True  |
        +-----+-----+-------+
        
        >>> aminusb = complement(a, b)
        >>> look(aminusb)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        
        >>> bminusa = complement(b, a)
        >>> look(bminusa)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+
        
    Note that the field names of each table are ignored - rows are simply compared
    following a lexical sort. See also the :func:`recordcomplement` function.
    
    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    """

    return ComplementView(a, b, presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)


class ComplementView(RowContainer):
    
    def __init__(self, a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.a = a
            self.b = b
        else:
            self.a = sort(a, buffersize=buffersize, tempdir=tempdir, cache=cache)
            self.b = sort(b, buffersize=buffersize, tempdir=tempdir, cache=cache)
            
    def __iter__(self):
        return itercomplement(self.a, self.b)


def itercomplement(ta, tb):
    # coerce rows to tuples to ensure hashable and comparable
    ita = (tuple(row) for row in iter(ta)) 
    itb = (tuple(row) for row in iter(tb))
    aflds = tuple(str(f) for f in ita.next())
    itb.next() # ignore b fields
    yield aflds

    try:
        a = ita.next()
    except StopIteration:
        debug('a is empty, nothing to yield')
        pass
    else:
        try:
            b = itb.next()
        except StopIteration:
            debug('b is empty, just iterate through a')
            yield a
            for row in ita:
                yield row
        else:
            # we want the elements in a that are not in b
            while True:
                debug('current rows: %r %r', a, b)
                if b is None or SortableItem(a) < SortableItem(b):
                    yield a
                    debug('advance a')
                    try:
                        a = ita.next()
                    except StopIteration:
                        break
                elif a == b:
                    debug('advance both')
                    try:
                        a = ita.next()
                    except StopIteration:
                        break
                    try:
                        b = itb.next()
                    except StopIteration:
                        b = None
                else:
                    debug('advance b')
                    try:
                        b = itb.next()
                    except StopIteration:
                        b = None
        
    
def recordcomplement(a, b, buffersize=None, tempdir=None, cache=True):
    """
    Find records in `a` that are not in `b`. E.g.::
    
        >>> from petl import recordcomplement, look
        >>> look(a)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+
        
        >>> look(b)
        +-------+-------+-------+
        | 'bar' | 'foo' | 'baz' |
        +=======+=======+=======+
        | 2     | 'B'   | False |
        +-------+-------+-------+
        | 9     | 'A'   | False |
        +-------+-------+-------+
        | 3     | 'B'   | True  |
        +-------+-------+-------+
        | 9     | 'C'   | True  |
        +-------+-------+-------+
        
        >>> aminusb = recordcomplement(a, b)
        >>> look(aminusb)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        
        >>> bminusa = recordcomplement(b, a)
        >>> look(bminusa)
        +-------+-------+-------+
        | 'bar' | 'foo' | 'baz' |
        +=======+=======+=======+
        | 3     | 'B'   | True  |
        +-------+-------+-------+
        | 9     | 'A'   | False |
        +-------+-------+-------+
        
    Note that both tables must have the same set of fields, but that the order
    of the fields does not matter. See also the :func:`complement` function.
    
    See also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the :func:`sort` 
    function.
    
    .. versionadded:: 0.3
    
    """
    
    ha = header(a)
    hb = header(b)
    assert set(ha) == set(hb), 'both tables must have the same set of fields'
    # make sure fields are in the same order
    bv = cut(b, *ha)
    return complement(a, bv, buffersize=buffersize, tempdir=tempdir, cache=cache)


def diff(a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Find the difference between rows in two tables. Returns a pair of tables. 
    E.g.::
    
        >>> from petl import diff, look
        >>> look(a)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+
        
        >>> look(b)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'B' | 2   | False |
        +-----+-----+-------+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+
        | 'C' | 9   | True  |
        +-----+-----+-------+
        
        >>> added, subtracted = diff(a, b)
        >>> # rows in b not in a
        ... look(added)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+
        
        >>> # rows in a not in b
        ... look(subtracted)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        
    Convenient shorthand for ``(complement(b, a), complement(a, b))``. See also
    :func:`complement`.

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    """

    if not presorted:    
        a = sort(a)
        b = sort(b)
    added = complement(b, a, presorted=True, buffersize=buffersize, tempdir=tempdir, cache=cache)
    subtracted = complement(a, b, presorted=True, buffersize=buffersize, tempdir=tempdir, cache=cache)
    return added, subtracted
    
    
def recorddiff(a, b, buffersize=None, tempdir=None, cache=True):
    """
    Find the difference between records in two tables. E.g.::

        >>> from petl import recorddiff, look    
        >>> look(a)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+
        
        >>> look(b)
        +-------+-------+-------+
        | 'bar' | 'foo' | 'baz' |
        +=======+=======+=======+
        | 2     | 'B'   | False |
        +-------+-------+-------+
        | 9     | 'A'   | False |
        +-------+-------+-------+
        | 3     | 'B'   | True  |
        +-------+-------+-------+
        | 9     | 'C'   | True  |
        +-------+-------+-------+
        
        >>> added, subtracted = recorddiff(a, b)
        >>> look(added)
        +-------+-------+-------+
        | 'bar' | 'foo' | 'baz' |
        +=======+=======+=======+
        | 3     | 'B'   | True  |
        +-------+-------+-------+
        | 9     | 'A'   | False |
        +-------+-------+-------+
        
        >>> look(subtracted)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+

    Convenient shorthand for ``(recordcomplement(b, a), recordcomplement(a, b))``. 
    See also :func:`recordcomplement`.

    See also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the :func:`sort` 
    function.
    
    .. versionadded:: 0.3
    
    """

    added = recordcomplement(b, a, buffersize=buffersize, tempdir=tempdir, cache=cache)
    subtracted = recordcomplement(a, b, buffersize=buffersize, tempdir=tempdir, cache=cache)
    return added, subtracted
    
    
def capture(table, field, pattern, newfields=None, include_original=False, 
            flags=0, fill=None):
    """
    Add one or more new fields with values captured from an
    existing field searched via a regular expression. E.g.::

        >>> from petl import capture, look
        >>> look(table1)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | '1'  | 'A1'       | '12'    |
        +------+------------+---------+
        | '2'  | 'A2'       | '15'    |
        +------+------------+---------+
        | '3'  | 'B1'       | '18'    |
        +------+------------+---------+
        | '4'  | 'C12'      | '19'    |
        +------+------------+---------+
        
        >>> table2 = capture(table1, 'variable', '(\\w)(\\d+)', ['treat', 'time'])
        >>> look(table2)
        +------+---------+---------+--------+
        | 'id' | 'value' | 'treat' | 'time' |
        +======+=========+=========+========+
        | '1'  | '12'    | 'A'     | '1'    |
        +------+---------+---------+--------+
        | '2'  | '15'    | 'A'     | '2'    |
        +------+---------+---------+--------+
        | '3'  | '18'    | 'B'     | '1'    |
        +------+---------+---------+--------+
        | '4'  | '19'    | 'C'     | '12'   |
        +------+---------+---------+--------+
        
        >>> # using the include_original argument
        ... table3 = capture(table1, 'variable', '(\\w)(\\d+)', ['treat', 'time'], include_original=True)
        >>> look(table3)
        +------+------------+---------+---------+--------+
        | 'id' | 'variable' | 'value' | 'treat' | 'time' |
        +======+============+=========+=========+========+
        | '1'  | 'A1'       | '12'    | 'A'     | '1'    |
        +------+------------+---------+---------+--------+
        | '2'  | 'A2'       | '15'    | 'A'     | '2'    |
        +------+------------+---------+---------+--------+
        | '3'  | 'B1'       | '18'    | 'B'     | '1'    |
        +------+------------+---------+---------+--------+
        | '4'  | 'C12'      | '19'    | 'C'     | '12'   |
        +------+------------+---------+---------+--------+
        
    By default the field on which the capture is performed is omitted. It can
    be included using the `include_original` argument.
    
    See also :func:`split`, :func:`re.search`.

    ..versionchanged:: 0.18

    The ``fill`` parameter can be used to provide a list or tuple of values to use if the regular expression does not
    match. The ``fill`` parameter should contain as many values as there are capturing groups in the regular expression.
    If ``fill`` is ``None`` (default) then a ``petl.transform.TransformError`` will be raised on the first non-matching
    value.

    """
    
    return CaptureView(table, field, pattern,
                       newfields=newfields,
                       include_original=include_original,
                       flags=flags,
                       fill=fill)


class CaptureView(RowContainer):
    
    def __init__(self, source, field, pattern, newfields=None, 
                 include_original=False, flags=0, fill=None):
        self.source = source
        self.field = field
        self.pattern = pattern
        self.newfields = newfields
        self.include_original = include_original
        self.flags = flags
        self.fill = fill
        
    def __iter__(self):
        return itercapture(self.source, self.field, self.pattern, self.newfields, 
                           self.include_original, self.flags, self.fill)


def itercapture(source, field, pattern, newfields, include_original, flags, fill):
    it = iter(source)
    prog = re.compile(pattern, flags)
    
    flds = it.next()
    if field in flds:
        field_index = flds.index(field)
    elif isinstance(field, int) and field < len(flds):
        field_index = field
    else:
        raise Exception('field invalid: must be either field name or index')
    
    # determine output fields
    out_flds = list(flds)
    if not include_original:
        out_flds.remove(field)
    if newfields:   
        out_flds.extend(newfields)
    yield tuple(out_flds)
    
    # construct the output data
    for row in it:
        value = row[field_index]
        if include_original:
            out_row = list(row)
        else:
            out_row = [v for i, v in enumerate(row) if i != field_index]
        match = prog.search(value)
        if match is None:
            if fill is not None:
                out_row.extend(fill)
            else:
                raise TransformError('value %r did not match pattern %r' % (value, pattern))
        else:
            out_row.extend(match.groups())
        yield tuple(out_row)
        
        
def split(table, field, pattern, newfields=None, include_original=False,
          maxsplit=0, flags=0):
    """
    Add one or more new fields with values generated by 
    splitting an existing value around occurrences of a regular expression. 
    E.g.::

        >>> from petl import split, look
        >>> look(table1)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | '1'  | 'parad1'   | '12'    |
        +------+------------+---------+
        | '2'  | 'parad2'   | '15'    |
        +------+------------+---------+
        | '3'  | 'tempd1'   | '18'    |
        +------+------------+---------+
        | '4'  | 'tempd2'   | '19'    |
        +------+------------+---------+
        
        >>> table2 = split(table1, 'variable', 'd', ['variable', 'day'])
        >>> look(table2)
        +------+---------+------------+-------+
        | 'id' | 'value' | 'variable' | 'day' |
        +======+=========+============+=======+
        | '1'  | '12'    | 'para'     | '1'   |
        +------+---------+------------+-------+
        | '2'  | '15'    | 'para'     | '2'   |
        +------+---------+------------+-------+
        | '3'  | '18'    | 'temp'     | '1'   |
        +------+---------+------------+-------+
        | '4'  | '19'    | 'temp'     | '2'   |
        +------+---------+------------+-------+
        
    See also :func:`re.split`.

    """
    
    return SplitView(table, field, pattern, newfields, include_original, maxsplit,
                     flags)


class SplitView(RowContainer):
    
    def __init__(self, source, field, pattern, newfields=None, 
                 include_original=False, maxsplit=0, flags=0):
        self.source = source
        self.field = field
        self.pattern = pattern
        self.newfields = newfields
        self.include_original = include_original
        self.maxsplit = maxsplit
        self.flags = flags
        
    def __iter__(self):
        return itersplit(self.source, self.field, self.pattern, self.newfields, 
                         self.include_original, self.maxsplit, self.flags)


def itersplit(source, field, pattern, newfields, include_original, maxsplit,
              flags):
        
    it = iter(source)
    prog = re.compile(pattern, flags)

    flds = it.next()
    if field in flds:
        field_index = flds.index(field)
    elif isinstance(field, int) and field < len(flds):
        field_index = field
    else:
        raise Exception('field invalid: must be either field name or index')
    
    # determine output fields
    out_flds = list(flds)
    if not include_original:
        out_flds.remove(field)
    if newfields:
        out_flds.extend(newfields)
    yield tuple(out_flds)
    
    # construct the output data
    for row in it:
        value = row[field_index]
        if include_original:
            out_row = list(row)
        else:
            out_row = [v for i, v in enumerate(row) if i != field_index]
        out_row.extend(prog.split(value, maxsplit))
        yield tuple(out_row)
        
    
def select(table, *args, **kwargs):
    """
    Select rows meeting a condition. E.g.::
    
        >>> from petl import select, look     
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 4     | 9.3   |
        +-------+-------+-------+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+
        | 'b'   | 1     | 23.3  |
        +-------+-------+-------+
        | 'c'   | 8     | 42.0  |
        +-------+-------+-------+
        | 'd'   | 7     | 100.9 |
        +-------+-------+-------+
        | 'c'   | 2     |       |
        +-------+-------+-------+
        
        >>> # the second positional argument can be a function accepting a record
        ... table2 = select(table1, lambda rec: rec[0] == 'a' and rec[1] > 88.1)
        ... # table2 = select(table1, lambda rec: rec['foo'] == 'a' and rec['baz'] > 88.1)
        ... # table2 = select(table1, lambda rec: rec.foo == 'a' and rec.baz > 88.1)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+
        
        >>> # the second positional argument can also be an expression string, which 
        ... # will be converted to a function using expr()
        ... table3 = select(table1, "{foo} == 'a' and {baz} > 88.1")
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+
        
        >>> # the condition can also be applied to a single field
        ... table4 = select(table1, 'foo', lambda v: v == 'a')
        >>> look(table4)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 4     | 9.3   |
        +-------+-------+-------+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+
        
    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    if 'missing' in kwargs:
        missing = kwargs['missing']
    else:
        missing = None
        
    if 'complement' in kwargs:
        complement = kwargs['complement']
    else:
        complement = False
        
    if len(args) == 0:
        raise Exception('missing positional argument')
    elif len(args) == 1:
        where = args[0]
        if isinstance(where, basestring):
            where = expr(where)
        else:
            assert callable(where), 'second argument must be string or callable'
        return RowSelectView(table, where, missing=missing, complement=complement)
    else:
        field = args[0]
        where = args[1]
        assert callable(where), 'third argument must be callable'
        return FieldSelectView(table, field, where, complement=complement)
        

def recordselect(table, where, missing=None, complement=False):
    """
    Select rows matching a condition. The `where` argument should be a function
    accepting a record (row as dictionary of values indexed by field name) as 
    argument and returning True or False.
    
    .. deprecated:: 0.9 
    
    Use :func:`select` instead.

    """
    
    return rowselect(table, where, missing=missing, complement=complement)
        
        
def rowselect(table, where, complement=False):
    """
    Select rows matching a condition. The `where` argument should be a function
    accepting a hybrid row object (supports accessing values either by 
    position or by field name) as argument and returning True or False.

    .. deprecated:: 0.10
    
    Use :func:`select` instead, it supports the same signature.
    
    """
    
    return RowSelectView(table, where, complement=complement)
        
        
class RowSelectView(RowContainer):
    
    def __init__(self, source, where, missing=None, complement=False):
        self.source = source
        self.where = where
        self.missing = missing
        self.complement = complement
        
    def __iter__(self):
        return iterrowselect(self.source, self.where, self.missing, self.complement)
    

def iterrowselect(source, where, missing, complement):
    it = iter(source)
    flds = it.next()
    yield tuple(flds)
    for row in hybridrows(flds, it, missing): # convert to hybrid row/record
        if where(row) != complement: # XOR
            yield tuple(row) # need to convert back to tuple?
     
     
def rowlenselect(table, n, complement=False):
    """
    Select rows of length `n`.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.
    
    """
    
    where = lambda row: len(row) == n
    return rowselect(table, where, complement=complement)   
        
        
def fieldselect(table, field, where, complement=False):
    """
    Select rows matching a condition. The `where` argument should be a function
    accepting a single data value as argument and returning True or False.

    .. deprecated:: 0.10
    
    Use :func:`select` instead, it supports the same signature.
    
    """
    
    return FieldSelectView(table, field, where, complement=complement)
        
        
class FieldSelectView(RowContainer):
    
    def __init__(self, source, field, where, complement=False):
        self.source = source
        self.field = field
        self.where = where
        self.complement = complement
        
    def __iter__(self):
        return iterfieldselect(self.source, self.field, self.where, self.complement)

    
def iterfieldselect(source, field, where, complement):
    it = iter(source)
    flds = it.next()
    yield tuple(flds)
    indices = asindices(flds, field)
    getv = itemgetter(*indices)
    for row in it:
        v = getv(row)
        if where(v) != complement: # XOR
            yield tuple(row)
        
        
def fieldmap(table, mappings=None, failonerror=False, errorvalue=None):
    """
    Transform a table, mapping fields arbitrarily between input and output. E.g.::
    
        >>> from petl import fieldmap, look
        >>> look(table1)
        +------+----------+-------+----------+----------+
        | 'id' | 'sex'    | 'age' | 'height' | 'weight' |
        +======+==========+=======+==========+==========+
        | 1    | 'male'   | 16    | 1.45     | 62.0     |
        +------+----------+-------+----------+----------+
        | 2    | 'female' | 19    | 1.34     | 55.4     |
        +------+----------+-------+----------+----------+
        | 3    | 'female' | 17    | 1.78     | 74.4     |
        +------+----------+-------+----------+----------+
        | 4    | 'male'   | 21    | 1.33     | 45.2     |
        +------+----------+-------+----------+----------+
        | 5    | '-'      | 25    | 1.65     | 51.9     |
        +------+----------+-------+----------+----------+
        
        >>> from collections import OrderedDict
        >>> mappings = OrderedDict()
        >>> # rename a field
        ... mappings['subject_id'] = 'id'
        >>> # translate a field
        ... mappings['gender'] = 'sex', {'male': 'M', 'female': 'F'}
        >>> # apply a calculation to a field
        ... mappings['age_months'] = 'age', lambda v: v * 12
        >>> # apply a calculation to a combination of fields
        ... mappings['bmi'] = lambda rec: rec['weight'] / rec['height']**2 
        >>> # transform and inspect the output
        ... table2 = fieldmap(table1, mappings)
        >>> look(table2)
        +--------------+----------+--------------+--------------------+
        | 'subject_id' | 'gender' | 'age_months' | 'bmi'              |
        +==============+==========+==============+====================+
        | 1            | 'M'      | 192          | 29.48870392390012  |
        +--------------+----------+--------------+--------------------+
        | 2            | 'F'      | 228          | 30.8531967030519   |
        +--------------+----------+--------------+--------------------+
        | 3            | 'F'      | 204          | 23.481883600555488 |
        +--------------+----------+--------------+--------------------+
        | 4            | 'M'      | 252          | 25.55260331279326  |
        +--------------+----------+--------------+--------------------+
        | 5            | '-'      | 300          | 19.0633608815427   |
        +--------------+----------+--------------+--------------------+
        
        >>> # field mappings can also be added and/or updated after the table is created 
        ... # via the suffix notation
        ... table3 = fieldmap(table1)
        >>> table3['subject_id'] = 'id'
        >>> table3['gender'] = 'sex', {'male': 'M', 'female': 'F'}
        >>> table3['age_months'] = 'age', lambda v: v * 12
        >>> # use an expression string this time
        ... table3['bmi'] = '{weight} / {height}**2'
        >>> look(table3)
        +--------------+----------+--------------+--------------------+
        | 'subject_id' | 'gender' | 'age_months' | 'bmi'              |
        +==============+==========+==============+====================+
        | 1            | 'M'      | 192          | 29.48870392390012  |
        +--------------+----------+--------------+--------------------+
        | 2            | 'F'      | 228          | 30.8531967030519   |
        +--------------+----------+--------------+--------------------+
        | 3            | 'F'      | 204          | 23.481883600555488 |
        +--------------+----------+--------------+--------------------+
        | 4            | 'M'      | 252          | 25.55260331279326  |
        +--------------+----------+--------------+--------------------+
        | 5            | '-'      | 300          | 19.0633608815427   |
        +--------------+----------+--------------+--------------------+
        
    Note also that the mapping value can be an expression string, which will be 
    converted to a lambda function via :func:`expr`. 

    """    
    
    return FieldMapView(table, mappings=mappings, failonerror=failonerror,
                        errorvalue=errorvalue)
    
    
class FieldMapView(RowContainer):
    
    def __init__(self, source, mappings=None, failonerror=False, errorvalue=None):
        self.source = source
        if mappings is None:
            self.mappings = OrderedDict()
        else:
            self.mappings = mappings
        self.failonerror = failonerror
        self.errorvalue = errorvalue
        
    def __setitem__(self, key, value):
        self.mappings[key] = value
        
    def __iter__(self):
        return iterfieldmap(self.source, self.mappings, self.failonerror, self.errorvalue)
    
    
def iterfieldmap(source, mappings, failonerror, errorvalue):
    it = iter(source)
    flds = it.next()
    outflds = mappings.keys()
    yield tuple(outflds)
    
    mapfuns = dict()
    for outfld, m in mappings.items():
        if m in flds:
            mapfuns[outfld] = itemgetter(m)
        elif isinstance(m, int) and m < len(flds):
            mapfuns[outfld] = itemgetter(m)
        elif isinstance(m, basestring):
            mapfuns[outfld] = expr(m)
        elif callable(m):
            mapfuns[outfld] = m
        elif isinstance(m, (tuple, list)) and len(m) == 2:
            srcfld = m[0]
            fm = m[1]
            if callable(fm):
                mapfuns[outfld] = composefun(fm, srcfld)
            elif isinstance(fm, dict):
                mapfuns[outfld] = composedict(fm, srcfld)
            else:
                raise Exception('expected callable or dict') # TODO better error
        else:
            raise Exception('invalid mapping', outfld, m) # TODO better error
            
    for row in it:
        rec = asdict(flds, row)
        try:
            # use list comprehension if possible
            outrow = [mapfuns[outfld](rec) for outfld in outflds]
        except:
            # fall back to doing it one field at a time
            outrow = list()
            for outfld in outflds:
                try:
                    val = mapfuns[outfld](rec)
                except:
                    if failonerror:
                        raise
                    else:
                        val = errorvalue
                outrow.append(val)
        yield tuple(outrow)
                
        
def composefun(f, srcfld):
    def g(rec):
        return f(rec[srcfld])
    return g


def composedict(d, srcfld):
    def g(rec):
        k = rec[srcfld]
        if k in d:
            return d[k]
        else:
            return k
    return g


def facet(table, field):
    """
    Return a dictionary mapping field values to tables. 
    
    E.g.::
    
        >>> from petl import facet, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 4     | 9.3   |
        +-------+-------+-------+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+
        | 'b'   | 1     | 23.3  |
        +-------+-------+-------+
        | 'c'   | 8     | 42.0  |
        +-------+-------+-------+
        | 'd'   | 7     | 100.9 |
        +-------+-------+-------+
        | 'c'   | 2     |       |
        +-------+-------+-------+
        
        >>> foo = facet(table1, 'foo')
        >>> foo.keys()
        ['a', 'c', 'b', 'd']
        >>> look(foo['a'])
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 4     | 9.3   |
        +-------+-------+-------+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+
        
        >>> look(foo['c'])
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'c'   | 8     | 42.0  |
        +-------+-------+-------+
        | 'c'   | 2     |       |
        +-------+-------+-------+
        
    See also :func:`facetcolumns`.
    
    """
    
    fct = dict()
    for v in valueset(table, field):
        fct[v] = selecteq(table, field, v)
    return fct


def selectop(table, field, value, op, complement=False):
    """
    Select rows where the function `op` applied to the given field and the given 
    value returns true.
    
    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return fieldselect(table, field, lambda v: op(v, value), complement=complement)


def selecteq(table, field, value, complement=False):
    """
    Select rows where the given field equals the given value.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, operator.eq, complement=complement)


def selectne(table, field, value, complement=False):
    """
    Select rows where the given field does not equal the given value.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, operator.ne, complement=complement)


def selectlt(table, field, value, complement=False):
    """
    Select rows where the given field is less than the given value.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, operator.lt, complement=complement)


def selectle(table, field, value, complement=False):
    """
    Select rows where the given field is less than or equal to the given value.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, operator.le, complement=complement)


def selectgt(table, field, value, complement=False):
    """
    Select rows where the given field is greater than the given value.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, operator.gt, complement=complement)


def selectge(table, field, value, complement=False):
    """
    Select rows where the given field is greater than or equal to the given value.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, operator.ge, complement=complement)


def selectcontains(table, field, value, complement=False):
    """
    Select rows where the given field contains the given value.

    .. versionadded:: 0.10
    
    """
    
    return selectop(table, field, value, operator.contains, complement=complement)


def selectin(table, field, value, complement=False):
    """
    Select rows where the given field is a member of the given value.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return fieldselect(table, field, lambda v: v in value, complement=complement)


def selectnotin(table, field, value, complement=False):
    """
    Select rows where the given field is not a member of the given value.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return fieldselect(table, field, lambda v: v not in value, complement=complement)


def selectis(table, field, value, complement=False):
    """
    Select rows where the given field `is` the given value.
    
    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, operator.is_, complement=complement)


def selectisnot(table, field, value, complement=False):
    """
    Select rows where the given field `is not` the given value.
    
    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, operator.is_not, complement=complement)


def selectisinstance(table, field, value, complement=False):
    """
    Select rows where the given field is an instance of the given type.
    
    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, isinstance, complement=complement)


def selectrangeopenleft(table, field, minv, maxv, complement=False):
    """
    Select rows where the given field is greater than or equal to `minv` and 
    less than `maxv`.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return fieldselect(table, field, lambda v: minv <= v < maxv, complement=complement)


def selectrangeopenright(table, field, minv, maxv, complement=False):
    """
    Select rows where the given field is greater than `minv` and 
    less than or equal to `maxv`.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return fieldselect(table, field, lambda v: minv < v <= maxv, complement=complement)


def selectrangeopen(table, field, minv, maxv, complement=False):
    """
    Select rows where the given field is greater than or equal to `minv` and 
    less than or equal to `maxv`.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return fieldselect(table, field, lambda v: minv <= v <= maxv, complement=complement)


def selectrangeclosed(table, field, minv, maxv, complement=False):
    """
    Select rows where the given field is greater than `minv` and 
    less than `maxv`.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return fieldselect(table, field, lambda v: minv < v < maxv, complement=complement)


def selectre(table, field, pattern, flags=0, complement=False):
    """
    Select rows where a regular expression search using the given pattern on the
    given field returns a match. E.g.::

        >>> from petl import selectre, look    
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'aa'  | 4     | 9.3   |
        +-------+-------+-------+
        | 'aaa' | 2     | 88.2  |
        +-------+-------+-------+
        | 'b'   | 1     | 23.3  |
        +-------+-------+-------+
        | 'ccc' | 8     | 42.0  |
        +-------+-------+-------+
        | 'bb'  | 7     | 100.9 |
        +-------+-------+-------+
        | 'c'   | 2     |       |
        +-------+-------+-------+
        
        >>> table2 = selectre(table1, 'foo', '[ab]{2}')
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'aa'  | 4     | 9.3   |
        +-------+-------+-------+
        | 'aaa' | 2     | 88.2  |
        +-------+-------+-------+
        | 'bb'  | 7     | 100.9 |
        +-------+-------+-------+
        
    See also :func:`re.search`.
    
    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    prog = re.compile(pattern, flags)
    test = lambda v: prog.search(v) is not None
    return fieldselect(table, field, test, complement=complement)


def rowreduce(table, key, reducer, fields=None, missing=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Group rows under the given key then apply `reducer` to produce a single 
    output row for each input group of rows. E.g.::
    
        >>> from petl import rowreduce, look    
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 3     |
        +-------+-------+
        | 'a'   | 7     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'b'   | 1     |
        +-------+-------+
        | 'b'   | 9     |
        +-------+-------+
        | 'c'   | 4     |
        +-------+-------+
        
        >>> def reducer(key, rows):
        ...     return [key, sum(row[1] for row in rows)]
        ... 
        >>> table2 = rowreduce(table1, key='foo', reducer=reducer, fields=['foo', 'barsum'])
        >>> look(table2)
        +-------+----------+
        | 'foo' | 'barsum' |
        +=======+==========+
        | 'a'   | 10       |
        +-------+----------+
        | 'b'   | 12       |
        +-------+----------+
        | 'c'   | 4        |
        +-------+----------+
    
    N.B., this is not strictly a "reduce" in the sense of the standard Python
    :func:`reduce` function, i.e., the `reducer` function is *not* applied 
    recursively to values within a group, rather it is applied once to each row 
    group as a whole.
    
    See also :func:`aggregate` and :func:`fold`.
    
    ..versionchanged:: 0.12
    
    Was previously deprecated, now resurrected as it is a useful function in it's
    own right.
    
    """

    return RowReduceView(table, key, reducer, fields=fields,
                         presorted=presorted, 
                         buffersize=buffersize, tempdir=tempdir, cache=cache)


class RowReduceView(RowContainer):
    
    def __init__(self, source, key, reducer, fields=None, 
                  presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key
        self.fields = fields
        self.reducer = reducer

    def __iter__(self):
        return iterrowreduce(self.source, self.key, self.reducer, self.fields)

    
def iterrowreduce(source, key, reducer, fields):
    if fields is None:
        # output fields from source
        fields, source = iterpeek(source)
    yield tuple(fields)
    for key, rows in rowgroupby(source, key):
        yield tuple(reducer(key, rows))
        

def recordreduce(table, key, reducer, fields=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    .. deprecated:: 0.9
    
    Use :func:`rowreduce` instead.
    
    """

    return rowreduce(table, key, reducer, fields=fields, presorted=presorted, 
                     buffersize=buffersize, tempdir=tempdir, cache=cache)


def mergeduplicates(table, key, missing=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Merge duplicate rows under the given key. E.g.::
    
        >>> from petl import mergeduplicates, look    
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | 2.7   |
        +-------+-------+-------+
        | 'B'   | 2     | None  |
        +-------+-------+-------+
        | 'D'   | 3     | 9.4   |
        +-------+-------+-------+
        | 'B'   | None  | 7.8   |
        +-------+-------+-------+
        | 'E'   | None  | 42.0  |
        +-------+-------+-------+
        | 'D'   | 3     | 12.3  |
        +-------+-------+-------+
        | 'A'   | 2     | None  |
        +-------+-------+-------+
        
        >>> table2 = mergeduplicates(table1, 'foo')
        >>> look(table2)
        +-------+------------------+-----------------------+
        | 'foo' | 'bar'            | 'baz'                 |
        +=======+==================+=======================+
        | 'A'   | Conflict([1, 2]) | 2.7                   |
        +-------+------------------+-----------------------+
        | 'B'   | 2                | 7.8                   |
        +-------+------------------+-----------------------+
        | 'D'   | 3                | Conflict([9.4, 12.3]) |
        +-------+------------------+-----------------------+
        | 'E'   | None             | 42.0                  |
        +-------+------------------+-----------------------+
        
    Missing values are overridden by non-missing values. Conflicting values are
    reported as an instance of the Conflict class (sub-class of frozenset).
    
    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    .. versionchanged:: 0.3
    
    Previously conflicts were reported as a list, this is changed to a tuple in 
    version 0.3.
    
    .. versionchanged:: 0.10
    
    Renamed from 'mergereduce' to 'mergeduplicates'. Conflicts now reported as
    instance of Conflict.
    
    """

    return MergeDuplicatesView(table, key, missing=missing, presorted=presorted,
                               buffersize=buffersize, tempdir=tempdir, cache=cache)


class MergeDuplicatesView(RowContainer):
    
    def __init__(self, table, key, missing=None, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.table = table
        else:
            self.table = sort(table, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key
        self.missing = missing
        
    def __iter__(self):
        return itermergeduplicates(self.table, self.key, self.missing)
    
    
def itermergeduplicates(table, key, missing):
    it = iter(table)
    fields, it = iterpeek(it)
    
    # determine output fields
    if isinstance(key, basestring):
        outflds = [key]
        keyflds = set([key])
    else:
        outflds = list(key)
        keyflds = set(key)
    valflds = [f for f in fields if f not in keyflds]
    valfldidxs = [fields.index(f) for f in valflds]
    outflds.extend(valflds)
    yield tuple(outflds)

    # do the work
    for k, grp in rowgroupby(it, key):
        grp = list(grp)
        if isinstance(key, basestring):
            outrow = [k]
        else:
            outrow = list(k)
        mergedvals = [set(row[i] for row in grp if len(row) > i and row[i] != missing) for i in valfldidxs]
        normedvals = [vals.pop() if len(vals) == 1 else missing if len(vals) == 0 else Conflict(vals) for vals in mergedvals]
        outrow.extend(normedvals)
        yield tuple(outrow)


mergereduce = mergeduplicates # for backwards compatibility


class Conflict(frozenset):
    
    def __new__(cls, items):
        s = super(Conflict, cls).__new__(cls, items)
        return s
    

def aggregate(table, key, aggregation=None, value=None, presorted=False,
              buffersize=None, tempdir=None, cache=True):
    """
    Group rows under the given key then apply aggregation functions. E.g.::
    
        >>> from petl import aggregate, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   |     3 |  True |
        +-------+-------+-------+
        | 'a'   |     7 | False |
        +-------+-------+-------+
        | 'b'   |     2 |  True |
        +-------+-------+-------+
        | 'b'   |     2 | False |
        +-------+-------+-------+
        | 'b'   |     9 | False |
        +-------+-------+-------+
        | 'c'   |     4 |  True |
        +-------+-------+-------+
        
        >>> # aggregate whole rows
        ... table2 = aggregate(table1, 'foo', len)
        >>> look(table2)
        +-------+---------+
        | 'key' | 'value' |
        +=======+=========+
        | 'a'   |       2 |
        +-------+---------+
        | 'b'   |       3 |
        +-------+---------+
        | 'c'   |       1 |
        +-------+---------+
        
        >>> # aggregate single field
        ... table3 = aggregate(table1, 'foo', sum, 'bar')
        >>> look(table3)
        +-------+---------+
        | 'key' | 'value' |
        +=======+=========+
        | 'a'   |      10 |
        +-------+---------+
        | 'b'   |      13 |
        +-------+---------+
        | 'c'   |       4 |
        +-------+---------+
        
        >>> # alternative signature for single field aggregation using keyword args
        ... table4 = aggregate(table1, key=('foo', 'bar'), aggregation=list, value=('bar', 'baz'))
        >>> look(table4)
        +----------+-------------------------+
        | 'key'    | 'value'                 |
        +==========+=========================+
        | ('a', 3) | [(3, True)]             |
        +----------+-------------------------+
        | ('a', 7) | [(7, False)]            |
        +----------+-------------------------+
        | ('b', 2) | [(2, True), (2, False)] |
        +----------+-------------------------+
        | ('b', 9) | [(9, False)]            |
        +----------+-------------------------+
        | ('c', 4) | [(4, True)]             |
        +----------+-------------------------+
        
        >>> # aggregate multiple fields
        ... from collections import OrderedDict
        >>> from petl import strjoin
        >>> aggregation = OrderedDict()
        >>> aggregation['count'] = len
        >>> aggregation['minbar'] = 'bar', min
        >>> aggregation['maxbar'] = 'bar', max
        >>> aggregation['sumbar'] = 'bar', sum
        >>> aggregation['listbar'] = 'bar' # default aggregation function is list
        >>> aggregation['listbarbaz'] = ('bar', 'baz'), list
        >>> aggregation['bars'] = 'bar', strjoin(', ')
        >>> table5 = aggregate(table1, 'foo', aggregation)
        >>> look(table5)
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        | 'key' | 'count' | 'minbar' | 'maxbar' | 'sumbar' | 'listbar' | 'listbarbaz'                        | 'bars'    |
        +=======+=========+==========+==========+==========+===========+=====================================+===========+
        | 'a'   |       2 |        3 |        7 |       10 | [3, 7]    | [(3, True), (7, False)]             | '3, 7'    |
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        | 'b'   |       3 |        2 |        9 |       13 | [2, 2, 9] | [(2, True), (2, False), (9, False)] | '2, 2, 9' |
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        | 'c'   |       1 |        4 |        4 |        4 | [4]       | [(4, True)]                         | '4'       |
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        
        >>> # can also use list or tuple to specify multiple field aggregation
        ... aggregation = [('count', len),
        ...                ('minbar', 'bar', min),
        ...                ('maxbar', 'bar', max),
        ...                ('sumbar', 'bar', sum),
        ...                ('listbar', 'bar'), # default aggregation function is list
        ...                ('listbarbaz', ('bar', 'baz'), list),
        ...                ('bars', 'bar', strjoin(', '))]
        >>> table6 = aggregate(table1, 'foo', aggregation)
        >>> look(table6)
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        | 'key' | 'count' | 'minbar' | 'maxbar' | 'sumbar' | 'listbar' | 'listbarbaz'                        | 'bars'    |
        +=======+=========+==========+==========+==========+===========+=====================================+===========+
        | 'a'   |       2 |        3 |        7 |       10 | [3, 7]    | [(3, True), (7, False)]             | '3, 7'    |
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        | 'b'   |       3 |        2 |        9 |       13 | [2, 2, 9] | [(2, True), (2, False), (9, False)] | '2, 2, 9' |
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        | 'c'   |       1 |        4 |        4 |        4 | [4]       | [(4, True)]                         | '4'       |
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        
        >>> # can also use suffix notation
        ... table7 = aggregate(table1, 'foo')
        >>> table7['count'] = len
        >>> table7['minbar'] = 'bar', min
        >>> table7['maxbar'] = 'bar', max
        >>> table7['sumbar'] = 'bar', sum
        >>> table7['listbar'] = 'bar' # default aggregation function is list
        >>> table7['listbarbaz'] = ('bar', 'baz'), list
        >>> table7['bars'] = 'bar', strjoin(', ')
        >>> look(table7)
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        | 'key' | 'count' | 'minbar' | 'maxbar' | 'sumbar' | 'listbar' | 'listbarbaz'                        | 'bars'    |
        +=======+=========+==========+==========+==========+===========+=====================================+===========+
        | 'a'   |       2 |        3 |        7 |       10 | [3, 7]    | [(3, True), (7, False)]             | '3, 7'    |
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        | 'b'   |       3 |        2 |        9 |       13 | [2, 2, 9] | [(2, True), (2, False), (9, False)] | '2, 2, 9' |
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+
        | 'c'   |       1 |        4 |        4 |        4 | [4]       | [(4, True)]                         | '4'       |
        +-------+---------+----------+----------+----------+-----------+-------------------------------------+-----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    """

    if callable(aggregation):
        return SimpleAggregateView(table, key, aggregation=aggregation, value=value, 
                                   presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)
    elif aggregation is None or isinstance(aggregation, (list, tuple, dict)):
        # ignore value arg
        return MultiAggregateView(table, key, aggregation=aggregation,  
                                  presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)
    else:
        raise Exception('expected aggregation is callable, list, tuple, dict or None')


class SimpleAggregateView(RowContainer):
    
    def __init__(self, table, key, aggregation=list, value=None, presorted=False,
                 buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.table = table
        else:
            self.table = sort(table, key, buffersize=buffersize, tempdir=tempdir, cache=cache)    
        self.key = key
        self.aggregation = aggregation
        self.value = value
        
    def __iter__(self):
        return itersimpleaggregate(self.table, self.key, self.aggregation, self.value)


def itersimpleaggregate(table, key, aggregation, value):
    if aggregation == len:
        aggregation = lambda grp: sum(1 for _ in grp) # count length of iterable
    yield ('key', 'value')
    for k, grp in rowgroupby(table, key, value):
        yield k, aggregation(grp)


class MultiAggregateView(RowContainer):
    
    def __init__(self, source, key, aggregation=None, presorted=False, 
                 buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key
        if aggregation is None:
            self.aggregation = OrderedDict()
        elif isinstance(aggregation, (list, tuple)):
            self.aggregation = OrderedDict()
            for t in aggregation:
                self.aggregation[t[0]] = t[1:]
        elif isinstance(aggregation, dict):
            self.aggregation = aggregation
        else:
            raise Exception('expected aggregation is None, list, tuple or dict')

    def __iter__(self):
        return itermultiaggregate(self.source, self.key, self.aggregation)
    
    def __setitem__(self, key, value):
        self.aggregation[key] = value

    
def itermultiaggregate(source, key, aggregation):
    aggregation = OrderedDict(aggregation.items()) # take a copy
    it = iter(source)
    srcflds = it.next()
    it = chain([srcflds], it) # push back header to ensure we iterate only once

    # normalise aggregators
    for outfld in aggregation:
        agg = aggregation[outfld]
        if callable(agg):
            aggregation[outfld] = None, agg
        elif isinstance(agg, basestring):
            aggregation[outfld] = agg, list # list is default
        elif len(agg) == 1 and isinstance(agg[0], basestring):
            aggregation[outfld] = agg[0], list # list is default 
        elif len(agg) == 1 and callable(agg[0]):
            aggregation[outfld] = None, agg[0] # aggregate whole rows
        elif len(agg) == 2:
            pass # no need to normalise
        else:
            raise Exception('invalid aggregation: %r, %r' % (outfld, agg))
        
    outflds = ['key']
    for outfld in aggregation:
        outflds.append(outfld)
    yield tuple(outflds)
    
    for k, rows in rowgroupby(it, key):
        rows = list(rows) # may need to iterate over these more than once
        outrow = [k]
        for outfld in aggregation:
            srcfld, aggfun = aggregation[outfld]
            if srcfld is None:
                aggval = aggfun(rows)
                outrow.append(aggval)
            elif isinstance(srcfld, (list, tuple)):
                idxs = [srcflds.index(f) for f in srcfld]
                valgetter = itemgetter(*idxs)
                vals = (valgetter(row) for row in rows)
                aggval = aggfun(vals)
                outrow.append(aggval)
            else:
                idx = srcflds.index(srcfld)
                # try using generator comprehension
                vals = (row[idx] for row in rows)
                aggval = aggfun(vals)
                outrow.append(aggval)
        yield tuple(outrow)
            

def rangerowreduce(table, key, width, reducer, fields=None, minv=None, maxv=None, 
                     presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Group rows into bins of a given `width` under the given numeric key then 
    apply `reducer` to produce a single output row for each input group of rows. 
    E.g.::
    
        >>> from petl import rangerowreduce, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 3     |
        +-------+-------+
        | 'a'   | 7     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'b'   | 1     |
        +-------+-------+
        | 'b'   | 9     |
        +-------+-------+
        | 'c'   | 4     |
        +-------+-------+
        
        >>> def reducer(key, rows):
        ...     return [key[0], key[1], ''.join(row[0] for row in rows)]
        ... 
        >>> table2 = rangerowreduce(table1, 'bar', 2, reducer=reducer, fields=['frombar', 'tobar', 'foos'])
        >>> look(table2)
        +-----------+---------+--------+
        | 'frombar' | 'tobar' | 'foos' |
        +===========+=========+========+
        | 1         | 3       | 'bb'   |
        +-----------+---------+--------+
        | 3         | 5       | 'ac'   |
        +-----------+---------+--------+
        | 5         | 7       | ''     |
        +-----------+---------+--------+
        | 7         | 9       | 'a'    |
        +-----------+---------+--------+
        | 9         | 11      | 'b'    |
        +-----------+---------+--------+

    N.B., this is not strictly a "reduce" in the sense of the standard Python
    :func:`reduce` function, i.e., the `reducer` function is *not* applied 
    recursively to values within a group, rather it is applied once to each row 
    group as a whole.
    
    See also :func:`rangeaggregate` and :func:`rangecounts`.
    
    ..versionchanged:: 0.12
    
    Was previously deprecated, now resurrected as it is a useful function in it's
    own right.
    
    """
    
    return RangeRowReduceView(table, key, width, reducer, fields=fields, minv=minv, 
                              maxv=maxv, presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)
        

class RangeRowReduceView(RowContainer):
    
    def __init__(self, source, key, width, reducer, fields=None, minv=None, maxv=None, 
                  presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key
        self.width = width
        self.reducer = reducer
        self.fields = fields
        self.minv, self.maxv = minv, maxv

    def __iter__(self):
        return iterrangerowreduce(self.source, self.key, self.width, self.reducer,
                                  self.fields, self.minv, self.maxv)


def iterrangerowreduce(table, key, width, reducer, fields, minv, maxv):
    yield tuple(fields)
    for k, grp in rowgroupbybin(table, key, width, minv=minv, maxv=maxv):
        yield tuple(reducer(k, grp))
    
        
def rangerecordreduce(table, key, width, reducer, fields=None, minv=None, maxv=None, 
                      failonerror=False, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Reduce records grouped into bins under the given key via an arbitrary function. 

    .. deprecated:: 0.9
    
    Use :func:`rangeaggregate` instead.
    
    """
    
    return rangerowreduce(table, key, width, reducer, fields=fields, minv=minv, 
                                 maxv=maxv, failonerror=failonerror, 
                                 presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)
        

def rangecounts(table, key, width, minv=None, maxv=None, presorted=False,
                buffersize=None, tempdir=None, cache=True):
    """
    Group rows into bins then count the number of rows in each bin. E.g.::

        >>> from petl import rangecounts, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 3     |
        +-------+-------+
        | 'a'   | 7     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'b'   | 1     |
        +-------+-------+
        | 'b'   | 9     |
        +-------+-------+
        | 'c'   | 4     |
        +-------+-------+
        | 'd'   | 3     |
        +-------+-------+
        
        >>> table2 = rangecounts(table1, 'bar', 2)
        >>> look(table2)
        +---------+---------+
        | 'key'   | 'value' |
        +=========+=========+
        | (1, 3)  | 2       |
        +---------+---------+
        | (3, 5)  | 3       |
        +---------+---------+
        | (5, 7)  | 0       |
        +---------+---------+
        | (7, 9)  | 1       |
        +---------+---------+
        | (9, 11) | 1       |
        +---------+---------+

    See also :func:`rangeaggregate`.
    
    """
    
    return rangeaggregate(table, key, width, len, minv=minv, maxv=maxv,
                          presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)
    
    
###############################################################################
###############################################################################
## rangeaggregate
###############################################################################
###############################################################################    
def rangeaggregate(table, key, width, aggregation=None, value=None, minv=None, 
                   maxv=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Group rows into bins then apply aggregation functions. E.g.::
    
        >>> from petl import rangeaggregate, look, strjoin
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 3     |
        +-------+-------+
        | 'a'   | 7     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'b'   | 1     |
        +-------+-------+
        | 'b'   | 9     |
        +-------+-------+
        | 'c'   | 4     |
        +-------+-------+
        | 'd'   | 3     |
        +-------+-------+
        
        >>> # aggregate whole rows
        ... table2 = rangeaggregate(table1, 'bar', 2, len)
        >>> look(table2)
        +---------+---------+
        | 'key'   | 'value' |
        +=========+=========+
        | (1, 3)  | 2       |
        +---------+---------+
        | (3, 5)  | 3       |
        +---------+---------+
        | (5, 7)  | 0       |
        +---------+---------+
        | (7, 9)  | 1       |
        +---------+---------+
        | (9, 11) | 1       |
        +---------+---------+
        
        >>> # aggregate single field
        ... table3 = rangeaggregate(table1, 'bar', 2, list, 'foo')
        >>> look(table3)
        +---------+-----------------+
        | 'key'   | 'value'         |
        +=========+=================+
        | (1, 3)  | ['b', 'b']      |
        +---------+-----------------+
        | (3, 5)  | ['a', 'd', 'c'] |
        +---------+-----------------+
        | (5, 7)  | []              |
        +---------+-----------------+
        | (7, 9)  | ['a']           |
        +---------+-----------------+
        | (9, 11) | ['b']           |
        +---------+-----------------+
        
        >>> # aggregate single field - alternative signature using keyword args
        ... table4 = rangeaggregate(table1, key='bar', width=2, aggregation=list, value='foo')
        >>> look(table4)
        +---------+-----------------+
        | 'key'   | 'value'         |
        +=========+=================+
        | (1, 3)  | ['b', 'b']      |
        +---------+-----------------+
        | (3, 5)  | ['a', 'd', 'c'] |
        +---------+-----------------+
        | (5, 7)  | []              |
        +---------+-----------------+
        | (7, 9)  | ['a']           |
        +---------+-----------------+
        | (9, 11) | ['b']           |
        +---------+-----------------+
        
        >>> # aggregate multiple fields
        ... from collections import OrderedDict
        >>> aggregation = OrderedDict()
        >>> aggregation['foocount'] = len 
        >>> aggregation['foojoin'] = 'foo', strjoin('')
        >>> aggregation['foolist'] = 'foo' # default is list
        >>> table5 = rangeaggregate(table1, 'bar', 2, aggregation)
        >>> look(table5)
        +---------+------------+-----------+-----------------+
        | 'key'   | 'foocount' | 'foojoin' | 'foolist'       |
        +=========+============+===========+=================+
        | (1, 3)  | 2          | 'bb'      | ['b', 'b']      |
        +---------+------------+-----------+-----------------+
        | (3, 5)  | 3          | 'adc'     | ['a', 'd', 'c'] |
        +---------+------------+-----------+-----------------+
        | (5, 7)  | 0          | ''        | []              |
        +---------+------------+-----------+-----------------+
        | (7, 9)  | 1          | 'a'       | ['a']           |
        +---------+------------+-----------+-----------------+
        | (9, 11) | 1          | 'b'       | ['b']           |
        +---------+------------+-----------+-----------------+
        
    .. versionchanged:: 0.12
    
    Changed signature to simplify and make consistent with :func:`aggregate`.
    
    """

    if callable(aggregation):
        return SimpleRangeAggregateView(table, key, width, 
                                        aggregation=aggregation, 
                                        value=value, minv=minv, maxv=maxv,
                                        presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)
    elif aggregation is None or isinstance(aggregation, (list, tuple, dict)):
        # ignore value arg
        return MultiRangeAggregateView(table, key, width, 
                                       aggregation=aggregation, 
                                       minv=minv, maxv=maxv, # ignore value
                                       presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)
    else:
        raise Exception('expected aggregation is callable, list, tuple, dict or None')
    
    
class SimpleRangeAggregateView(RowContainer):
    
    def __init__(self, table, key, width, aggregation=list, value=None, 
                 minv=None, maxv=None, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.table = table
        else:
            self.table = sort(table, key, buffersize=buffersize, tempdir=tempdir, cache=cache)    
        self.key = key
        self.width = width
        self.aggregation = aggregation
        self.value = value
        self.minv, self.maxv = minv, maxv
        
    def __iter__(self):
        return itersimplerangeaggregate(self.table, self.key, self.width, 
                                        self.aggregation, self.value, self.minv, 
                                        self.maxv)


def itersimplerangeaggregate(table, key, width, aggregation, value, minv, maxv):
    if aggregation == len:
        aggregation = lambda grp: sum(1 for _ in grp) # count length of iterable
    yield ('key', 'value')
    for k, grp in rowgroupbybin(table, key, width, value=value, minv=minv, maxv=maxv):
        yield k, aggregation(grp)


class MultiRangeAggregateView(RowContainer):
    
    def __init__(self, source, key, width, aggregation=None, 
                  minv=None, maxv=None, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key
        self.width = width
        if aggregation is None:
            self.aggregation = OrderedDict()
        elif isinstance(aggregation, (list, tuple)):
            self.aggregation = OrderedDict()
            for t in aggregation:
                self.aggregation[t[0]] = t[1:]
        elif isinstance(aggregation, dict):
            self.aggregation = aggregation
        else:
            raise Exception('expected aggregation is None, list, tuple or dict')
        self.minv, self.maxv = minv, maxv

    def __iter__(self):
        return itermultirangeaggregate(self.source, self.key, self.width, 
                                       self.aggregation, self.minv, self.maxv)
    
    def __setitem__(self, key, value):
        self.aggregation[key] = value

    
def itermultirangeaggregate(source, key, width, aggregation, minv, maxv):
    aggregation = OrderedDict(aggregation.items()) # take a copy
    it = iter(source)
    srcflds = it.next()
    it = chain([srcflds], it) # push back header to ensure we iterate only once

    # normalise aggregators
    for outfld in aggregation:
        agg = aggregation[outfld]
        if callable(agg):
            aggregation[outfld] = None, agg
        elif isinstance(agg, basestring):
            aggregation[outfld] = agg, list # list is default
        elif len(agg) == 1 and isinstance(agg[0], basestring):
            aggregation[outfld] = agg[0], list # list is default 
        elif len(agg) == 1 and callable(agg[0]):
            aggregation[outfld] = None, agg[0] # aggregate whole rows
        elif len(agg) == 2:
            pass # no need to normalise
        else:
            raise Exception('invalid aggregation: %r, %r' % (outfld, agg))
        
    outflds = ['key']
    for outfld in aggregation:
        outflds.append(outfld)
    yield tuple(outflds)
    
    for k, rows in rowgroupbybin(it, key, width, minv=minv, maxv=maxv):
#        rows = list(rows) # not strictly necessary as rowgroupbybin returns a list already
        outrow = [k]
        for outfld in aggregation:
            srcfld, aggfun = aggregation[outfld]
            if srcfld is None:
                aggval = aggfun(rows)
                outrow.append(aggval)
            else:
                idx = srcflds.index(srcfld)
                # try using generator comprehension
                vals = (row[idx] for row in rows)
                aggval = aggfun(vals)
                outrow.append(aggval)
        yield tuple(outrow)
            
    
###############################################################################
###############################################################################
## rowmap
###############################################################################
###############################################################################    
def rowmap(table, rowmapper, fields, failonerror=False, missing=None):
    """
    Transform rows via an arbitrary function. E.g.::

        >>> from petl import rowmap, look
        >>> look(table1)
        +------+----------+-------+----------+----------+
        | 'id' | 'sex'    | 'age' | 'height' | 'weight' |
        +======+==========+=======+==========+==========+
        | 1    | 'male'   | 16    | 1.45     | 62.0     |
        +------+----------+-------+----------+----------+
        | 2    | 'female' | 19    | 1.34     | 55.4     |
        +------+----------+-------+----------+----------+
        | 3    | 'female' | 17    | 1.78     | 74.4     |
        +------+----------+-------+----------+----------+
        | 4    | 'male'   | 21    | 1.33     | 45.2     |
        +------+----------+-------+----------+----------+
        | 5    | '-'      | 25    | 1.65     | 51.9     |
        +------+----------+-------+----------+----------+
        
        >>> def rowmapper(row):
        ...     transmf = {'male': 'M', 'female': 'F'}
        ...     return [row[0],
        ...             transmf[row[1]] if row[1] in transmf else row[1],
        ...             row[2] * 12,
        ...             row[4] / row[3] ** 2]
        ... 
        >>> table2 = rowmap(table1, rowmapper, fields=['subject_id', 'gender', 'age_months', 'bmi'])  
        >>> look(table2)    
        +--------------+----------+--------------+--------------------+
        | 'subject_id' | 'gender' | 'age_months' | 'bmi'              |
        +==============+==========+==============+====================+
        | 1            | 'M'      | 192          | 29.48870392390012  |
        +--------------+----------+--------------+--------------------+
        | 2            | 'F'      | 228          | 30.8531967030519   |
        +--------------+----------+--------------+--------------------+
        | 3            | 'F'      | 204          | 23.481883600555488 |
        +--------------+----------+--------------+--------------------+
        | 4            | 'M'      | 252          | 25.55260331279326  |
        +--------------+----------+--------------+--------------------+
        | 5            | '-'      | 300          | 19.0633608815427   |
        +--------------+----------+--------------+--------------------+

    The `rowmapper` function should return a single row (list or tuple).
    
    .. versionchanged:: 0.9
    
    Hybrid row objects supporting data value access by either position or by 
    field name are now passed to the `rowmapper` function.
    
    """
    
    return RowMapView(table, rowmapper, fields, failonerror=failonerror,
                      missing=missing)
    
    
class RowMapView(RowContainer):
    
    def __init__(self, source, rowmapper, fields, failonerror=False, missing=None):
        self.source = source
        self.rowmapper = rowmapper
        self.fields = fields
        self.failonerror = failonerror
        self.missing = missing
        
    def __iter__(self):
        return iterrowmap(self.source, self.rowmapper, self.fields, self.failonerror,
                          self.missing)

    
def iterrowmap(source, rowmapper, fields, failonerror, missing):
    it = iter(source)
    srcflds = it.next() 
    yield tuple(fields)
    for row in hybridrows(srcflds, it, missing):
        try:
            outrow = rowmapper(row)
            yield tuple(outrow)
        except:
            if failonerror:
                raise
        
        
def recordmap(table, recmapper, fields, failonerror=False):
    """
    Transform records via an arbitrary function. 
    
    .. deprecated:: 0.9
    
    Use :func:`rowmap` insteand.
    
    """
    
    return rowmap(table, recmapper, fields, failonerror=failonerror)
    
    
def rowmapmany(table, rowgenerator, fields, failonerror=False, missing=None):
    """
    Map each input row to any number of output rows via an arbitrary function.
    E.g.::

        >>> from petl import rowmapmany, look    
        >>> look(table1)
        +------+----------+-------+----------+----------+
        | 'id' | 'sex'    | 'age' | 'height' | 'weight' |
        +======+==========+=======+==========+==========+
        | 1    | 'male'   | 16    | 1.45     | 62.0     |
        +------+----------+-------+----------+----------+
        | 2    | 'female' | 19    | 1.34     | 55.4     |
        +------+----------+-------+----------+----------+
        | 3    | '-'      | 17    | 1.78     | 74.4     |
        +------+----------+-------+----------+----------+
        | 4    | 'male'   | 21    | 1.33     |          |
        +------+----------+-------+----------+----------+
        
        >>> def rowgenerator(row):
        ...     transmf = {'male': 'M', 'female': 'F'}
        ...     yield [row[0], 'gender', transmf[row[1]] if row[1] in transmf else row[1]]
        ...     yield [row[0], 'age_months', row[2] * 12]
        ...     yield [row[0], 'bmi', row[4] / row[3] ** 2]
        ... 
        >>> table2 = rowmapmany(table1, rowgenerator, fields=['subject_id', 'variable', 'value'])  
        >>> look(table2)
        +--------------+--------------+--------------------+
        | 'subject_id' | 'variable'   | 'value'            |
        +==============+==============+====================+
        | 1            | 'gender'     | 'M'                |
        +--------------+--------------+--------------------+
        | 1            | 'age_months' | 192                |
        +--------------+--------------+--------------------+
        | 1            | 'bmi'        | 29.48870392390012  |
        +--------------+--------------+--------------------+
        | 2            | 'gender'     | 'F'                |
        +--------------+--------------+--------------------+
        | 2            | 'age_months' | 228                |
        +--------------+--------------+--------------------+
        | 2            | 'bmi'        | 30.8531967030519   |
        +--------------+--------------+--------------------+
        | 3            | 'gender'     | '-'                |
        +--------------+--------------+--------------------+
        | 3            | 'age_months' | 204                |
        +--------------+--------------+--------------------+
        | 3            | 'bmi'        | 23.481883600555488 |
        +--------------+--------------+--------------------+
        | 4            | 'gender'     | 'M'                |
        +--------------+--------------+--------------------+

    The `rowgenerator` function should yield zero or more rows (lists or tuples).
    
    See also the :func:`melt` function.
    
    .. versionchanged:: 0.9
    
    Hybrid row objects supporting data value access by either position or by 
    field name are now passed to the `rowgenerator` function.
    
    """
    
    return RowMapManyView(table, rowgenerator, fields, failonerror=failonerror,
                          missing=missing)
    
    
class RowMapManyView(RowContainer):
    
    def __init__(self, source, rowgenerator, fields, failonerror=False, missing=None):
        self.source = source
        self.rowgenerator = rowgenerator
        self.fields = fields
        self.failonerror = failonerror
        self.missing = missing
        
    def __iter__(self):
        return iterrowmapmany(self.source, self.rowgenerator, self.fields, 
                              self.failonerror, self.missing)
    
    
def iterrowmapmany(source, rowgenerator, fields, failonerror, missing):
    it = iter(source)
    srcflds = it.next() 
    yield tuple(fields)
    for row in hybridrows(srcflds, it, missing):
        try:
            for outrow in rowgenerator(row):
                yield tuple(outrow)
        except:
            if failonerror:
                raise
        
        
def recordmapmany(table, rowgenerator, fields, failonerror=False):
    """
    Map each input row (as a record) to any number of output rows via an 
    arbitrary function. 
    
    .. deprecated:: 0.9
    
    Use :func:`rowmapmany` instead.

    """
    
    return rowmapmany(table, rowgenerator, fields, failonerror=failonerror)
    
    
def setheader(table, fields):
    """
    Override fields in the given table. E.g.::
    
        >>> from petl import setheader, look    
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        
        >>> table2 = setheader(table1, ['foofoo', 'barbar'])
        >>> look(table2)
        +----------+----------+
        | 'foofoo' | 'barbar' |
        +==========+==========+
        | 'a'      | 1        |
        +----------+----------+
        | 'b'      | 2        |
        +----------+----------+

    See also :func:`extendheader`, :func:`pushheader`.
    
    """
    
    return SetHeaderView(table, fields) 


class SetHeaderView(RowContainer):
    
    def __init__(self, source, fields):
        self.source = source
        self.fields = fields
        
    def __iter__(self):
        return itersetheader(self.source, self.fields)   


def itersetheader(source, fields):
    it = iter(source)
    it.next() # discard source fields
    yield tuple(fields)
    for row in it:
        yield tuple(row)
        
        
def extendheader(table, fields):
    """
    Extend fields in the given table. E.g.::
    
        >>> from petl import extendheader, look
        >>> look(table1)
        +-------+---+-------+
        | 'foo' |   |       |
        +=======+===+=======+
        | 'a'   | 1 | True  |
        +-------+---+-------+
        | 'b'   | 2 | False |
        +-------+---+-------+
        
        >>> table2 = extendheader(table1, ['bar', 'baz'])
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 1     | True  |
        +-------+-------+-------+
        | 'b'   | 2     | False |
        +-------+-------+-------+

    See also :func:`setheader`, :func:`pushheader`.
    """
    
    return ExtendHeaderView(table, fields) 


class ExtendHeaderView(RowContainer):
    
    def __init__(self, source, fields):
        self.source = source
        self.fields = fields
        
    def __iter__(self):
        return iterextendheader(self.source, self.fields)   


def iterextendheader(source, fields):
    it = iter(source)
    srcflds = it.next() 
    outflds = list(srcflds)
    outflds.extend(fields)
    yield tuple(outflds)
    for row in it:
        yield tuple(row)
        
        
def pushheader(table, fields):
    """
    Push rows down and prepend a header row. E.g.::

        >>> from petl import pushheader, look    
        >>> look(table1)
        +-----+---+
        | 'a' | 1 |
        +=====+===+
        | 'b' | 2 |
        +-----+---+
        
        >>> table2 = pushheader(table1, ['foo', 'bar'])
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+

    Useful, e.g., where data are from a CSV file that has not included a header
    row.
    
    """ 

    return PushHeaderView(table, fields)


class PushHeaderView(RowContainer):
    
    def __init__(self, source, fields):
        self.source = source
        self.fields = fields
        
    def __iter__(self):
        return iterpushheader(self.source, self.fields)   


def iterpushheader(source, fields):
    it = iter(source)
    yield tuple(fields)
    for row in it:
        yield tuple(row)
        
    
def skip(table, n):
    """
    Skip `n` rows (including the header row). 
    
    E.g.::
    
        >>> from petl import skip, look
        >>> look(table1)
        +--------+-------+-------+
        | '#aaa' | 'bbb' | 'ccc' |
        +========+=======+=======+
        | '#mmm' |       |       |
        +--------+-------+-------+
        | 'foo'  | 'bar' |       |
        +--------+-------+-------+
        | 'a'    | 1     |       |
        +--------+-------+-------+
        | 'b'    | 2     |       |
        +--------+-------+-------+
        
        >>> table2 = skip(table1, 2)
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
    
    See also :func:`skipcomments`.
    
    """ 

    return SkipView(table, n)


class SkipView(RowContainer):
    
    def __init__(self, source, n):
        self.source = source
        self.n = n
        
    def __iter__(self):
        return iterskip(self.source, self.n)   


def iterskip(source, n):
    return islice(source, n, None)
        
    
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
        
    
def unpack(table, field, newfields=None, maxunpack=None, include_original=False):
    """
    Unpack data values that are lists or tuples. E.g.::
    
        >>> from petl import unpack, look    
        >>> look(table1)
        +-------+------------+
        | 'foo' | 'bar'      |
        +=======+============+
        | 1     | ['a', 'b'] |
        +-------+------------+
        | 2     | ['c', 'd'] |
        +-------+------------+
        | 3     | ['e', 'f'] |
        +-------+------------+
        
        >>> table2 = unpack(table1, 'bar', ['baz', 'quux'])
        >>> look(table2)
        +-------+-------+--------+
        | 'foo' | 'baz' | 'quux' |
        +=======+=======+========+
        | 1     | 'a'   | 'b'    |
        +-------+-------+--------+
        | 2     | 'c'   | 'd'    |
        +-------+-------+--------+
        | 3     | 'e'   | 'f'    |
        +-------+-------+--------+
    
    See also :func:`unpackdict`.
    
    """
    
    return UnpackView(table, field, newfields, maxunpack, include_original)


class UnpackView(RowContainer):
    
    def __init__(self, source, field, newfields=None, maxunpack=None, 
                 include_original=False):
        self.source = source
        self.field = field
        self.newfields = newfields
        self.maxunpack = maxunpack
        self.include_original = include_original
        
    def __iter__(self):
        return iterunpack(self.source, self.field, self.newfields, self.maxunpack, 
                          self.include_original)


def iterunpack(source, field, newfields, maxv, include_original):
    it = iter(source)

    flds = it.next()
    if field in flds:
        field_index = flds.index(field)
    elif isinstance(field, int) and field < len(flds):
        field_index = field
    else:
        raise Exception('field invalid: must be either field name or index')
    
    # determine output fields
    out_flds = list(flds)
    if not include_original:
        out_flds.remove(field)
    if newfields:   
        out_flds.extend(newfields)
    yield tuple(out_flds)
    
    # construct the output data
    for row in it:
        value = row[field_index]
        if include_original:
            out_row = list(row)
        else:
            out_row = [v for i, v in enumerate(row) if i != field_index]
        out_row.extend(value[:maxv])
        yield tuple(out_row)
        
        
def join(left, right, key=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Perform an equi-join on the given tables. E.g.::
        
        >>> from petl import join, look    
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+
        
        >>> look(table2)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 3    | 'square'  |
        +------+-----------+
        | 4    | 'ellipse' |
        +------+-----------+
        
        >>> table3 = join(table1, table2, key='id')
        >>> look(table3)
        +------+----------+----------+
        | 'id' | 'colour' | 'shape'  |
        +======+==========+==========+
        | 1    | 'blue'   | 'circle' |
        +------+----------+----------+
        | 3    | 'purple' | 'square' |
        +------+----------+----------+
        
        >>> # if no key is given, a natural join is tried
        ... table4 = join(table1, table2)
        >>> look(table4)
        +------+----------+----------+
        | 'id' | 'colour' | 'shape'  |
        +======+==========+==========+
        | 1    | 'blue'   | 'circle' |
        +------+----------+----------+
        | 3    | 'purple' | 'square' |
        +------+----------+----------+
        
        >>> # note behaviour if the key is not unique in either or both tables
        ... look(table5)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 1    | 'red'    |
        +------+----------+
        | 2    | 'purple' |
        +------+----------+
        
        >>> look(table6)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 1    | 'square'  |
        +------+-----------+
        | 2    | 'ellipse' |
        +------+-----------+
        
        >>> table7 = join(table5, table6, key='id')
        >>> look(table7)
        +------+----------+-----------+
        | 'id' | 'colour' | 'shape'   |
        +======+==========+===========+
        | 1    | 'blue'   | 'circle'  |
        +------+----------+-----------+
        | 1    | 'blue'   | 'square'  |
        +------+----------+-----------+
        | 1    | 'red'    | 'circle'  |
        +------+----------+-----------+
        | 1    | 'red'    | 'square'  |
        +------+----------+-----------+
        | 2    | 'purple' | 'ellipse' |
        +------+----------+-----------+
        
        >>> # compound keys are supported
        ... look(table8)
        +------+--------+----------+
        | 'id' | 'time' | 'height' |
        +======+========+==========+
        | 1    | 1      | 12.3     |
        +------+--------+----------+
        | 1    | 2      | 34.5     |
        +------+--------+----------+
        | 2    | 1      | 56.7     |
        +------+--------+----------+
        
        >>> look(table9)
        +------+--------+----------+
        | 'id' | 'time' | 'weight' |
        +======+========+==========+
        | 1    | 2      | 4.5      |
        +------+--------+----------+
        | 2    | 1      | 6.7      |
        +------+--------+----------+
        | 2    | 2      | 8.9      |
        +------+--------+----------+
        
        >>> table10 = join(table8, table9, key=['id', 'time'])
        >>> look(table10)
        +------+--------+----------+----------+
        | 'id' | 'time' | 'height' | 'weight' |
        +======+========+==========+==========+
        | 1    | 2      | 34.5     | 4.5      |
        +------+--------+----------+----------+
        | 2    | 1      | 56.7     | 6.7      |
        +------+--------+----------+----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    """
    
    if key is None:
        return ImplicitJoinView(left, right, presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)
    else:
        return JoinView(left, right, key, presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)


class ImplicitJoinView(RowContainer):
    
    def __init__(self, left, right, presorted=False, leftouter=False, 
                 rightouter=False, missing=None, buffersize=None, tempdir=None, cache=True):
        self.left = left
        self.right = right
        self.presorted = presorted
        self.leftouter = leftouter
        self.rightouter = rightouter
        self.missing = missing
        self.buffersize = buffersize
        
    def __iter__(self):
        return iterimplicitjoin(self.left, self.right, self.presorted, 
                               leftouter=self.leftouter, 
                               rightouter=self.rightouter, 
                               missing=self.missing,
                               buffersize=self.buffersize)


class JoinView(RowContainer):
    
    def __init__(self, left, right, key, presorted=False, leftouter=False, 
                 rightouter=False, missing=None, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.left = left
            self.right = right
        else:
            self.left = sort(left, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
            self.right = sort(right, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
            # TODO what if someone sets self.key to something else after __init__?
            # (sort will be incorrect - maybe need to protect key with property setter?)
        self.key = key
        self.leftouter = leftouter
        self.rightouter = rightouter
        self.missing = missing
        
    def __iter__(self):
        return iterjoin(self.left, self.right, self.key, leftouter=self.leftouter,
                        rightouter=self.rightouter, missing=self.missing)
    
    
def leftjoin(left, right, key=None, missing=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Perform a left outer join on the given tables. E.g.::
    
        >>> from petl import leftjoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+
        
        >>> look(table2)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 3    | 'square'  |
        +------+-----------+
        | 4    | 'ellipse' |
        +------+-----------+
        
        >>> table3 = leftjoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+----------+
        | 'id' | 'colour' | 'shape'  |
        +======+==========+==========+
        | 1    | 'blue'   | 'circle' |
        +------+----------+----------+
        | 2    | 'red'    | None     |
        +------+----------+----------+
        | 3    | 'purple' | 'square' |
        +------+----------+----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    """
    
    if key is None:
        return ImplicitJoinView(left, right, presorted=presorted, leftouter=True, 
                               rightouter=False, missing=missing, buffersize=buffersize, tempdir=tempdir, cache=cache)
    else:
        return JoinView(left, right, key, presorted=presorted, leftouter=True, 
                        rightouter=False, missing=missing, buffersize=buffersize, tempdir=tempdir, cache=cache)

    
def rightjoin(left, right, key=None, missing=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Perform a right outer join on the given tables. E.g.::

        >>> from petl import rightjoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+
        
        >>> look(table2)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 3    | 'square'  |
        +------+-----------+
        | 4    | 'ellipse' |
        +------+-----------+
        
        >>> table3 = rightjoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+-----------+
        | 'id' | 'colour' | 'shape'   |
        +======+==========+===========+
        | 1    | 'blue'   | 'circle'  |
        +------+----------+-----------+
        | 3    | 'purple' | 'square'  |
        +------+----------+-----------+
        | 4    | None     | 'ellipse' |
        +------+----------+-----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    """
    
    if key is None:
        return ImplicitJoinView(left, right, presorted=presorted, leftouter=False, 
                               rightouter=True, missing=missing, buffersize=buffersize, tempdir=tempdir, cache=cache)
    else:
        return JoinView(left, right, key, presorted=presorted, leftouter=False, 
                        rightouter=True, missing=missing, buffersize=buffersize, tempdir=tempdir, cache=cache)
    
    
def outerjoin(left, right, key=None, missing=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Perform a full outer join on the given tables. E.g.::

        >>> from petl import outerjoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+
        
        >>> look(table2)
        +------+-----------+
        | 'id' | 'shape'   |
        +======+===========+
        | 1    | 'circle'  |
        +------+-----------+
        | 3    | 'square'  |
        +------+-----------+
        | 4    | 'ellipse' |
        +------+-----------+
        
        >>> table3 = outerjoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+-----------+
        | 'id' | 'colour' | 'shape'   |
        +======+==========+===========+
        | 1    | 'blue'   | 'circle'  |
        +------+----------+-----------+
        | 2    | 'red'    | None      |
        +------+----------+-----------+
        | 3    | 'purple' | 'square'  |
        +------+----------+-----------+
        | 4    | None     | 'ellipse' |
        +------+----------+-----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    """

    if key is None:
        return ImplicitJoinView(left, right, presorted=presorted, leftouter=True, 
                               rightouter=True, missing=missing, buffersize=buffersize, tempdir=tempdir, cache=cache)
    else:
        return JoinView(left, right, key, presorted=presorted, leftouter=True, 
                        rightouter=True, missing=missing, buffersize=buffersize, tempdir=tempdir, cache=cache)
    
    
def iterjoin(left, right, key, leftouter=False, rightouter=False, missing=None):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from both tables
    lgetk = itemgetter(*lkind)
    rgetk = itemgetter(*rkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    outflds = list(lflds)
    outflds.extend(rgetv(rflds))
    yield tuple(outflds)
    
    # define a function to join two groups of rows
    def joinrows(lrowgrp, rrowgrp):
        if rrowgrp is None:
            for lrow in lrowgrp:
                outrow = list(lrow) # start with the left row
                # extend with missing values in place of the right row
                outrow.extend([missing] * len(rvind))
                yield tuple(outrow)
        elif lrowgrp is None:
            for rrow in rrowgrp:
                # start with missing values in place of the left row
                outrow = [missing] * len(lflds)
                # set key values
                for li, ri in zip(lkind, rkind):
                    outrow[li] = rrow[ri]
                # extend with non-key values from the right row  
                outrow.extend(rgetv(rrow))
                yield tuple(outrow)
        else:
            rrowgrp = list(rrowgrp) # may need to iterate more than once
            for lrow in lrowgrp:
                for rrow in rrowgrp:
                    # start with the left row
                    outrow = list(lrow)
                    # extend with non-key values from the right row
                    outrow.extend(rgetv(rrow))
                    yield tuple(outrow)

    # construct group iterators for both tables
    lgit = groupby(lit, key=lgetk)
    rgit = groupby(rit, key=rgetk)
    
    # loop until *either* of the iterators is exhausted
    lkval, rkval = None, None # initialise here to handle empty tables
    try:

        # pick off initial row groups
        lkval, lrowgrp = lgit.next() 
        rkval, rrowgrp = rgit.next()
    
        while True:
            if lkval < rkval:
                if leftouter:
                    for row in joinrows(lrowgrp, None):
                        yield tuple(row)
                # advance left
                lkval, lrowgrp = lgit.next()
            elif lkval > rkval:
                if rightouter:
                    for row in joinrows(None, rrowgrp):
                        yield tuple(row)
                # advance right
                rkval, rrowgrp = rgit.next()
            else:
                for row in joinrows(lrowgrp, rrowgrp):
                    yield tuple(row)
                # advance both
                lkval, lrowgrp = lgit.next()
                rkval, rrowgrp = rgit.next()
        
    except StopIteration:
        pass
    
    # make sure any left rows remaining are yielded
    if leftouter:
        if lkval > rkval:
            # yield anything that got left hanging
            for row in joinrows(lrowgrp, None):
                yield tuple(row)
        # yield the rest
        for lkval, lrowgrp in lgit:
            for row in joinrows(lrowgrp, None):
                yield tuple(row)

    # make sure any right rows remaining are yielded
    if rightouter:
        if lkval < rkval:
            # yield anything that got left hanging
            for row in joinrows(None, rrowgrp):
                yield tuple(row)
        # yield the rest
        for rkval, rrowgrp in rgit:
            for row in joinrows(None, rrowgrp):
                yield tuple(row)
            
        
def iterimplicitjoin(left, right, presorted=False, leftouter=False, 
                    rightouter=False, missing=None, buffersize=None, tempdir=None, cache=True):
    # determine key field or fields
    lflds = header(left)
    rflds = header(right)
    key = []
    for f in lflds:
        if f in rflds:
            key.append(f)
    assert len(key) > 0, 'no fields in common'
    if len(key) == 1:
        key = key[0] # deal with singletons
    if not presorted:
        # this is not optimal, have to sort each time, because key is determined
        # dynamically from the data
        left = sort(left, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        right = sort(right, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
    # from here on it's the same as a normal join
    return iterjoin(left, right, key, leftouter=leftouter, rightouter=rightouter,
                    missing=missing)


def crossjoin(*tables):
    """
    Form the cartesian product of the given tables. E.g.::

        >>> from petl import crossjoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        
        >>> look(table2)
        +------+----------+
        | 'id' | 'shape'  |
        +======+==========+
        | 1    | 'circle' |
        +------+----------+
        | 3    | 'square' |
        +------+----------+
        
        >>> table3 = crossjoin(table1, table2)
        >>> look(table3)
        +------+----------+------+----------+
        | 'id' | 'colour' | 'id' | 'shape'  |
        +======+==========+======+==========+
        | 1    | 'blue'   | 1    | 'circle' |
        +------+----------+------+----------+
        | 1    | 'blue'   | 3    | 'square' |
        +------+----------+------+----------+
        | 2    | 'red'    | 1    | 'circle' |
        +------+----------+------+----------+
        | 2    | 'red'    | 3    | 'square' |
        +------+----------+------+----------+

    See also :func:`join`, :func:`leftjoin`, :func:`rightjoint`, :func:`outerjoin`.
    
    """
    
    return CrossJoinView(*tables)


class CrossJoinView(RowContainer):
    
    def __init__(self, *sources):
        self.sources = sources
        
    def __iter__(self):
        return itercrossjoin(self.sources)
    
    
def itercrossjoin(sources):

    # construct fields
    outflds = list()
    for s in sources:
        outflds.extend(header(s))
    yield tuple(outflds)

    datasrcs = [data(src) for src in sources]
    for prod in product(*datasrcs):
        outrow = list()
        for row in prod:
            outrow.extend(row)
        yield tuple(outrow)
        
        
def antijoin(left, right, key=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Return rows from the `left` table where the key value does not occur in the
    `right` table. E.g.::

        >>> from petl import antijoin, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 0    | 'black'  |
        +------+----------+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 4    | 'yellow' |
        +------+----------+
        | 5    | 'white'  |
        +------+----------+
        
        >>> look(table2)
        +------+----------+
        | 'id' | 'shape'  |
        +======+==========+
        | 1    | 'circle' |
        +------+----------+
        | 3    | 'square' |
        +------+----------+
        
        >>> table3 = antijoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 0    | 'black'  |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 4    | 'yellow' |
        +------+----------+
        | 5    | 'white'  |
        +------+----------+
    
    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    """
    
    if key is None:
        return ImplicitAntiJoinView(left, right, presorted, buffersize)
    else:
        return AntiJoinView(left, right, key, presorted, buffersize)


class AntiJoinView(RowContainer):
    
    def __init__(self, left, right, key, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.left = left
            self.right = right
        else:
            self.left = sort(left, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
            self.right = sort(right, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
            # TODO what if someone sets self.key to something else after __init__?
            # (sort will be incorrect - maybe need to protect key with property setter?)
        self.key = key
        
    def __iter__(self):
        return iterantijoin(self.left, self.right, self.key)
    
    
def iterantijoin(left, right, key):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    yield tuple(lflds)

    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from both tables
    lgetk = itemgetter(*lkind)
    rgetk = itemgetter(*rkind)
    
    # construct group iterators for both tables
    lgit = groupby(lit, key=lgetk)
    rgit = groupby(rit, key=rgetk)
    
    # loop until *either* of the iterators is exhausted
    lkval, rkval = None, None # initialise here to handle empty tables
    try:

        # pick off initial row groups
        lkval, lrowgrp = lgit.next() 
        rkval, _ = rgit.next()

        while True:
            if lkval < rkval:
                for row in lrowgrp:
                    yield tuple(row)
                # advance left
                lkval, lrowgrp = lgit.next()
            elif lkval > rkval:
                # advance right
                rkval, _ = rgit.next()
            else:
                # advance both
                lkval, lrowgrp = lgit.next()
                rkval, _ = rgit.next()
        
    except StopIteration:
        pass
    
    # any left over?
    if lkval > rkval:
        # yield anything that got left hanging
        for row in lrowgrp:
            yield tuple(row)
    # and the rest...
    for lkval, lrowgrp in lgit:
        for row in lrowgrp:
            yield tuple(row)

        
class ImplicitAntiJoinView(RowContainer):
    
    def __init__(self, left, right, presorted=False, buffersize=None, tempdir=None, cache=True):
        self.left = left
        self.right = right
        self.presorted = presorted
        self.buffersize = buffersize
        
    def __iter__(self):
        return iterimplicitantijoin(self.left, self.right, self.presorted, self.buffersize)
    
    
def iterimplicitantijoin(left, right, presorted=False, buffersize=None, tempdir=None, cache=True):
    # determine key field or fields
    lflds = header(left)
    rflds = header(right)
    key = []
    for f in lflds:
        if f in rflds:
            key.append(f)
    assert len(key) > 0, 'no fields in common'
    if len(key) == 1:
        key = key[0] # deal with singletons
    if not presorted:
        # this is not optimal, have to sort each time, because key is determined
        # dynamically from the data
        left = sort(left, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        right = sort(right, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
    # from here on it's the same as a normal antijoin
    return iterantijoin(left, right, key)


def rangefacet(table, field, width, minv=None, maxv=None, 
               presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Return a dictionary mapping ranges to tables. E.g.::
    
        >>> from petl import rangefacet, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 3     |
        +-------+-------+
        | 'a'   | 7     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'b'   | 1     |
        +-------+-------+
        | 'b'   | 9     |
        +-------+-------+
        | 'c'   | 4     |
        +-------+-------+
        | 'd'   | 3     |
        +-------+-------+
        
        >>> rf = rangefacet(table1, 'bar', 2)
        >>> rf.keys()
        [(1, 3), (3, 5), (5, 7), (7, 9)]
        >>> look(rf[(1, 3)])
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'b'   | 2     |
        +-------+-------+
        | 'b'   | 1     |
        +-------+-------+
        
        >>> look(rf[(7, 9)])
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 7     |
        +-------+-------+
        | 'b'   | 9     |
        +-------+-------+

    Note that the last bin includes both edges.
    
    """

    # determine minimum and maximum values
    if minv is None and maxv is None:
        minv, maxv = limits(table, field)
    elif minv is None:
        minv = min(itervalues(table, field))
    elif max is None:
        maxv = max(itervalues(table, field))
        
    fct = OrderedDict()
    for binminv in xrange(minv, maxv, width):
        binmaxv = binminv + width
        if binmaxv >= maxv: # final bin
            binmaxv = maxv
            # final bin includes right edge
            fct[(binminv, binmaxv)] = selectrangeopen(table, field, binminv, binmaxv)
        else:
            fct[(binminv, binmaxv)] = selectrangeopenleft(table, field, binminv, binmaxv)

    return fct
    

def transpose(table):
    """
    Transpose rows into columns. E.g.::

        >>> from petl import transpose, look    
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+
        | 5    | 'yellow' |
        +------+----------+
        | 7    | 'orange' |
        +------+----------+
        
        >>> table2 = transpose(table1)
        >>> look(table2)
        +----------+--------+-------+----------+----------+----------+
        | 'id'     | 1      | 2     | 3        | 5        | 7        |
        +==========+========+=======+==========+==========+==========+
        | 'colour' | 'blue' | 'red' | 'purple' | 'yellow' | 'orange' |
        +----------+--------+-------+----------+----------+----------+

    See also :func:`recast`.
    
    """
    
    return TransposeView(table)


class TransposeView(RowContainer):
    
    def __init__(self, source):
        self.source = source
        
    def __iter__(self):
        return itertranspose(self.source)


def itertranspose(source):
    fields = header(source)
    its = [iter(source) for _ in fields]
    for i in range(len(fields)):
        yield tuple(row[i] for row in its[i])
        

def intersection(a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Return rows in `a` that are also in `b`. E.g.::
    
        >>> from petl import intersection, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+
        
        >>> look(table2)
        +-----+-----+-------+
        | 'x' | 'y' | 'z'   |
        +=====+=====+=======+
        | 'B' | 2   | False |
        +-----+-----+-------+
        | 'A' | 9   | False |
        +-----+-----+-------+
        | 'B' | 3   | True  |
        +-----+-----+-------+
        | 'C' | 9   | True  |
        +-----+-----+-------+
        
        >>> table3 = intersection(table1, table2)
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and `cache` arguments under the 
    :func:`sort` function.
    
    """
    
    return IntersectionView(a, b, presorted, buffersize)


class IntersectionView(RowContainer):
    
    def __init__(self, a, b, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.a = a
            self.b = b
        else:
            self.a = sort(a, buffersize=buffersize, tempdir=tempdir, cache=cache)
            self.b = sort(b, buffersize=buffersize, tempdir=tempdir, cache=cache)
            
    def __iter__(self):
        return iterintersection(self.a, self.b)


def iterintersection(a, b):
    ita = iter(a) 
    itb = iter(b)
    aflds = ita.next()
    itb.next() # ignore b fields
    yield tuple(aflds)
    try:
        a = tuple(ita.next())
        b = tuple(itb.next())
        while True:
            if a < b:
                a = tuple(ita.next())
            elif a == b:
                yield a
                a = tuple(ita.next())
                b = tuple(itb.next())
            else:
                b = tuple(itb.next())
    except StopIteration:
        pass
    
    
def pivot(table, f1, f2, f3, aggfun, missing=None, 
          presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Construct a pivot table. E.g.::

        >>> from petl import pivot, look
        >>> look(table1)
        +----------+----------+---------+---------+
        | 'region' | 'gender' | 'style' | 'units' |
        +==========+==========+=========+=========+
        | 'east'   | 'boy'    | 'tee'   | 12      |
        +----------+----------+---------+---------+
        | 'east'   | 'boy'    | 'golf'  | 14      |
        +----------+----------+---------+---------+
        | 'east'   | 'boy'    | 'fancy' | 7       |
        +----------+----------+---------+---------+
        | 'east'   | 'girl'   | 'tee'   | 3       |
        +----------+----------+---------+---------+
        | 'east'   | 'girl'   | 'golf'  | 8       |
        +----------+----------+---------+---------+
        | 'east'   | 'girl'   | 'fancy' | 18      |
        +----------+----------+---------+---------+
        | 'west'   | 'boy'    | 'tee'   | 12      |
        +----------+----------+---------+---------+
        | 'west'   | 'boy'    | 'golf'  | 15      |
        +----------+----------+---------+---------+
        | 'west'   | 'boy'    | 'fancy' | 8       |
        +----------+----------+---------+---------+
        | 'west'   | 'girl'   | 'tee'   | 6       |
        +----------+----------+---------+---------+
        
        >>> table2 = pivot(table1, 'region', 'gender', 'units', sum)
        >>> look(table2)
        +----------+-------+--------+
        | 'region' | 'boy' | 'girl' |
        +==========+=======+========+
        | 'east'   | 33    | 29     |
        +----------+-------+--------+
        | 'west'   | 35    | 23     |
        +----------+-------+--------+
        
        >>> table3 = pivot(table1, 'region', 'style', 'units', sum)
        >>> look(table3)
        +----------+---------+--------+-------+
        | 'region' | 'fancy' | 'golf' | 'tee' |
        +==========+=========+========+=======+
        | 'east'   | 25      | 22     | 15    |
        +----------+---------+--------+-------+
        | 'west'   | 9       | 31     | 18    |
        +----------+---------+--------+-------+
        
        >>> table4 = pivot(table1, 'gender', 'style', 'units', sum)
        >>> look(table4)
        +----------+---------+--------+-------+
        | 'gender' | 'fancy' | 'golf' | 'tee' |
        +==========+=========+========+=======+
        | 'boy'    | 15      | 29     | 24    |
        +----------+---------+--------+-------+
        | 'girl'   | 19      | 24     | 9     |
        +----------+---------+--------+-------+
        
    See also :func:`recast`.

    """
    
    return PivotView(table, f1, f2, f3, aggfun, missing=missing, 
                     presorted=presorted, buffersize=buffersize, tempdir=tempdir,
                     cache=cache)


class PivotView(RowContainer):
    
    def __init__(self, source, f1, f2, f3, aggfun, missing=None, 
                 presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key=(f1, f2), buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.f1, self.f2, self.f3 = f1, f2, f3
        self.aggfun = aggfun
        self.missing = missing
        
    def __iter__(self):
        return iterpivot(self.source, self.f1, self.f2, self.f3, self.aggfun, self.missing)
    
    
def iterpivot(source, f1, f2, f3, aggfun, missing):
    
    # first pass - collect fields
    f2vals = set(itervalues(source, f2)) # TODO sampling
    f2vals = list(f2vals)
    f2vals.sort()
    outflds = [f1]
    outflds.extend(f2vals)
    yield tuple(outflds)
    
    # second pass - generate output
    it = iter(source)
    srcflds = it.next()
    f1i = srcflds.index(f1)
    f2i = srcflds.index(f2)
    f3i = srcflds.index(f3)
    for v1, v1rows in groupby(it, key=itemgetter(f1i)):
        outrow = [v1] + [missing] * len(f2vals)
        for v2, v12rows in groupby(v1rows, key=itemgetter(f2i)):
            aggval = aggfun([row[f3i] for row in v12rows])
            outrow[1 + f2vals.index(v2)] = aggval
        yield tuple(outrow) 
    
    
def hashjoin(left, right, key=None, cache=True):
    """
    Alternative implementation of :func:`join`, where the join is executed
    by constructing an in-memory lookup for the right hand table, then iterating over rows 
    from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.5
    
    .. versionchanged:: 0.16
    
    Added support for caching data from right hand table (only available when
    `key` is given).

    """
    
    if key is None:
        return ImplicitHashJoinView(left, right)
    else:
        return HashJoinView(left, right, key, cache=cache)


class ImplicitHashJoinView(RowContainer):
    
    def __init__(self, left, right):
        self.left = left
        self.right = right
        
    def __iter__(self):
        return iterimplicithashjoin(self.left, self.right)


class HashJoinView(RowContainer):
    
    def __init__(self, left, right, key, cache=True):
        self.left = left
        self.right = right
        self.key = key
        self.cache = True
        self.rlookup = None
        
    def __iter__(self):
        if not self.cache or self.rlookup is None:
            self.rlookup = lookup(self.right, self.key)
        return iterhashjoin(self.left, self.right, self.key, self.rlookup)
    

def iterhashjoin(left, right, key, rlookup):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from left table
    lgetk = itemgetter(*lkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    outflds = list(lflds)
    outflds.extend(rgetv(rflds))
    yield tuple(outflds)
    
    # define a function to join rows
    def joinrows(lrow, rrows):
        for rrow in rrows:
            # start with the left row
            outrow = list(lrow)
            # extend with non-key values from the right row
            outrow.extend(rgetv(rrow))
            yield tuple(outrow)

    for lrow in lit:
        k = lgetk(lrow)
        if k in rlookup:
            rrows = rlookup[k]
            for outrow in joinrows(lrow, rrows):
                yield outrow
        
        
def iterimplicithashjoin(left, right):
    # determine key field or fields
    lflds = header(left)
    rflds = header(right)
    key = []
    for f in lflds:
        if f in rflds:
            key.append(f)
    assert len(key) > 0, 'no fields in common'
    if len(key) == 1:
        key = key[0] # deal with singletons
    rlookup = lookup(right, key)
    # from here on it's the same as a normal join
    return iterhashjoin(left, right, key, rlookup)


def hashleftjoin(left, right, key=None, missing=None, cache=True):
    """
    Alternative implementation of :func:`leftjoin`, where the join is executed
    by constructing an in-memory lookup for the right hand table, then iterating over rows 
    from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.5

    .. versionchanged:: 0.16
    
    Added support for caching data from right hand table (only available when
    `key` is given).

    """

    if key is None:
        return ImplicitHashLeftJoinView(left, right, missing=missing)
    else:
        return HashLeftJoinView(left, right, key, missing=missing, cache=cache)


class ImplicitHashLeftJoinView(RowContainer):
    
    def __init__(self, left, right, missing=None):
        self.left = left
        self.right = right
        self.missing = missing
        
    def __iter__(self):
        return iterimplicithashleftjoin(self.left, self.right, self.missing)


class HashLeftJoinView(RowContainer):
    
    def __init__(self, left, right, key, missing=None, cache=True):
        self.left = left
        self.right = right
        self.key = key
        self.missing = missing
        self.cache = cache
        self.rlookup = None
        
    def __iter__(self):
        if not self.cache or self.rlookup is None:
            self.rlookup = lookup(self.right, self.key)
        return iterhashleftjoin(self.left, self.right, self.key, self.missing, self.rlookup)
    

def iterhashleftjoin(left, right, key, missing, rlookup):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from left table
    lgetk = itemgetter(*lkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    outflds = list(lflds)
    outflds.extend(rgetv(rflds))
    yield tuple(outflds)
    
    # define a function to join rows
    def joinrows(lrow, rrows):
        for rrow in rrows:
            # start with the left row
            outrow = list(lrow)
            # extend with non-key values from the right row
            outrow.extend(rgetv(rrow))
            yield tuple(outrow)

    for lrow in lit:
        k = lgetk(lrow)
        if k in rlookup:
            rrows = rlookup[k]
            for outrow in joinrows(lrow, rrows):
                yield outrow
        else:
            outrow = list(lrow) # start with the left row
            # extend with missing values in place of the right row
            outrow.extend([missing] * len(rvind))
            yield tuple(outrow)
        
        
def iterimplicithashleftjoin(left, right, missing):
    # determine key field or fields
    lflds = header(left)
    rflds = header(right)
    key = []
    for f in lflds:
        if f in rflds:
            key.append(f)
    assert len(key) > 0, 'no fields in common'
    if len(key) == 1:
        key = key[0] # deal with singletons
    rlookup = lookup(right, key)
    # from here on it's the same as a normal join
    return iterhashleftjoin(left, right, key, missing, rlookup)


def hashrightjoin(left, right, key=None, missing=None, cache=True):
    """
    Alternative implementation of :func:`rightjoin`, where the join is executed
    by constructing an in-memory lookup for the left hand table, then iterating over rows 
    from the right hand table.
    
    May be faster and/or more resource efficient where the left table is small
    and the right table is large.
    
    .. versionadded:: 0.5

    .. versionchanged:: 0.16
    
    Added support for caching data from left hand table (only available when
    `key` is given).

    """

    if key is None:
        return ImplicitHashRightJoinView(left, right, missing=missing)
    else:
        return HashRightJoinView(left, right, key, missing=missing, cache=cache)


class ImplicitHashRightJoinView(RowContainer):
    
    def __init__(self, left, right, missing=None):
        self.left = left
        self.right = right
        self.missing = missing
        
    def __iter__(self):
        return iterimplicithashrightjoin(self.left, self.right, self.missing)


class HashRightJoinView(RowContainer):
    
    def __init__(self, left, right, key, missing=None, cache=True):
        self.left = left
        self.right = right
        self.key = key
        self.missing = missing
        self.cache = cache
        self.llookup = None
        
    def __iter__(self):
        if not self.cache or self.llookup is None:
            self.llookup = lookup(self.left, self.key)
        return iterhashrightjoin(self.left, self.right, self.key, self.missing, self.llookup)
    

def iterhashrightjoin(left, right, key, missing, llookup):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from left table
    rgetk = itemgetter(*rkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    outflds = list(lflds)
    outflds.extend(rgetv(rflds))
    yield tuple(outflds)
    
    # define a function to join rows
    def joinrows(rrow, lrows):
        for lrow in lrows:
            # start with the left row
            outrow = list(lrow)
            # extend with non-key values from the right row
            outrow.extend(rgetv(rrow))
            yield tuple(outrow)

    for rrow in rit:
        k = rgetk(rrow)
        if k in llookup:
            lrows = llookup[k]
            for outrow in joinrows(rrow, lrows):
                yield outrow
        else:
            # start with missing values in place of the left row
            outrow = [missing] * len(lflds)
            # set key values
            for li, ri in zip(lkind, rkind):
                outrow[li] = rrow[ri]
            # extend with non-key values from the right row  
            outrow.extend(rgetv(rrow))
            yield tuple(outrow)
        
        
def iterimplicithashrightjoin(left, right, missing):
    # determine key field or fields
    lflds = header(left)
    rflds = header(right)
    key = []
    for f in lflds:
        if f in rflds:
            key.append(f)
    assert len(key) > 0, 'no fields in common'
    if len(key) == 1:
        key = key[0] # deal with singletons
    llookup = lookup(left, key)
    # from here on it's the same as a normal join
    return iterhashrightjoin(left, right, key, missing, llookup)


def hashantijoin(left, right, key=None):
    """
    Alternative implementation of :func:`antijoin`, where the join is executed
    by constructing an in-memory set for all keys found in the right hand table, then 
    iterating over rows from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.5

    """
    
    if key is None:
        return ImplicitHashAntiJoinView(left, right)
    else:
        return HashAntiJoinView(left, right, key)


class HashAntiJoinView(RowContainer):
    
    def __init__(self, left, right, key):
        self.left = left
        self.right = right
        self.key = key
        
    def __iter__(self):
        return iterhashantijoin(self.left, self.right, self.key)
    
    
def iterhashantijoin(left, right, key):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    yield tuple(lflds)

    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from both tables
    lgetk = itemgetter(*lkind)
    rgetk = itemgetter(*rkind)
    
    rkeys = set()
    for rrow in rit:
        rk = rgetk(rrow)
        rkeys.add(rk)
        
    for lrow in lit:
        lk = lgetk(lrow)
        if lk not in rkeys:
            yield tuple(lrow)

        
class ImplicitHashAntiJoinView(RowContainer):
    
    def __init__(self, left, right):
        self.left = left
        self.right = right
        
    def __iter__(self):
        return iterimplicithashantijoin(self.left, self.right)
    
    
def iterimplicithashantijoin(left, right):
    # determine key field or fields
    lflds = header(left)
    rflds = header(right)
    key = []
    for f in lflds:
        if f in rflds:
            key.append(f)
    assert len(key) > 0, 'no fields in common'
    if len(key) == 1:
        key = key[0] # deal with singletons
    # from here on it's the same as a normal antijoin
    return iterhashantijoin(left, right, key)


def hashcomplement(a, b):
    """
    Alternative implementation of :func:`complement`, where the complement is executed
    by constructing an in-memory set for all rows found in the right hand table, then 
    iterating over rows from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.5

    """
    
    return HashComplementView(a, b)


class HashComplementView(RowContainer):
    
    def __init__(self, a, b):
        self.a = a
        self.b = b
            
    def __iter__(self):
        return iterhashcomplement(self.a, self.b)


def iterhashcomplement(a, b):
    ita = iter(a) 
    aflds = ita.next()
    yield tuple(aflds)
    itb = iter(b)
    itb.next() # discard b fields, assume they are the same

    # n.b., need to account for possibility of duplicate rows
    bcnt = Counter(tuple(row) for row in itb)
    for ar in ita:
        t = tuple(ar)
        if bcnt[t] > 0:
            bcnt[t] -= 1
        else:
            yield t 
        
    
def hashintersection(a, b):
    """
    Alternative implementation of :func:`intersection`, where the intersection is executed
    by constructing an in-memory set for all rows found in the right hand table, then 
    iterating over rows from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
        
    .. versionadded:: 0.5

    """
    
    return HashIntersectionView(a, b)


class HashIntersectionView(RowContainer):
    
    def __init__(self, a, b):
        self.a = a
        self.b = b
            
    def __iter__(self):
        return iterhashintersection(self.a, self.b)


def iterhashintersection(a, b):
    ita = iter(a) 
    aflds = ita.next()
    yield tuple(aflds)
    itb = iter(b)
    itb.next() # discard b fields, assume they are the same

    # n.b., need to account for possibility of duplicate rows
    bcnt = Counter(tuple(row) for row in itb)
    for ar in ita:
        t = tuple(ar)
        if bcnt[t] > 0:
            yield t 
            bcnt[t] -= 1
        
        
def flatten(table):
    """
    Convert a table to a sequence of values in row-major order. E.g.::

        >>> from petl import flatten, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+
        
        >>> list(flatten(table1))
        ['A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9, True]
    
    See also :func:`unflatten`.
    
    .. versionadded:: 0.7
    
    """
    
    return FlattenView(table)


class FlattenView(RowContainer):
    
    def __init__(self, table):
        self.table = table
        
    def __iter__(self):
        for row in data(self.table):
            for value in row:
                yield value
    
    
def unflatten(*args, **kwargs):
    """
    Convert a sequence of values in row-major order into a table. E.g.::
    
        >>> from petl import unflatten, look
        >>> input = ['A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9]
        >>> table = unflatten(input, 3)
        >>> look(table)
        +------+------+-------+
        | 'f0' | 'f1' | 'f2'  |
        +======+======+=======+
        | 'A'  | 1    | True  |
        +------+------+-------+
        | 'C'  | 7    | False |
        +------+------+-------+
        | 'B'  | 2    | False |
        +------+------+-------+
        | 'C'  | 9    | None  |
        +------+------+-------+
        
        >>> # a table and field name can also be provided as arguments
        ... look(table1)
        +---------+
        | 'lines' |
        +=========+
        | 'A'     |
        +---------+
        | 1       |
        +---------+
        | True    |
        +---------+
        | 'C'     |
        +---------+
        | 7       |
        +---------+
        | False   |
        +---------+
        | 'B'     |
        +---------+
        | 2       |
        +---------+
        | False   |
        +---------+
        | 'C'     |
        +---------+
        
        >>> table2 = unflatten(table1, 'lines', 3)
        >>> look(table2)
        +------+------+-------+
        | 'f0' | 'f1' | 'f2'  |
        +======+======+=======+
        | 'A'  | 1    | True  |
        +------+------+-------+
        | 'C'  | 7    | False |
        +------+------+-------+
        | 'B'  | 2    | False |
        +------+------+-------+
        | 'C'  | 9    | None  |
        +------+------+-------+
        
    See also :func:`flatten`.
    
    .. versionadded:: 0.7
    
    """
    
    return UnflattenView(*args, **kwargs)


class UnflattenView(RowContainer):
    
    def __init__(self, *args, **kwargs):
        if len(args) == 2:
            self.input = args[0]
            self.period = args[1]
        elif len(args) == 3:
            self.input = values(args[0], args[1])
            self.period = args[2]
        else:
            assert False, 'invalid arguments'
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
        else:
            self.missing = None
        
    def __iter__(self):
        inpt = self.input
        period = self.period
        missing = self.missing
        
        # generate header row
        fields = tuple('f%s' % i for i in range(period))
        yield fields
        
        # generate data rows
        row = list()
        for v in inpt:
            if len(row) < period:
                row.append(v)
            else:
                yield tuple(row)
                row = [v]
        
        # deal with last row
        if len(row) > 0:
            if len(row) < period:
                row.extend([missing] * (period - len(row)))
            yield tuple(row)
            

def mergesort(*tables, **kwargs):
    """
    Combine multiple input tables into one sorted output table. E.g.::
    
        >>> from petl import mergesort, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 9     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'A'   | 6     |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+
        
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'B'   | 3     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'A'   | 10    |
        +-------+-------+
        | 'F'   | 4     |
        +-------+-------+
        
        >>> table3 = mergesort(table1, table2, key='foo')
        >>> look(table3)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 9     |
        +-------+-------+
        | 'A'   | 6     |
        +-------+-------+
        | 'A'   | 10    |
        +-------+-------+
        | 'B'   | 3     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+
        | 'F'   | 4     |
        +-------+-------+
        
    If the input tables are already sorted by the given key, give ``presorted=True``
    as a keyword argument.
    
    This function is equivalent to concatenating the input tables using :func:`cat` 
    then sorting, however this function will typically be more efficient, 
    especially if the input tables are presorted.
    
    Keyword arguments:
    
        - `key` - field name or tuple of fields to sort by (defaults to `None` - lexical sort)
        - `reverse` - `True` if sort in reverse (descending) order (defaults to `False`)
        - `presorted` - `True` if inputs are already sorted by the given key (defaults to `False`)
        - `missing` - value to fill with when input tables have different fields (defaults to `None`)
        - `header` - specify a fixed header for the output table
        - `buffersize` - limit the number of rows in memory per input table when inputs are not presorted
        
    .. versionadded:: 0.9
    
    """
    
    return MergeSortView(tables, **kwargs)


class MergeSortView(RowContainer):
    
    def __init__(self, tables, key=None, reverse=False, presorted=False, 
                 missing=None, header=None, buffersize=None, tempdir=None, cache=True):
        self.key = key
        if presorted:
            self.tables = tables
        else:
            self.tables = [sort(t, key=key, reverse=reverse, buffersize=buffersize, tempdir=tempdir, cache=cache) for t in tables]
        self.missing = missing
        self.header = header
        self.reverse = reverse

    def __iter__(self):
        return itermergesort(self.tables, self.key, self.header, self.missing, self.reverse)

    
def itermergesort(sources, key, header, missing, reverse):
    
    # first need to standardise headers of all input tables
    # borrow this from itercat - TODO remove code smells
    
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

    def _standardisedata(it, flds, outflds):
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
        
    # wrap all iterators to standardise fields
    sits = [_standardisedata(it, flds, outflds) for flds, it in zip(source_flds_lists, its)]

    # now determine key function
    getkey = None
    if key is not None:
        # convert field selection into field indices
        indices = asindices(outflds, key)
        # now use field indices to construct a _getkey function
        # N.B., this will probably raise an exception on short rows
        getkey = itemgetter(*indices)
    
    # OK, do the merge sort
    for row in shortlistmergesorted(getkey, reverse, *sits):
        yield row
    

def merge(*tables, **kwargs):
    """
    Convenience function to combine multiple tables (via :func:`mergesort`) then 
    combine duplicate rows by merging under the given key (via :func:`mergeduplicates`). E.g.::
    
        >>> from petl import look, merge
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'A'   | True  |
        +-------+-------+-------+
        | 2     | 'B'   | None  |
        +-------+-------+-------+
        | 4     | 'C'   | True  |
        +-------+-------+-------+
        
        >>> look(table2)
        +-------+-------+--------+
        | 'bar' | 'baz' | 'quux' |
        +=======+=======+========+
        | 'A'   | True  | 42.0   |
        +-------+-------+--------+
        | 'B'   | False | 79.3   |
        +-------+-------+--------+
        | 'C'   | False | 12.4   |
        +-------+-------+--------+
        
        >>> table3 = merge(table1, table2, key='bar')
        >>> look(table3)
        +-------+-------+-------------------------+--------+
        | 'bar' | 'foo' | 'baz'                   | 'quux' |
        +=======+=======+=========================+========+
        | 'A'   | 1     | True                    | 42.0   |
        +-------+-------+-------------------------+--------+
        | 'B'   | 2     | False                   | 79.3   |
        +-------+-------+-------------------------+--------+
        | 'C'   | 4     | Conflict([False, True]) | 12.4   |
        +-------+-------+-------------------------+--------+
        
    Keyword arguments are the same as for :func:`mergesort`, except `key` is
    required.
    
    .. versionchanged:: 0.9
    
    Now uses :func:`mergesort`, should be more efficient for presorted inputs.

    """
    
    assert 'key' in kwargs, 'keyword argument "key" is required'
    key = kwargs['key']
    t1 = mergesort(*tables, **kwargs)
    t2 = mergeduplicates(t1, key=key, presorted=True)
    return t2


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
          
    
def unpackdict(table, field, keys=None, includeoriginal=False,
               samplesize=1000, missing=None):
    """
    Unpack dictionary values into separate fields. E.g.::
    
        >>> from petl import unpackdict, look
        >>> look(table1)
        +-------+---------------------------+
        | 'foo' | 'bar'                     |
        +=======+===========================+
        | 1     | {'quux': 'b', 'baz': 'a'} |
        +-------+---------------------------+
        | 2     | {'quux': 'd', 'baz': 'c'} |
        +-------+---------------------------+
        | 3     | {'quux': 'f', 'baz': 'e'} |
        +-------+---------------------------+
        
        >>> table2 = unpackdict(table1, 'bar')
        >>> look(table2)
        +-------+-------+--------+
        | 'foo' | 'baz' | 'quux' |
        +=======+=======+========+
        | 1     | 'a'   | 'b'    |
        +-------+-------+--------+
        | 2     | 'c'   | 'd'    |
        +-------+-------+--------+
        | 3     | 'e'   | 'f'    |
        +-------+-------+--------+

    .. versionadded:: 0.10
    
    """
    
    return UnpackDictView(table, field, keys=keys, 
                          includeoriginal=includeoriginal,
                          samplesize=samplesize, missing=missing)


class UnpackDictView(RowContainer):

    def __init__(self, table, field, keys=None, includeoriginal=False,
                 samplesize=1000, missing=None):
        self.table = table
        self.field = field
        self.keys = keys
        self.includeoriginal = includeoriginal
        self.samplesize = samplesize
        self.missing = missing

    def __iter__(self):
        return iterunpackdict(self.table, self.field, self.keys, 
                              self.includeoriginal, self.samplesize,
                              self.missing)
    

def iterunpackdict(table, field, keys, includeoriginal, samplesize, missing):

    # set up
    it = iter(table)
    fields = it.next()
    fidx = fields.index(field)
    outfields = list(fields)
    if not includeoriginal:
        del outfields[fidx]

    # are keys specified?
    if not keys:
        # need to sample to find keys
        sample = list(islice(it, samplesize))
        keys = set()
        for row in sample:
            try:
                keys |= set(row[fidx].keys())
            except AttributeError:
                pass
        it = chain(sample, it)
        keys = sorted(keys)
    outfields.extend(keys)
    yield tuple(outfields)    
        
    # generate the data rows
    for row in it:
        outrow = list(row)
        if not includeoriginal:
            del outrow[fidx]
        for key in keys:
            try:
                outrow.append(row[fidx][key])
            except:
                outrow.append(missing)
        yield tuple(outrow)
        

def fold(table, key, f, value=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Reduce rows recursively via the Python standard :func:`reduce` function. E.g.::

        >>> from petl import fold, look
        >>> look(table1)
        +------+---------+
        | 'id' | 'count' |
        +======+=========+
        | 1    | 3       |
        +------+---------+
        | 1    | 5       |
        +------+---------+
        | 2    | 4       |
        +------+---------+
        | 2    | 8       |
        +------+---------+
        
        >>> import operator
        >>> table2 = fold(table1, 'id', operator.add, 'count', presorted=True)
        >>> look(table2)
        +-------+---------+
        | 'key' | 'value' |
        +=======+=========+
        | 1     | 8       |
        +-------+---------+
        | 2     | 12      |
        +-------+---------+

    See also :func:`aggregate`, :func:`rowreduce`.
    
    .. versionadded:: 0.10
    
    """
    
    return FoldView(table, key, f, value=value, presorted=presorted, 
                    buffersize=buffersize, tempdir=tempdir, cache=cache)
    

class FoldView(RowContainer):
    
    def __init__(self, table, key, f, value=None, presorted=False, 
                 buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.table = table
        else:
            self.table = sort(table, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key
        self.f = f
        self.value = value
        
    def __iter__(self):
        return iterfold(self.table, self.key, self.f, self.value)
    

def iterfold(table, key, f, value):
    yield ('key', 'value')
    for k, grp in rowgroupby(table, key, value):
        yield k, reduce(f, grp)


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
        

def search(table, *args, **kwargs):
    """
    Perform a regular expression search, returning rows that match a given
    pattern, either anywhere in the row or within a specific field. E.g.::

        >>> from petl import search, look
        >>> look(table1)
        +------------+-------+--------------------------+
        | 'foo'      | 'bar' | 'baz'                    |
        +============+=======+==========================+
        | 'orange'   | 12    | 'oranges are nice fruit' |
        +------------+-------+--------------------------+
        | 'mango'    | 42    | 'I like them'            |
        +------------+-------+--------------------------+
        | 'banana'   | 74    | 'lovely too'             |
        +------------+-------+--------------------------+
        | 'cucumber' | 41    | 'better than mango'      |
        +------------+-------+--------------------------+
        
        >>> # search any field
        ... table2 = search(table1, '.g.')
        >>> look(table2)
        +------------+-------+--------------------------+
        | 'foo'      | 'bar' | 'baz'                    |
        +============+=======+==========================+
        | 'orange'   | 12    | 'oranges are nice fruit' |
        +------------+-------+--------------------------+
        | 'mango'    | 42    | 'I like them'            |
        +------------+-------+--------------------------+
        | 'cucumber' | 41    | 'better than mango'      |
        +------------+-------+--------------------------+
        
        >>> # search a specific field
        ... table3 = search(table1, 'foo', '.g.')
        >>> look(table3)
        +----------+-------+--------------------------+
        | 'foo'    | 'bar' | 'baz'                    |
        +==========+=======+==========================+
        | 'orange' | 12    | 'oranges are nice fruit' |
        +----------+-------+--------------------------+
        | 'mango'  | 42    | 'I like them'            |
        +----------+-------+--------------------------+
        
    
    .. versionadded:: 0.10
    
    """
    
    if len(args) == 1:
        field = None
        pattern = args[0]
    elif len(args) == 2:
        field = args[0]
        pattern = args[1]
    else:
        raise Exception('expected 1 or 2 arguments')
    return SearchView(table, pattern, field=field, **kwargs)


class SearchView(RowContainer):
    
    def __init__(self, table, pattern, field=None, flags=0):
        self.table = table
        self.pattern = pattern
        self.field = field
        self.flags = flags
        
    def __iter__(self):
        return itersearch(self.table, self.pattern, self.field, self.flags)
    
    
def itersearch(table, pattern, field, flags):
    prog = re.compile(pattern, flags)
    it = iter(table)
    fields = [str(f) for f in it.next()]
    yield tuple(fields)
    
    if field is None:
        # search whole row
        test = lambda row: any(prog.search(str(v)) for v in row)
    elif isinstance(field, basestring):
        # search single field
        index = fields.index(field)
        test = lambda row: prog.search(str(row[index]))
    else: # list or tuple or ...
        # search selection of fields
        indices = asindices(fields, field)
        getvals = itemgetter(*indices)
        test = lambda row: any(prog.search(str(v)) for v in getvals(row))

    for row in it:
        if test(row):
            yield tuple(row)
        
            
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
        
        
def lookupjoin(left, right, key=None, missing=None, presorted=False, 
               buffersize=None, tempdir=None, cache=True):
    """
    Perform a left join, but where the key is not unique in the right-hand
    table, arbitrarily choose the first row and ignore others. E.g.::

        >>> from petl import lookupjoin, look
        >>> look(table1)
        +------+----------+--------+
        | 'id' | 'color'  | 'cost' |
        +======+==========+========+
        | 1    | 'blue'   | 12     |
        +------+----------+--------+
        | 2    | 'red'    | 8      |
        +------+----------+--------+
        | 3    | 'purple' | 4      |
        +------+----------+--------+
        
        >>> look(table2)
        +------+-----------+---------+
        | 'id' | 'shape'   | 'size'  |
        +======+===========+=========+
        | 1    | 'circle'  | 'big'   |
        +------+-----------+---------+
        | 1    | 'circle'  | 'small' |
        +------+-----------+---------+
        | 2    | 'square'  | 'tiny'  |
        +------+-----------+---------+
        | 2    | 'square'  | 'big'   |
        +------+-----------+---------+
        | 3    | 'ellipse' | 'small' |
        +------+-----------+---------+
        | 3    | 'ellipse' | 'tiny'  |
        +------+-----------+---------+
        
        >>> table3 = lookupjoin(table1, table2, key='id')
        >>> look(table3)
        +------+----------+--------+-----------+---------+
        | 'id' | 'color'  | 'cost' | 'shape'   | 'size'  |
        +======+==========+========+===========+=========+
        | 1    | 'blue'   | 12     | 'circle'  | 'big'   |
        +------+----------+--------+-----------+---------+
        | 2    | 'red'    | 8      | 'square'  | 'tiny'  |
        +------+----------+--------+-----------+---------+
        | 3    | 'purple' | 4      | 'ellipse' | 'small' |
        +------+----------+--------+-----------+---------+

    See also :func:`leftjoin`.
    
    .. versionadded:: 0.11
    
    """

    if key is None:
        return ImplicitLookupJoinView(left, right, presorted=presorted, 
                                      missing=missing, buffersize=buffersize, tempdir=tempdir, cache=cache)
    else:
        return LookupJoinView(left, right, key, presorted=presorted, 
                              missing=missing, buffersize=buffersize, tempdir=tempdir, cache=cache)

    
class ImplicitLookupJoinView(RowContainer):
    
    def __init__(self, left, right, presorted=False, missing=None, 
                 buffersize=None, tempdir=None, cache=True):
        self.left = left
        self.right = right
        self.presorted = presorted
        self.missing = missing
        self.buffersize = buffersize
        
    def __iter__(self):
        return iterimplicitlookupjoin(self.left, self.right, 
                                      self.presorted, 
                                      missing=self.missing,
                                      buffersize=self.buffersize)


class LookupJoinView(RowContainer):
    
    def __init__(self, left, right, key, presorted=False, missing=None, 
                 buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.left = left
            self.right = right
        else:
            self.left = sort(left, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
            self.right = sort(right, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
            # TODO what if someone sets self.key to something else after __init__?
            # (sort will be incorrect - maybe need to protect key with property setter?)
        self.key = key
        self.missing = missing
        
    def __iter__(self):
        return iterlookupjoin(self.left, self.right, self.key, 
                              missing=self.missing)
    
    
def iterlookupjoin(left, right, key, missing=None):
    lit = iter(left)
    rit = iter(right)

    lflds = lit.next()
    rflds = rit.next()
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from both tables
    lgetk = itemgetter(*lkind)
    rgetk = itemgetter(*rkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    outflds = list(lflds)
    outflds.extend(rgetv(rflds))
    yield tuple(outflds)
    
    # define a function to join two groups of rows
    def joinrows(lrowgrp, rrowgrp):
        if rrowgrp is None:
            for lrow in lrowgrp:
                outrow = list(lrow) # start with the left row
                # extend with missing values in place of the right row
                outrow.extend([missing] * len(rvind))
                yield tuple(outrow)
        else:
            rrow = iter(rrowgrp).next() # pick first arbitrarily
            for lrow in lrowgrp:
                # start with the left row
                outrow = list(lrow)
                # extend with non-key values from the right row
                outrow.extend(rgetv(rrow))
                yield tuple(outrow)

    # construct group iterators for both tables
    lgit = groupby(lit, key=lgetk)
    rgit = groupby(rit, key=rgetk)
    
    # loop until *either* of the iterators is exhausted
    lkval, rkval = None, None # initialise here to handle empty tables
    try:

        # pick off initial row groups
        lkval, lrowgrp = lgit.next() 
        rkval, rrowgrp = rgit.next()
    
        while True:
            if lkval < rkval:
                for row in joinrows(lrowgrp, None):
                    yield tuple(row)
                # advance left
                lkval, lrowgrp = lgit.next()
            elif lkval > rkval:
                # advance right
                rkval, rrowgrp = rgit.next()
            else:
                for row in joinrows(lrowgrp, rrowgrp):
                    yield tuple(row)
                # advance both
                lkval, lrowgrp = lgit.next()
                rkval, rrowgrp = rgit.next()
        
    except StopIteration:
        pass
    
    # make sure any left rows remaining are yielded
    if lkval > rkval:
        # yield anything that got left hanging
        for row in joinrows(lrowgrp, None):
            yield tuple(row)
    # yield the rest
    for lkval, lrowgrp in lgit:
        for row in joinrows(lrowgrp, None):
            yield tuple(row)
            
        
def iterimplicitlookupjoin(left, right, presorted=False, missing=None, 
                           buffersize=None, tempdir=None, cache=True):
    # determine key field or fields
    lflds = header(left)
    rflds = header(right)
    key = [f for f in lflds if f in rflds]
    assert len(key) > 0, 'no fields in common'
    if len(key) == 1:
        key = key[0] # deal with singletons
    if not presorted:
        # this is not optimal, have to sort each time, because key is determined
        # dynamically from the data
        left = sort(left, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        right = sort(right, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
    # from here on it's the same as a normal join
    return iterlookupjoin(left, right, key, missing=missing)




def hashlookupjoin(left, right, key=None, missing=None):
    """
    Alternative implementation of :func:`lookupjoin`, where the join is executed
    by constructing an in-memory lookup for the right hand table, then iterating 
    over rows from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.11
    
    """

    if key is None:
        return ImplicitHashLookupJoinView(left, right, missing=missing)
    else:
        return HashLookupJoinView(left, right, key, missing=missing)


class ImplicitHashLookupJoinView(RowContainer):
    
    def __init__(self, left, right, missing=None):
        self.left = left
        self.right = right
        self.missing = missing
        
    def __iter__(self):
        return iterimplicithashlookupjoin(self.left, self.right, self.missing)


class HashLookupJoinView(RowContainer):
    
    def __init__(self, left, right, key, missing=None):
        self.left = left
        self.right = right
        self.key = key
        self.missing = missing
        
    def __iter__(self):
        return iterhashlookupjoin(self.left, self.right, self.key, self.missing)
    

def iterhashlookupjoin(left, right, key, missing):
    lit = iter(left)
    lflds = lit.next()

    rflds, rit = iterpeek(right) # need the whole lot to pass to lookup
    from petl.util import lookupone
    rlookup = lookupone(rit, key, strict=False)
    
    # determine indices of the key fields in left and right tables
    lkind = asindices(lflds, key)
    rkind = asindices(rflds, key)
    
    # construct functions to extract key values from left table
    lgetk = itemgetter(*lkind)
    
    # determine indices of non-key fields in the right table
    # (in the output, we only include key fields from the left table - we
    # don't want to duplicate fields)
    rvind = [i for i in range(len(rflds)) if i not in rkind]
    rgetv = rowgetter(*rvind)
    
    # determine the output fields
    outflds = list(lflds)
    outflds.extend(rgetv(rflds))
    yield tuple(outflds)
    
    # define a function to join rows
    def joinrows(lrow, rrow):
        # start with the left row
        outrow = list(lrow)
        # extend with non-key values from the right row
        outrow.extend(rgetv(rrow))
        return tuple(outrow)

    for lrow in lit:
        k = lgetk(lrow)
        if k in rlookup:
            rrow = rlookup[k]
            yield joinrows(lrow, rrow)
        else:
            outrow = list(lrow) # start with the left row
            # extend with missing values in place of the right row
            outrow.extend([missing] * len(rvind))
            yield tuple(outrow)
        
        
def iterimplicithashlookupjoin(left, right, missing):
    # determine key field or fields
    lflds, lit = iterpeek(left)
    rflds, rit = iterpeek(right)
    key = [f for f in lflds if f in rflds]
    assert len(key) > 0, 'no fields in common'
    if len(key) == 1:
        key = key[0] # deal with singletons
    # from here on it's the same as a normal join
    return iterhashlookupjoin(lit, rit, key, missing)


def filldown(table, *fields, **kwargs):
    """
    Replace missing values with non-missing values from the row above.
    E.g.::
    
        >>> from petl import filldown, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | None  | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | None  | None  |
        +-------+-------+-------+
        | 2     | None  | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+
        
        >>> table2 = filldown(table1)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | 'a'   | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | 0.23  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.23  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | 0.72  |
        +-------+-------+-------+
        
        >>> table3 = filldown(table1, 'bar')
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | 'a'   | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+
        
        >>> table4 = filldown(table1, 'bar', 'baz')
        >>> look(table4)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | 'a'   | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | 0.23  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.23  |
        +-------+-------+-------+
        | 2     | 'b'   | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | 0.56  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+

    .. versionadded:: 0.11
    
    """
    
    return FillDownView(table, fields, **kwargs)


class FillDownView(RowContainer):
    
    def __init__(self, table, fields, missing=None):
        self.table = table
        self.fields = fields
        self.missing = missing
        
    def __iter__(self):
        return iterfilldown(self.table, self.fields, self.missing)
    
    
def iterfilldown(table, fillfields, missing):
    it = iter(table)
    fields = it.next()
    yield tuple(fields)
    if not fillfields: # fill down all fields
        fillfields = fields
    fillindices = asindices(fields, fillfields)
    fill = list(it.next()) # fill values
    yield tuple(fill)
    for row in it:
        outrow = list(row)
        for idx in fillindices:
            if row[idx] == missing:
                outrow[idx] = fill[idx] # fill down
            else:
                fill[idx] = row[idx] # new fill value
        yield tuple(outrow)
        

def fillright(table, missing=None):
    """
    Replace missing values with preceding non-missing values. E.g.::

        >>> from petl import fillright, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | None  | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | None  | None  |
        +-------+-------+-------+
        | 2     | None  | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+
        
        >>> table2 = fillright(table1)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | 'a'   |
        +-------+-------+-------+
        | 1     | 1     | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | 'b'   |
        +-------+-------+-------+
        | 2     | 2     | 2     |
        +-------+-------+-------+
        | 2     | 2     | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | 'c'   |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+

    .. versionadded:: 0.11
    
    """
    
    return FillRightView(table, missing=missing)


class FillRightView(RowContainer):
    
    def __init__(self, table, missing=None):
        self.table = table
        self.missing = missing
        
    def __iter__(self):
        return iterfillright(self.table, self.missing)
    
    
def iterfillright(table, missing):
    it = iter(table)
    fields = it.next()
    yield tuple(fields)
    for row in it:
        outrow = list(row)
        for i, _ in enumerate(outrow):
            if i > 0 and outrow[i] == missing and outrow[i-1] != missing:
                outrow[i] = outrow[i-1]
        yield tuple(outrow)
        

def fillleft(table, missing=None):
    """
    Replace missing values with following non-missing values. E.g.::

        >>> from petl import fillleft, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | None  | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | None  | None  |
        +-------+-------+-------+
        | None  | None  | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | None  | 'c'   | 0.72  |
        +-------+-------+-------+
        
        >>> table2 = fillleft(table1)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 'a'   | None  |
        +-------+-------+-------+
        | 1     | 0.23  | 0.23  |
        +-------+-------+-------+
        | 1     | 'b'   | None  |
        +-------+-------+-------+
        | 2     | None  | None  |
        +-------+-------+-------+
        | 0.56  | 0.56  | 0.56  |
        +-------+-------+-------+
        | 2     | 'c'   | None  |
        +-------+-------+-------+
        | 'c'   | 'c'   | 0.72  |
        +-------+-------+-------+
        
    .. versionadded:: 0.11
    
    """
    
    return FillLeftView(table, missing=missing)


class FillLeftView(RowContainer):
    
    def __init__(self, table, missing=None):
        self.table = table
        self.missing = missing
        
    def __iter__(self):
        return iterfillleft(self.table, self.missing)
    
    
def iterfillleft(table, missing):
    it = iter(table)
    fields = it.next()
    yield tuple(fields)
    for row in it:
        outrow = list(reversed(row))
        for i, _ in enumerate(outrow):
            if i > 0 and outrow[i] == missing and outrow[i-1] != missing:
                outrow[i] = outrow[i-1]
        yield tuple(reversed(outrow))
        
             
# N.B., there are issues with this function as it currently stands. The usability
# may be dubious given the different behaviour with mins and maxs specified
# vs not specified. The implementation itself may also be very innefficient.
        
def multirangeaggregate(table, keys, widths, aggregation, value=None, 
                        mins=None, maxs=None):
    """
    Group rows at multiple levels then aggregate whole rows or specified values.
    E.g.::

        >>> from petl import look, multirangeaggregate
        >>> look(table1)
        +-----+-----+-----+
        | 'x' | 'y' | 'z' |
        +=====+=====+=====+
        | 1   | 3   | 9   |
        +-----+-----+-----+
        | 2   | 3   | 12  |
        +-----+-----+-----+
        | 4   | 2   | 17  |
        +-----+-----+-----+
        | 2   | 7   | 3   |
        +-----+-----+-----+
        | 1   | 6   | 1   |
        +-----+-----+-----+
        
        >>> table2 = multirangeaggregate(table1, keys=('x', 'y'), widths=(2, 2), aggregation=sum, mins=(0, 0), maxs=(4, 4), value='z')
        >>> look(table2)
        +------------------+---------+
        | 'key'            | 'value' |
        +==================+=========+
        | ((0, 2), (0, 2)) | 0       |
        +------------------+---------+
        | ((0, 2), (2, 4)) | 9       |
        +------------------+---------+
        | ((2, 4), (0, 2)) | 0       |
        +------------------+---------+
        | ((2, 4), (2, 4)) | 29      |
        +------------------+---------+

    .. versionadded:: 0.12
    
    """
    
    assert callable(aggregation), 'aggregation argument must be callable'
    return SimpleMultiRangeAggregateView(table, keys, widths, aggregation, 
                                         value=value,
                                         mins=mins,
                                         maxs=maxs)
    
    
class SimpleMultiRangeAggregateView(RowContainer):
    
    def __init__(self, table, keys, widths, aggregation, 
                  value=None, mins=None, maxs=None):
        assert len(keys) == len(widths), 'one width must be specified for each key field'
        assert mins is None or len(keys) == len(mins), 'bad value for mins'
        assert maxs is None or len(keys) == len(maxs), 'bad value for maxs'
        self.table = table
        self.keys = keys
        self.widths = widths
        self.aggregation = aggregation
        self.value = value
        if mins is None:
            self.mins = [None] * len(keys)
        else:
            self.mins = mins
        if maxs is None:
            self.maxs = [None] * len(keys)
        else:
            self.maxs = maxs
        
    def __iter__(self):
        return itersimplemultirangeaggregate(self.table, self.keys, self.widths,
                                             self.aggregation, self.value,
                                             self.mins, self.maxs)
        

def _recursive_bin(outerbin, level, bindef, fields, keys, widths, getval, mins, maxs):

    # TODO this is almost impossible to comprehend, needs to be tidied up!
    
    bindef = list(bindef) # take a copy
    
    if level == len(keys): # bottom out
        vals = (getval(row) for row in outerbin)
        yield tuple(bindef), vals
    
    else: # go deeper
        
        key = keys[level]
        getkey = rowitemgetter(fields, key)
        width = widths[level]
        minv = mins[level]
        maxv = maxs[level]
        
        # initialise at this level
        tbl = chain([fields], outerbin) # reconstitute table with header
        tbl_sorted = sort(tbl, key) # sort at this level
        it = iter(tbl_sorted) # get an iterator
        it.next() # throw away header

        if minv is not None and maxv is not None:
            # use a different algorithm if minv and maxv are specified - fixed bins
            numbins = int(ceil((maxv - minv) / width))
            keyv = None
            for n in xrange(0, numbins):
                binminv = minv + n*width
                binmaxv = binminv + width
                if binmaxv >= maxv: # final bin
                    binmaxv = maxv # truncate final bin to specified maximum
                thisbindef = list(bindef)
                thisbindef.append((binminv, binmaxv))
                binnedrows = []
                try:
                    while keyv < binminv: # advance until we're within the bin's range
                        row = it.next()
                        keyv = getkey(row)
                    while binminv <= keyv < binmaxv: # within the bin
                        binnedrows.append(row)
                        row = it.next()
                        keyv = getkey(row)
                    while keyv == binmaxv == maxv: # possible floating point precision bug here?
                        binnedrows.append(row) # last bin is open if maxv is specified
                        row = it.next()
                        keyv = getkey(row)
                except StopIteration:
                    pass
                for r in _recursive_bin(binnedrows, level+1, thisbindef, fields, keys, widths, getval, mins, maxs):
                    yield r
    
        else:
                
            # use a different algorithm if min or max is not specified, where
            # the unspecified limit is determined from the data
            
            # initialise minimum value
            try:
                row = it.next() # what happens if this raises StopIteration?
            except StopIteration:
                pass
            else:
                keyv = getkey(row)
                if minv is None:
                    minv = keyv
                    
                # N.B., we need to account for two possible scenarios
                # (1) maxv is not specified, so keep making bins until we run out of rows
                # (2) maxv is specified, so iterate over bins up to maxv
                try:
                    for binminv in count(minv, width):
                        binmaxv = binminv + width
                        if maxv is not None and binmaxv >= maxv: # final bin
                            binmaxv = maxv # truncate final bin to specified maximum
                        thisbindef = list(bindef)
                        thisbindef.append((binminv, binmaxv))
                        binnedrows = []
                        while keyv < binminv: # advance until we're within the bin's range
                            row = it.next()
                            keyv = getkey(row)
                        while binminv <= keyv < binmaxv: # within the bin
                            binnedrows.append(row)
                            row = it.next()
                            keyv = getkey(row)
                        while maxv is not None and keyv == binmaxv == maxv: # possible floating point precision bug here?
                            binnedrows.append(row) # last bin is open if maxv is specified
                            row = it.next()
                            keyv = getkey(row)
                            
                        for r in _recursive_bin(binnedrows, level+1, thisbindef, fields, keys, widths, getval, mins, maxs):
                            yield r
                        
                        if maxv is not None and binmaxv == maxv: # possible floating point precision bug here?
                            break
                except StopIteration:
                    # don't forget to handle the last bin
                    for r in _recursive_bin(binnedrows, level+1, thisbindef, fields, keys, widths, getval, mins, maxs):
                        yield r
    

def itersimplemultirangeaggregate(table, keys, widths, aggregation, value,
                                      mins, maxs):
    
    if aggregation == len:
        aggregation = lambda grp: sum(1 for _ in grp) # count length of iterable
    yield ('key', 'value')

    # we want a recursive grouping algorithm so we could cope with any number of 
    # key fields
    
    it = iter(table)
    fields = it.next()
    
    # wrap rows 
    it = hybridrows(fields, it)

    # determine value function
    if value is None:
        getval = lambda v: v # identity function - i.e., whole row
    else:
        if callable(value):
            getval = value
        else:
            vindices = asindices(fields, value)
            getval = itemgetter(*vindices)
            
    for bindef, vals in _recursive_bin(it, 0, [], fields, keys, widths, getval, mins, maxs):
        yield bindef, aggregation(vals)

    
################################################################################
################################################################################
## UNJOIN
################################################################################
################################################################################

    
def unjoin(table, value, key=None, autoincrement=(1, 1), presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Split a table into two tables by reversing an inner join.
    
    E.g., if the join key is present in the table::

        >>> from petl import look, unjoin
        >>> look(table1)
        +-------+-------+----------+
        | 'foo' | 'bar' | 'baz'    |
        +=======+=======+==========+
        | 'A'   | 1     | 'apple'  |
        +-------+-------+----------+
        | 'B'   | 1     | 'apple'  |
        +-------+-------+----------+
        | 'C'   | 2     | 'orange' |
        +-------+-------+----------+
        
        >>> table2, table3 = unjoin(table1, 'baz', key='bar')
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 1     |
        +-------+-------+
        | 'B'   | 1     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        
        >>> look(table3)    
        +-------+----------+
        | 'bar' | 'baz'    |
        +=======+==========+
        | 1     | 'apple'  |
        +-------+----------+
        | 2     | 'orange' |
        +-------+----------+
        
    An integer join key can also be reconstructed, e.g.::
    
        >>> look(table4)
        +-------+----------+
        | 'foo' | 'bar'    |
        +=======+==========+
        | 'A'   | 'apple'  |
        +-------+----------+
        | 'B'   | 'apple'  |
        +-------+----------+
        | 'C'   | 'orange' |
        +-------+----------+
        
        >>> table5, table6 = unjoin(table4, 'bar')
        >>> look(table5)
        +-------+----------+
        | 'foo' | 'bar_id' |
        +=======+==========+
        | 'A'   | 1        |
        +-------+----------+
        | 'B'   | 1        |
        +-------+----------+
        | 'C'   | 2        |
        +-------+----------+
        
        >>> look(table6)
        +------+----------+
        | 'id' | 'bar'    |
        +======+==========+
        | 1    | 'apple'  |
        +------+----------+
        | 2    | 'orange' |
        +------+----------+
        
    .. versionadded:: 0.12
    
    """

    if key is None:
        # first sort the table by the value field
        if presorted:
            tbl_sorted = table
        else:
            tbl_sorted = sort(table, value, buffersize=buffersize, tempdir=tempdir, cache=cache)
        # on the left, return the original table but with the value field
        # replaced by an incrementing integer
        left = ConvertToIncrementingCounterView(tbl_sorted, value, autoincrement)
        # on the right, return a new table with distinct values from the
        # given field
        right = EnumerateDistinctView(tbl_sorted, value, autoincrement)
    else:
        # on the left, return distinct rows from the original table
        # with the value field cut out
        left = distinct(cutout(table, value))
        # on the right, return distinct rows from the original table
        # with all fields but the key and value cut out
        right = distinct(cut(table, key, value))
    return left, right
    
    
class ConvertToIncrementingCounterView(RowContainer):
    
    def __init__(self, tbl, value, autoincrement):
        self.table = tbl
        self.value = value
        self.autoincrement = autoincrement
        
    def __iter__(self):
        it = iter(self.table)
        fields = it.next()
        table = chain([fields], it)
        value = self.value
        vidx = fields.index(value)
        outflds = list(fields)
        outflds[vidx] = '%s_id' % value
        yield tuple(outflds)
        offset, multiplier = self.autoincrement
        for n, (_, group) in enumerate(rowgroupby(table, value)):
            for row in group:
                outrow = list(row)
                outrow[vidx] = (n * multiplier) + offset 
                yield tuple(outrow)
        

class EnumerateDistinctView(RowContainer):
    
    def __init__(self, tbl, value, autoincrement):
        self.table = tbl
        self.value = value
        self.autoincrement = autoincrement
        
    def __iter__(self):
        offset, multiplier = self.autoincrement
        yield ('id', self.value)
        for n, (v, _) in enumerate(rowgroupby(self.table, self.value)):
            yield ((n * multiplier) + offset, v) 
        

def rowgroupmap(table, key, mapper, fields=None, missing=None, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Group rows under the given key then apply `mapper` to yield zero or more
    output rows for each input group of rows. 
    
    .. versionadded:: 0.12
    
    """

    return RowGroupMapView(table, key, mapper, fields=fields,
                           presorted=presorted, 
                           buffersize=buffersize, tempdir=tempdir, cache=cache)


class RowGroupMapView(RowContainer):
    
    def __init__(self, source, key, mapper, fields=None, 
                 presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.key = key
        self.fields = fields
        self.mapper = mapper

    def __iter__(self):
        return iterrowgroupmap(self.source, self.key, self.mapper, self.fields)

    
def iterrowgroupmap(source, key, mapper, fields):
    yield tuple(fields)
    for key, rows in rowgroupby(source, key):
        for row in mapper(key, rows):
            yield row
        

def distinct(table, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Return only distinct rows in the table. See also :func:`duplicates` and
    :func:`unique`.
    
    .. versionadded:: 0.12
    
    """
    
    return DistinctView(table, presorted=presorted, buffersize=buffersize, tempdir=tempdir, cache=cache)


class DistinctView(RowContainer):
    
    def __init__(self, table, presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.table = table
        else:
            self.table = sort(table, buffersize=buffersize, tempdir=tempdir, cache=cache)
        
    def __iter__(self):
        it = iter(self.table)
        yield it.next()
        previous = None
        for row in it:
            if row != previous:
                yield row
            previous = row
            
            
def groupcountdistinctvalues(table, key, value):
    """
    Group by the `key` field then count the number of distinct values in the 
    `value` field.
    
    .. versionadded:: 0.14
    
    """
    
    s1 = cut(table, key, value)
    s2 = distinct(s1)
    s3 = aggregate(s2, key, len)
    return s3


def groupselectfirst(table, key):
    """
    Group by the `key` field then return the first row within each group.
    
    .. versionadded:: 0.14

    """

    _reducer = lambda key, rows: rows.next()
    return rowreduce(table, key, reducer=_reducer)


def groupselectmin(table, key, value):
    """
    Group by the `key` field then return the row with the maximum of the `value`
    field within each group. N.B., will only return one row for each group,
    even if multiple rows have the same (maximum) value.

    .. versionadded:: 0.14
    
    """

    return groupselectfirst(sort(table, value, reverse=False), key)
    
    
def groupselectmax(table, key, value):
    """
    Group by the `key` field then return the row with the minimum of the `value`
    field within each group. N.B., will only return one row for each group,
    even if multiple rows have the same (maximum) value.

    .. versionadded:: 0.14
    
    """

    return groupselectfirst(sort(table, value, reverse=True), key)
    

def coalesce(*fields, **kwargs):
    try:
        missing = kwargs['missing']
    except:
        missing = None
    try:
        default = kwargs['default']
    except:
        default = None
    def _coalesce(row):
        for f in fields:
            v = row[f]
            if v is not missing:
                return v
        return default
    return _coalesce
            

class TransformError(Exception):
    pass


