"""
TODO doc me

"""

from itertools import islice, groupby, product, count
from collections import deque, defaultdict, OrderedDict, namedtuple
from operator import itemgetter
import cPickle as pickle


from petl.util import asindices, rowgetter, asdict,\
    expr, valueset, records, header, data, limits, itervalues, parsenumber, lookup,\
    values
import re
from petl.io import Uncacheable
from tempfile import NamedTemporaryFile
import heapq
import operator
from petl.base import RowContainer


def rename(table, *args):
    """
    Replace one or more fields in the table's header row. E.g., to rename a 
    single field::
    
        >>> from petl import look, rename
        >>> tbl1 = [['sex', 'age'],
        ...         ['M', 12],
        ...         ['F', 34],
        ...         ['-', 56]]
        >>> tbl2 = rename(tbl1, 'sex', 'gender')
        >>> look(tbl2)
        +----------+-------+
        | 'gender' | 'age' |
        +==========+=======+
        | 'M'      | 12    |
        +----------+-------+
        | 'F'      | 34    |
        +----------+-------+
        | '-'      | 56    |
        +----------+-------+

    To rename multiple fields, pass a dictionary as the second argument, e.g.::

        >>> from petl import look, rename
        >>> tbl1 = [['sex', 'age'],
        ...         ['M', 12],
        ...         ['F', 34],
        ...         ['-', 56]]
        >>> tbl2 = rename(tbl1, {'sex': 'gender', 'age': 'age_years'})
        >>> look(tbl2)
        +----------+-------------+
        | 'gender' | 'age_years' |
        +==========+=============+
        | 'M'      | 12          |
        +----------+-------------+
        | 'F'      | 34          |
        +----------+-------------+
        | '-'      | 56          |
        +----------+-------------+

    The returned table object can also be used to modify the field mapping, 
    using the suffix notation, e.g.::
    
        >>> tbl1 = [['sex', 'age'],
        ...         ['M', 12],
        ...         ['F', 34],
        ...         ['-', 56]]
        >>> tbl2 = rename(tbl1)
        >>> look(tbl2)
        +-------+-------+
        | 'sex' | 'age' |
        +=======+=======+
        | 'M'   | 12    |
        +-------+-------+
        | 'F'   | 34    |
        +-------+-------+
        | '-'   | 56    |
        +-------+-------+
        
        >>> tbl2['sex'] = 'gender'
        >>> look(tbl2)
        +----------+-------+
        | 'gender' | 'age' |
        +==========+=======+
        | 'M'      | 12    |
        +----------+-------+
        | 'F'      | 34    |
        +----------+-------+
        | '-'      | 56    |
        +----------+-------+
        
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
        
    def __getitem__(self, key):
        return self.spec[key]
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), tuple(self.spec.items())))
        except Exception as e:
            raise Uncacheable(e)
    
    
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
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, 2.7],
        ...           ['B', 2, 3.4],
        ...           ['B', 3, 7.8],
        ...           ['D', 42, 9.0],
        ...           ['E', 12]]
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

    Note that any short rows will be padded with `None` values (or whatever is
    provided via the `missing` keyword argument).
    
    Fields can also be specified by index, starting from zero. E.g.::

        >>> table3 = cut(table1, 0, 2)
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

    Field names and indices can be mixed, e.g.::

        >>> table4 = cut(table1, 'bar', 0)
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

    Use the standard :func:`range` runction to select a range of fields, e.g.::
    
        >>> table5 = cut(table1, *range(0, 2))
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

    See also :func:`cutout`.
    
    """
    
    return CutView(table, args, **kwargs)


class CutView(RowContainer):
    
    def __init__(self, source, spec, missing=None):
        self.source = source
        self.spec = spec
        self.missing = missing
        
    def __iter__(self):
        return itercut(self.source, self.spec, self.missing)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.spec, self.missing))
        except Exception as e:
            raise Uncacheable(e)
        
        
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
        >>> table1 = (('foo', 'bar', 'baz'),
        ...           ('A', 1, 2),
        ...           ('B', '2', '3.4'),
        ...           (u'B', u'3', u'7.8', True),
        ...           ('D', 'xyz', 9.0),
        ...           ('E', None))
        >>> table2 = cutout(table1, 'bar')
        >>> look(table2)
        +-------+--------+
        | 'foo' | 'baz'  |
        +=======+========+
        | 'A'   | 2      |
        +-------+--------+
        | 'B'   | '3.4'  |
        +-------+--------+
        | u'B'  | u'7.8' |
        +-------+--------+
        | 'D'   | 9.0    |
        +-------+--------+
        | 'E'   | None   |
        +-------+--------+

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
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.spec, self.missing))
        except Exception as e:
            raise Uncacheable(e)
        
        
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
    Concatenate data from two or more tables. Note that the tables do not need
    to share exactly the same fields, any missing fields will be padded with
    `None` (or whatever is provided via the `missing` keyword argument). E.g.::

        >>> from petl import look, cat    
        >>> table1 = [['foo', 'bar'],
        ...           [1, 'A'],
        ...           [2, 'B']]
        >>> table2 = [['bar', 'baz'],
        ...           ['C', True],
        ...           ['D', False]]
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

    This function can also be used to square up a table with uneven rows, e.g.::

        >>> table4 = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', '2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', 'xyz', 9.0],
        ...          ['E', None]]
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

    .. versionchanged:: 0.5
    
    By default, the fields for the output table will be determined as the 
    union of all fields found in the input tables. Use the `header` keyword 
    argument to override this behaviour and specify a fixed set of fields for 
    the output table. E.g., with a single input table::
    
        >>> table5 = [['bar', 'foo'],
        ...           ['A', 1],
        ...           ['B', 2]]
        >>> table6 = cat(table5, header=['A', 'foo', 'B', 'bar', 'C'])
        >>> look(table6)
        +------+-------+------+-------+------+
        | 'A'  | 'foo' | 'B'  | 'bar' | 'C'  |
        +======+=======+======+=======+======+
        | None | 1     | None | 'A'   | None |
        +------+-------+------+-------+------+
        | None | 2     | None | 'B'   | None |
        +------+-------+------+-------+------+

    E.g., with two input tables::

        >>> table7 = [['bar', 'foo'],
        ...           ['A', 1],
        ...           ['B', 2]]
        >>> table8 = [['bar', 'baz'],
        ...           ['C', True],
        ...           ['D', False]]
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
    
    def cachetag(self):
        try:
            sourcetags = tuple(source.cachetag() for source in self.sources)
            return hash((sourcetags, self.missing, self.header))
        except Exception as e:
            raise Uncacheable(e)
        

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


def convert(table, field, *args, **kwargs):
    """
    Transform values under the given field via an arbitrary function or method
    invocation. E.g., using the built-in :func:`float` function:
    
        >>> from petl import convert, look
        >>> table1 = [['foo', 'bar'],
        ...           ['A', '2.4'],
        ...           ['B', '5.7'],
        ...           ['C', '1.2'],
        ...           ['D', '8.3']]
        >>> table2 = convert(table1, 'bar', float)
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 2.4   |
        +-------+-------+
        | 'B'   | 5.7   |
        +-------+-------+
        | 'C'   | 1.2   |
        +-------+-------+
        | 'D'   | 8.3   |
        +-------+-------+
    
    E.g., using a lambda function::
        
        >>> table3 = convert(table2, 'bar', lambda v: v**2)
        >>> look(table3)    
        +-------+-------------------+
        | 'foo' | 'bar'             |
        +=======+===================+
        | 'A'   | 5.76              |
        +-------+-------------------+
        | 'B'   | 32.49             |
        +-------+-------------------+
        | 'C'   | 1.44              |
        +-------+-------------------+
        | 'D'   | 68.89000000000001 |
        +-------+-------------------+
        
    A method of the data value can also be invoked by passing the method name. E.g.::
    
        >>> table4 = convert(table1, 'foo', 'lower')
        >>> look(table4)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '2.4' |
        +-------+-------+
        | 'b'   | '5.7' |
        +-------+-------+
        | 'c'   | '1.2' |
        +-------+-------+
        | 'd'   | '8.3' |
        +-------+-------+
        
    Arguments to the method invocation can also be given, e.g.::
        
        >>> table5 = convert(table4, 'foo', 'replace', 'a', 'aa')
        >>> look(table5)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'aa'  | '2.4' |
        +-------+-------+
        | 'b'   | '5.7' |
        +-------+-------+
        | 'c'   | '1.2' |
        +-------+-------+
        | 'd'   | '8.3' |
        +-------+-------+

    Useful for, among other things, string manipulation, see also the methods
    on the `str <http://docs.python.org/library/stdtypes.html#string-methods>`_ 
    type.
    
    Values can also be translated via a dictionary, e.g.::
    
        >>> table6 = [['gender', 'age'],
        ...           ['M', 12],
        ...           ['F', 34],
        ...           ['-', 56]]
        >>> table7 = convert(table6, 'gender', {'M': 'male', 'F': 'female'})
        >>> look(table7)
        +----------+-------+
        | 'gender' | 'age' |
        +==========+=======+
        | 'male'   | 12    |
        +----------+-------+
        | 'female' | 34    |
        +----------+-------+
        | '-'      | 56    |
        +----------+-------+
    
    Note that the `field` argument can be a list or tuple of fields, in which
    case the conversion will be applied to all of the fields given.
    
    """
    
    converters = dict()
    if len(args) == 1:
        conv = args[0]
    elif len(args) > 1:
        conv = args
    if isinstance(field, (list, tuple)): # allow for multiple fields
        for f in field:
            converters[f] = conv
    else:
        converters[field] = conv
    return fieldconvert(table, converters, **kwargs)

    
def convertall(table, *args, **kwargs):
    """
    Convenience function to convert all fields in the table using a common 
    function or mapping. See also :func:`convert`.
    
    .. versionadded:: 0.4
    
    """
    
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
    Convenience function to convert all field values to numbers where possible. E.g.::

        >>> from petl import convertnumbers, look
        >>> table1 = [['foo', 'bar', 'baz', 'quux'],
        ...           ['1', '3.0', '9+3j', 'aaa'],
        ...           ['2', '1.3', '7+2j', None]]
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
    E.g.::

        >>> from petl import fieldconvert, look    
        >>> table1 = [['foo', 'bar'],
        ...           ['1', '2.4'],
        ...           ['3', '7.9'],
        ...           ['7', '2'],
        ...           ['8.3', '42.0'],
        ...           ['2', 'abc']]
        >>> table2 = fieldconvert(table1, {'foo': int, 'bar': float})
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 1     | 2.4   |
        +-------+-------+
        | 3     | 7.9   |
        +-------+-------+
        | 7     | 2.0   |
        +-------+-------+
        | None  | 42.0  |
        +-------+-------+
        | 2     | None  |
        +-------+-------+

    Converters can also be added or updated using the suffix notation on the
    returned table object. E.g.::

        >>> table3 = [['foo', 'bar', 'baz'],
        ...           ['1', '2.4', 14],
        ...           ['3', '7.9', 47],
        ...           ['7', '2', 11],
        ...           ['8.3', '42.0', 33],
        ...           ['2', 'abc', 'xyz']]
        >>> table4 = fieldconvert(table3)
        >>> look(table4)
        +-------+--------+-------+
        | 'foo' | 'bar'  | 'baz' |
        +=======+========+=======+
        | '1'   | '2.4'  | 14    |
        +-------+--------+-------+
        | '3'   | '7.9'  | 47    |
        +-------+--------+-------+
        | '7'   | '2'    | 11    |
        +-------+--------+-------+
        | '8.3' | '42.0' | 33    |
        +-------+--------+-------+
        | '2'   | 'abc'  | 'xyz' |
        +-------+--------+-------+
        
        >>> table4['foo'] = int
        >>> table4['bar'] = float
        >>> table4['baz'] = lambda v: v**2
        >>> look(table4)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 1     | 2.4   | 196   |
        +-------+-------+-------+
        | 3     | 7.9   | 2209  |
        +-------+-------+-------+
        | 7     | 2.0   | 121   |
        +-------+-------+-------+
        | None  | 42.0  | 1089  |
        +-------+-------+-------+
        | 2     | None  | None  |
        +-------+-------+-------+

    Converters can be functions, method names, or method names with arguments. E.g.::
    
        >>> table5 = [['foo', 'bar', 'baz'],
        ...           ['2', 'A', 'x'],
        ...           ['5', 'B', 'y'],
        ...           ['1', 'C', 'y'],
        ...           ['8.3', 'D', 'z']]
        >>> table6 = fieldconvert(table5)
        >>> table6['foo'] = int
        >>> table6['bar'] = 'lower'
        >>> table6['baz'] = 'replace', 'y', 'yy'
        >>> look(table6)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 2     | 'a'   | 'x'   |
        +-------+-------+-------+
        | 5     | 'b'   | 'yy'  |
        +-------+-------+-------+
        | 1     | 'c'   | 'yy'  |
        +-------+-------+-------+
        | None  | 'd'   | 'z'   |
        +-------+-------+-------+

    Converters can also be dictionaries, which will be used to translate values
    under the specified field.
    
    """

    return FieldConvertView(table, converters, failonerror, errorvalue)


class FieldConvertView(RowContainer):
    
    def __init__(self, source, converters=None, failonerror=False, errorvalue=None):
        self.source = source
        self.converters = converters if converters is not None else dict()
        self.failonerror = failonerror
        self.errorvalue = errorvalue
        
    def __iter__(self):
        return iterfieldconvert(self.source, self.converters, self.failonerror, self.errorvalue)
    
    def __setitem__(self, key, value):
        self.converters[key] = value
        
    def __getitem__(self, key):
        return self.converters[key]

    def cachetag(self):
        try:
            # need to make converters hashable
            convhashable = list()
            for f, c in self.converters.items():
                if isinstance(c, list):
                    convhashable.append((f, tuple(c)))
                elif isinstance(c, dict):
                    convhashable.append((f, tuple(c.items())))
                else:
                    convhashable.append((f, c))
            return hash((self.source.cachetag(), 
                         tuple(convhashable),
                         self.failonerror,
                         self.errorvalue))
        except Exception as e:
            raise Uncacheable(e)
    
    
def iterfieldconvert(source, converters, failonerror, errorvalue):
    it = iter(source)
    converters = converters.copy()
    # normalise converters
    for f, c in converters.items():
        if callable(c):
            pass 
        elif isinstance(c, basestring):
            # assume method name
            converters[f] = methodcaller(c)
        elif isinstance(c, (tuple, list)):
            # assume method name and args
            methnm = c[0]
            methargs = c[1:]
            converters[f] = methodcaller(methnm, *methargs)
        elif isinstance(c, dict):
            converters[f] = dictconverter(c)
        else:
            raise Exception('unexpected converter specification on field %r: %r' % (f, c))
    
    # grab the fields in the source table
    flds = it.next()
    yield tuple(flds) # these are not modified
    
    # define a function to transform a value
    def transform_value(i, v):
        try:
            f = flds[i]
        except IndexError:
            # row is long, just return value as-is
            return v
        else:
            if f not in converters:
                # no converter defined on this field, return value as-is
                return v
            else:
                c = converters[f]
                try:
                    return c(v)
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


def resub(table, field, pattern, repl, count=0, flags=0):
    """
    Convenience function to convert values under the given field using a 
    regular expression substitution. See also :func:`re.sub`.
    
    .. versionadded:: 0.5

    """
    
    prog = re.compile(pattern, flags)
    conv = lambda v: prog.sub(repl, v, count=count)
    return convert(table, field, conv)

    
def extend(table, field, value, failonerror=False, errorvalue=None):
    """
    Extend a table with a fixed value or calculated field. E.g., using a fixed
    value::
    
        >>> from petl import extend, look
        >>> table1 = [['foo', 'bar'],
        ...           ['M', 12],
        ...           ['F', 34],
        ...           ['-', 56]]
        >>> table2 = extend(table1, 'baz', 42)
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

    E.g., calculating the value::
    
        >>> table2 = extend(table1, 'baz', lambda rec: rec['bar'] * 2)
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

    When using a calculated value, the function should accept a record, i.e., a
    dictionary representation of the row, with values indexed by field name.
    
    An expression string can also be used via :func:`expr`, e.g.::

        >>> table3 = extend(table1, 'baz', expr('{bar} * 2'))
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

    """
    
    return ExtendView(table, field, value, failonerror=failonerror, errorvalue=errorvalue)


class ExtendView(RowContainer):
    
    def __init__(self, source, field, value, failonerror=False, errorvalue=None):
        self.source = source
        self.field = field
        self.value = value
        self.failonerror = failonerror
        self.errorvalue = errorvalue
        
    def __iter__(self):
        return iterextend(self.source, self.field, self.value, self.failonerror,
                          self.errorvalue)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.field, self.value))
        except Exception as e:
            raise Uncacheable(e)


def iterextend(source, field, value, failonerror, errorvalue):
    it = iter(source)
    flds = it.next()
    outflds = list(flds)
    outflds.append(field)
    yield tuple(outflds)

    for row in it:
        outrow = list(row) 
        if callable(value):
            rec = asdict(flds, row)
            try:
                outrow.append(value(rec))
            except:
                if failonerror:
                    raise
                else:
                    outrow.append(errorvalue)
        else:
            outrow.append(value)
        yield tuple(outrow)
        
    
def rowslice(table, *sliceargs):
    """
    Choose a subsequence of data rows. E.g.::
    
        >>> from petl import rowslice, look
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 5],
        ...           ['d', 7],
        ...           ['f', 42]]
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

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.sliceargs))
        except Exception as e:
            raise Uncacheable(e)


def iterrowslice(source, sliceargs):    
    it = iter(source)
    yield tuple(it.next()) # fields
    for row in islice(it, *sliceargs):
        yield tuple(row)


def head(table, n=10):
    """
    Choose the first n data rows. E.g.::

        >>> from petl import head, look    
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 5],
        ...           ['d', 7],
        ...           ['f', 42],
        ...           ['f', 3],
        ...           ['h', 90],
        ...           ['k', 12],
        ...           ['l', 77],
        ...           ['q', 2]]
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
    
    Syntactic sugar: equivalent to ``rowslice(table, n)``.
    
    """
    
    return rowslice(table, n)

        
def tail(table, n=10):
    """
    Choose the last n data rows. E.g.::

        >>> from petl import tail, look    
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 5],
        ...           ['d', 7],
        ...           ['f', 42],
        ...           ['f', 3],
        ...           ['h', 90],
        ...           ['k', 12],
        ...           ['l', 77],
        ...           ['q', 2]]
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

    """

    return TailView(table, n)


class TailView(RowContainer):
    
    def __init__(self, source, n):
        self.source = source
        self.n = n
        
    def __iter__(self):
        return itertail(self.source, self.n)

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.n))
        except Exception as e:
            raise Uncacheable(e)


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


def sort(table, key=None, reverse=False, buffersize=None):
    """
    Sort the table. E.g.::
    
        >>> from petl import sort, look
        >>> table1 = [['foo', 'bar'],
        ...           ['C', 2],
        ...           ['A', 9],
        ...           ['A', 6],
        ...           ['F', 1],
        ...           ['D', 10]]
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

    Sorting by compound key is supported, e.g.::
    
        >>> table3 = sort(table1, key=['foo', 'bar'])
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

    Field names or indices (from zero) can be used to specify the key.
    
    If no key is specified, the default is a lexical sort, e.g.::

        >>> table4 = sort(table1)
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

    """
    
    return SortView(table, key=key, reverse=reverse, buffersize=buffersize)
    

def iterchunk(filename):
    with open(filename, 'rb') as f:
        try:
            while True:
                yield pickle.load(f)
        except EOFError:
            pass


Keyed = namedtuple('Keyed', ['key', 'obj'])
    
    
def mergesort(key=None, reverse=False, *iterables):

    # N.B., I've used heapq for normal merge sort and shortlist merge sort for reverse
    # merge sort because I've assumed that heapq.merge is faster and so is preferable
    # but it doesn't support reverse sorting so the shortlist merge sort has to
    # be used for reverse sorting. Some casual profiling suggests there isn't much
    # between the two in terms of speed, but might be worth profiling more carefully
    
    if reverse:
        return shortlistmergesort(key, True, *iterables)
    else:
        return heapqmergesort(key, *iterables)


def heapqmergesort(key=None, *iterables):            
    if key is None:
        keyed_iterables = iterables
        for element in heapq.merge(*keyed_iterables):
            yield element
    else:
        keyed_iterables = [(Keyed(key(obj), obj) for obj in iterable) for iterable in iterables]
        for element in heapq.merge(*keyed_iterables):
            yield element.obj


def shortlistmergesort(key=None, reverse=False, *iterables):
    if reverse:
        op = max
    else:
        op = min
    if key is not None:
        opkwargs = {'key': key}
    else:
        opkwargs = dict()
    iterators = [iter(iterable) for iterable in iterables]
    shortlist = [it.next() for it in iterators]
    while iterators:
        next = op(shortlist, **opkwargs)
        yield next
        nextidx = shortlist.index(next)
        try:
            shortlist[nextidx] = iterators[nextidx].next()
        except StopIteration:
            del shortlist[nextidx]
            del iterators[nextidx]
        
    
defaultbuffersize = 100000
    
    
class SortView(RowContainer):
    
    def __init__(self, source, key=None, reverse=False, buffersize=None):
        self.source = source
        self.key = key
        self.reverse = reverse
        if buffersize is None:
            self.buffersize = defaultbuffersize
        else:
            self.buffersize = buffersize
        self._fldcache = None
        self._memcache = None
        self._filecache = None
        self._internalcachetag = None
        self._getkey = None
        
    def _clearcache(self):
        self._internalcachetag = None
        self._fldcache = None
        self._memcache = None
        self._filecache = None
        self._getkey = None
        
    def __iter__(self):
        source = self.source
        key = self.key
        reverse = self.reverse
        try:
            currcachetag = self.cachetag()
            if self._internalcachetag == currcachetag and self._memcache is not None:
                return self._iterfrommemcache()
            elif self._internalcachetag == currcachetag and self._filecache is not None:
                return self._iterfromfilecache()
            else:
                return self._iternocache(source, key, reverse)
        except Uncacheable:
            return self._iternocache(source, key, reverse)
        
    def _iterfrommemcache(self):
        yield tuple(self._fldcache)
        for row in self._memcache:
            yield tuple(row)
            
    def _iterfromfilecache(self):
        yield tuple(self._fldcache)
        chunkiters = [iterchunk(f.name) for f in self._filecache]
        for row in mergesort(self._getkey, self.reverse, *chunkiters):
            yield tuple(row)
        
    def _iternocache(self, source, key, reverse):
        self._clearcache()
        it = iter(source)

        flds = it.next()
        yield tuple(flds)
        
        getkey = None
        if key is not None:
            # convert field selection into field indices
            indices = asindices(flds, key)
            # now use field indices to construct a _getkey function
            # N.B., this will probably raise an exception on short rows
            getkey = itemgetter(*indices)
        
        # initialise the first chunk
        rows = list(islice(it, 0, self.buffersize))
        rows.sort(key=getkey, reverse=reverse)
        
        # have we exhausted the source iterator?
        if self.buffersize is None or len(rows) < self.buffersize:

            try:
                # TODO possible race condition here, attributes determining
                # cachetag have changed since we entered this function?
                self._internalcachetag = self.cachetag()
                self._fldcache = flds
                self._memcache = rows
                self._getkey = getkey
            except Uncacheable:
                pass
    
            for row in rows:
                yield tuple(row)
                
        else:

            chunkfiles = []  
            
            while rows:
            
                # dump the chunk
                f = NamedTemporaryFile(delete=False)
                for row in rows:
                    pickle.dump(row, f, protocol=-1)
                f.close()
                chunkfiles.append(f)
                
                # grab the next chunk
                rows = list(islice(it, 0, self.buffersize))
                rows.sort(key=getkey, reverse=reverse)

            try:
                # TODO possible race condition here, attributes determining
                # cachetag have changed since we entered this function?
                self._internalcachetag = self.cachetag()
                self._fldcache = flds
                self._filecache = chunkfiles
                self._getkey = getkey
            except Uncacheable:
                pass

            chunkiters = [iterchunk(f.name) for f in chunkfiles]
            for row in mergesort(getkey, reverse, *chunkiters):
                yield tuple(row)

        
    def cachetag(self):
        try:
            return hash((self.key, self.reverse, self.source.cachetag()))
        except Exception as e:
            raise Uncacheable(e)


def melt(table, key=None, variables=None, variablefield='variable', valuefield='value'):
    """
    Reshape a table, melting fields into data. E.g.::

        >>> from petl import melt, look
        >>> table1 = [['id', 'gender', 'age'],
        ...           [1, 'F', 12],
        ...           [2, 'M', 17],
        ...           [3, 'M', 16]]
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

    Compound keys are supported, e.g.::
    
        >>> table3 = [['id', 'time', 'height', 'weight'],
        ...           [1, 11, 66.4, 12.2],
        ...           [2, 16, 53.2, 17.3],
        ...           [3, 12, 34.5, 9.4]]
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

    A subset of variable fields can be selected, e.g.::
    
        >>> table5 = melt(table3, key=['id', 'time'], variables=['height'])    
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
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), 
                         self.key, 
                         self.variables,
                         self.variablefield,
                         self.valuefield))
        except Exception as e:
            raise Uncacheable(e)


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
        >>> table1 = [['id', 'variable', 'value'],
        ...           [3, 'age', 16],
        ...           [1, 'gender', 'F'],
        ...           [2, 'gender', 'M'],
        ...           [2, 'age', 17],
        ...           [1, 'age', 12],
        ...           [3, 'gender', 'M']]
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

    If variable and value fields are different from the defaults, e.g.::
    
        >>> table3 = [['id', 'vars', 'vals'],
        ...           [3, 'age', 16],
        ...           [1, 'gender', 'F'],
        ...           [2, 'gender', 'M'],
        ...           [2, 'age', 17],
        ...           [1, 'age', 12],
        ...           [3, 'gender', 'M']]
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

    If there are multiple values for each key/variable pair, and no reducers
    function is provided, then all values will be listed. E.g.::
    
        >>> table6 = [['id', 'time', 'variable', 'value'],
        ...           [1, 11, 'weight', 66.4],
        ...           [1, 14, 'weight', 55.2],
        ...           [2, 12, 'weight', 53.2],
        ...           [2, 16, 'weight', 43.3],
        ...           [3, 12, 'weight', 34.5],
        ...           [3, 17, 'weight', 49.4]]
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

    Multiple values can be reduced via an aggregation function, e.g.::

        >>> def mean(values):
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

    Missing values are padded with whatever is provided via the `missing` 
    keyword argument (`None` by default), e.g.::

        >>> table9 = [['id', 'variable', 'value'],
        ...           [1, 'gender', 'F'],
        ...           [2, 'age', 17],
        ...           [1, 'age', 12],
        ...           [3, 'gender', 'M']]
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

    def cachetag(self):
        try:
            return hash((self.source.cachetag(),
                         tuple(self.key) if isinstance(self.key, list) else self.key,
                         self.variablefield,
                         self.valuefield,
                         self.samplesize,
                         tuple(self.reducers.items()) if self.reducers is not None else self.reducers,
                         self.missing))
        except Exception as e:
            print e
            raise Uncacheable(e)


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
    getkey = itemgetter(*keyindices)
    
    # process sorted data in newfields
    groups = groupby(it, key=getkey)
    for key_value, group in groups:
        group = list(group) # may need to iterate over the group more than once
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
                
            
def duplicates(table, key, presorted=False, buffersize=None):
    """
    Select rows with duplicate values under a given key. E.g.::

        >>> from petl import duplicates, look    
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, 2.0],
        ...           ['B', 2, 3.4],
        ...           ['D', 6, 9.3],
        ...           ['B', 3, 7.8],
        ...           ['B', 2, 12.3],
        ...           ['E', None, 1.3],
        ...           ['D', 4, 14.5]]
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

    Compound keys are supported, e.g.::
    
        >>> table3 = duplicates(table1, key=['foo', 'bar'])
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'B'   | 2     | 3.4   |
        +-------+-------+-------+
        | 'B'   | 2     | 12.3  |
        +-------+-------+-------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """
    
    return DuplicatesView(table, key, presorted, buffersize)


class DuplicatesView(RowContainer):
    
    def __init__(self, source, key, presorted=False, buffersize=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize)
        self.key = key # TODO property
        
    def __iter__(self):
        return iterduplicates(self.source, self.key)

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.key))
        except Exception as e:
            raise Uncacheable(e)


def iterduplicates(source, key):
    # assume source is sorted
    # first need to sort the data
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
                if not previous_yielded:
                    yield tuple(previous)
                    previous_yielded = True
                yield tuple(row)
            else:
                # reset
                previous_yielded = False
            previous = row
    
    
def conflicts(table, key, missing=None, ignore=None, presorted=False, buffersize=None):
    """
    Select rows with the same key value but differing in some other field. E.g.::

        >>> from petl import conflicts, look    
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, 2.7],
        ...           ['B', 2, None],
        ...           ['D', 3, 9.4],
        ...           ['B', None, 7.8],
        ...           ['E', None],
        ...           ['D', 3, 12.3],
        ...           ['A', 2, None]]
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

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    .. versionchanged:: 0.5
    
    One or more fields can be ignored when determining conflicts by providing
    the `ignore` keyword argument. E.g.::

        >>> table3 = conflicts(table1, 'foo', ignore='baz')
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | 2.7   |
        +-------+-------+-------+
        | 'A'   | 2     | None  |
        +-------+-------+-------+
    
    """
    
    return ConflictsView(table, key, missing=missing, ignore=ignore,
                         presorted=presorted, buffersize=buffersize)


class ConflictsView(RowContainer):
    
    def __init__(self, source, key, missing=None, ignore=None, presorted=False, buffersize=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize)
        self.key = key
        self.missing = missing
        self.ignore = ignore
        
    def __iter__(self):
        return iterconflicts(self.source, self.key, self.missing, self.ignore)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.key, self.missing, self.ignore))
        except Exception as e:
            raise Uncacheable(e)

    
def iterconflicts(source, key, missing, ignore):
    # normalise ignore
    if isinstance(ignore, basestring):
        ignore = (ignore,)
        
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
                    if ignore is None or f not in ignore:
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
    

def complement(a, b, presorted=False, buffersize=None):
    """
    Return rows in `a` that are not in `b`. E.g.::
    
        >>> from petl import complement, look
        >>> a = [['foo', 'bar', 'baz'],
        ...      ['A', 1, True],
        ...      ['C', 7, False],
        ...      ['B', 2, False],
        ...      ['C', 9, True]]
        >>> b = [['x', 'y', 'z'],
        ...      ['B', 2, False],
        ...      ['A', 9, False],
        ...      ['B', 3, True],
        ...      ['C', 9, True]]
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
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """
    
    return ComplementView(a, b, presorted=presorted, buffersize=buffersize)


class ComplementView(RowContainer):
    
    def __init__(self, a, b, presorted=False, buffersize=None):
        if presorted:
            self.a = a
            self.b = b
        else:
            self.a = sort(a, buffersize=buffersize)
            self.b = sort(b, buffersize=buffersize)
            
    def __iter__(self):
        return itercomplement(self.a, self.b)

    def cachetag(self):
        try:
            return hash((self.a.cachetag(), self.b.cachetag()))
        except Exception as e:
            raise Uncacheable(e)


def itercomplement(a, b):
    ita = iter(a) 
    itb = iter(b)
    aflds = ita.next()
    itb.next() # ignore b fields
    yield tuple(aflds)
    
    a = tuple(ita.next())
    b = tuple(itb.next())
    # we want the elements in a that are not in b
    while True:
        if b is None or a < b:
            yield a
            try:
                a = tuple(ita.next())
            except StopIteration:
                break
        elif a == b:
            try:
                a = tuple(ita.next())
            except StopIteration:
                break
        else:
            try:
                b = tuple(itb.next())
            except StopIteration:
                b = None
        
    
def recordcomplement(a, b, buffersize=None):
    """
    Find records in `a` that are not in `b`. E.g.::
    
        >>> from petl import recordcomplement, look
        >>> tablea = (('foo', 'bar', 'baz'),
        ...           ('A', 1, True),
        ...           ('C', 7, False),
        ...           ('B', 2, False),
        ...           ('C', 9, True))
        >>> tableb = (('bar', 'foo', 'baz'),
        ...           (2, 'B', False),
        ...           (9, 'A', False),
        ...           (3, 'B', True),
        ...           (9, 'C', True))
        >>> aminusb = recordcomplement(tablea, tableb)
        >>> look(aminusb)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        
        >>> bminusa = recordcomplement(tableb, tablea)
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
    
    See also the discussion of the `buffersize` argument under the :func:`sort` 
    function.
    
    .. versionadded:: 0.3
    
    """
    
    ha = header(a)
    hb = header(b)
    assert set(ha) == set(hb), 'both tables must have the same set of fields'
    # make sure fields are in the same order
    bv = cut(b, *ha)
    return complement(a, bv, buffersize=buffersize)


def diff(a, b, presorted=False, buffersize=None):
    """
    Find the difference between rows in two tables. Returns a pair of tables, e.g.::
    
        >>> from petl import diff, look
        >>> a = [['foo', 'bar', 'baz'],
        ...      ['A', 1, True],
        ...      ['C', 7, False],
        ...      ['B', 2, False],
        ...      ['C', 9, True]]
        >>> b = [['x', 'y', 'z'],
        ...      ['B', 2, False],
        ...      ['A', 9, False],
        ...      ['B', 3, True],
        ...      ['C', 9, True]]
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
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """

    if not presorted:    
        a = sort(a)
        b = sort(b)
    added = complement(b, a, presorted=True, buffersize=buffersize)
    subtracted = complement(a, b, presorted=True, buffersize=buffersize)
    return added, subtracted
    
    
def recorddiff(a, b, buffersize=None):
    """
    Find the difference between records in two tables. E.g.::

        >>> from petl import recorddiff, look    
        >>> tablea = (('foo', 'bar', 'baz'),
        ...           ('A', 1, True),
        ...           ('C', 7, False),
        ...           ('B', 2, False),
        ...           ('C', 9, True))
        >>> tableb = (('bar', 'foo', 'baz'),
        ...           (2, 'B', False),
        ...           (9, 'A', False),
        ...           (3, 'B', True),
        ...           (9, 'C', True))
        >>> added, subtracted = recorddiff(tablea, tableb)
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

    See also the discussion of the `buffersize` argument under the :func:`sort` 
    function.
    
    .. versionadded:: 0.3
    
    """

    added = recordcomplement(b, a, buffersize=buffersize)
    subtracted = recordcomplement(a, b, buffersize=buffersize)
    return added, subtracted
    
    
def capture(table, field, pattern, newfields=None, include_original=False, 
            flags=0):
    """
    Extend the table with one or more new fields with values captured from an
    existing field searched via a regular expression. E.g.::

        >>> from petl import capture, look
        >>> table1 = [['id', 'variable', 'value'],
        ...           ['1', 'A1', '12'],
        ...           ['2', 'A2', '15'],
        ...           ['3', 'B1', '18'],
        ...           ['4', 'C12', '19']]
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

    See also :func:`re.search`.
    
    By default the field on which the capture is performed is omitted. It can
    be included using the `include_original` argument, e.g.::
    
        >>> table3 = capture(table1, 'variable', '(\\w)(\\d+)', ['treat', 'time'], include_original=True)
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

    """
    
    return CaptureView(table, field, pattern, newfields, include_original, flags)


class CaptureView(RowContainer):
    
    def __init__(self, source, field, pattern, newfields=None, 
                 include_original=False, flags=0):
        self.source = source
        self.field = field
        self.pattern = pattern
        self.newfields = newfields
        self.include_original = include_original
        self.flags = flags
        
    def __iter__(self):
        return itercapture(self.source, self.field, self.pattern, self.newfields, 
                           self.include_original, self.flags)

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), 
                         self.field,
                         self.pattern,
                         tuple(self.newfields),
                         self.include_original,
                         self.flags))
        except Exception as e:
            raise Uncacheable(e)


def itercapture(source, field, pattern, newfields, include_original, flags):
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
        out_row.extend(prog.search(value).groups())
        yield tuple(out_row)
        
        
def split(table, field, pattern, newfields=None, include_original=False,
          maxsplit=0, flags=0):
    """
    Extend the table with one or more new fields with values generated by 
    splitting an existing value around occurrences of a regular expression. 
    E.g.::

        >>> from petl import split, look
        >>> table1 = [['id', 'variable', 'value'],
        ...           ['1', 'parad1', '12'],
        ...           ['2', 'parad2', '15'],
        ...           ['3', 'tempd1', '18'],
        ...           ['4', 'tempd2', '19']]
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

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), 
                         self.field,
                         self.pattern,
                         tuple(self.newfields),
                         self.include_original,
                         self.maxsplit,
                         self.flags))
        except Exception as e:
            raise Uncacheable(e)


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
    Select rows meeting a condition. The second positional argument can be a function
    accepting a record (i.e., a dictionary representation of a row) e.g.::
    
        >>> from petl import select, look     
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 4, 9.3],
        ...           ['a', 2, 88.2],
        ...           ['b', 1, 23.3],
        ...           ['c', 8, 42.0],
        ...           ['d', 7, 100.9],
        ...           ['c', 2]]
        >>> table2 = select(table1, lambda rec: rec['foo'] == 'a' and rec['baz'] > 88.1)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+

    The second positional argument can also be an expression string, which will be converted
    to a function using :func:`expr`, e.g.::
    
        >>> table3 = select(table1, "{foo} == 'a' and {baz} > 88.1")
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+
        
    The condition can also be applied to a single field, e.g.::
    
        >>> table4 = select(table1, 'foo', lambda v: v == 'a')
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
        return RecordSelectView(table, where, missing=missing, complement=complement)
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

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return RecordSelectView(table, where, missing=missing, complement=complement)
        
        
class RecordSelectView(RowContainer):
    
    def __init__(self, source, where, missing=None, complement=False):
        self.source = source
        self.where = where
        self.missing = missing
        self.complement = complement
        
    def __iter__(self):
        return iterselect(self.source, self.where, self.missing, self.complement)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.where, self.missing, self.complement))
        except Exception as e:
            raise Uncacheable(e)

    
def iterselect(source, where, missing, complement):
    it = iter(source)
    flds = it.next()
    yield tuple(flds)
    for row in it:
        rec = asdict(flds, row, missing)
        if where(rec) != complement: # XOR
            yield tuple(row)
        
        
def rowselect(table, where, complement=False):
    """
    Select rows matching a condition. The `where` argument should be a function
    accepting a row (list or tuple) as argument and returning True or False.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return RowSelectView(table, where, complement=complement)
        
        
class RowSelectView(RowContainer):
    
    def __init__(self, source, where, complement=False):
        self.source = source
        self.where = where
        self.complement = complement
        
    def __iter__(self):
        return iterrowselect(self.source, self.where, self.complement)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.where, self.complement))
        except Exception as e:
            raise Uncacheable(e)

    
def iterrowselect(source, where, complement):
    it = iter(source)
    flds = it.next()
    yield tuple(flds)
    for row in it:
        if where(row) != complement: # XOR
            yield tuple(row)
     
     
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

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

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

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.field, self.where, self.complement))
        except Exception as e:
            raise Uncacheable(e)
    
    
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
        >>> from collections import OrderedDict
        >>> table1 = [['id', 'sex', 'age', 'height', 'weight'],
        ...           [1, 'male', 16, 1.45, 62.0],
        ...           [2, 'female', 19, 1.34, 55.4],
        ...           [3, 'female', 17, 1.78, 74.4],
        ...           [4, 'male', 21, 1.33, 45.2],
        ...           [5, '-', 25, 1.65, 51.9]]
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

    Field mappings can also be added and/or updated after the table is created via
    the suffix notation. E.g.::
    
        >>> table3 = fieldmap(table1)
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
        
    def __getitem__(self, key):
        return self.mappings[key]
    
    def __setitem__(self, key, value):
        self.mappings[key] = value
        
    def __iter__(self):
        return iterfieldmap(self.source, self.mappings, self.failonerror, self.errorvalue)
    
    def cachetag(self):
        try:
            # need to make converters hashable
            maphashable = list()
            for outfld, m in self.mappings.items():
                if isinstance(m, (tuple, list)) and len(m) == 2:
                    srcfld = m[0]
                    fm = m[1]
                    if isinstance(fm, dict):
                        maphashable.append((outfld, srcfld, tuple(fm.items())))
                    else:
                        maphashable.append((outfld, srcfld, fm))
                else:
                    maphashable.append((outfld, m))
            return hash((self.source.cachetag(), 
                         tuple(maphashable),
                         self.failonerror,
                         self.errorvalue))
        except Exception as e:
            raise Uncacheable(e)
    
    
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
    Return a dictionary mapping field values to tables. E.g.::
    
        >>> from petl import facet, look
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 4, 9.3],
        ...           ['a', 2, 88.2],
        ...           ['b', 1, 23.3],
        ...           ['c', 8, 42.0],
        ...           ['d', 7, 100.9],
        ...           ['c', 2]]
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


def selectin(table, field, value, complement=False):
    """
    Select rows where the given field is a member of the given value.

    .. versionchanged:: 0.4
    
    The complement of the selection can be returned (i.e., the query can be 
    inverted) by providing `complement=True` as a keyword argument.

    """
    
    return selectop(table, field, value, operator.contains, complement=complement)


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
        >>> table1 = (('foo', 'bar', 'baz'),
        ...           ('aa', 4, 9.3),
        ...           ('aaa', 2, 88.2),
        ...           ('b', 1, 23.3),
        ...           ('ccc', 8, 42.0),
        ...           ('bb', 7, 100.9),
        ...           ('c', 2))
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


def rowreduce(table, key, reducer, fields=None, presorted=False, buffersize=None):
    """
    Reduce rows grouped under the given key via an arbitrary function. E.g.::

        >>> from petl import rowreduce, look    
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 3],
        ...           ['a', 7],
        ...           ['b', 2],
        ...           ['b', 1],
        ...           ['b', 9],
        ...           ['c', 4]]
        >>> def sumbar(key, rows):
        ...     return [key, sum([row[1] for row in rows])]
        ... 
        >>> table2 = rowreduce(table1, key='foo', reducer=sumbar, fields=['foo', 'barsum'])
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

    The `reducer` function should accept two arguments, a key and a sequence
    of rows, and return a single row.

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """

    return RowReduceView(table, key, reducer, fields, presorted, buffersize)


class RowReduceView(RowContainer):
    
    def __init__(self, source, key, reducer, fields=None, presorted=False, buffersize=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize)
        self.key = key
        self.fields = fields
        self.reducer = reducer

    def __iter__(self):
        return iterrowreduce(self.source, self.key, self.reducer, self.fields)

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.key, 
                         tuple(self.fields) if self.fields else self.fields, 
                         self.reducer))
        except Exception as e:
            raise Uncacheable(e)

    
def iterrowreduce(source, key, reducer, fields):
    it = iter(source)

    srcflds = it.next()
    if fields is None:
        yield tuple(srcflds)
    else:
        yield tuple(fields)

    # convert field selection into field indices
    indices = asindices(srcflds, key)
    
    # now use field indices to construct a _getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    for key, rows in groupby(it, key=getkey):
        yield tuple(reducer(key, rows))
        

def recordreduce(table, key, reducer, fields=None, presorted=False, buffersize=None):
    """
    Reduce records grouped under the given key via an arbitrary function. E.g.::

        >>> from petl import recordreduce, look    
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 3],
        ...           ['a', 7],
        ...           ['b', 2],
        ...           ['b', 1],
        ...           ['b', 9],
        ...           ['c', 4]]
        >>> def sumbar(key, records):
        ...     return [key, sum([rec['bar'] for rec in records])]
        ... 
        >>> table2 = recordreduce(table1, key='foo', reducer=sumbar, fields=['foo', 'barsum'])
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

    The `reducer` function should accept two arguments, a key and a sequence
    of records (i.e., dictionaries of values indexed by field) and return a single
    row.
    
    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """

    return RecordReduceView(table, key, reducer, fields, presorted, buffersize)


class RecordReduceView(RowContainer):
    
    def __init__(self, source, key, reducer, fields=None, presorted=False,
                 buffersize=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize)
        self.key = key
        self.fields = fields
        self.reducer = reducer

    def __iter__(self):
        return iterrecordreduce(self.source, self.key, self.reducer, self.fields)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.key, 
                         tuple(self.fields) if self.fields else self.fields, 
                         self.reducer))
        except Exception as e:
            raise Uncacheable(e)

    
def iterrecordreduce(source, key, reducer, fields):
    it = iter(source)

    srcflds = it.next()
    if fields is None:
        yield tuple(srcflds)
    else:
        yield tuple(fields)

    # convert field selection into field indices
    indices = asindices(srcflds, key)
    
    # now use field indices to construct a _getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    for key, rows in groupby(it, key=getkey):
        records = [asdict(srcflds, row) for row in rows]
        yield tuple(reducer(key, records))
    

def mergereduce(table, key, missing=None, presorted=False, buffersize=None):
    """
    Merge rows under the given key. E.g.::
    
        >>> from petl import mergereduce, look    
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, 2.7],
        ...           ['B', 2, None],
        ...           ['D', 3, 9.4],
        ...           ['B', None, 7.8],
        ...           ['E', None],
        ...           ['D', 3, 12.3],
        ...           ['A', 2, None]]
        >>> table2 = mergereduce(table1, 'foo')
        >>> look(table2)
        +-------+--------+-------------+
        | 'foo' | 'bar'  | 'baz'       |
        +=======+========+=============+
        | 'A'   | (1, 2) | 2.7         |
        +-------+--------+-------------+
        | 'B'   | 2      | 7.8         |
        +-------+--------+-------------+
        | 'D'   | 3      | (9.4, 12.3) |
        +-------+--------+-------------+
        | 'E'   | None   |             |
        +-------+--------+-------------+

    Missing values are overridden by non-missing values. Conflicting values are
    reported as a tuple.
    
    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    .. versionchanged:: 0.3
    
    Previously conflicts were reported as a list, this is changed to a tuple in 
    version 0.3.
    
    """

    def _mergereducer(key, rows):
        merged = list()
        for row in rows:
            for i, v in enumerate(row):
                if i == len(merged):
                    merged.append(list())
                if v != missing and v not in merged[i]:
                    merged[i].append(v)    
        # replace singletons and empty lists
        merged = [vals[0] if len(vals) == 1 else missing if len(vals) == 0 else tuple(vals) for vals in merged]
        return merged
    
    return rowreduce(table, key, reducer=_mergereducer, presorted=presorted, buffersize=buffersize)


def merge(*tables, **kwargs):
    """
    Convenience function to concatenate multiple tables (via :func:`cat`) then 
    reduce rows by merging under the given key (via :func:`mergereduce`). E.g.::
    
        >>> from petl import look, merge
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           [1, 'A', True],
        ...           [2, 'B', None],
        ...           [4, 'C', True]]
        >>> table2 = [['bar', 'baz', 'quux'],
        ...           ['A', True, 42.0],
        ...           ['B', False, 79.3],
        ...           ['C', False, 12.4]]
        >>> table3 = merge(table1, table2, key='bar')
        >>> look(table3)
        +-------+-------+---------------+--------+
        | 'foo' | 'bar' | 'baz'         | 'quux' |
        +=======+=======+===============+========+
        | 1     | 'A'   | True          | 42.0   |
        +-------+-------+---------------+--------+
        | 2     | 'B'   | False         | 79.3   |
        +-------+-------+---------------+--------+
        | 4     | 'C'   | (False, True) | 12.4   |
        +-------+-------+---------------+--------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """
    
    assert 'key' in kwargs, 'keyword argument "key" is required'
    key = kwargs['key']
    missing = kwargs['missing'] if 'missing' in kwargs else None
    presorted = kwargs['presorted'] if 'presorted' in kwargs else False
    buffersize = kwargs['buffersize'] if 'buffersize' in kwargs else None
    t1 = cat(*tables, missing=missing)
    t2 = mergereduce(t1, key=key, presorted=presorted, buffersize=buffersize)
    return t2


def aggregate(table, key, aggregators=None, failonerror=False, errorvalue=None,
              presorted=False, buffersize=None):
    """
    Group rows under the given key then apply aggregation functions. E.g.::

        >>> from petl import aggregate, look
        >>> from collections import OrderedDict
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 3],
        ...           ['a', 7],
        ...           ['b', 2],
        ...           ['b', 1],
        ...           ['b', 9],
        ...           ['c', 4],
        ...           ['d', 3],
        ...           ['d'],
        ...           ['e']]
        >>> aggregators = OrderedDict()
        >>> aggregators['minbar'] = 'bar', min
        >>> aggregators['maxbar'] = 'bar', max
        >>> aggregators['sumbar'] = 'bar', sum
        >>> aggregators['listbar'] = 'bar', list
        >>> table2 = aggregate(table1, 'foo', aggregators)
        >>> look(table2)
        +-------+----------+----------+----------+-----------+
        | 'foo' | 'minbar' | 'maxbar' | 'sumbar' | 'listbar' |
        +=======+==========+==========+==========+===========+
        | 'a'   | 3        | 7        | 10       | [3, 7]    |
        +-------+----------+----------+----------+-----------+
        | 'b'   | 1        | 9        | 12       | [2, 1, 9] |
        +-------+----------+----------+----------+-----------+
        | 'c'   | 4        | 4        | 4        | [4]       |
        +-------+----------+----------+----------+-----------+
        | 'd'   | 3        | 3        | 3        | [3]       |
        +-------+----------+----------+----------+-----------+
        | 'e'   | None     | None     | 0        | []        |
        +-------+----------+----------+----------+-----------+

    Aggregation functions can also be added and/or updated using the suffix
    notation on the returned table object, e.g.::
    
        >>> table3 = aggregate(table1, 'foo')
        >>> table3['minbar'] = 'bar', min
        >>> table3['maxbar'] = 'bar', max
        >>> table3['sumbar'] = 'bar', sum
        >>> table3['listbar'] = 'bar' # default aggregation is list
        >>> look(table3)
        +-------+----------+----------+----------+-----------+
        | 'foo' | 'minbar' | 'maxbar' | 'sumbar' | 'listbar' |
        +=======+==========+==========+==========+===========+
        | 'a'   | 3        | 7        | 10       | [3, 7]    |
        +-------+----------+----------+----------+-----------+
        | 'b'   | 1        | 9        | 12       | [2, 1, 9] |
        +-------+----------+----------+----------+-----------+
        | 'c'   | 4        | 4        | 4        | [4]       |
        +-------+----------+----------+----------+-----------+
        | 'd'   | 3        | 3        | 3        | [3]       |
        +-------+----------+----------+----------+-----------+
        | 'e'   | None     | None     | 0        | []        |
        +-------+----------+----------+----------+-----------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """

    return AggregateView(table, key, 
                         aggregators=aggregators, 
                         failonerror=failonerror,
                         errorvalue=errorvalue, 
                         presorted=presorted, 
                         buffersize=buffersize)


class AggregateView(RowContainer):
    
    def __init__(self, source, key, aggregators=None, failonerror=False,
                 errorvalue=None, presorted=False, buffersize=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize)
        self.key = key
        if aggregators is None:
            self.aggregators = OrderedDict()
        else:
            self.aggregators = aggregators
        self.failonerror = failonerror
        self.errorvalue = errorvalue

    def __iter__(self):
        return iteraggregate(self.source, self.key, self.aggregators, 
                             self.failonerror, self.errorvalue)
    
    def __getitem__(self, key):
        return self.aggregators[key]
    
    def __setitem__(self, key, value):
        self.aggregators[key] = value

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.key, self.failonerror,
                         self.errorvalue, tuple(self.aggregators.items())))
        except Exception as e:
            raise Uncacheable(e)

    
def iteraggregate(source, key, aggregators, failonerror, errorvalue):
    aggregators = OrderedDict(aggregators.items()) # take a copy
    it = iter(source)
    srcflds = it.next()

    # normalise aggregators
    for outfld in aggregators:
        agg = aggregators[outfld]
        if not isinstance(agg, (list, tuple)):
            aggregators[outfld] = agg, list # list is default aggregation function
        
    # convert field selection into field indices
    indices = asindices(srcflds, key)
    
    # now use field indices to construct a _getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    if len(indices) == 1:
        outflds = [getkey(srcflds)]
    else:
        outflds = list(getkey(srcflds))
    outflds.extend(aggregators.keys())
    yield tuple(outflds)
    
    for key, rows in groupby(it, key=getkey):
        rows = list(rows) # may need to iterate over these more than once
        if isinstance(key, basestring):
            outrow = [key]
        else:
            outrow = list(key)
        for outfld in aggregators:
            srcfld, aggfun = aggregators[outfld]
            idx = srcflds.index(srcfld)
            try:
                # try using list comprehension
                vals = [row[idx] for row in rows]
            except IndexError:
                # fall back to slower for loop
                vals = list()
                for row in rows:
                    try:
                        vals.append(row[idx])
                    except IndexError:
                        pass
            try:
                aggval = aggfun(vals)
            except:
                if failonerror:
                    raise
                else:
                    aggval = errorvalue
            outrow.append(aggval)
        yield tuple(outrow)
            

def rangerowreduce(table, key, width, reducer, fields=None, minv=None, maxv=None, 
                   failonerror=False, presorted=False, buffersize=None):
    """
    Reduce rows grouped into bins under the given key via an arbitrary function. 
    E.g.::

        >>> from petl import rangerowreduce, look
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 3],
        ...           ['a', 7],
        ...           ['b', 2],
        ...           ['b', 1],
        ...           ['b', 9],
        ...           ['c', 4]]
        >>> def redu(minv, maxunpack, rows):
        ...     return [minv, maxunpack, ''.join([row[0] for row in rows])]
        ... 
        >>> table2 = rangerowreduce(table1, 'bar', 2, reducer=redu, fields=['frombar', 'tobar', 'foos'])
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

    """
    
    return RangeRowReduceView(table, key, width, reducer, fields=fields, minv=minv, 
                              maxv=maxv, failonerror=failonerror,  
                              presorted=presorted, buffersize=buffersize)
        

class RangeRowReduceView(RowContainer):
    
    def __init__(self, source, key, width, reducer, fields=None, minv=None, maxv=None, 
                 failonerror=False, presorted=False, buffersize=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize)
        self.key = key
        self.width = width
        self.reducer = reducer
        self.fields = fields
        self.minv, self.maxunpack = minv, maxv
        self.failonerror = failonerror

    def __iter__(self):
        return iterrangerowreduce(self.source, self.key, self.width, self.reducer,
                                  self.fields, self.minv, self.maxunpack, self.failonerror)

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.key, self.width,
                         tuple(self.fields) if self.fields else self.fields, 
                         self.reducer, self.minv, self.maxunpack, self.failonerror))
        except Exception as e:
            raise Uncacheable(e)


def iterrangerowreduce(source, key, width, reducer, fields, minv, maxv, failonerror):

    it = iter(source)
    srcflds = it.next()
    if fields is None:
        yield tuple(srcflds)
    else:
        yield tuple(fields)

    # convert field selection into field indices
    indices = asindices(srcflds, key)

    # now use field indices to construct a getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    # initialise minimum
    row = it.next()
    keyv = getkey(row)
    if minv is None:
        minv = keyv

    # iterate over bins
    # N.B., we need to account for two possible scenarios
    # (1) maxunpack is not specified, so keep making bins until we run out of rows
    # (2) maxunpack is specified, so iterate over bins 
    try:
        for binminv in count(minv, width):
            binmaxv = binminv + width
            if maxv is not None and binmaxv >= maxv: # final bin
                binmaxv = maxv
            bin = []
            while keyv < binminv:
                row = it.next()
                keyv = getkey(row)
            while binminv <= keyv < binmaxv:
                bin.append(row)
                row = it.next()
                keyv = getkey(row)
            while maxv is not None and keyv == binmaxv == maxv:
                bin.append(row) # last bin is open
                row = it.next()
                keyv = getkey(row)
            try:
                yield tuple(reducer(binminv, binmaxv, bin))
            except:
                if failonerror:
                    raise
            if maxv is not None and binmaxv == maxv:
                break
    except StopIteration:
        # don't forget the last one
        try:
            yield tuple(reducer(binminv, binmaxv, bin))
        except:
            if failonerror:
                raise
    
        
def rangerecordreduce(table, key, width, reducer, fields=None, minv=None, maxv=None, 
                      failonerror=False, presorted=False, buffersize=None):
    """
    Reduce records grouped into bins under the given key via an arbitrary function. 
    E.g.::

        >>> from petl import rangerecordreduce, look    
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 3],
        ...           ['a', 7],
        ...           ['b', 2],
        ...           ['b', 1],
        ...           ['b', 9],
        ...           ['c', 4]]
        >>> def redu(minv, maxunpack, recs):
        ...     return [minv, maxunpack, ''.join([rec['foo'] for rec in recs])]
        ... 
        >>> table2 = rangerecordreduce(table1, 'bar', 2, reducer=redu, fields=['frombar', 'tobar', 'foos'])
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

    """
    
    return RangeRecordReduceView(table, key, width, reducer, fields=fields, minv=minv, 
                                 maxv=maxv, failonerror=failonerror, 
                                 presorted=presorted, buffersize=buffersize)
        

class RangeRecordReduceView(RowContainer):
    
    def __init__(self, source, key, width, reducer, fields=None, minv=None, maxv=None, 
                 failonerror=False, presorted=False, buffersize=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize)
        self.key = key
        self.width = width
        self.reducer = reducer
        self.fields = fields
        self.minv, self.maxunpack = minv, maxv
        self.failonerror = failonerror

    def __iter__(self):
        return iterrangerecordreduce(self.source, self.key, self.width, self.reducer,
                                     self.fields, self.minv, self.maxunpack, self.failonerror)

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.key, self.width,
                         tuple(self.fields) if self.fields else self.fields, 
                         self.reducer, self.minv, self.maxunpack, self.failonerror))
        except Exception as e:
            raise Uncacheable(e)


def iterrangerecordreduce(source, key, width, reducer, fields, minv, maxv, failonerror):

    it = iter(source)
    srcflds = it.next()
    if fields is None:
        yield tuple(srcflds)
    else:
        yield tuple(fields)

    # convert field selection into field indices
    indices = asindices(srcflds, key)

    # now use field indices to construct a getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    # initialise minimum
    row = it.next()
    keyv = getkey(row)
    if minv is None:
        minv = keyv

    # iterate over bins
    # N.B., we need to account for two possible scenarios
    # (1) maxunpack is not specified, so keep making bins until we run out of rows
    # (2) maxunpack is specified, so iterate over bins 
    try:
        for binminv in count(minv, width):
            binmaxv = binminv + width
            if maxv is not None and binmaxv >= maxv: # final bin
                binmaxv = maxv
            bin = []
            while keyv < binminv:
                row = it.next()
                keyv = getkey(row)
            while binminv <= keyv < binmaxv:
                rec = asdict(srcflds, row)
                bin.append(rec)
                row = it.next()
                keyv = getkey(row)
            while maxv is not None and keyv == binmaxv == maxv:
                rec = asdict(srcflds, row)
                bin.append(rec) # last bin is open
                row = it.next()
                keyv = getkey(row)
            try:
                yield tuple(reducer(binminv, binmaxv, bin))
            except:
                if failonerror:
                    raise
            if maxv is not None and binmaxv == maxv:
                break
    except StopIteration:
        # don't forget the last one
        try:
            yield tuple(reducer(binminv, binmaxv, bin))
        except:
            if failonerror:
                raise
    
        
def rangecounts(table, key, width, minv=None, maxv=None, presorted=False,
                buffersize=None):
    """
    Group rows into bins then count the number of rows in each bin. E.g.::

        >>> from petl import rangecounts, look
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 3],
        ...           ['a', 7],
        ...           ['b', 2],
        ...           ['b', 1],
        ...           ['b', 9],
        ...           ['c', 4],
        ...           ['d', 3]]
        >>> table2 = rangecounts(table1, 'bar', width=2)
        >>> look(table2)
        +---------+---------+
        | 'range' | 'count' |
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

    """
    
    return RangeCountsView(table, key, width, minv=minv, maxv=maxv, 
                           presorted=presorted, buffersize=buffersize)
    
    
class RangeCountsView(RowContainer):
    
    def __init__(self, source, key, width, minv=None, maxv=None, 
                 presorted=False, buffersize=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize)
        self.key = key
        self.width = width
        self.minv, self.maxunpack = minv, maxv

    def __iter__(self):
        return iterrangecounts(self.source, self.key, self.width, self.minv, 
                               self.maxunpack)

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.key, self.width,
                         self.minv, self.maxunpack))
        except Exception as e:
            raise Uncacheable(e)


def iterrangecounts(source, key, width, minv, maxv):

    it = iter(source)
    srcflds = it.next()

    # convert field selection into field indices
    indices = asindices(srcflds, key)

    # now use field indices to construct a getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    outflds = ('range', 'count')
    yield outflds
    
    # initialise minimum
    row = it.next()
    keyv = getkey(row)
    if minv is None:
        minv = keyv

    # iterate over bins
    # N.B., we need to account for two possible scenarios
    # (1) maxunpack is not specified, so keep making bins until we run out of rows
    # (2) maxunpack is specified, so iterate over bins 
    try:
        for binminv in count(minv, width):
            binmaxv = binminv + width
            if maxv is not None and binmaxv >= maxv: # final bin
                binmaxv = maxv
            bin = []
            while keyv < binminv:
                row = it.next()
                keyv = getkey(row)
            while binminv <= keyv < binmaxv:
                bin.append(row)
                row = it.next()
                keyv = getkey(row)
            while maxv is not None and keyv == binmaxv == maxv:
                bin.append(row) # last bin is open
                row = it.next()
                keyv = getkey(row)
            yield ((binminv, binmaxv), len(bin))
            if maxv is not None and binmaxv == maxv:
                break
    except StopIteration:
        # don't forget the last one
        yield ((binminv, binmaxv), len(bin))
    
    
def rangeaggregate(table, key, width, aggregators=None, minv=None, maxv=None,
                   failonerror=False, errorvalue=None, presorted=False, buffersize=None):
    """
    Group rows into bins then apply aggregation functions. E.g.::
    
        >>> from petl import rangeaggregate, look
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 3],
        ...           ['a', 7],
        ...           ['b', 2],
        ...           ['b', 1],
        ...           ['b', 9],
        ...           ['c', 4],
        ...           ['d', 3]]
        >>> table2 = rangeaggregate(table1, 'bar', width=2)
        >>> table2['foocount'] = 'foo', len
        >>> table2['foolist'] = 'foo' # default is list
        >>> look(table2)
        +---------+------------+-----------------+
        | 'bar'   | 'foocount' | 'foolist'       |
        +=========+============+=================+
        | (1, 3)  | 2          | ['b', 'b']      |
        +---------+------------+-----------------+
        | (3, 5)  | 3          | ['a', 'd', 'c'] |
        +---------+------------+-----------------+
        | (5, 7)  | 0          | []              |
        +---------+------------+-----------------+
        | (7, 9)  | 1          | ['a']           |
        +---------+------------+-----------------+
        | (9, 11) | 1          | ['b']           |
        +---------+------------+-----------------+

    """
    
    return RangeAggregateView(table, key, width, 
                              aggregators=aggregators, minv=minv, maxv=maxv,
                              failonerror=failonerror, errorvalue=errorvalue, 
                              presorted=presorted, buffersize=buffersize)
    
    
class RangeAggregateView(RowContainer):
    
    def __init__(self, source, key, width, aggregators=None, minv=None, maxv=None, 
                 failonerror=False, errorvalue=None, presorted=False, buffersize=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize)
        self.key = key
        self.width = width
        if aggregators is None:
            self.aggregators = OrderedDict()
        else:
            self.aggregators = aggregators
        self.minv, self.maxunpack = minv, maxv
        self.failonerror = failonerror
        self.errorvalue = errorvalue

    def __iter__(self):
        return iterrangeaggregate(self.source, self.key, self.width, 
                                  self.aggregators, self.minv, self.maxunpack, 
                                  self.failonerror, self.errorvalue)

    def __getitem__(self, key):
        return self.aggregators[key]
    
    def __setitem__(self, key, value):
        self.aggregators[key] = value

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.key, self.width, 
                         self.minv, self.maxunpack, self.failonerror,
                         self.errorvalue, tuple(self.aggregators.items())))
        except Exception as e:
            raise Uncacheable(e)

    
def iterrangeaggregate(source, key, width, aggregators, minv, maxv, failonerror, errorvalue):

    aggregators = OrderedDict(aggregators.items()) # take a copy
    # normalise aggregators
    for outfld in aggregators:
        agg = aggregators[outfld]
        if not isinstance(agg, (list, tuple)):
            aggregators[outfld] = agg, list # list is default aggregation function

    it = iter(source)
    srcflds = it.next()

    # convert field selection into field indices
    indices = asindices(srcflds, key)

    # now use field indices to construct a getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = itemgetter(*indices)
    
    if len(indices) == 1:
        outflds = [getkey(srcflds)]
    else:
        outflds = list(getkey(srcflds))
    outflds.extend(aggregators.keys())
    yield tuple(outflds)
    
    def buildoutrow(binminv, binmaxv, bin):
        outrow = [(binminv, binmaxv)]
        for outfld in aggregators:
            srcfld, aggfun = aggregators[outfld]
            idx = srcflds.index(srcfld)
            try:
                # try using list comprehension
                vals = [row[idx] for row in bin]
            except IndexError:
                # fall back to slower for loop
                vals = list()
                for row in bin:
                    try:
                        vals.append(row[idx])
                    except IndexError:
                        pass
            try:
                aggval = aggfun(vals)
            except:
                if failonerror:
                    raise
                else:
                    aggval = errorvalue
            outrow.append(aggval)
        return tuple(outrow)

    # initialise minimum
    row = it.next()
    keyv = getkey(row)
    if minv is None:
        minv = keyv

    # iterate over bins
    # N.B., we need to account for two possible scenarios
    # (1) maxunpack is not specified, so keep making bins until we run out of rows
    # (2) maxunpack is specified, so iterate over bins 
    try:
        for binminv in count(minv, width):
            binmaxv = binminv + width
            if maxv is not None and binmaxv >= maxv: # final bin
                binmaxv = maxv
            bin = []
            while keyv < binminv:
                row = it.next()
                keyv = getkey(row)
            while binminv <= keyv < binmaxv:
                bin.append(row)
                row = it.next()
                keyv = getkey(row)
            while maxv is not None and keyv == binmaxv == maxv:
                bin.append(row) # last bin is open
                row = it.next()
                keyv = getkey(row)
            yield buildoutrow(binminv, binmaxv, bin)
            if maxv is not None and binmaxv == maxv:
                break
    except StopIteration:
        # don't forget the last one
        yield buildoutrow(binminv, binmaxv, bin)

        
def rowmap(table, rowmapper, fields, failonerror=False):
    """
    Transform rows via an arbitrary function. E.g.::

        >>> from petl import rowmap, look
        >>> table1 = [['id', 'sex', 'age', 'height', 'weight'],
        ...           [1, 'male', 16, 1.45, 62.0],
        ...           [2, 'female', 19, 1.34, 55.4],
        ...           [3, 'female', 17, 1.78, 74.4],
        ...           [4, 'male', 21, 1.33, 45.2],
        ...           [5, '-', 25, 1.65, 51.9]]
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

    The `rowmapper` function should accept a row (list or tuple) and return a
    single row (list or tuple).
    
    """
    
    return RowMapView(table, rowmapper, fields, failonerror)
    
    
class RowMapView(RowContainer):
    
    def __init__(self, source, rowmapper, fields, failonerror=False):
        self.source = source
        self.rowmapper = rowmapper
        self.fields = fields
        self.failonerror = failonerror
        
    def __iter__(self):
        return iterrowmap(self.source, self.rowmapper, self.fields, self.failonerror)

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.rowmapper, tuple(self.fields),
                         self.failonerror))
        except Exception as e:
            raise Uncacheable(e)

    
def iterrowmap(source, rowmapper, fields, failonerror):
    it = iter(source)
    it.next() # discard source fields
    yield tuple(fields)
    for row in it:
        try:
            outrow = rowmapper(row)
            yield tuple(outrow)
        except:
            if failonerror:
                raise
        
        
def recordmap(table, recmapper, fields, failonerror=False):
    """
    Transform records via an arbitrary function. E.g.::

        >>> from petl import recordmap, look
        >>> table1 = [['id', 'sex', 'age', 'height', 'weight'],
        ...           [1, 'male', 16, 1.45, 62.0],
        ...           [2, 'female', 19, 1.34, 55.4],
        ...           [3, 'female', 17, 1.78, 74.4],
        ...           [4, 'male', 21, 1.33, 45.2],
        ...           [5, '-', 25, 1.65, 51.9]]
        >>> def recmapper(rec):
        ...     transmf = {'male': 'M', 'female': 'F'}
        ...     return [rec['id'],
        ...             transmf[rec['sex']] if rec['sex'] in transmf else rec['sex'],
        ...             rec['age'] * 12,
        ...             rec['weight'] / rec['height'] ** 2]
        ... 
        >>> table2 = recordmap(table1, recmapper, fields=['subject_id', 'gender', 'age_months', 'bmi'])  
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

    The `recmapper` function should accept a record (dictionary of data values
    indexed by field) and return a single row (list or tuple).
    
    """
    
    return RecordMapView(table, recmapper, fields, failonerror)
    
    
class RecordMapView(RowContainer):
    
    def __init__(self, source, recmapper, fields, failonerror=False):
        self.source = source
        self.recmapper = recmapper
        self.fields = fields
        self.failonerror = failonerror
        
    def __iter__(self):
        return iterrecordmap(self.source, self.recmapper, self.fields, self.failonerror)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.recmapper, tuple(self.fields),
                         self.failonerror))
        except Exception as e:
            raise Uncacheable(e)

    
   
def iterrecordmap(source, recmapper, fields, failonerror):
    it = iter(source)
    flds = it.next() # discard source fields
    yield tuple(fields)
    for row in it:
        rec = asdict(flds, row)
        try:
            outrow = recmapper(rec)
            yield tuple(outrow)
        except:
            if failonerror:
                raise
        
        
def rowmapmany(table, rowgenerator, fields, failonerror=False):
    """
    Map each input row to any number of output rows via an arbitrary function.
    E.g.::

        >>> from petl import rowmapmany, look    
        >>> table1 = [['id', 'sex', 'age', 'height', 'weight'],
        ...           [1, 'male', 16, 1.45, 62.0],
        ...           [2, 'female', 19, 1.34, 55.4],
        ...           [3, '-', 17, 1.78, 74.4],
        ...           [4, 'male', 21, 1.33]]
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

    The `rowgenerator` function should accept a row (list or tuple) and yield
    zero or more rows (lists or tuples).
    
    See also the :func:`melt` function.
    
    """
    
    return RowMapManyView(table, rowgenerator, fields, failonerror)
    
    
class RowMapManyView(RowContainer):
    
    def __init__(self, source, rowgenerator, fields, failonerror=False):
        self.source = source
        self.rowgenerator = rowgenerator
        self.fields = fields
        self.failonerror = failonerror
        
    def __iter__(self):
        return iterrowmapmany(self.source, self.rowgenerator, self.fields, self.failonerror)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.rowgenerator, tuple(self.fields),
                         self.failonerror))
        except Exception as e:
            raise Uncacheable(e)

    
def iterrowmapmany(source, rowgenerator, fields, failonerror):
    it = iter(source)
    it.next() # discard source fields
    yield tuple(fields)
    for row in it:
        try:
            for outrow in rowgenerator(row):
                yield tuple(outrow)
        except:
            if failonerror:
                raise
        
        
def recordmapmany(table, rowgenerator, fields, failonerror=False):
    """
    Map each input row (as a record) to any number of output rows via an 
    arbitrary function. E.g.::

        >>> from petl import recordmapmany, look    
        >>> table1 = [['id', 'sex', 'age', 'height', 'weight'],
        ...           [1, 'male', 16, 1.45, 62.0],
        ...           [2, 'female', 19, 1.34, 55.4],
        ...           [3, '-', 17, 1.78, 74.4],
        ...           [4, 'male', 21, 1.33]]
        >>> def rowgenerator(rec):
        ...     transmf = {'male': 'M', 'female': 'F'}
        ...     yield [rec['id'], 'gender', transmf[rec['sex']] if rec['sex'] in transmf else rec['sex']]
        ...     yield [rec['id'], 'age_months', rec['age'] * 12]
        ...     yield [rec['id'], 'bmi', rec['weight'] / rec['height'] ** 2]
        ... 
        >>> table2 = recordmapmany(table1, rowgenerator, fields=['subject_id', 'variable', 'value'])  
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

    The `rowgenerator` function should accept a record (dictionary of data values
    indexed by field) and yield zero or more rows (lists or tuples).
    
    See also the :func:`melt` function.
    
    """
    
    return RecordMapManyView(table, rowgenerator, fields, failonerror)
    
    
class RecordMapManyView(RowContainer):
    
    def __init__(self, source, rowgenerator, fields, failonerror=False):
        self.source = source
        self.rowgenerator = rowgenerator
        self.fields = fields
        self.failonerror = failonerror
        
    def __iter__(self):
        return iterrecordmapmany(self.source, self.rowgenerator, self.fields, self.failonerror)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.rowgenerator, tuple(self.fields),
                         self.failonerror))
        except Exception as e:
            raise Uncacheable(e)

    
def iterrecordmapmany(source, rowgenerator, fields, failonerror):
    it = iter(source)
    flds = it.next() # discard source fields
    yield tuple(fields)
    for row in it:
        rec = asdict(flds, row)
        try:
            for outrow in rowgenerator(rec):
                yield tuple(outrow)
        except:
            if failonerror:
                raise
        
        
def setheader(table, fields):
    """
    Override fields in the given table. E.g.::
    
        >>> from petl import setheader, look
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2]]
        >>> table2 = setheader(table1, ['foofoo', 'barbar'])
        >>> look(table2)
        +----------+----------+
        | 'foofoo' | 'barbar' |
        +==========+==========+
        | 'a'      | 1        |
        +----------+----------+
        | 'b'      | 2        |
        +----------+----------+

    """
    
    return SetHeaderView(table, fields) 


class SetHeaderView(RowContainer):
    
    def __init__(self, source, fields):
        self.source = source
        self.fields = fields
        
    def __iter__(self):
        return itersetheader(self.source, self.fields)   

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), tuple(self.fields)))
        except Exception as e:
            raise Uncacheable(e)


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
        >>> table1 = [['foo'],
        ...           ['a', 1, True],
        ...           ['b', 2, False]]
        >>> table2 = extendheader(table1, ['bar', 'baz'])
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 1     | True  |
        +-------+-------+-------+
        | 'b'   | 2     | False |
        +-------+-------+-------+

    """
    
    return ExtendHeaderView(table, fields) 


class ExtendHeaderView(RowContainer):
    
    def __init__(self, source, fields):
        self.source = source
        self.fields = fields
        
    def __iter__(self):
        return iterextendheader(self.source, self.fields)   

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), tuple(self.fields)))
        except Exception as e:
            raise Uncacheable(e)


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
        >>> table1 = [['a', 1],
        ...           ['b', 2]]
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

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), tuple(self.fields)))
        except Exception as e:
            raise Uncacheable(e)


def iterpushheader(source, fields):
    it = iter(source)
    yield tuple(fields)
    for row in it:
        yield tuple(row)
        
    
def skip(table, n):
    """
    Skip `n` rows (including the header row). E.g.::
    
        >>> from petl import skip, look
        >>> table1 = [['#aaa', 'bbb', 'ccc'],
        ...           ['#mmm'],
        ...           ['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2]]
        >>> table2 = skip(table1, 2)
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
    
    """ 

    return SkipView(table, n)


class SkipView(RowContainer):
    
    def __init__(self, source, n):
        self.source = source
        self.n = n
        
    def __iter__(self):
        return iterskip(self.source, self.n)   

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.n))
        except Exception as e:
            raise Uncacheable(e)


def iterskip(source, n):
    return islice(source, n, None)
        
    
def skipcomments(table, prefix):
    """
    Skip any row where the first value is a string and starts with 
    `prefix`. E.g.::
    
        >>> from petl import skipcomments, look
        >>> table1 = [['##aaa', 'bbb', 'ccc'],
        ...           ['##mmm',],
        ...           ['#foo', 'bar'],
        ...           ['##nnn', 1],
        ...           ['a', 1],
        ...           ['b', 2]]
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

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.prefix))
        except Exception as e:
            raise Uncacheable(e)


def iterskipcomments(source, prefix):
    return (row for row in source if len(row) > 0 and not(isinstance(row[0], basestring) and row[0].startswith(prefix)))
        
    
def unpack(table, field, newfields=None, maxunpack=None, include_original=False):
    """
    Unpack data values that are lists or tuples. E.g.::
    
        >>> from petl import unpack, look    
        >>> table1 = [['foo', 'bar'],
        ...           [1, ['a', 'b']],
        ...           [2, ['c', 'd']],
        ...           [3, ['e', 'f']]]
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

    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.field, 
                         tuple(self.newfields) if self.newfields else self.newfields,
                         self.maxunpack, self.include_original))
        except Exception as e:
            raise Uncacheable(e)


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
        
        
def join(left, right, key=None, presorted=False, buffersize=None):
    """
    Perform an equi-join on the given tables. E.g.::
        
        >>> from petl import join, look    
        >>> table1 = [['id', 'colour'],
        ...           [1, 'blue'],
        ...           [2, 'red'],
        ...           [3, 'purple']]
        >>> table2 = [['id', 'shape'],
        ...           [1, 'circle'],
        ...           [3, 'square'],
        ...           [4, 'ellipse']]
        >>> table3 = join(table1, table2, key='id')
        >>> look(table3)
        +------+----------+----------+
        | 'id' | 'colour' | 'shape'  |
        +======+==========+==========+
        | 1    | 'blue'   | 'circle' |
        +------+----------+----------+
        | 3    | 'purple' | 'square' |
        +------+----------+----------+

    If no `key` is given, a natural join is tried, e.g.::
    
        >>> table4 = join(table1, table2)
        >>> look(table4)
        +------+----------+----------+
        | 'id' | 'colour' | 'shape'  |
        +======+==========+==========+
        | 1    | 'blue'   | 'circle' |
        +------+----------+----------+
        | 3    | 'purple' | 'square' |
        +------+----------+----------+

    Note behaviour if the key is not unique in either or both tables::

        >>> table5 = [['id', 'colour'],
        ...           [1, 'blue'],
        ...           [1, 'red'],
        ...           [2, 'purple']]
        >>> table6 = [['id', 'shape'],
        ...           [1, 'circle'],
        ...           [1, 'square'],
        ...           [2, 'ellipse']]
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

    Compound keys are supported, e.g.::
    
        >>> table8 = [['id', 'time', 'height'],
        ...           [1, 1, 12.3],
        ...           [1, 2, 34.5],
        ...           [2, 1, 56.7]]
        >>> table9 = [['id', 'time', 'weight'],
        ...           [1, 2, 4.5],
        ...           [2, 1, 6.7],
        ...           [2, 2, 8.9]]
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
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """
    
    if key is None:
        return ImplicitJoinView(left, right, presorted=presorted, buffersize=buffersize)
    else:
        return JoinView(left, right, key, presorted=presorted, buffersize=buffersize)


class ImplicitJoinView(RowContainer):
    
    def __init__(self, left, right, presorted=False, leftouter=False, 
                 rightouter=False, missing=None, buffersize=None):
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

    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(),
                         self.presorted, self.leftouter, self.rightouter,
                         self.missing))
        except Exception as e:
            raise Uncacheable(e)


class JoinView(RowContainer):
    
    def __init__(self, left, right, key, presorted=False, leftouter=False, 
                 rightouter=False, missing=None, buffersize=None):
        if presorted:
            self.left = left
            self.right = right
        else:
            self.left = sort(left, key, buffersize=buffersize)
            self.right = sort(right, key, buffersize=buffersize)
            # TODO what if someone sets self.key to something else after __init__?
            # (sort will be incorrect - maybe need to protect key with property setter?)
        self.key = key
        self.leftouter = leftouter
        self.rightouter = rightouter
        self.missing = missing
        
    def __iter__(self):
        return iterjoin(self.left, self.right, self.key, leftouter=self.leftouter,
                        rightouter=self.rightouter, missing=self.missing)
    
    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(), 
                         tuple(self.key) if isinstance(self.key, list) else self.key,
                         self.leftouter, self.rightouter, self.missing))
        except Exception as e:
            raise Uncacheable(e)

    
def leftjoin(left, right, key=None, missing=None, presorted=False, buffersize=None):
    """
    Perform a left outer join on the given tables. E.g.::
    
        >>> from petl import leftjoin, look
        >>> table1 = [['id', 'colour'],
        ...           [1, 'blue'],
        ...           [2, 'red'],
        ...           [3, 'purple']]
        >>> table2 = [['id', 'shape'],
        ...           [1, 'circle'],
        ...           [3, 'square'],
        ...           [4, 'ellipse']]
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
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """
    
    if key is None:
        return ImplicitJoinView(left, right, presorted=presorted, leftouter=True, 
                               rightouter=False, missing=missing, buffersize=buffersize)
    else:
        return JoinView(left, right, key, presorted=presorted, leftouter=True, 
                        rightouter=False, missing=missing, buffersize=buffersize)

    
def rightjoin(left, right, key=None, missing=None, presorted=False, buffersize=None):
    """
    Perform a right outer join on the given tables. E.g.::

        >>> from petl import rightjoin, look
        >>> table1 = [['id', 'colour'],
        ...           [1, 'blue'],
        ...           [2, 'red'],
        ...           [3, 'purple']]
        >>> table2 = [['id', 'shape'],
        ...           [1, 'circle'],
        ...           [3, 'square'],
        ...           [4, 'ellipse']]
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
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """
    
    if key is None:
        return ImplicitJoinView(left, right, presorted=presorted, leftouter=False, 
                               rightouter=True, missing=missing, buffersize=buffersize)
    else:
        return JoinView(left, right, key, presorted=presorted, leftouter=False, 
                        rightouter=True, missing=missing, buffersize=buffersize)
    
    
def outerjoin(left, right, key=None, missing=None, presorted=False, buffersize=None):
    """
    Perform a full outer join on the given tables. E.g.::

        >>> from petl import outerjoin, look
        >>> table1 = [['id', 'colour'],
        ...           [1, 'blue'],
        ...           [2, 'red'],
        ...           [3, 'purple']]
        >>> table2 = [['id', 'shape'],
        ...           [1, 'circle'],
        ...           [3, 'square'],
        ...           [4, 'ellipse']]
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
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """

    if key is None:
        return ImplicitJoinView(left, right, presorted=presorted, leftouter=True, 
                               rightouter=True, missing=missing, buffersize=buffersize)
    else:
        return JoinView(left, right, key, presorted=presorted, leftouter=True, 
                        rightouter=True, missing=missing, buffersize=buffersize)
    
    
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
                    rightouter=False, missing=None, buffersize=None):
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
        left = sort(left, key, buffersize=buffersize)
        right = sort(right, key, buffersize=buffersize)
    # from here on it's the same as a normal join
    return iterjoin(left, right, key, leftouter=leftouter, rightouter=rightouter,
                    missing=missing)


def crossjoin(*tables):
    """
    Form the cartesian product of the given tables. E.g.::

        >>> from petl import crossjoin, look
        >>> table1 = [['id', 'colour'],
        ...           [1, 'blue'],
        ...           [2, 'red']]
        >>> table2 = [['id', 'shape'],
        ...           [1, 'circle'],
        ...           [3, 'square']]
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

    """
    
    return CrossJoinView(*tables)


class CrossJoinView(RowContainer):
    
    def __init__(self, *sources):
        self.sources = sources
        
    def __iter__(self):
        return itercrossjoin(self.sources)
    
    def cachetag(self):
        try:
            return hash(tuple(source.cachetag() for source in self.sources))
        except Exception as e:
            raise Uncacheable(e)

    
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
        
        
def antijoin(left, right, key=None, presorted=False, buffersize=None):
    """
    Return rows from the `left` table where the key value does not occur in the
    `right` table. E.g.::

        >>> from petl import antijoin, look
        >>> table1 = [['id', 'colour'],
        ...           [0, 'black'],
        ...           [1, 'blue'],
        ...           [2, 'red'],
        ...           [4, 'yellow'],
        ...           [5, 'white']]
        >>> table2 = [['id', 'shape'],
        ...           [1, 'circle'],
        ...           [3, 'square']]
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
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """
    
    if key is None:
        return ImplicitAntiJoinView(left, right, presorted, buffersize)
    else:
        return AntiJoinView(left, right, key, presorted, buffersize)


class AntiJoinView(RowContainer):
    
    def __init__(self, left, right, key, presorted=False, buffersize=None):
        if presorted:
            self.left = left
            self.right = right
        else:
            self.left = sort(left, key, buffersize=buffersize)
            self.right = sort(right, key, buffersize=buffersize)
            # TODO what if someone sets self.key to something else after __init__?
            # (sort will be incorrect - maybe need to protect key with property setter?)
        self.key = key
        
    def __iter__(self):
        return iterantijoin(self.left, self.right, self.key)
    
    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(), 
                         tuple(self.key) if isinstance(self.key, list) else self.key))
        except Exception as e:
            raise Uncacheable(e)

    
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
    try:

        # pick off initial row groups
        lkval, lrowgrp = lgit.next() 
        rkval, rrowgrp = rgit.next()

        while True:
            if lkval < rkval:
                for row in lrowgrp:
                    yield tuple(row)
                # advance left
                lkval, lrowgrp = lgit.next()
            elif lkval > rkval:
                # advance right
                rkval, rrowgrp = rgit.next()
            else:
                # advance both
                lkval, lrowgrp = lgit.next()
                rkval, rrowgrp = rgit.next()
        
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
    
    def __init__(self, left, right, presorted=False, buffersize=None):
        self.left = left
        self.right = right
        self.presorted = presorted
        self.buffersize = buffersize
        
    def __iter__(self):
        return iterimplicitantijoin(self.left, self.right, self.presorted, self.buffersize)
    
    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(), 
                         self.presorted))
        except Exception as e:
            raise Uncacheable(e)

    
def iterimplicitantijoin(left, right, presorted=False, buffersize=None):
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
        left = sort(left, key, buffersize=buffersize)
        right = sort(right, key, buffersize=buffersize)
    # from here on it's the same as a normal antijoin
    return iterantijoin(left, right, key)


def rangefacet(table, field, width, minv=None, maxv=None, 
               presorted=False, buffersize=None):
    """
    Return a dictionary mapping ranges to tables. E.g.::
    
        >>> from petl import rangefacet, look
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 3],
        ...           ['a', 7],
        ...           ['b', 2],
        ...           ['b', 1],
        ...           ['b', 9],
        ...           ['c', 4],
        ...           ['d', 3]]
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
        >>> table1 = (('id', 'colour'),
        ...           (1, 'blue'),
        ...           (2, 'red'),
        ...           (3, 'purple'),
        ...           (5, 'yellow'),
        ...           (7, 'orange'))
        >>> table2 = transpose(table1)
        >>> look(table2)
        +----------+--------+-------+----------+----------+----------+
        | 'id'     | 1      | 2     | 3        | 5        | 7        |
        +==========+========+=======+==========+==========+==========+
        | 'colour' | 'blue' | 'red' | 'purple' | 'yellow' | 'orange' |
        +----------+--------+-------+----------+----------+----------+

    """
    
    return TransposeView(table)


class TransposeView(RowContainer):
    
    def __init__(self, source):
        self.source = source
        
    def __iter__(self):
        return itertranspose(self.source)


def itertranspose(source):
    fields = header(source)
    its = [iter(source) for f in fields]
    for i in range(len(fields)):
        yield tuple(row[i] for row in its[i])
        

def intersection(a, b, presorted=False, buffersize=None):
    """
    Return rows in `a` that are also in `b`. E.g.::
    
        >>> from petl import intersection, look
        >>> table1 = (('foo', 'bar', 'baz'),
        ...           ('A', 1, True),
        ...           ('C', 7, False),
        ...           ('B', 2, False),
        ...           ('C', 9, True))
        >>> table2 = (('x', 'y', 'z'),
        ...           ('B', 2, False),
        ...           ('A', 9, False),
        ...           ('B', 3, True),
        ...           ('C', 9, True))
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
    the given key, and the `buffersize` argument is ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize` argument under the 
    :func:`sort` function.
    
    """
    
    return IntersectionView(a, b, presorted, buffersize)


class IntersectionView(RowContainer):
    
    def __init__(self, a, b, presorted=False, buffersize=None):
        if presorted:
            self.a = a
            self.b = b
        else:
            self.a = sort(a, buffersize=buffersize)
            self.b = sort(b, buffersize=buffersize)
            
    def __iter__(self):
        return iterintersection(self.a, self.b)

    def cachetag(self):
        try:
            return hash((self.a.cachetag(), self.b.cachetag()))
        except Exception as e:
            raise Uncacheable(e)


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
    
    
def pivot(table, f1, f2, f3, aggfun, presorted=False, buffersize=None, missing=None):
    """
    Construct a pivot table. E.g.::

        >>> from petl import pivot, look
        >>> table1 = (('region', 'gender', 'style', 'units'),
        ...           ('east', 'boy', 'tee', 12),
        ...           ('east', 'boy', 'golf', 14),
        ...           ('east', 'boy', 'fancy', 7),
        ...           ('east', 'girl', 'tee', 3),
        ...           ('east', 'girl', 'golf', 8),
        ...           ('east', 'girl', 'fancy', 18),
        ...           ('west', 'boy', 'tee', 12),
        ...           ('west', 'boy', 'golf', 15),
        ...           ('west', 'boy', 'fancy', 8),
        ...           ('west', 'girl', 'tee', 6),
        ...           ('west', 'girl', 'golf', 16),
        ...           ('west', 'girl', 'fancy', 1))
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
    
    return PivotView(table, f1, f2, f3, aggfun,
                     presorted=presorted, buffersize=buffersize, missing=missing)


class PivotView(RowContainer):
    
    def __init__(self, source, f1, f2, f3, aggfun, presorted=False, buffersize=None,
                 missing=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key=(f1, f2), buffersize=buffersize)
        self.f1, self.f2, self.f3 = f1, f2, f3
        self.aggfun = aggfun
        self.missing = missing
        
    def __iter__(self):
        return iterpivot(self.source, self.f1, self.f2, self.f3, self.aggfun, self.missing)
    
    def cachetag(self):
        try:
            return hash((self.source.cachetag(), self.f1, self.f2, self.f3,
                         self.aggfun, self.missing))
        except Exception as e:
            raise Uncacheable(e)
    
    
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
    
    
def hashjoin(left, right, key=None):
    """
    Alternative implementation of :func:`join`, where the join is executed
    by constructing an in-memory lookup for the right hand table, then iterating over rows 
    from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.5

    """
    
    if key is None:
        return ImplicitHashJoinView(left, right)
    else:
        return HashJoinView(left, right, key)


class ImplicitHashJoinView(RowContainer):
    
    def __init__(self, left, right):
        self.left = left
        self.right = right
        
    def __iter__(self):
        return iterimplicithashjoin(self.left, self.right)

    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag()))
        except Exception as e:
            raise Uncacheable(e)


class HashJoinView(RowContainer):
    
    def __init__(self, left, right, key):
        self.left = left
        self.right = right
        self.key = key
        
    def __iter__(self):
        return iterhashjoin(self.left, self.right, self.key)
    
    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(), 
                         tuple(self.key) if isinstance(self.key, list) else self.key))
        except Exception as e:
            raise Uncacheable(e)


def iterhashjoin(left, right, key):
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

    rlookup = lookup(right, key)
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
    # from here on it's the same as a normal join
    return iterhashjoin(left, right, key)


def hashleftjoin(left, right, key=None, missing=None):
    """
    Alternative implementation of :func:`leftjoin`, where the join is executed
    by constructing an in-memory lookup for the right hand table, then iterating over rows 
    from the left hand table.
    
    May be faster and/or more resource efficient where the right table is small
    and the left table is large.
    
    .. versionadded:: 0.5

    """

    if key is None:
        return ImplicitHashLeftJoinView(left, right)
    else:
        return HashLeftJoinView(left, right, key)


class ImplicitHashLeftJoinView(RowContainer):
    
    def __init__(self, left, right, missing=None):
        self.left = left
        self.right = right
        self.missing = missing
        
    def __iter__(self):
        return iterimplicithashleftjoin(self.left, self.right, self.missing)

    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(), self.missing))
        except Exception as e:
            raise Uncacheable(e)


class HashLeftJoinView(RowContainer):
    
    def __init__(self, left, right, key, missing=None):
        self.left = left
        self.right = right
        self.key = key
        self.missing = missing
        
    def __iter__(self):
        return iterhashleftjoin(self.left, self.right, self.key, self.missing)
    
    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(), 
                         tuple(self.key) if isinstance(self.key, list) else self.key,
                         self.missing))
        except Exception as e:
            raise Uncacheable(e)


def iterhashleftjoin(left, right, key, missing):
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

    rlookup = lookup(right, key)
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
    # from here on it's the same as a normal join
    return iterhashleftjoin(left, right, key, missing)



def hashrightjoin(left, right, key=None, missing=None):
    """
    Alternative implementation of :func:`rightjoin`, where the join is executed
    by constructing an in-memory lookup for the left hand table, then iterating over rows 
    from the right hand table.
    
    May be faster and/or more resource efficient where the left table is small
    and the right table is large.
    
    .. versionadded:: 0.5

    """

    if key is None:
        return ImplicitHashRightJoinView(left, right)
    else:
        return HashRightJoinView(left, right, key)


class ImplicitHashRightJoinView(RowContainer):
    
    def __init__(self, left, right, missing=None):
        self.left = left
        self.right = right
        self.missing = missing
        
    def __iter__(self):
        return iterimplicithashrightjoin(self.left, self.right, self.missing)

    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(), self.missing))
        except Exception as e:
            raise Uncacheable(e)


class HashRightJoinView(RowContainer):
    
    def __init__(self, left, right, key, missing=None):
        self.left = left
        self.right = right
        self.key = key
        self.missing = missing
        
    def __iter__(self):
        return iterhashrightjoin(self.left, self.right, self.key, self.missing)
    
    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(), 
                         tuple(self.key) if isinstance(self.key, list) else self.key,
                         self.missing))
        except Exception as e:
            raise Uncacheable(e)


def iterhashrightjoin(left, right, key, missing):
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

    llookup = lookup(left, key)
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
    # from here on it's the same as a normal join
    return iterhashrightjoin(left, right, key, missing)


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
    
    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag(), 
                         tuple(self.key) if isinstance(self.key, list) else self.key))
        except Exception as e:
            raise Uncacheable(e)

    
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
    
    def cachetag(self):
        try:
            return hash((self.left.cachetag(), self.right.cachetag()))
        except Exception as e:
            raise Uncacheable(e)

    
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

    def cachetag(self):
        try:
            return hash((self.a.cachetag(), self.b.cachetag()))
        except Exception as e:
            raise Uncacheable(e)


def iterhashcomplement(a, b):
    ita = iter(a) 
    aflds = ita.next()
    yield tuple(aflds)
    itb = iter(b)
    itb.next() # discard b fields, assume they are the same

    blkp = set(tuple(br) for br in itb)
    for ar in ita:
        t = tuple(ar)
        if t not in blkp:
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

    def cachetag(self):
        try:
            return hash((self.a.cachetag(), self.b.cachetag()))
        except Exception as e:
            raise Uncacheable(e)


def iterhashintersection(a, b):
    ita = iter(a) 
    aflds = ita.next()
    yield tuple(aflds)
    itb = iter(b)
    itb.next() # discard b fields, assume they are the same

    blkp = set(tuple(br) for br in itb)
    for ar in ita:
        t = tuple(ar)
        if t in blkp:
            yield t 
        
        
def flatten(table):
    """
    Convert a table to a sequence of values in row-major order. E.g.::

        >>> from petl import flatten    
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, True],
        ...           ['C', 7, False],
        ...           ['B', 2, False],
        ...           ['C', 9, True]]
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

    A table and field name can also be provided as arguments, e.g.::

        >>> table1 = [['lines',],
        ...           ['A',], 
        ...           [1,], 
        ...           [True,], 
        ...           ['C',], 
        ...           [7,], 
        ...           [False,],
        ...           ['B',], 
        ...           [2,], 
        ...           [False,],
        ...           ['C'], 
        ...           [9,]]
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
        input = self.input
        period = self.period
        missing = self.missing
        
        # generate header row
        fields = tuple('f%s' % i for i in range(period))
        yield fields
        
        # generate data rows
        row = list()
        for v in input:
            if len(row) < period:
                row.append(v)
            else:
                yield tuple(row)
                row = [v]
        
        # deal with last row
        if len(row) < period:
            row.extend([missing] * (period - len(row)))
        yield tuple(row)
            
