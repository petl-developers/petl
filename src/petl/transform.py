"""
TODO doc me

"""

from itertools import islice, groupby
from collections import deque, defaultdict, OrderedDict
from operator import itemgetter


from petl.util import close, asindices, rowgetter, FieldSelectionError, asdict,\
    expr, valueset, records, fields, data
import re


def rename(table, spec=dict()):
    """
    Replace one or more fields in the table's header row. E.g.::

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

    """
    
    return RenameView(table, spec)


class RenameView(object):
    
    def __init__(self, table, spec=dict()):
        self.source = table
        self.spec = spec
        
    def __iter__(self):
        return iterrename(self.source, self.spec)
    
    def __setitem__(self, key, value):
        self.spec[key] = value
        
    def __getitem__(self, key):
        return self.spec[key]
    
    
def iterrename(source, spec):
    it = iter(source)
    spec = spec.copy() # make sure nobody can change this midstream
    try:
        sourceflds = it.next()
        newflds = [spec[f] if f in spec else f for f in sourceflds]
        yield newflds
        for row in it:
            yield row
    finally:
        close(it)
        
        
def project(table, *args, **kwargs):
    """
    Choose and/or re-order columns. E.g.::

        >>> from petl import look, project    
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, 2.7],
        ...           ['B', 2, 3.4],
        ...           ['B', 3, 7.8],
        ...           ['D', 42, 9.0],
        ...           ['E', 12]]
        >>> table2 = project(table1, 'foo', 'baz')
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

        >>> table3 = project(table1, 0, 2)
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

        >>> table4 = project(table1, 'bar', 0)
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
    
        >>> table5 = project(table1, *range(0, 2))
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

    """
    
    return ProjectView(table, args, **kwargs)


class ProjectView(object):
    
    def __init__(self, source, spec, missing=None):
        self.source = source
        self.spec = spec
        self.missing = missing
        
    def __iter__(self):
        return iterproject(self.source, self.spec, self.missing)
        
        
def iterproject(source, spec, missing=None):
    it = iter(source)
    spec = tuple(spec) # make sure no-one can change midstream
    try:
        
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
                yield [row[i] if i < len(row) else missing for i in indices]

    finally:
        close(it)
    
    
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

        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', '2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', 'xyz', 9.0],
        ...          ['E', None]]
        >>> look(cat(table))
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

    """
    
    return CatView(tables, **kwargs)
    
    
class CatView(object):
    
    def __init__(self, sources, missing=None):
        self.sources = sources
        self.missing = missing

    def __iter__(self):
        return itercat(self.sources, self.missing)
    

def itercat(sources, missing=None):
    its = [iter(t) for t in sources]
    try:
        
        # determine output flds by gathering all flds found in the sources
        source_flds_lists = [it.next() for it in its]
        out_flds = list()
        for flds in source_flds_lists:
            for f in flds:
                if f not in out_flds:
                    # add any new flds as we find them
                    out_flds.append(f)
        yield out_flds

        # output data rows
        for source_index, it in enumerate(its):
            flds = source_flds_lists[source_index]
            
            # let's define a function which will, for any row and field name,
            # return the corresponding value, or fill in any missing values
            def get_value(row, f):
                try:
                    value = row[flds.index(f)]
                except ValueError: # source does not have f in flds
                    value = missing
                except IndexError: # row is short
                    value = missing
                return value
            
            # now construct and yield the data rows
            for row in it:
                out_row = [get_value(row, f) for f in out_flds]
                yield out_row

    finally:
        # make sure all iterators are closed
        for it in its:
            close(it)


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
    
    """
    
    converters = dict()
    if len(args) == 1:
        converters[field] = args[0]
    elif len(args) > 1:
        converters[field] = args
    return fieldconvert(table, converters, **kwargs)

    
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

    """

    return FieldConvertView(table, converters, failonerror, errorvalue)


class FieldConvertView(object):
    
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
        else:
            raise Exception('unexpected converter specification on field %r: %r' % (f, c))
    try:
        
        # grab the fields in the source table
        flds = it.next()
        yield flds # these are not modified
        
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
            yield [transform_value(i, v) for i, v in enumerate(row)]

    finally:
        close(it)
            
            
def methodcaller(nm, *args):
    return lambda v: getattr(v, nm)(*args)


def translate(table, field, dictionary=dict()):
    """
    Translate values in a given field using a dictionary. E.g.::
    
        >>> from petl import translate, look
        >>> table1 = [['gender', 'age'],
        ...           ['M', 12],
        ...           ['F', 34],
        ...           ['-', 56]]
        >>> table2 = translate(table1, 'gender', {'M': 'male', 'F': 'female'})
        >>> look(table2)
        +----------+-------+
        | 'gender' | 'age' |
        +==========+=======+
        | 'male'   | 12    |
        +----------+-------+
        | 'female' | 34    |
        +----------+-------+
        | '-'      | 56    |
        +----------+-------+

    """
    
    return TranslateView(table, field, dictionary)


class TranslateView(object):
    
    def __init__(self, source, field, dictionary=dict()):
        self.source = source
        self.field = field
        self.dictionary = dictionary
        
    def __iter__(self):
        return itertranslate(self.source, self.field, self.dictionary)


def itertranslate(source, field, dictionary):
    it = iter(source)
    dictionary = dictionary.copy()
    try:
        
        flds = it.next()
        yield flds 
        
        if field in flds:
            index = flds.index(field)
        elif isinstance(field, int) and field < len(flds):
            index = field
        else:
            raise FieldSelectionError(field)
        
        for row in it:
            row = list(row) # copy, so we don't modify the source
            value = row[index]
            if value in dictionary:
                row[index] = dictionary[value]
            yield row
            
    finally:
        close(it)
        
        
def extend(table, field, value):
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
    
    return ExtendView(table, field, value)


class ExtendView(object):
    
    def __init__(self, source, field, value):
        self.source = source
        self.field = field
        self.value = value
        
    def __iter__(self):
        return iterextend(self.source, self.field, self.value)


def iterextend(source, field, value):
    it = iter(source)
    try:
        flds = it.next()
        out_flds = list(flds)
        out_flds.append(field)
        yield out_flds

        for row in it:
            out_row = list(row) # copy so we don't modify source
            if callable(value):
                rec = asdict(flds, row)
                out_row.append(value(rec))
            else:
                out_row.append(value)
            yield out_row
    finally:
        close(it)
        
    
def rowslice(table, start=0, stop=None, step=1):
    """
    Choose a subset of data rows. E.g.::
    
        >>> from petl import rowslice, look
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 5],
        ...           ['d', 7],
        ...           ['f', 42]]
        >>> table2 = rowslice(table1, 0, 2)
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
        
        >>> table5 = rowslice(table1, step=2)
        >>> look(table5)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'c'   | 5     |
        +-------+-------+
        | 'f'   | 42    |
        +-------+-------+

    """
    
    return RowSliceView(table, start, stop, step)


class RowSliceView(object):
    
    def __init__(self, source, start=0, stop=None, step=1):
        self.source = source
        self.start = start
        self.stop = stop
        self.step = step
        
    def __iter__(self):
        return iterrowslice(self.source, self.start, self.stop, self.step)


def iterrowslice(source, start, stop, step):    
    it = iter(source)
    try:
        yield it.next() # fields
        for row in islice(it, start, stop, step):
            yield row
    finally:
        close(it)


def head(table, n):
    """
    Choose the first n rows. E.g.::

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
    
    """
    
    return rowslice(table, stop=n)

        
def tail(table, n):
    """
    Choose the last n rows. E.g.::

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


class TailView(object):
    
    def __init__(self, source, n):
        self.source = source
        self.n = n
        
    def __iter__(self):
        return itertail(self.source, self.n)


def itertail(source, n):
    it = iter(source)
    try:
        yield it.next() # fields
        cache = deque()
        for row in it:
            cache.append(row)
            if len(cache) > n:
                cache.popleft()
        for row in cache:
            yield row
    finally:
        close(it)


def sort(table, key=None, reverse=False):
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
        
    TODO currently this sorts data in memory, need to add option to limit
    memory usage and merge sort from chunks on disk

    """
    
    return SortView(table, key, reverse)
    
    
class SortView(object):
    
    def __init__(self, source, key=None, reverse=False):
        self.source = source
        self.key = key
        self.reverse = reverse
        
    def __iter__(self):
        return itersort(self.source, self.key, self.reverse)
    

def itersort(source, key, reverse):
    it = iter(source)
    try:
        flds = it.next()
        yield flds
        
        # TODO merge sort on large dataset!!!
        rows = list(it)

        if key is not None:

            # convert field selection into field indices
            indices = asindices(flds, key)
             
            # now use field indices to construct a getkey function
            # N.B., this will probably raise an exception on short rows
            getkey = itemgetter(*indices)

            rows.sort(key=getkey, reverse=reverse)

        else:
            rows.sort(reverse=reverse)

        for row in rows:
            yield row
        
    finally:
        close(it)
    
    
def melt(table, key=[], variables=[], variable_field='variable', value_field='value'):
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
    
    return MeltView(table, key, variables, variable_field, value_field)
    
    
class MeltView(object):
    
    def __init__(self, source, key=[], variables=[], 
                 variable_field='variable', value_field='value'):
        self.source = source
        self.key = key
        self.variables = variables
        self.variable_field = variable_field
        self.value_field = value_field
        
    def __iter__(self):
        return itermelt(self.source, self.key, self.variables, 
                        self.variable_field, self.value_field)
    
    
def itermelt(source, key, variables, variable_field, value_field):
    it = iter(source)
    try:
        
        # normalise some stuff
        flds = it.next()
        if isinstance(key, basestring):
            key = (key,) # normalise to a tuple
        if isinstance(variables, basestring):
            # shouldn't expect this, but ... ?
            variables = (variables,) # normalise to a tuple
        if not key:
            # assume key is flds not in variables
            key = [f for f in flds if f not in variables]
        if not variables:
            # assume variables are flds not in key
            variables = [f for f in flds if f not in key]
        
        # determine the output flds
        out_flds = list(key)
        out_flds.append(variable_field)
        out_flds.append(value_field)
        yield out_flds
        
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
                yield o
                
    finally:
        close(it)



def recast(table, key=[], variable_field='variable', value_field='value', 
           sample_size=1000, reducers=dict(), missing=None):
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
        >>> table4 = recast(table3, variable_field='vars', value_field='vals')
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

    """
    
    return RecastView(table, key, variable_field, value_field, sample_size, reducers, missing)
    

class RecastView(object):
    
    def __init__(self, source, key=[], variable_field='variable', 
                 value_field='value', sample_size=1000, reducers=dict(), 
                 missing=None):
        self.source = source
        self.key = key
        self.variable_field = variable_field
        self.value_field = value_field
        self.sample_size = sample_size
        self.reducers = reducers
        self.missing = missing
        
    def __iter__(self):
        return iterrecast(self.source, self.key, self.variable_field, 
                          self.value_field, self.sample_size, self.reducers,
                          self.missing)


def iterrecast(source, key=[], variable_field='variable', value_field='value', 
               sample_size=1000, reducers=dict(), missing=None):        
    #
    # TODO implementing this by making two passes through the data is a bit
    # ugly, and could be costly if there are several upstream transformations
    # that would need to be re-executed each pass - better to make one pass,
    # caching the rows sampled to discover variables to be recast as fields?
    #
    
    try:
        
        it = iter(source)
        fields = it.next()
        
        # normalise some stuff
        key_fields = key
        variable_fields = variable_field # N.B., could be more than one
        if isinstance(key_fields, basestring):
            key_fields = (key_fields,)
        if isinstance(variable_fields, basestring):
            variable_fields = (variable_fields,)
        if not key_fields:
            # assume key_fields is fields not in variables
            key_fields = [f for f in fields if f not in variable_fields and f != value_field]
        if not variable_fields:
            # assume variables are fields not in key_fields
            variable_fields = [f for f in fields if f not in key_fields and f != value_field]
        
        # sanity checks
        assert value_field in fields, 'invalid value field: %s' % value_field
        assert value_field not in key_fields, 'value field cannot be key_fields'
        assert value_field not in variable_fields, 'value field cannot be variable field'
        for f in key_fields:
            assert f in fields, 'invalid key_fields field: %s' % f
        for f in variable_fields:
            assert f in fields, 'invalid variable field: %s' % f

        # we'll need these later
        value_index = fields.index(value_field)
        key_indices = [fields.index(f) for f in key_fields]
        variable_indices = [fields.index(f) for f in variable_fields]
        
        # determine the actual variable names to be cast as fields
        if isinstance(variable_fields, dict):
            # user supplied dictionary
            variables = variable_fields
        else:
            variables = defaultdict(set)
            # sample the data to discover variables to be cast as fields
            for row in islice(it, 0, sample_size):
                for i, f in zip(variable_indices, variable_fields):
                    variables[f].add(row[i])
            for f in variables:
                variables[f] = sorted(variables[f]) # turn from sets to sorted lists
        
        close(it) # finished the first pass
        
        # determine the output fields
        out_fields = list(key_fields)
        for f in variable_fields:
            out_fields.extend(variables[f])
        yield out_fields
        
        # output data
        
        source = sort(source, key=key_fields)
        it = iter(source)
        it = islice(it, 1, None) # skip header row
        getkey = itemgetter(*key_indices)
        
        # process sorted data in newfields
        groups = groupby(it, key=getkey)
        for key_value, group in groups:
            group = list(group) # may need to iterate over the group more than once
            if len(key_fields) > 1:
                out_row = list(key_value)
            else:
                out_row = [key_value]
            for f, i in zip(variable_fields, variable_indices):
                for variable in variables[f]:
                    # collect all values for the current variable
                    values = [r[value_index] for r in group if r[i] == variable]
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
            yield out_row
                    
    finally:
        close(it)

    
        
            
def duplicates(table, key, presorted=False):
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

    """
    
    return DuplicatesView(table, key, presorted)


class DuplicatesView(object):
    
    def __init__(self, source, key, presorted=False):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key)
        self.key = key
        
    def __iter__(self):
        return iterduplicates(self.source, self.key)


def iterduplicates(source, key):
    # assume source is sorted
    # first need to sort the data
    it = iter(source)

    try:
        flds = it.next()
        yield flds

        # convert field selection into field indices
        indices = asindices(flds, key)
            
        # now use field indices to construct a getkey function
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
                        yield previous
                        previous_yielded = True
                    yield row
                else:
                    # reset
                    previous_yielded = False
                previous = row
        
    finally:
        close(it)

    
    
    
def conflicts(table, key, missing=None, presorted=False):
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
    
    """
    
    return ConflictsView(table, key, missing, presorted)


class ConflictsView(object):
    
    def __init__(self, source, key, missing=None, presorted=False):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key)
        self.key = key
        self.missing = missing
        
        
    def __iter__(self):
        return iterconflicts(self.source, self.key, self.missing)
    
    
def iterconflicts(source, key, missing):
    it = iter(source)
    try:
        flds = it.next()
        yield flds

        # convert field selection into field indices
        indices = asindices(flds, key)
                        
        # now use field indices to construct a getkey function
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
                    for x, y in zip(previous, row):
                        if missing not in (x, y) and x != y:
                            conflict = True
                            break
                    if conflict:
                        if not previous_yielded:
                            yield previous
                            previous_yielded = True
                        yield row
                else:
                    # reset
                    previous_yielded = False
                previous = row
        
    finally:
        close(it)


def complement(a, b, presorted=False):
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

    """
    
    return ComplementView(a, b, presorted)


class ComplementView(object):
    
    def __init__(self, a, b, presorted=False):
        if presorted:
            self.a = a
            self.b = b
        else:
            self.a = sort(a)
            self.b = sort(b)
            
    def __iter__(self):
        return itercomplement(self.a, self.b)


def itercomplement(a, b):
    ita = iter(a) 
    itb = iter(b)
    try:
        aflds = ita.next()
        itb.next() # ignore b fields
        yield aflds
        
        a = ita.next()
        b = itb.next()
        # we want the elements in a that are not in b
        while True:
            if b is None or a < b:
                yield a
                try:
                    a = ita.next()
                except StopIteration:
                    break
            elif a == b:
                try:
                    a = ita.next()
                except StopIteration:
                    break
            else:
                try:
                    b = itb.next()
                except StopIteration:
                    b = None
    finally:
        close(ita)
        close(itb)
        
    
def diff(a, b, presorted=False):
    """
    Find the difference between two tables. Returns a pair of tables, e.g.::
    
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
        
    Convenient shorthand for ``(complement(b, a), complement(a, b))``.

    """

    if not presorted:    
        a = sort(a)
        b = sort(b)
    added = complement(b, a, presorted=True)
    subtracted = complement(a, b, presorted=True)
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


class CaptureView(object):
    
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


def itercapture(source, field, pattern, newfields, include_original, flags):
    it = iter(source)
    try:
        prog = re.compile(pattern, flags)
        
        flds = it.next()
        if field in flds:
            field_index = flds.index(field)
        elif isinstance(field, int) and field < len(flds):
            field_index = field
        else:
            raise Exception('field invalid: must be either field name or index')
        
        # determine output flds
        out_flds = list(flds)
        if not include_original:
            out_flds.remove(field)
        if newfields:   
            out_flds.extend(newfields)
        yield out_flds
        
        # construct the output data
        for row in it:
            value = row[field_index]
            if include_original:
                out_row = list(row)
            else:
                out_row = [v for i, v in enumerate(row) if i != field_index]
            out_row.extend(prog.search(value).groups())
            yield out_row
            
    finally:
        close(it)
    
        
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


class SplitView(object):
    
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
    try:
        prog = re.compile(pattern, flags)

        flds = it.next()
        if field in flds:
            field_index = flds.index(field)
        elif isinstance(field, int) and field < len(flds):
            field_index = field
        else:
            raise Exception('field invalid: must be either field name or index')
        
        # determine output flds
        out_flds = list(flds)
        if not include_original:
            out_flds.remove(field)
        if newfields:
            out_flds.extend(newfields)
        yield out_flds
        
        # construct the output data
        for row in it:
            value = row[field_index]
            if include_original:
                out_row = list(row)
            else:
                out_row = [v for i, v in enumerate(row) if i != field_index]
            out_row.extend(prog.split(value, maxsplit))
            yield out_row
            
    finally:
        close(it)
        
    
def select(table, where, padding=None):
    """
    Select rows meeting a condition. The `where` argument can be a function
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

    The `where` argument can also be an expression string, which will be converted
    to a function using :func:`expr`, e.g.::
    
        >>> table3 = select(table1, "{foo} == 'a' and {baz} > 88.1")
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+

    """
    
    if isinstance(where, basestring):
        # be kind to the user
        where = expr(where)
    return SelectView(table, where, padding)


class SelectView(object):
    
    def __init__(self, source, where, padding=None):
        self.source = source
        self.where = where
        self.missing = padding
        
        
    def __iter__(self):
        return iterselect(self.source, self.where, self.missing)
    
    
def iterselect(source, where, padding):
    it = iter(source)
    try:
        flds = it.next()
        yield flds
        for row in it:
            rec = asdict(flds, row, padding)
            if where(rec):
                yield row
    finally:
        close(it)
        
        
def fieldmap(table, mappings=None, errorvalue=None):
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
    
    return FieldMapView(table, mappings, errorvalue)
    
    
class FieldMapView(object):
    
    def __init__(self, source, mappings=None, errorvalue=None):
        self.source = source
        if mappings is None:
            self.mappings = OrderedDict()
        else:
            self.mappings = mappings
        self.errorvalue = errorvalue
        
    def __getitem__(self, key):
        return self.mappings[key]
    
    def __setitem__(self, key, value):
        self.mappings[key] = value
        
    def __iter__(self):
        return iterfieldmap(self.source, self.mappings, self.errorvalue)
    
    
def iterfieldmap(source, mappings, errorvalue):
    it = iter(source)
    try:
        flds = it.next()
        outflds = mappings.keys()
        yield outflds
        
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
                        val = errorvalue
                    outrow.append(val)
            yield outrow
                    
    finally:
        close(it)
        
        
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


def selecteq(table, field, value):
    """
    Select rows where values in the given field equal the given value. E.g.::
    
        >>> from petl import selecteq, look     
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 4, 9.3],
        ...           ['a', 2, 88.2],
        ...           ['b', 1, 23.3],
        ...           ['c', 8, 42.0],
        ...           ['d', 7, 100.9],
        ...           ['c', 2]]
        >>> table2 = selecteq(table1, 'foo', 'a')
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 4     | 9.3   |
        +-------+-------+-------+
        | 'a'   | 2     | 88.2  |
        +-------+-------+-------+

    """
    
    return select(table, lambda rec: rec[field] == value)


def rowreduce(table, key, reducer, header=None, presorted=False):
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
        >>> table2 = rowreduce(table1, key='foo', reducer=sumbar, header=['foo', 'barsum'])
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
    
    """

    return RowReduceView(table, key, reducer, header, presorted)


class RowReduceView(object):
    
    def __init__(self, source, key, reducer, header=None, presorted=False):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key)
        self.key = key
        self.header = header
        self.reducer = reducer

    def __iter__(self):
        return iterrowreduce(self.source, self.key, self.reducer, self.header)
    
    
def iterrowreduce(source, key, reducer, header):
    it = iter(source)

    try:
        srcflds = it.next()
        if header is None:
            yield srcflds
        else:
            yield header

        # convert field selection into field indices
        indices = asindices(srcflds, key)
        
        # now use field indices to construct a getkey function
        # N.B., this may raise an exception on short rows, depending on
        # the field selection
        getkey = itemgetter(*indices)
        
        for key, rows in groupby(it, key=getkey):
            yield reducer(key, rows)
        
    finally:
        close(it)


def recordreduce(table, key, reducer, header=None, presorted=False):
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
        >>> table2 = recordreduce(table1, key='foo', reducer=sumbar, header=['foo', 'barsum'])
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
    
    """

    return RecordReduceView(table, key, reducer, header, presorted)


class RecordReduceView(object):
    
    def __init__(self, source, key, reducer, header=None, presorted=False):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key)
        self.key = key
        self.header = header
        self.reducer = reducer

    def __iter__(self):
        return iterrecordreduce(self.source, self.key, self.reducer, self.header)
    
    
def iterrecordreduce(source, key, reducer, header):
    it = iter(source)

    try:
        srcflds = it.next()
        if header is None:
            yield srcflds
        else:
            yield header

        # convert field selection into field indices
        indices = asindices(srcflds, key)
        
        # now use field indices to construct a getkey function
        # N.B., this may raise an exception on short rows, depending on
        # the field selection
        getkey = itemgetter(*indices)
        
        for key, rows in groupby(it, key=getkey):
            records = [asdict(srcflds, row) for row in rows]
            yield reducer(key, records)
        
    finally:
        close(it)


def mergereduce(table, key, missing=None, presorted=False):
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
        | 'A'   | [1, 2] | 2.7         |
        +-------+--------+-------------+
        | 'B'   | 2      | 7.8         |
        +-------+--------+-------------+
        | 'D'   | 3      | [9.4, 12.3] |
        +-------+--------+-------------+
        | 'E'   | []     |             |
        +-------+--------+-------------+

    Missing values are overridden by non-missing values. Conflicting values are
    reported as a list.
    
    """

    def _mergereducer(key, rows):
        merged = list()
        for row in rows:
            for i, v in enumerate(row):
                if i == len(merged):
                    merged.append(list())
                if v != missing and v not in merged[i]:
                    merged[i].append(v)    
        # replace singletons
        merged = [vals[0] if len(vals) == 1 else vals for vals in merged]
        return merged
    
    return rowreduce(table, key, reducer=_mergereducer, presorted=presorted)


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
        | 4     | 'C'   | [False, True] | 12.4   |
        +-------+-------+---------------+--------+

    """
    
    assert 'key' in kwargs, 'keyword argument "key" is required'
    key = kwargs['key']
    missing = kwargs['missing'] if 'missing' in kwargs else None
    presorted = kwargs['presorted'] if 'presorted' in kwargs else False
    t1 = cat(*tables, missing=missing)
    t2 = mergereduce(t1, key=key, presorted=presorted)
    return t2


def aggregate(table, key, aggregators=None, presorted=False, errorvalue=None):
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

    """

    return AggregateView(table, key, aggregators, presorted, errorvalue)


class AggregateView(object):
    
    def __init__(self, source, key, aggregators=None, presorted=False, errorvalue=None):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key)
        self.key = key
        if aggregators is None:
            self.aggregators = OrderedDict()
        else:
            self.aggregators = aggregators
        self.errorvalue = errorvalue

    def __iter__(self):
        return iteraggregate(self.source, self.key, self.aggregators, self.errorvalue)
    
    def __getitem__(self, key):
        return self.aggregators[key]
    
    def __setitem__(self, key, value):
        self.aggregators[key] = value

    
def iteraggregate(source, key, aggregators, errorvalue):
    it = iter(source)

    try:
        srcflds = it.next()

        # normalise aggregators
        for outfld in aggregators:
            agg = aggregators[outfld]
            if not isinstance(agg, (list, tuple)):
                aggregators[outfld] = agg, list # list is default aggregation function
            
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
        yield outflds
        
        for key, rows in groupby(it, key=getkey):
            rows = list(rows) # may need to iterate over these more than once
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
                    aggval = errorvalue
                outrow.append(aggval)
            yield outrow
            
    finally:
        close(it)


def rowmap(table, rowmapper, header, failonerror=False):
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
        >>> table2 = rowmap(table1, rowmapper, header=['subject_id', 'gender', 'age_months', 'bmi'])  
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
    
    return RowMapView(table, rowmapper, header, failonerror)
    
    
class RowMapView(object):
    
    def __init__(self, source, rowmapper, header, failonerror=False):
        self.source = source
        self.rowmapper = rowmapper
        self.header = header
        self.failonerror = failonerror
        
    def __iter__(self):
        return iterrowmap(self.source, self.rowmapper, self.header, self.failonerror)
    
    
def iterrowmap(source, rowmapper, header, failonerror):
    it = iter(source)
    try:
        it.next() # discard source header
        yield header
        for row in it:
            try:
                outrow = rowmapper(row)
                yield outrow
            except:
                if failonerror:
                    raise
    finally:
        close(it)
        
        
def recordmap(table, recmapper, header, failonerror=False):
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
        >>> table2 = recordmap(table1, recmapper, header=['subject_id', 'gender', 'age_months', 'bmi'])  
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
    
    return RecordMapView(table, recmapper, header, failonerror)
    
    
class RecordMapView(object):
    
    def __init__(self, source, recmapper, header, failonerror=False):
        self.source = source
        self.recmapper = recmapper
        self.header = header
        self.failonerror = failonerror
        
    def __iter__(self):
        return iterrecordmap(self.source, self.recmapper, self.header, self.failonerror)
    
    
def iterrecordmap(source, recmapper, header, failonerror):
    it = iter(source)
    try:
        flds = it.next() # discard source header
        yield header
        for row in it:
            rec = asdict(flds, row)
            try:
                outrow = recmapper(rec)
                yield outrow
            except:
                if failonerror:
                    raise
    finally:
        close(it)
        
        
def rowmapmany(table, rowgenerator, header, failonerror=False):
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
        >>> table2 = rowmapmany(table1, rowgenerator, header=['subject_id', 'variable', 'value'])  
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
    
    return RowMapManyView(table, rowgenerator, header, failonerror)
    
    
class RowMapManyView(object):
    
    def __init__(self, source, rowgenerator, header, failonerror=False):
        self.source = source
        self.rowgenerator = rowgenerator
        self.header = header
        self.failonerror = failonerror
        
    def __iter__(self):
        return iterrowmapmany(self.source, self.rowgenerator, self.header, self.failonerror)
    
    
def iterrowmapmany(source, rowgenerator, header, failonerror):
    it = iter(source)
    try:
        it.next() # discard source header
        yield header
        for row in it:
            try:
                for outrow in rowgenerator(row):
                    yield outrow
            except:
                if failonerror:
                    raise
    finally:
        close(it)
        
        
def recordmapmany(table, rowgenerator, header, failonerror=False):
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
        >>> table2 = recordmapmany(table1, rowgenerator, header=['subject_id', 'variable', 'value'])  
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
    
    return RecordMapManyView(table, rowgenerator, header, failonerror)
    
    
class RecordMapManyView(object):
    
    def __init__(self, source, rowgenerator, header, failonerror=False):
        self.source = source
        self.rowgenerator = rowgenerator
        self.header = header
        self.failonerror = failonerror
        
    def __iter__(self):
        return iterrecordmapmany(self.source, self.rowgenerator, self.header, self.failonerror)
    
    
def iterrecordmapmany(source, rowgenerator, header, failonerror):
    it = iter(source)
    try:
        flds = it.next() # discard source header
        yield header
        for row in it:
            rec = asdict(flds, row)
            try:
                for outrow in rowgenerator(rec):
                    yield outrow
            except:
                if failonerror:
                    raise
    finally:
        close(it)
        
        
def setfields(table, flds):
    """
    Override fields in the given table. E.g.::
    
        >>> from petl import setfields, look
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2]]
        >>> table2 = setfields(table1, ['foofoo', 'barbar'])
        >>> look(table2)
        +----------+----------+
        | 'foofoo' | 'barbar' |
        +==========+==========+
        | 'a'      | 1        |
        +----------+----------+
        | 'b'      | 2        |
        +----------+----------+

    """
    
    return SetFieldsView(table, flds) 


class SetFieldsView(object):
    
    def __init__(self, source, flds):
        self.source = source
        self.flds = flds
        
    def __iter__(self):
        return itersetfields(self.source, self.flds)   


def itersetfields(source, flds):
    it = iter(source)
    try:
        it.next() # discard source fields
        yield flds
        for row in it:
            yield row
    finally:
        close(it)
        
        
def extendfields(table, flds):
    """
    Extend fields in the given table. E.g.::
    
        >>> from petl import extendfields, look
        >>> table1 = [['foo'],
        ...           ['a', 1, True],
        ...           ['b', 2, False]]
        >>> table2 = extendfields(table1, ['bar', 'baz'])
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | 1     | True  |
        +-------+-------+-------+
        | 'b'   | 2     | False |
        +-------+-------+-------+

    """
    
    return ExtendFieldsView(table, flds) 


class ExtendFieldsView(object):
    
    def __init__(self, source, flds):
        self.source = source
        self.flds = flds
        
    def __iter__(self):
        return iterextendfields(self.source, self.flds)   


def iterextendfields(source, flds):
    it = iter(source)
    try:
        srcflds = it.next() 
        outflds = list(srcflds)
        outflds.extend(flds)
        yield outflds
        for row in it:
            yield row
    finally:
        close(it)
        
        
def pushfields(table, flds):
    """
    Push rows down and prepend a header row. E.g.::

        >>> from petl import pushfields, look    
        >>> table1 = [['a', 1],
        ...           ['b', 2]]
        >>> table2 = pushfields(table1, ['foo', 'bar'])
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

    return PushFieldsView(table, flds)


class PushFieldsView(object):
    
    def __init__(self, source, flds):
        self.source = source
        self.flds = flds
        
    def __iter__(self):
        return iterpushfields(self.source, self.flds)   


def iterpushfields(source, flds):
    it = iter(source)
    try:
        yield flds
        for row in it:
            yield row
    finally:
        close(it)
        
    
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


class SkipView(object):
    
    def __init__(self, source, n):
        self.source = source
        self.n = n
        
    def __iter__(self):
        return iterskip(self.source, self.n)   


def iterskip(source, n):
    it = iter(source)
    try:
        for row in islice(it, n, None):
            yield row
    finally:
        close(it)
        
    
def unpack(table, field, newfields=None, max=None, include_original=False):
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
    
    return UnpackView(table, field, newfields, max, include_original)


class UnpackView(object):
    
    def __init__(self, source, field, newfields=None, max=None, 
                 include_original=False):
        self.source = source
        self.field = field
        self.newfields = newfields
        self.max = max
        self.include_original = include_original
        
    def __iter__(self):
        return iterunpack(self.source, self.field, self.newfields, self.max, 
                          self.include_original)


def iterunpack(source, field, newfields, max, include_original):
    it = iter(source)
    try:

        flds = it.next()
        if field in flds:
            field_index = flds.index(field)
        elif isinstance(field, int) and field < len(flds):
            field_index = field
        else:
            raise Exception('field invalid: must be either field name or index')
        
        # determine output flds
        out_flds = list(flds)
        if not include_original:
            out_flds.remove(field)
        if newfields:   
            out_flds.extend(newfields)
        yield out_flds
        
        # construct the output data
        for row in it:
            value = row[field_index]
            if include_original:
                out_row = list(row)
            else:
                out_row = [v for i, v in enumerate(row) if i != field_index]
            out_row.extend(value[:max])
            yield out_row
            
    finally:
        close(it)
        
        
def join(left, right, key=None, presorted=False):
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

    """
    
    if key is None:
        return NaturalJoinView(left, right, presorted)
    else:
        return JoinView(left, right, key, presorted)


class NaturalJoinView(object):
    
    def __init__(self, left, right, presorted=False, leftouter=False, 
                 rightouter=False, missing=None):
        self.left = left
        self.right = right
        self.presorted = presorted
        self.leftouter = leftouter
        self.rightouter = rightouter
        self.missing = missing
        
    def __iter__(self):
        return iternaturaljoin(self.left, self.right, self.presorted, 
                               leftouter=self.leftouter, 
                               rightouter=self.rightouter, 
                               missing=self.missing)
    

class JoinView(object):
    
    def __init__(self, left, right, key, presorted=False, leftouter=False, 
                 rightouter=False, missing=None):
        if presorted:
            self.left = left
            self.right = right
        else:
            self.left = sort(left, key)
            self.right = sort(right, key)
            # TODO what if someone sets self.key to something else after __init__?
            # (sort will be incorrect - maybe need to protect key with property setter?)
        self.key = key
        self.leftouter = leftouter
        self.rightouter = rightouter
        self.missing = missing
        
    def __iter__(self):
        return iterjoin(self.left, self.right, self.key, leftouter=self.leftouter,
                        rightouter=self.rightouter, missing=self.missing)
    
    
def leftjoin(left, right, key=None, missing=None, presorted=False):
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

    """
    
    if key is None:
        return NaturalJoinView(left, right, presorted=presorted, leftouter=True, 
                               rightouter=False, missing=missing)
    else:
        return JoinView(left, right, key, presorted=presorted, leftouter=True, 
                        rightouter=False, missing=missing)

    
def rightjoin(left, right, key=None, missing=None, presorted=False):
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

    """
    
    if key is None:
        return NaturalJoinView(left, right, presorted=presorted, leftouter=False, 
                               rightouter=True, missing=missing)
    else:
        return JoinView(left, right, key, presorted=presorted, leftouter=False, 
                        rightouter=True, missing=missing)
    
    
def outerjoin(left, right, key=None, missing=None, presorted=False):
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

    """

    if key is None:
        return NaturalJoinView(left, right, presorted=presorted, leftouter=True, 
                               rightouter=True, missing=missing)
    else:
        return JoinView(left, right, key, presorted=presorted, leftouter=True, 
                        rightouter=True, missing=missing)
    
    
def iterjoin(left, right, key, leftouter=False, rightouter=False, missing=None):
    lit = iter(left)
    rit = iter(right)
    try:
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
        yield outflds
        
        # define a function to join two groups of rows
        def joinrows(lrowgrp, rrowgrp):
            if rrowgrp is None:
                for lrow in lrowgrp:
                    outrow = list(lrow) # start with the left row
                    # extend with missing values in place of the right row
                    outrow.extend([missing] * len(rvind))
                    yield outrow
            elif lrowgrp is None:
                for rrow in rrowgrp:
                    # start with missing values in place of the left row
                    outrow = [missing] * len(lflds)
                    # set key values
                    for li, ri in zip(lkind, rkind):
                        outrow[li] = rrow[ri]
                    # extend with non-key values from the right row  
                    outrow.extend(rgetv(rrow))
                    yield outrow
            else:
                rrowgrp = list(rrowgrp) # may need to iterate more than once
                for lrow in lrowgrp:
                    for rrow in rrowgrp:
                        # start with the left row
                        outrow = list(lrow)
                        # extend with non-key values from the right row
                        outrow.extend(rgetv(rrow))
                        yield outrow

        # construct group iterators for both tables
        lgit = groupby(lit, key=lgetk)
        rgit = groupby(rit, key=rgetk)
        
        # pick off initial row groups
        lkval, lrowgrp = lgit.next() 
        rkval, rrowgrp = rgit.next()
        
        # loop until *either* of the iterators is exhausted
        try:
            while True:
                if lkval < rkval:
                    if leftouter:
                        for row in joinrows(lrowgrp, None):
                            yield row
                    # advance left
                    lkval, lrowgrp = lgit.next()
                elif lkval > rkval:
                    if rightouter:
                        for row in joinrows(None, rrowgrp):
                            yield row
                    # advance right
                    rkval, rrowgrp = rgit.next()
                else:
                    for row in joinrows(lrowgrp, rrowgrp):
                        yield row
                    # advance both
                    lkval, lrowgrp = lgit.next()
                    rkval, rrowgrp = rgit.next()
            
        except StopIteration:
            pass
        
        if leftouter:
            # any left over?
            for lkval, lrowgrp in lgit:
                for row in joinrows(lrowgrp, None):
                    yield row

        if rightouter:
            # any left over?
            for rkval, rrowgrp in rgit:
                for row in joinrows(None, rrowgrp):
                    yield row
                
    finally:
        close(lit)
        close(rit)
        
        
def iternaturaljoin(left, right, presorted=False, leftouter=False, 
                    rightouter=False, missing=None):
    # determine key field or fields
    lflds = fields(left)
    rflds = fields(right)
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
        left = sort(left, key)
        right = sort(right, key)
    # from here on it's the same as a normal join
    return iterjoin(left, right, key, leftouter=leftouter, rightouter=rightouter,
                    missing=missing)


def crossjoin(left, right):
    """
    Construct the cartesian product of the two tables. E.g.::

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
    
    return CrossJoinView(left, right)


class CrossJoinView(object):
    
    def __init__(self, left, right):
        self.left = left
        self.right = right
        
    def __iter__(self):
        return itercrossjoin(self.left, self.right)
    
    
def itercrossjoin(left, right):
    lflds = fields(left)
    rflds = fields(right)

    outflds = list(lflds)
    outflds.extend(rflds)
    yield outflds
    
    for lrow in data(left):
        for rrow in data(right):
            outrow = list(lrow)
            outrow.extend(rrow)
            yield outrow
        
