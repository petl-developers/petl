"""
TODO doc me

"""

from itertools import islice
from collections import deque
from operator import itemgetter


from petl.util import close, asindices, rowgetter, FieldSelectionError, asdict

__all__ = ['rename', 'cut', 'cat', 'convert', 'translate', 'extend', 'rowslice', \
           'head', 'tail', 'sort', 'melt', 'recast', 'duplicates', 'conflicts', \
           'mergeduplicates', 'select', 'complement', 'diff', 'stringcapture', \
           'stringsplit']


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
        
        
def cut(table, *args, **kwargs):
    """
    Choose and/or re-order columns. E.g.::

        >>> from petl import look, cut    
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2.7],
        ...          ['B', 2, 3.4],
        ...          ['B', 3, 7.8],
        ...          ['D', 42, 9.0],
        ...          ['E', 12]]
        >>> cut1 = cut(table, 'foo', 'baz')
        >>> look(cut1)
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

        >>> cut2 = cut(table, 0, 2)
        >>> look(cut2)
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

        >>> cut3 = cut(table, 'bar', 0)
        >>> look(cut3)
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
    
        >>> cut4 = cut(table, *range(0, 2))
        >>> look(cut4)    
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
    
    return CutView(table, args, **kwargs)


class CutView(object):
    
    def __init__(self, source, spec, missing=None):
        self.source = source
        self.spec = spec
        self.missing = missing
        
    def __iter__(self):
        return itercut(self.source, self.spec, self.missing)
        
        
def itercut(source, spec, missing=None):
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
    
    
def convert(table, converters=dict(), errorvalue=None):
    """
    Transform values in invidual fields. E.g.::

        >>> from petl import convert, look    
        >>> table1 = [['foo', 'bar'],
        ...           ['1', '2.4'],
        ...           ['3', '7.9'],
        ...           ['7', '2'],
        ...           ['8.3', '42.0'],
        ...           ['2', 'abc']]
        >>> table2 = convert(table1, {'foo': int, 'bar': float})
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

    Converter functions can also be specified by using the suffix notation on the
    returned table object. E.g.::

        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['1', '2.4', 14],
        ...           ['3', '7.9', 47],
        ...           ['7', '2', 11],
        ...           ['8.3', '42.0', 33],
        ...           ['2', 'abc', 'xyz']]
        >>> table2 = convert(table1)
        >>> look(table2)
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
        
        >>> table2['foo'] = int
        >>> table2['bar'] = float
        >>> table2['baz'] = lambda v: v**2
        >>> look(table2)
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

    """
    
    return ConvertView(table, converters, errorvalue)


class ConvertView(object):
    
    def __init__(self, source, converters=dict(), errorvalue=None):
        self.source = source
        self.converters = converters
        self.errorvalue = errorvalue
        
    def __iter__(self):
        return iterconvert(self.source, self.converters, self.errorvalue)
    
    def __setitem__(self, key, value):
        self.converters[key] = value
        
    def __getitem__(self, key):
        return self.converters[key]
    
    
def iterconvert(source, converters, errorvalue):
    it = iter(source)
    converters = converters.copy()
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
                try:
                    c = converters[f]
                except KeyError:
                    # no converter defined on this field, return value as-is
                    return v
                else:
                    try:
                        return c(v)
                    except ValueError:
                        return errorvalue
                    except TypeError:
                        return errorvalue

        # construct the data rows
        for row in it:
            yield [transform_value(i, v) for i, v in enumerate(row)]

    finally:
        close(it)
            

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
    
        >>> table1 = [['foo', 'bar'],
        ...           ['M', 12],
        ...           ['F', 34],
        ...           ['-', 56]]
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
    
        >>> table3 = sort(table1, key=('foo', 'bar'))
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
    
    
def melt(table):
    """
    TODO doc me
    
    """
    
    
def recast(table):
    """
    TODO doc me
    
    """
    
    
def duplicates(table):
    """
    TODO doc me
    
    """
    
    
def conflicts(table):
    """
    TODO doc me
    
    """
    
    
def mergeduplicates(table):
    """
    TODO doc me
    
    """
    
    
def select(table):
    """
    TODO doc me
    
    """
    
    
def complement(table):
    """
    TODO doc me
    
    """
    
    
def diff(table):
    """
    TODO doc me
    
    """
    
    
def stringcapture(table):
    """
    TODO doc me
    
    """
    
    
def stringsplit(table):
    """
    TODO doc me
    
    """
    
    
    
        