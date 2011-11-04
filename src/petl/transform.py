"""
TODO doc me

"""


from petl.util import close, asindices, rowgetter, FieldSelectionError

__all__ = ['rename', 'cut', 'cat', 'convert', 'translate']


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
        