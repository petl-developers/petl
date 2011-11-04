"""
TODO doc me

"""


from petl.util import close, asindices, rowgetter

__all__ = ['rename', 'cut']


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
        source = self.source
        spec = self.spec
        return iterrename(source, spec)
    
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
        
        
def cut(table, *args):
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

    Note that any short rows will be padded with `None` values.
    
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
    
    return CutView(table, args)


class CutView(object):
    
    def __init__(self, source, spec, missing=None):
        self.source = source
        self.spec = spec
        self.missing = missing
        
    def __iter__(self):
        source = self.source
        spec = self.spec
        missing = self.missing
        return itercut(source, spec, missing)
        
        
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
    
    