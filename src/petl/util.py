"""
TODO doc me

"""


from itertools import islice
from collections import defaultdict, Counter
from operator import itemgetter


__all__ = ['fields', 'data', 'records', 'count', 'look', 'see', 'values', \
           'valueset', 'unique', 'index', 'indexone', 'recindex', 'recindexone', \
           'types', 'parsetypes', 'stats', 'rowlengths', 'DuplicateKeyError']


def fields(table):
    """
    Return the header row for the given table. E.g.::
    
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> fields(table)
        ['foo', 'bar']
    
    """
    
    it = iter(table)
    flds = None
    try:
        flds = it.next()
    except:
        raise
    finally:
        close(it)
    return flds

    
def data(table):
    """
    Return an iterator over the data rows for the given table. E.g.::
    
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> it = data(table)
        >>> it.next()
        ['a', 1]
        >>> it.next()
        ['b', 2]
    
    """
    
    return islice(table, 1, None)

    
def records(table, missing=None):
    """
    Return an iterator over the data in the table, yielding each row as a 
    dictionary of values indexed by field. E.g.::
    
        >>> from petl import records
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> it = records(table)
        >>> it.next()
        {'foo': 'a', 'bar': 1}
        >>> it.next()
        {'foo': 'b', 'bar': 2}

    """
    
    it = iter(table)
    flds = it.next()
    for row in it:
        yield asdict(flds, row, missing)
    
    
def asdict(flds, row, missing=None):
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
    
    
def count(table):
    """
    Count the number of rows in a table. E.g.::
    
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> count(table)
        2
    """
    
    n = 0
    for row in data(table):
        n += 1
    return n
    
    
def look(table, start=0, stop=10, step=1):
    """
    Format a portion of the table as text for inspection in an interactive
    session. E.g.::
    
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
        
    """
    
    return Look(table, start, stop, step)
    
    
class Look(object):
    
    def __init__(self, table, start=0, stop=10, step=1):
        self.table = table
        self.start = start
        self.stop = stop
        self.step = step
        
    def __repr__(self):
        it = iter(self.table)
        try:
            
            # fields representation
            flds = it.next()
            fldsrepr = [repr(f) for f in flds]
            
            # rows representations
            rows = list(islice(it, self.start, self.stop, self.step))
            rowsrepr = [[repr(v) for v in row] for row in rows]
            
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
            for valsrepr in rowsrepr:
                rowline = u'|'
                for i, w in enumerate(colwidths):
                    v = valsrepr[i]
                    rowline += u' ' + v
                    rowline += u' ' * (w - len(v)) # padding
                    rowline += u' |'
                rowline += u'\n'
                rowlines.append(rowline)
                
            # put it all together
            output = sep + fldsline + hedsep
            for line in rowlines:
                output += line + sep
            
            return output
        except:
            raise
        finally:
            close(it)
    
    
    def __str__(self):
        return repr(self)
        
        
def see(table, start=0, stop=5, step=1):
    """
    Format a portion of a table as text in a column-oriented layout for 
    inspection in an interactive session. E.g.::
    
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> see(table)
        'foo': 'a', 'b'
        'bar': 1, 2

    Useful for tables with a larger number of fields.
    
    """

    return See(table, start, stop, step)


class See(object):
    
    def __init__(self, table, start=0, stop=5, step=1):
        self.table = table
        self.start = start
        self.stop = stop
        self.step = step
        
    def __repr__(self):    
        it = iter(self.table)
        try:
            flds = it.next()
            cols = defaultdict(list)
            for row in islice(it, self.start, self.stop, self.step):
                for i, f in enumerate(flds):
                    try:
                        cols[f].append(repr(row[i]))
                    except IndexError:
                        cols[f].append('')
            close(it)
            output = u''
            for f in flds:
                output += u'%r: %s\n' % (f, u', '.join(cols[f]))
            return output
        
        except:
            raise
        finally:
            close(it)
    
    
def values(table, field, start=0, stop=None, step=1):    
    """
    Find distinct values for the given field and count the number of 
    occurrences. Returns a table mapping values to counts. E.g.::

        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> values(table, 'foo')
        [('value', 'count'), ('b', 2), ('a', 1), ('c', 1)]

    Can be combined with `look`, e.g.::
    
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> look(values(table, 'foo'))
        +---------+---------+
        | 'value' | 'count' |
        +=========+=========+
        | 'b'     | 2       |
        +---------+---------+
        | 'a'     | 1       |
        +---------+---------+
        | 'c'     | 1       |
        +---------+---------+
        
        >>> look(values(table, 'bar'))
        +---------+---------+
        | 'value' | 'count' |
        +=========+=========+
        | True    | 2       |
        +---------+---------+
        | False   | 1       |
        +---------+---------+
            
    """
    
    it = iter(table)
    try:
        flds = it.next()
        assert field in flds, 'field not found: %s' % field
        field_index = flds.index(field)
        it = islice(it, start, stop, step)
        counter = Counter()
        for row in it:
            try:
                counter[row[field_index]] += 1
            except IndexError:
                pass # short row
        output = [('value', 'count')]
        output.extend(counter.most_common())
        return output
    except:
        raise
    finally:
        close(it)
        
        
def valueset(table, field, start=0, stop=None, step=1):
    """
    Find distinct values for the given field. Returns a set. E.g.::

        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> valueset(table, 'foo')
        set(['a', 'c', 'b'])
        >>> valueset(table, 'bar')
        set([False, True])
        
    """

    it = iter(table)
    try:
        flds = it.next()
        assert field in flds, 'field not found: %s' % field
        field_index = flds.index(field)
        it = islice(it, start, stop, step)
        vals = set()
        for row in it:
            try:
                vals.add(row[field_index])
            except IndexError:
                pass # short row
        return vals
    except:
        raise
    finally:
        close(it)
        
        
def unique(table, field):
    """
    Return True if there are no duplicate values for the given field, otherwise
    False. E.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b'], ['b', 2], ['c', 3, True]]
        >>> unique(table, 'foo')
        False
        >>> unique(table, 'bar')
        True
    
    """    

    it = iter(table)
    try:
        flds = it.next()
        assert field in flds, 'field not found: %s' % field
        field_index = flds.index(field)
        vals = set()
        for row in it:
            try:
                val = row[field_index]
            except IndexError:
                pass # ignore short rows
            else:
                if val in vals:
                    return False
                else:
                    vals.add(val)
        return True
    except:
        raise
    finally:
        close(it)
       
        
def index(table, key, value=None):
    """TODO doc me"""
    
    it = iter(table)
    try:
        flds = it.next()
        if value is None:
            value = flds
        keyindices = asindices(flds, key)
        assert len(keyindices) > 0, 'no key selected'
        valueindices = asindices(flds, value)
        assert len(valueindices) > 0, 'no value selected'
        idx = defaultdict(list)
        getkey = itemgetter(*keyindices)
        getvalue = itemgetter(*valueindices)
        # TODO handle short rows?
        for row in it:
            k = getkey(row)
            v = getvalue(row)
            idx[k].append(v)
        return idx
    except:
        raise
    finally:
        close(it)
    
    
def indexone(table, key, value=None, strict=True):
    """TODO doc me"""

    it = iter(table)
    try:
        flds = it.next()
        if value is None:
            value = flds
        keyindices = asindices(flds, key)
        assert len(keyindices) > 0, 'no key selected'
        valueindices = asindices(flds, value)
        assert len(valueindices) > 0, 'no value selected'
        idx = defaultdict(list)
        getkey = itemgetter(*keyindices)
        getvalue = itemgetter(*valueindices)
        # TODO handle short rows?
        for row in it:
            k = getkey(row)
            if strict and k in idx:
                raise DuplicateKeyError
            v = getvalue(row)
            idx[k] = v
        return idx
    except:
        raise
    finally:
        close(it)
    
    
def recindex(table, key):
    """TODO doc me"""
    
    it = iter(table)
    try:
        flds = it.next()
        keyindices = asindices(flds, key)
        assert len(keyindices) > 0, 'no key selected'
        idx = defaultdict(list)
        getkey = itemgetter(*keyindices)
        # TODO handle short rows?
        for row in it:
            k = getkey(row)
            d = asdict(flds, row)
            idx[k].append(d)
        return idx
    except:
        raise
    finally:
        close(it)
    
        
def recindexone(table, key, strict=True):
    """TODO doc me"""
    
    it = iter(table)
    try:
        flds = it.next()
        keyindices = asindices(flds, key)
        assert len(keyindices) > 0, 'no key selected'
        idx = defaultdict(list)
        getkey = itemgetter(*keyindices)
        # TODO handle short rows?
        for row in it:
            k = getkey(row)
            if strict and k in idx:
                raise DuplicateKeyError
            d = asdict(flds, row)
            idx[k] = d
        return idx
    except:
        raise
    finally:
        close(it)
    
            
def types(table):
    """TODO doc me"""
    
    
def parsetypes(table):
    """TODO doc me"""
    
    
def stats(table):
    """TODO doc me"""
    
    
def rowlengths(table):
    """TODO doc me"""
    
    
def close(o):
    """
    If the object has a 'close' method, call it. 
    
    """

    if hasattr(o, 'close') and callable(getattr(o, 'close')):
        o.close()
        
        
class DuplicateKeyError(Exception):
    pass


def asindices(flds, selection):
    """
    TODO doc me
    
    """
    indices = list()
    if isinstance(selection, basestring):
        selection = (selection,)
    if isinstance(selection, int):
        selection = (selection,)
    for s in selection:
        # selection could be a field name
        if s in flds:
            indices.append(flds.index(s))
        # or selection could be a field index
        elif isinstance(s, int) and s - 1 < len(flds):
            indices.append(s - 1) # index fields from 1, not 0
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
    