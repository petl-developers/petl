"""
TODO doc me

"""


from itertools import islice
from collections import defaultdict, Counter
from operator import itemgetter
import datetime


__all__ = ['fields', 'data', 'records', 'count', 'look', 'see', 'values', 'valuecounter', 'valuecounts', \
           'valueset', 'unique', 'lookup', 'lookupone', 'recordlookup', 'recordlookupone', \
           'typecounter', 'typecounts', 'typeset', 'parsecounter', 'parsecounts', \
           'stats', 'rowlengths', 'DuplicateKeyError', 'datetimeparser', 'dateparser', 'timeparser', 'boolparser']


def fields(table):
    """
    Return the header row for the given table. E.g.::
    
        >>> from petl import fields
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

    
def data(table, start=0, stop=None, step=1):
    """
    Return an iterator over the data rows for the given table. E.g.::
    
        >>> from petl import data
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> it = data(table)
        >>> it.next()
        ['a', 1]
        >>> it.next()
        ['b', 2]
    
    """
    
    return islice(table, start + 1, stop, step)

    
def records(table, start=0, stop=None, step=1, missing=None):
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
    for row in islice(it, start, stop, step):
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
    
        >>> from petl import count
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
    
        >>> from petl import look
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
        
        
def see(table, start=0, stop=10, step=1):
    """
    Format a portion of a table as text in a column-oriented layout for 
    inspection in an interactive session. E.g.::
    
        >>> from petl import see
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
    Return an iterator over values in a given field. E.g.::
    
        >>> from petl import values
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> foo = values(table, 'foo')
        >>> foo.next()
        'a'
        >>> foo.next()
        'b'
        >>> foo.next()
        'b'
        >>> foo.next()
        'c'
        >>> foo.next()
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration

    If rows are uneven, any missing values are skipped, e.g.::
    
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> bar = values(table, 'bar')
        >>> bar.next()
        True
        >>> bar.next()
        True
        >>> bar.next()
        False
        >>> bar.next()
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration
    
    """
    
    it = iter(table)
    try:
        flds = it.next()
        indices = asindices(flds, field)
        assert len(indices) > 0, 'no field selected'
        getvalue = itemgetter(*indices)
        it = islice(it, start, stop, step)
        for row in it:
            try:
                value = getvalue(row)
                yield value
            except IndexError:
                pass # ignore short rows
    except:
        raise
    finally:
        close(it)
    
    
def valueset(table, field, start=0, stop=None, step=1):
    """
    Find distinct values for the given field. Returns a set. E.g.::

        >>> from petl import valueset
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> valueset(table, 'foo')
        set(['a', 'c', 'b'])
        >>> valueset(table, 'bar')
        set([False, True])
        
    Convenient shorthand for `set(values(table, field, start, stop, step))`.
        
    """

    return set(values(table, field, start, stop, step))


def valuecounter(table, field, start=0, stop=None, step=1):
    """
    Find distinct values for the given field and count the number of 
    occurrences. Returns a :class:`dict` mapping values to counts. E.g.::

        >>> from petl import valuecounter
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> c = valuecounter(table, 'foo')
        >>> c['a']
        1
        >>> c['b']
        2
        >>> c['c']
        1
        >>> c
        Counter({'b': 2, 'a': 1, 'c': 1})
    
    """

    counter = Counter()
    for v in values(table, field, start, stop, step):
        try:
            counter[v] += 1
        except IndexError:
            pass # short row
    return counter
            

def valuecounts(table, field, start=0, stop=None, step=1):    
    """
    Find distinct values for the given field and count the number of 
    occurrences. Returns a table mapping values to counts, with most common 
    values first. E.g.::

        >>> from petl import valuecounts
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> valuecounts(table, 'foo')
        [('value', 'count'), ('b', 2), ('a', 1), ('c', 1)]

    Can be combined with `look`, e.g.::
    
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> look(valuecounts(table, 'foo'))
        +---------+---------+
        | 'value' | 'count' |
        +=========+=========+
        | 'b'     | 2       |
        +---------+---------+
        | 'a'     | 1       |
        +---------+---------+
        | 'c'     | 1       |
        +---------+---------+
        
        >>> look(valuecounts(table, 'bar'))
        +---------+---------+
        | 'value' | 'count' |
        +=========+=========+
        | True    | 2       |
        +---------+---------+
        | False   | 1       |
        +---------+---------+
            
    """
    
    counter = valuecounter(table, field, start, stop, step)
    output = [('value', 'count')]
    output.extend(counter.most_common())
    return output
        
        
def unique(table, field):
    """
    Return True if there are no duplicate values for the given field, otherwise
    False. E.g.::

        >>> from petl import unique
        >>> table = [['foo', 'bar'], ['a', 1], ['b'], ['b', 2], ['c', 3, True]]
        >>> unique(table, 'foo')
        False
        >>> unique(table, 'bar')
        True
    
    """    

    vals = set()
    for v in values(table, field):
        if v in vals:
            return False
        else:
            vals.add(v)
    return True
       
        
# TODO handle short rows in lookup, lookupone, recordlookup, recordlookupone?


def lookup(table, key, value=None):
    """
    Construct a :class:`dict` (in memory) from the given table. E.g.::
    
        >>> from petl import lookup
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookup(table, 'foo', 'bar')
        >>> lkp['a']
        [1]
        >>> lkp['b']
        [2, 3]

    If no field or fields are given to select the value, defaults to the whole
    row (as a tuple), e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookup(table, 'foo')
        >>> lkp['a']
        [('a', 1)]
        >>> lkp['b']
        [('b', 2), ('b', 3)]

    Compound keys are supported, e.g.::
    
        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True],
        ...       ['b', 3, False]]
        >>> lkp = lookup(t2, ('foo', 'bar'), 'baz')
        >>> lkp[('a', 1)]
        [True]
        >>> lkp[('b', 2)]
        [False]
        >>> lkp[('b', 3)]
        [True, False]
    
    """
    
    it = iter(table)
    try:
        flds = it.next()
        if value is None:
            value = flds # default value is complete row
        keyindices = asindices(flds, key)
        assert len(keyindices) > 0, 'no key selected'
        valueindices = asindices(flds, value)
        assert len(valueindices) > 0, 'no value selected'
        lkp = defaultdict(list)
        getkey = itemgetter(*keyindices)
        getvalue = itemgetter(*valueindices)
        for row in it:
            k = getkey(row)
            v = getvalue(row)
            lkp[k].append(v)
        return lkp
    except:
        raise
    finally:
        close(it)
    
    
def lookupone(table, key, value=None, strict=True):
    """
    Construct a :class:`dict` (in memory) from the given table, assuming there is
    at most one value for each key. E.g.::
    
        >>> from petl import lookupone
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = lookupone(table, 'foo', 'bar')
        >>> lkp['a']
        1
        >>> lkp['b']
        2
        >>> lkp['c']
        2
        
    If the selected key is not unique, will raise DuplicateKeyError, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookupone(table, 'foo')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 451, in lookupone
        petl.util.DuplicateKeyError
        
    Unique checks can be overridden by providing `strict=False`, in which case
    the last value wins, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookupone(table, 'foo', 'bar', strict=False)
        >>> lkp['a']
        1
        >>> lkp['b']
        3
        
    Compound keys are supported, e.g.::
    
        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True]]
        >>> lkp = lookupone(t2, ('foo', 'bar'), 'baz')
        >>> lkp[('a', 1)]
        True
        >>> lkp[('b', 2)]
        False
        >>> lkp[('b', 3)]
        True
    
    """

    it = iter(table)
    try:
        flds = it.next()
        if value is None:
            value = flds
        keyindices = asindices(flds, key)
        assert len(keyindices) > 0, 'no key selected'
        valueindices = asindices(flds, value)
        assert len(valueindices) > 0, 'no value selected'
        lkp = defaultdict(list)
        getkey = itemgetter(*keyindices)
        getvalue = itemgetter(*valueindices)
        for row in it:
            k = getkey(row)
            if strict and k in lkp:
                raise DuplicateKeyError
            v = getvalue(row)
            lkp[k] = v
        return lkp
    except:
        raise
    finally:
        close(it)
    
    
def recordlookup(table, key):
    """
    Construct a :class:`dict` (in memory) from the given table, mapping to records. 
    E.g.::
    
        >>> from petl import recordlookup
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = recordlookup(table, 'foo')
        >>> lkp['a']
        [{'foo': 'a', 'bar': 1}]
        >>> lkp['b']
        [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]

    Compound keys are supported, e.g.::
    
        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True],
        ...       ['b', 3, False]]
        >>> lkp = recordlookup(t2, ('foo', 'bar'))
        >>> lkp[('a', 1)]
        [{'baz': True, 'foo': 'a', 'bar': 1}]
        >>> lkp[('b', 2)]
        [{'baz': False, 'foo': 'b', 'bar': 2}]
        >>> lkp[('b', 3)]
        [{'baz': True, 'foo': 'b', 'bar': 3}, {'baz': False, 'foo': 'b', 'bar': 3}]
    
    """
    
    it = iter(table)
    try:
        flds = it.next()
        keyindices = asindices(flds, key)
        assert len(keyindices) > 0, 'no key selected'
        lkp = defaultdict(list)
        getkey = itemgetter(*keyindices)
        for row in it:
            k = getkey(row)
            d = asdict(flds, row)
            lkp[k].append(d)
        return lkp
    except:
        raise
    finally:
        close(it)
    
        
def recordlookupone(table, key, strict=True):
    """
    Construct a :class:`dict` (in memory) from the given table, mapping to records,
    assuming there is at most one record for each key. E.g.::
    
        >>> from petl import recordlookupone
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = recordlookupone(table, 'foo')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}
        >>> lkp['c']
        {'foo': 'c', 'bar': 2}
        
    If the selected key is not unique, will raise DuplicateKeyError, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = recordlookupone(table, 'foo')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 451, in lookupone
        petl.util.DuplicateKeyError
        
    Unique checks can be overridden by providing `strict=False`, in which case
    the last record wins, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = recordlookupone(table, 'foo', strict=False)
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 3}
        
    Compound keys are supported, e.g.::
    
        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True]]
        >>> lkp = recordlookupone(t2, ('foo', 'bar'), strict=False)
        >>> lkp[('a', 1)]
        {'baz': True, 'foo': 'a', 'bar': 1}
        >>> lkp[('b', 2)]
        {'baz': False, 'foo': 'b', 'bar': 2}
        >>> lkp[('b', 3)]
        {'baz': True, 'foo': 'b', 'bar': 3}
    
    """    

    it = iter(table)
    try:
        flds = it.next()
        keyindices = asindices(flds, key)
        assert len(keyindices) > 0, 'no key selected'
        lkp = defaultdict(list)
        getkey = itemgetter(*keyindices)
        for row in it:
            k = getkey(row)
            if strict and k in lkp:
                raise DuplicateKeyError
            d = asdict(flds, row)
            lkp[k] = d
        return lkp
    except:
        raise
    finally:
        close(it)
    
            
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
    
    
def rowlengths(table, start=0, stop=None, step=1):
    """
    Report on row lengths found in the table. E.g.::
    
        >>> from petl import look, rowlengths
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', '2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', 'xyz', 9.0],
        ...          ['E', None],
        ...          ['F', 9]]
        >>> look(rowlengths(table))
        +----------+---------+
        | 'length' | 'count' |
        +==========+=========+
        | 3        | 3       |
        +----------+---------+
        | 2        | 2       |
        +----------+---------+
        | 4        | 1       |
        +----------+---------+

    """

    it = data(table, start, stop, step)
    try:
        counter = Counter()
        for row in it:
            counter[len(row)] += 1
        output = [('length', 'count')]
        output.extend(counter.most_common())
        return output
    except:
        raise
    finally:
        close(it)


def typecounter(table, field, start=0, stop=None, step=1):    
    """
    Count the number of values found for each Python type. E.g.::

        >>> from petl import typecounter
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', u'xyz', 9.0],
        ...          ['E', 42]]
        >>> typecounter(table, 'foo')
        Counter({'str': 4, 'unicode': 1})
        >>> typecounter(table, 'bar')
        Counter({'unicode': 3, 'int': 2})
        >>> typecounter(table, 'baz')
        Counter({'int': 1, 'float': 1, 'unicode': 1, 'str': 1})
    
    """
    
    counter = Counter()
    for v in values(table, field, start, stop, step):
        try:
            counter[v.__class__.__name__] += 1
        except IndexError:
            pass # ignore short rows
    return counter


def typecounts(table, field, start=0, stop=None, step=1):    
    """
    Count the number of values found for each Python type and return a table
    mapping class names to counts. E.g.::

        >>> from petl import look, typecounts
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', u'xyz', 9.0],
        ...          ['E', 42]]
        >>> look(typecounts(table, 'foo'))
        +-----------+---------+
        | 'type'    | 'count' |
        +===========+=========+
        | 'str'     | 4       |
        +-----------+---------+
        | 'unicode' | 1       |
        +-----------+---------+
        
        >>> look(typecounts(table, 'bar'))
        +-----------+---------+
        | 'type'    | 'count' |
        +===========+=========+
        | 'unicode' | 3       |
        +-----------+---------+
        | 'int'     | 2       |
        +-----------+---------+
        
        >>> look(typecounts(table, 'baz'))
        +-----------+---------+
        | 'type'    | 'count' |
        +===========+=========+
        | 'int'     | 1       |
        +-----------+---------+
        | 'float'   | 1       |
        +-----------+---------+
        | 'unicode' | 1       |
        +-----------+---------+
        | 'str'     | 1       |
        +-----------+---------+

    
    """
    
    counter = typecounter(table, field, start, stop, step)
    output = [('type', 'count')]
    output.extend(counter.most_common())
    return output


def typeset(table, field, start=0, stop=None, step=1):
    """
    Return a set containing all Python types found for values in the given field.
    E.g.::
    
        >>> from petl import typeset
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, '2'],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', '7.8', True],
        ...          ['D', u'xyz', 9.0],
        ...          ['E', 42]]
        >>> typeset(table, 'foo') 
        set([<type 'str'>, <type 'unicode'>])
        >>> typeset(table, 'bar') 
        set([<type 'int'>, <type 'unicode'>])
        >>> typeset(table, 'baz') 
        set([<type 'float'>, <type 'str'>])
    
    """

    s = set()
    for v in values(table, field, start, stop, step):
        try:
            s.add(v.__class__)
        except IndexError:
            pass # ignore short rows
    return s
    

def parsecounter(table, field, parsers={'int': int, 'float': float}, start=0, stop=None, step=1):    
    """
    Count the number of `str` or `unicode` values in the given fields that can 
    be parsed as ints, floats or via custom parser functions. Return a pair of 
    `Counter` objects, the first mapping parser names to the number of strings 
    successfully parsed, the second mapping parser names to the number of errors. 
    E.g.::
    
        >>> from petl import parsecounter
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 'aaa', 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', '3.7', 9.0],
        ...          ['E', 42]]
        >>> counter, errors = parsecounter(table, 'bar')
        >>> counter
        Counter({'float': 3, 'int': 2})
        >>> errors
        Counter({'int': 2, 'float': 1})
        
    """
    
    counter, errors = Counter(), Counter()
    for v in values(table, field, start, stop, step):
        if isinstance(v, basestring):
            for name, parser in parsers.items():
                try:
                    parser(v)
                except:
                    errors[name] += 1
                else:
                    counter[name] += 1
    return counter, errors


def parsecounts(table, field, parsers={'int': int, 'float': float}, start=0, stop=None, step=1):    
    """
    Count the number of `str` or `unicode` values that can be parsed as ints, 
    floats or via custom parser functions. Return a table mapping parser names
    to the number of values successfully parsed and the number of errors. E.g.::
    
        >>> from petl import look, parsecounts
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 'aaa', 2],
        ...          ['B', u'2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', '3.7', 9.0],
        ...          ['E', 42]]
        >>> look(parsecounts(table, 'bar'))
        +---------+---------+----------+
        | 'type'  | 'count' | 'errors' |
        +=========+=========+==========+
        | 'float' | 3       | 1        |
        +---------+---------+----------+
        | 'int'   | 2       | 2        |
        +---------+---------+----------+

    """
    
    counter, errors = parsecounter(table, field, parsers, start, stop, step)
    output_fields = [('type', 'count', 'errors')]
    output_data = [(item, count, errors[item]) for (item, count) in counter.most_common()]
    output = output_fields + output_data
    return output


def datetimeparser(format):
    """
    Return a function to parse strings as :class:`datetime.datetime` objects using a given format.
    E.g.::

        >>> from petl import datetimeparser
        >>> isodatetime = datetimeparser('%Y-%m-%dT%H:%M:%S')
        >>> isodatetime('2002-12-25T00:00:00')
        datetime.datetime(2002, 12, 25, 0, 0)
        >>> isodatetime('2002-12-25T00:00:99')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1018, in parser
            return datetime.strptime(value.strip(), format)
          File "/usr/lib/python2.7/_strptime.py", line 328, in _strptime
            data_string[found.end():])
        ValueError: unconverted data remains: 9

    Can be used with :func:`parsecounts`, e.g.::
    
        >>> from petl import look, parsecounts, datetimeparser
        >>> table = [['when', 'who'],
        ...          ['2002-12-25T00:00:00', 'Alex'],
        ...          ['2004-09-12T01:10:11', 'Gloria'],
        ...          ['2002-13-25T00:00:00', 'Marty'],
        ...          ['2002-02-30T07:09:00', 'Melman']]
        >>> parsers={'datetime': datetimeparser('%Y-%m-%dT%H:%M:%S')}
        >>> look(parsecounts(table, 'when', parsers))
        +------------+---------+----------+
        | 'type'     | 'count' | 'errors' |
        +============+=========+==========+
        | 'datetime' | 2       | 2        |
        +------------+---------+----------+
    
    """
    
    def parser(value):
        return datetime.datetime.strptime(value.strip(), format)
    return parser
    

def dateparser(format):
    """
    Return a function to parse strings as :class:`datetime.date` objects using a given format.
    E.g.::
    
        >>> from petl import dateparser
        >>> isodate = dateparser('%Y-%m-%d')
        >>> isodate('2002-12-25')
        datetime.date(2002, 12, 25)
        >>> isodate('2002-02-30')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1032, in parser
            return parser
          File "/usr/lib/python2.7/_strptime.py", line 440, in _strptime
            datetime_date(year, 1, 1).toordinal() + 1
        ValueError: day is out of range for month


    Can be used with :func:`parsecounts`, e.g.::
    
        >>> from petl import look, parsecounts, dateparser
        >>> table = [['when', 'who'],
        ...          ['2002-12-25', 'Alex'],
        ...          ['2004-09-12', 'Gloria'],
        ...          ['2002-13-25', 'Marty'],
        ...          ['2002-02-30', 'Melman']]
        >>> parsers={'date': dateparser('%Y-%m-%d')}
        >>> look(parsecounts(table, 'when', parsers))
        +--------+---------+----------+
        | 'type' | 'count' | 'errors' |
        +========+=========+==========+
        | 'date' | 2       | 2        |
        +--------+---------+----------+
        
    """
    
    def parser(value):
        return datetime.datetime.strptime(value.strip(), format).date()
    return parser
    

def timeparser(format):
    """
    Return a function to parse strings as :class:`datetime.time` objects using a given format.
    E.g.::
    
        >>> from petl import timeparser
        >>> isotime = timeparser('%H:%M:%S')
        >>> isotime('00:00:00')
        datetime.time(0, 0)
        >>> isotime('13:00:00')
        datetime.time(13, 0)
        >>> isotime('12:00:99')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1046, in parser
            
          File "/usr/lib/python2.7/_strptime.py", line 328, in _strptime
            data_string[found.end():])
        ValueError: unconverted data remains: 9
        >>> isotime('25:00:00')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1046, in parser
            
          File "/usr/lib/python2.7/_strptime.py", line 325, in _strptime
            (data_string, format))
        ValueError: time data '25:00:00' does not match format '%H:%M:%S'

    Can be used with :func:`parsecounts`, e.g.::
    
        >>> from petl import look, parsecounts, timeparser
        >>> table = [['when', 'who'],
        ...          ['00:00:00', 'Alex'],
        ...          ['12:02:45', 'Gloria'],
        ...          ['25:01:01', 'Marty'],
        ...          ['09:70:00', 'Melman']]
        >>> parsers={'time': timeparser('%H:%M:%S')}
        >>> look(parsecounts(table, 'when', parsers))
        +--------+---------+----------+
        | 'type' | 'count' | 'errors' |
        +========+=========+==========+
        | 'time' | 2       | 2        |
        +--------+---------+----------+
    
    """
    
    def parser(value):
        return datetime.datetime.strptime(value.strip(), format).time()
    return parser
    

def boolparser(true_strings=['true', 't', 'yes', 'y', '1'], 
               false_strings=['false', 'f', 'no', 'n', '0'],
               case_sensitive=False):
    """
    Return a function to parse strings as :class:`bool` objects using a given set of
    string representations for `True` and `False`.
    E.g.::

        >>> from petl import boolparser    
        >>> mybool = boolparser(true_strings=['yes', 'y'], false_strings=['no', 'n'])
        >>> mybool('y')
        True
        >>> mybool('Y')
        True
        >>> mybool('yes')
        True
        >>> mybool('No')
        False
        >>> mybool('nO')
        False
        >>> mybool('true')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1175, in parser
            raise ValueError('value is not one of recognised boolean strings: %r' % value)
        ValueError: value is not one of recognised boolean strings: 'true'
        >>> mybool('foo')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1175, in parser
            raise ValueError('value is not one of recognised boolean strings: %r' % value)
        ValueError: value is not one of recognised boolean strings: 'foo'
    
    Can be used with :func:`parsecounts`, e.g.::
    
        >>> from petl import look, parsecounts, boolparser
        >>> table = [['who', 'vote'],
        ...          ['Alex', 'yes'],
        ...          ['Gloria', 'N'],
        ...          ['Marty', 'hmmm'],
        ...          ['Melman', 'nope']]
        >>> mybool = boolparser(true_strings=['yes', 'y'], false_strings=['no', 'n'])
        >>> parsers = {'bool': mybool}
        >>> look(parsecounts(table, 'vote', parsers))
        +--------+---------+----------+
        | 'type' | 'count' | 'errors' |
        +========+=========+==========+
        | 'bool' | 2       | 2        |
        +--------+---------+----------+

    """
    
    if not case_sensitive:
        true_strings = [s.lower() for s in true_strings]
        false_strings = [s.lower() for s in false_strings]
    def parser(value):
        value = value.strip()
        if not case_sensitive:
            value = value.lower()
        if value in true_strings:
            return True
        elif value in false_strings:
            return False
        else:
            raise ValueError('value is not one of recognised boolean strings: %r' % value)
    return parser
    

def stats(table, field, start=0, stop=None, step=1):
    """
    Calculate basic descriptive statistics on a given field. E.g.::
    
        >>> from petl import stats
        >>> table = [['foo', 'bar', 'baz'],
        ...          ['A', 1, 2],
        ...          ['B', '2', '3.4'],
        ...          [u'B', u'3', u'7.8', True],
        ...          ['D', 'xyz', 9.0],
        ...          ['E', None]]
        >>> stats(table, 'bar')    
        {'count': 3, 'errors': 2, 'min': 1.0, 'max': 3.0, 'sum': 6.0, 'mean': 2.0}
        
    """
    
    output = {'min': None, 
              'max': None,
              'sum': None, 
              'mean': None, 
              'count': 0, 
              'errors': 0}
    for v in values(table, field, start, stop, step):
        try:
            v = float(v)
        except:
            output['errors'] += 1
        else:
            if output['min'] is None or v < output['min']:
                output['min'] = v
            if output['max'] is None or v > output['max']:
                output['max'] = v
            if output['sum'] is None:
                output['sum'] = v
            else:
                output['sum'] += v
            output['count'] += 1
    if output['count'] > 0:
        output['mean'] = output['sum'] / output['count']
    return output
        
# TODO string lengths, string patterns, ...
