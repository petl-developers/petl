"""
TODO doc me

"""


from itertools import islice
from collections import defaultdict, Counter
from operator import itemgetter
import datetime
import re


def header(table):
    """
    Return the header row for the given table. E.g.::
    
        >>> from petl import header
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> header(table)
        ['foo', 'bar']
    
    """
    
    it = iter(table)
    return it.next()


def fieldnames(table):
    """
    Return the string values of all fields for the given table. If the fields
    are strings, then this function is equivalent to :func:`header`, i.e.::
    
        >>> from petl import header, fieldnames
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> header(table)
        ['foo', 'bar']
        >>> fieldnames(table)
        ['foo', 'bar']
        >>> header(table) == fieldnames(table)
        True
    
    Allows for custom field objects, e.g.::

        >>> class CustomField(object):
        ...     def __init__(self, id, description):
        ...         self.id = id
        ...         self.description = description
        ...     def __str__(self):
        ...         return self.id
        ...     def __repr__(self):
        ...         return 'CustomField(%r, %r)' % (self.id, self.description)
        ... 
        >>> table = [[CustomField('foo', 'Get some foo.'), CustomField('bar', 'A lot of bar.')], 
        ...          ['a', 1], 
        ...          ['b', 2]]
        >>> header(table)
        [CustomField('foo', 'Get some foo.'), CustomField('bar', 'A lot of bar.')]
        >>> fieldnames(table)    
        ['foo', 'bar']

    """
    
    return [str(f) for f in header(table)]

    
def data(table, *sliceargs):
    """
    Return an iterator over the data rows for the given table. E.g.::
    
        >>> from petl import data
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> it = data(table)
        >>> it.next()
        ['a', 1]
        >>> it.next()
        ['b', 2]
        
    .. versionchanged:: 0.3
    
    Positional arguments can be used to slice the data rows. The `sliceargs` are 
    passed to :func:`itertools.islice`.
    
    """
    
    it = islice(table, 1, None) # skip header row
    if sliceargs:
        it = islice(it, *sliceargs)
    return it


def dataslice(table, *args):
    """
    .. deprecated:: 0.3
    
    Use :func:`data` instead, it supports slice arguments.
    
    """
    
    return islice(data(table), *args)

    
def records(table, missing=None):
    """
    Return an iterator over the data in the table, yielding each row as a 
    dictionary of values indexed by field name. E.g.::
    
        >>> from petl import records
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> it = records(table)
        >>> it.next()
        {'foo': 'a', 'bar': 1}
        >>> it.next()
        {'foo': 'b', 'bar': 2}
        >>> it.next()
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration
        
    Short rows are padded, e.g.::
    
        >>> table = [['foo', 'bar'], ['a', 1], ['b']]
        >>> it = records(table)
        >>> it.next()
        {'foo': 'a', 'bar': 1}
        >>> it.next()
        {'foo': 'b', 'bar': None}
        >>> it.next()
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration

    """
    
    it = iter(table)
    flds = it.next()
    for row in it:
        yield asdict(flds, row, missing)
    
    
def asdict(flds, row, missing=None):
    names = [str(f) for f in flds]
    try:
        # list comprehension should be faster
        items = [(names[i], row[i]) for i in range(len(names))]
    except IndexError:
        # short row, fall back to slower for loop
        items = list()
        for i, f in enumerate(names):
            try:
                v = row[i]
            except IndexError:
                v = missing
            items.append((f, v))
    return dict(items)
    
    
def rowcount(table):
    """
    Count the number of data rows in a table. E.g.::
    
        >>> from petl import rowcount
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> rowcount(table)
        2
    """
    
    n = 0
    for row in data(table):
        n += 1
    return n
    
    
def look(table, *sliceargs):
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
        
    .. versionchanged:: 0.3
    
    Positional arguments can be used to slice the data rows. The `sliceargs` are 
    passed to :func:`itertools.islice`.

    """
    
    return Look(table, *sliceargs)


def lookall(table):
    """
    Format the entire table as text for inspection in an interactive session.
    
    N.B., this will load the entire table into memory.
    """
    
    return look(table, 0, None)
    
    
class Look(object):
    
    def __init__(self, table, *sliceargs):
        self.table = table
        if not sliceargs:
            self.sliceargs = (10,)
        else:
            self.sliceargs = sliceargs
        
    def __repr__(self):
        it = iter(self.table)
            
        # fields representation
        flds = it.next()
        fldsrepr = [repr(f) for f in flds]
        
        # rows representations
        rows = list(islice(it, *self.sliceargs))
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
    
    
    def __str__(self):
        return repr(self)
        
        
def see(table, *sliceargs):
    """
    Format a portion of a table as text in a column-oriented layout for 
    inspection in an interactive session. E.g.::
    
        >>> from petl import see
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> see(table)
        'foo': 'a', 'b'
        'bar': 1, 2

    Useful for tables with a larger number of fields.
    
    .. versionchanged:: 0.3
    
    Positional arguments can be used to slice the data rows. The `sliceargs` are 
    passed to :func:`itertools.islice`.

    """

    return See(table, *sliceargs)


class See(object):
    
    def __init__(self, table, *sliceargs):
        self.table = table
        if not sliceargs:
            self.sliceargs = (10,)
        else:
            self.sliceargs = sliceargs
        
    def __repr__(self):    
        it = iter(self.table)
        flds = it.next()
        cols = defaultdict(list)
        for row in islice(it, *self.sliceargs):
            for i, f in enumerate(flds):
                try:
                    cols[str(f)].append(repr(row[i]))
                except IndexError:
                    cols[str(f)].append('')
        output = u''
        for f in flds:
            output += u'%r: %s\n' % (f, u', '.join(cols[str(f)]))
        return output
        
    
def values(table, field, *sliceargs):
    """
    Return an iterator over values in a given field or fields. E.g.::
    
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

    The `field` argument can be a single field name or index (starting from zero)
    or a tuple of field names and/or indexes.    

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
        
    More than one field can be selected, e.g.::
    
        >>> table = [['foo', 'bar', 'baz'],
        ...          [1, 'a', True],
        ...          [2, 'bb', True],
        ...          [3, 'd', False]]
        >>> foobaz = values(table, ('foo', 'baz'))
        >>> foobaz.next()
        (1, True)
        >>> foobaz.next()
        (2, True)
        >>> foobaz.next()
        (3, False)
        >>> foobaz.next()
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        StopIteration

    .. versionchanged:: 0.3
    
    Positional arguments can be used to slice the data rows. The `sliceargs` are 
    passed to :func:`itertools.islice`.

    """
    
    it = iter(table)
    srcflds = it.next()
    indices = asindices(srcflds, field)
    assert len(indices) > 0, 'no field selected'
    getvalue = itemgetter(*indices)
    if sliceargs:
        it = islice(it, *sliceargs)
    for row in it:
        try:
            value = getvalue(row)
            yield value
        except IndexError:
            pass # ignore short rows
    
    
def valueset(table, field):
    """
    .. deprecated:: 0.3
    
    Use ``set(values(table, *fields))`` instead, see also :func:`values`.
        
    """

    return set(values(table, field))


def valuecount(table, field, value):
    """
    Count the number of occurrences of `value` under the given field. Returns
    the absolute count and relative frequency as a pair. E.g.::
    
        >>> from petl import valuecount
        >>> table = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 7))
        >>> n, f = valuecount(table, 'foo', 'b')
        >>> n
        2
        >>> f
        0.6666666666666666

    The `field` argument can be a single field name or index (starting from zero)
    or a tuple of field names and/or indexes.    

    """
    
    if isinstance(field, (list, tuple)):
        it = values(table, *field)
    else:
        it = values(table, field)
    total = 0
    vs = 0
    for v in it:
        total += 1
        if v == value:
            vs += 1
    return vs, float(vs)/total
    
    
def valuecounter(table, field):
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
    
    The `field` argument can be a single field name or index (starting from zero)
    or a tuple of field names and/or indexes.    

    """

    counter = Counter()
    for v in values(table, field):
        try:
            counter[v] += 1
        except IndexError:
            pass # short row
    return counter
            

def valuecounts(table, field):    
    """
    Find distinct values for the given field and count the number and relative
    frequency of occurrences. Returns a table mapping values to counts, with most common 
    values first. E.g.::

        >>> from petl import valuecounts
        >>> table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
        >>> valuecounts(table, 'foo')
        [('value', 'count', 'frequency'), ('b', 2, 0.5), ('a', 1, 0.25), ('c', 1, 0.25)]

    The `field` argument can be a single field name or index (starting from zero)
    or a tuple of field names and/or indexes.    

    Can be combined with `look`, e.g.::

        >>> from petl import look    
        >>> look(valuecounts(table, 'foo'))
        +---------+---------+-------------+
        | 'value' | 'count' | 'frequency' |
        +=========+=========+=============+
        | 'b'     | 2       | 0.5         |
        +---------+---------+-------------+
        | 'a'     | 1       | 0.25        |
        +---------+---------+-------------+
        | 'c'     | 1       | 0.25        |
        +---------+---------+-------------+
        
        >>> look(valuecounts(table, 'bar'))
        +---------+---------+--------------------+
        | 'value' | 'count' | 'frequency'        |
        +=========+=========+====================+
        | True    | 2       | 0.6666666666666666 |
        +---------+---------+--------------------+
        | False   | 1       | 0.3333333333333333 |
        +---------+---------+--------------------+
            
    """
    
    counter = valuecounter(table, field)
    output = [('value', 'count', 'frequency')]
    counts = counter.most_common()
    total = sum(c[1] for c in counts)
    counts = [(c[0], c[1], float(c[1])/total) for c in counts]
    output.extend(counts)
    return output
        
        
def unique(table, field):
    """
    Return True if there are no duplicate values for the given field(s), otherwise
    False. E.g.::

        >>> from petl import unique
        >>> table = [['foo', 'bar'], ['a', 1], ['b'], ['b', 2], ['c', 3, True]]
        >>> unique(table, 'foo')
        False
        >>> unique(table, 'bar')
        True
    
    The `field` argument can be a single field name or index (starting from zero)
    or a tuple of field names and/or indexes.    

    """    

    vals = set()
    for v in values(table, field):
        if v in vals:
            return False
        else:
            vals.add(v)
    return True
       
        
# TODO handle short rows in lookup, lookupone, recordlookup, recordlookupone?


def lookup(table, keyspec, valuespec=None, dictionary=None):
    """
    Load a dictionary with data from the given table. E.g.::
    
        >>> from petl import lookup
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookup(table, 'foo', 'bar')
        >>> lkp['a']
        [1]
        >>> lkp['b']
        [2, 3]

    If no `valuespec` argument is given, defaults to the whole
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

    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::
    
        >>> import shelve
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = shelve.open('mylookup.dat')
        >>> lkp = lookup(table, 'foo', 'bar', lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24) 
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mylookup.dat')
        >>> lkp['a']
        [1]
        >>> lkp['b']
        [2, 3]

    """
    
    if dictionary is None:
        dictionary = dict()
        
    it = iter(table)
    flds = it.next()
    if valuespec is None:
        valuespec = flds # default valuespec is complete row
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    valueindices = asindices(flds, valuespec)
    assert len(valueindices) > 0, 'no valuespec selected'
    getkey = itemgetter(*keyindices)
    getvalue = itemgetter(*valueindices)
    for row in it:
        k = getkey(row)
        v = getvalue(row)
        if k in dictionary:
            # work properly with shelve
            l = dictionary[k]
            l.append(v)
            dictionary[k] = l
        else:
            dictionary[k] = [v]
    return dictionary
    
    
def lookupone(table, keyspec, valuespec=None, dictionary=None, strict=True):
    """
    Load a dictionary with data from the given table, assuming there is
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
        
    If the specified key is not unique, will raise DuplicateKeyError, e.g.::

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
    
    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::
    
        >>> from petl import lookupone
        >>> import shelve
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = shelve.open('mylookupone.dat')
        >>> lkp = lookupone(table, 'foo', 'bar', dictionary=lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24) 
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mylookupone.dat')
        >>> lkp['a']
        1
        >>> lkp['b']
        2
        >>> lkp['c']
        2

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = it.next()
    if valuespec is None:
        valuespec = flds
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    valueindices = asindices(flds, valuespec)
    assert len(valueindices) > 0, 'no valuespec selected'
    getkey = itemgetter(*keyindices)
    getvalue = itemgetter(*valueindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError
        v = getvalue(row)
        dictionary[k] = v
    return dictionary
    
    
def recordlookup(table, keyspec, dictionary=None):
    """
    Load a dictionary with data from the given table, mapping to records. E.g.::
    
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
    
    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::

        >>> import shelve
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = shelve.open('myrecordlookup.dat')
        >>> lkp = recordlookup(table, 'foo', dictionary=lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24) 
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('myrecordlookup.dat')
        >>> lkp['a']
        [{'foo': 'a', 'bar': 1}]
        >>> lkp['b']
        [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]

    """
    
    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = it.next()
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        rec = asdict(flds, row)
        if k in dictionary:
            # work properly with shelve
            l = dictionary[k]
            l.append(rec)
            dictionary[k] = l
        else:
            dictionary[k] = [rec]
    return dictionary
    
        
def recordlookupone(table, keyspec, dictionary=None, strict=True):
    """
    Load a dictionary with data from the given table, mapping to records,
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
        
    If the specified key is not unique, will raise DuplicateKeyError, e.g.::

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
    
    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::

        >>> import shelve
        >>> lkp = shelve.open('myrecordlookupone.dat')
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = recordlookupone(table, 'foo', dictionary=lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24) 
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('myrecordlookupone.dat')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}
        >>> lkp['c']
        {'foo': 'c', 'bar': 2}

    """    

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = it.next()
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError
        d = asdict(flds, row)
        dictionary[k] = d
    return dictionary
    
            
class DuplicateKeyError(Exception):
    pass


def asindices(flds, spec):
    """
    TODO doc me
    
    """

    names = [str(f) for f in flds]
    indices = list()
    if isinstance(spec, basestring):
        spec = (spec,)
    if isinstance(spec, int):
        spec = (spec,)
    for s in spec:
        # spec could be a field name
        if s in names:
            indices.append(names.index(s))
        # or spec could be a field index
        elif isinstance(s, int) and s < len(names):
            indices.append(s) # index fields from 0
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
    
    
def rowlengths(table):
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

    it = data(table)
    counter = Counter()
    for row in it:
        counter[len(row)] += 1
    output = [('length', 'count')]
    output.extend(counter.most_common())
    return output


def typecounter(table, field):    
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

    The `field` argument can be a field name or index (starting from zero).    
    
    """
    
    counter = Counter()
    for v in values(table, field):
        try:
            counter[v.__class__.__name__] += 1
        except IndexError:
            pass # ignore short rows
    return counter


def typecounts(table, field):    
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

    The `field` argument can be a field name or index (starting from zero).    
 
    """
    
    counter = typecounter(table, field)
    output = [('type', 'count')]
    output.extend(counter.most_common())
    return output


def typeset(table, field):
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
    
    The `field` argument can be a field name or index (starting from zero).    

    """

    s = set()
    for v in values(table, field):
        try:
            s.add(v.__class__)
        except IndexError:
            pass # ignore short rows
    return s
    

def parsecounter(table, field, parsers={'int': int, 'float': float}):    
    """
    Count the number of `str` or `unicode` values under the given fields that can 
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
        
    The `field` argument can be a field name or index (starting from zero).    

    """
    
    counter, errors = Counter(), Counter()
    for v in values(table, field):
        if isinstance(v, basestring):
            for name, parser in parsers.items():
                try:
                    parser(v)
                except:
                    errors[name] += 1
                else:
                    counter[name] += 1
    return counter, errors


def parsecounts(table, field, parsers={'int': int, 'float': float}):    
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

    The `field` argument can be a field name or index (starting from zero).    

    """
    
    counter, errors = parsecounter(table, field, parsers)
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
    

def limits(table, field):
    """
    Find minimum and maximum values under the given field. E.g.::
    
        >>> from petl import limits
        >>> t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> minv, maxv = limits(t1, 'bar')
        >>> minv
        1
        >>> maxv
        3
    
    The `field` argument can be a field name or index (starting from zero).    

    """
    
    vals = values(table, field)
    try:
        minv = maxv = vals.next()
    except StopIteration:
        return None, None
    else:
        for v in vals:
            if v < minv:
                minv = v
            if v > maxv:
                maxv = v
        return minv, maxv


def stats(table, field):
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
        
    The `field` argument can be a field name or index (starting from zero).    

    """
    
    output = {'min': None, 
              'max': None,
              'sum': None, 
              'mean': None, 
              'count': 0, 
              'errors': 0}
    for v in values(table, field):
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


def expr(s):
    """
    Construct a function operating on a record (i.e., a dictionary representation
    of a data row, indexed by field name).
    
    The expression string is converted into a lambda function by prepending
    the string with ``'lambda rec: '``, then replacing anything enclosed in 
    curly braces (e.g., ``"{foo}"``) with a lookup on the record (e.g., 
    ``"rec['foo']"``), then finally calling :func:`eval`.
    
    So, e.g., the expression string ``"{foo} * {bar}"`` is converted to the 
    function ``lambda rec: rec['foo'] * rec['bar']``
    
    """
    
    prog = re.compile('\{([^}]+)\}')
    def repl(matchobj):
        return "rec['%s']" % matchobj.group(1)
    return eval("lambda rec: " + prog.sub(repl, s))
    
    
def strjoin(s):
    """
    Return a function to join sequences using `s` as the separator.
    
    """
    
    return lambda l: s.join(map(str, l))


def parsenumber(v):
    """
    Attempt to parse the value as a number, trying :func:`int`, :func:`long`,
    :func:`float` and :func:`complex` in that order. If all fail, return the
    value as-is.
    
    .. versionadded:: 0.4
    
    """
    
    try:
        return int(v)
    except:
        pass
    try:
        return long(v)
    except:
        pass
    try:
        return float(v)
    except:
        pass
    try:
        return complex(v)
    except:
        pass
    return v
