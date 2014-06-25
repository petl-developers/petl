__author__ = 'aliman'


import operator


from petl.util import RowContainer, asindices
from petl.transform.sorts import sort


def duplicates(table, key=None, presorted=False, buffersize=None, tempdir=None, 
               cache=True):
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
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are 
    ignored. Otherwise, the data 
    are sorted, see also the discussion of the `buffersize`, `tempdir` and 
    `cache` arguments under the 
    :func:`sort` function.
    
    See also :func:`unique` and :func:`distinct`.
    
    """

    return DuplicatesView(table, key=key, presorted=presorted, 
                          buffersize=buffersize, tempdir=tempdir, cache=cache)


class DuplicatesView(RowContainer):
    
    def __init__(self, source, key=None, presorted=False, buffersize=None, 
                 tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, 
                               tempdir=tempdir, cache=cache)
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
    getkey = operator.itemgetter(*indices)
    
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
    
    
def unique(table, key=None, presorted=False, buffersize=None, tempdir=None,
           cache=True):
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
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and
    `cache` arguments under the
    :func:`sort` function.

    .. versionadded:: 0.10

    See also :func:`duplicates` and :func:`distinct`.
    
    """

    return UniqueView(table, key=key, presorted=presorted, 
                      buffersize=buffersize, tempdir=tempdir, cache=cache)


class UniqueView(RowContainer):
    
    def __init__(self, source, key=None, presorted=False, buffersize=None,
                 tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize,
                               tempdir=tempdir, cache=cache)
        self.key = key  # TODO property
        
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
    getkey = operator.itemgetter(*indices)
    
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
    Select rows with the same key value but differing in some other field.
    E.g.::

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
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data
    are sorted, see also the discussion of the `buffersize`, `tempdir` and
    `cache` arguments under the
    :func:`sort` function.
    
    .. versionchanged:: 0.8
    
    Added the `include` and `exclude` keyword arguments. The `exclude` keyword 
    argument replaces the `ignore` keyword argument in previous versions.
    
    """
    
    return ConflictsView(table, key, missing=missing, exclude=exclude,
                         include=include, presorted=presorted,
                         buffersize=buffersize, tempdir=tempdir, cache=cache)


class ConflictsView(RowContainer):
    
    def __init__(self, source, key, missing=None, exclude=None, include=None, 
                 presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize,
                               tempdir=tempdir, cache=cache)
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
    getkey = operator.itemgetter(*indices)
    
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
                    if (exclude and f not in exclude) \
                            or (include and f in include) \
                            or (not exclude and not include):
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
    

def distinct(table, presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Return only distinct rows in the table. See also :func:`duplicates` and
    :func:`unique`.
    
    .. versionadded:: 0.12
    
    """
    
    return DistinctView(table, presorted=presorted, buffersize=buffersize,
                        tempdir=tempdir, cache=cache)


class DistinctView(RowContainer):
    
    def __init__(self, table, presorted=False, buffersize=None, tempdir=None,
                 cache=True):
        if presorted:
            self.table = table
        else:
            self.table = sort(table, buffersize=buffersize, tempdir=tempdir,
                              cache=cache)
        
    def __iter__(self):
        it = iter(self.table)
        yield it.next()
        previous = None
        for row in it:
            if row != previous:
                yield row
            previous = row

