from __future__ import absolute_import, print_function, division


import operator
from petl.compat import text_type


from petl.util.base import Table, asindices, itervalues
from petl.transform.sorts import sort


def duplicates(table, key=None, presorted=False, buffersize=None, tempdir=None, 
               cache=True):
    """
    Select rows with duplicate values under a given key (or duplicate
    rows where no key is given). E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, 2.0],
        ...           ['B', 2, 3.4],
        ...           ['D', 6, 9.3],
        ...           ['B', 3, 7.8],
        ...           ['B', 2, 12.3],
        ...           ['E', None, 1.3],
        ...           ['D', 4, 14.5]]
        >>> table2 = etl.duplicates(table1, 'foo')
        >>> table2
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        | 'B' |   2 |  3.4 |
        +-----+-----+------+
        | 'B' |   3 |  7.8 |
        +-----+-----+------+
        | 'B' |   2 | 12.3 |
        +-----+-----+------+
        | 'D' |   6 |  9.3 |
        +-----+-----+------+
        | 'D' |   4 | 14.5 |
        +-----+-----+------+

        >>> # compound keys are supported
        ... table3 = etl.duplicates(table1, key=['foo', 'bar'])
        >>> table3
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        | 'B' |   2 |  3.4 |
        +-----+-----+------+
        | 'B' |   2 | 12.3 |
        +-----+-----+------+
        
    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are 
    ignored. Otherwise, the data are sorted, see also the discussion of the
    `buffersize`, `tempdir` and `cache` arguments under the
    :func:`petl.transform.sorts.sort` function.
    
    See also :func:`petl.transform.dedup.unique` and
    :func:`petl.transform.dedup.distinct`.
    
    """

    return DuplicatesView(table, key=key, presorted=presorted, 
                          buffersize=buffersize, tempdir=tempdir, cache=cache)


Table.duplicates = duplicates


class DuplicatesView(Table):
    
    def __init__(self, source, key=None, presorted=False, buffersize=None, 
                 tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize, 
                               tempdir=tempdir, cache=cache)
        self.key = key
        
    def __iter__(self):
        return iterduplicates(self.source, self.key)


def iterduplicates(source, key):
    # assume source is sorted
    # first need to sort the data
    it = iter(source)

    try:
        hdr = next(it)
    except StopIteration:
        if key is None:
            return  # nothing to do on a table without headers
        hdr = []
    yield tuple(hdr)

    # convert field selection into field indices
    if key is None:
        indices = range(len(hdr))
    else:
        indices = asindices(hdr, key)
        
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

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, 2],
        ...           ['B', '2', '3.4'],
        ...           ['D', 'xyz', 9.0],
        ...           ['B', u'3', u'7.8'],
        ...           ['B', '2', 42],
        ...           ['E', None, None],
        ...           ['D', 4, 12.3],
        ...           ['F', 7, 2.3]]
        >>> table2 = etl.unique(table1, 'foo')
        >>> table2
        +-----+------+------+
        | foo | bar  | baz  |
        +=====+======+======+
        | 'A' |    1 |    2 |
        +-----+------+------+
        | 'E' | None | None |
        +-----+------+------+
        | 'F' |    7 |  2.3 |
        +-----+------+------+
        
    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data are sorted, see also the discussion of the
    `buffersize`, `tempdir` and `cache` arguments under the
    :func:`petl.transform.sorts.sort` function.

    See also :func:`petl.transform.dedup.duplicates` and
    :func:`petl.transform.dedup.distinct`.
    
    """

    return UniqueView(table, key=key, presorted=presorted, 
                      buffersize=buffersize, tempdir=tempdir, cache=cache)


Table.unique = unique


class UniqueView(Table):
    
    def __init__(self, source, key=None, presorted=False, buffersize=None,
                 tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize,
                               tempdir=tempdir, cache=cache)
        self.key = key
        
    def __iter__(self):
        return iterunique(self.source, self.key)


def iterunique(source, key):
    # assume source is sorted
    # first need to sort the data
    it = iter(source)

    try:
        hdr = next(it)
    except StopIteration:
        return
    yield tuple(hdr)

    # convert field selection into field indices
    if key is None:
        indices = range(len(hdr))
    else:
        indices = asindices(hdr, key)
        
    # now use field indices to construct a _getkey function
    # N.B., this may raise an exception on short rows, depending on
    # the field selection
    getkey = operator.itemgetter(*indices)

    try:
        prev = next(it)
    except StopIteration:
        return

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

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, 2.7],
        ...           ['B', 2, None],
        ...           ['D', 3, 9.4],
        ...           ['B', None, 7.8],
        ...           ['E', None],
        ...           ['D', 3, 12.3],
        ...           ['A', 2, None]]
        >>> table2 = etl.conflicts(table1, 'foo')
        >>> table2
        +-----+-----+------+
        | foo | bar | baz  |
        +=====+=====+======+
        | 'A' |   1 |  2.7 |
        +-----+-----+------+
        | 'A' |   2 | None |
        +-----+-----+------+
        | 'D' |   3 |  9.4 |
        +-----+-----+------+
        | 'D' |   3 | 12.3 |
        +-----+-----+------+
        
    Missing values are not considered conflicts. By default, `None` is treated
    as the missing value, this can be changed via the `missing` keyword 
    argument.

    One or more fields can be ignored when determining conflicts by providing
    the `exclude` keyword argument. Alternatively, fields to use when
    determining conflicts can be specified explicitly with the `include`
    keyword argument. This provides a simple mechanism for analysing the
    source of conflicting rows from multiple tables, e.g.::

        >>> table1 = [['foo', 'bar'], [1, 'a'], [2, 'b']]
        >>> table2 = [['foo', 'bar'], [1, 'a'], [2, 'c']]
        >>> table3 = etl.cat(etl.addfield(table1, 'source', 1),
        ...                  etl.addfield(table2, 'source', 2))
        >>> table4 = etl.conflicts(table3, key='foo', exclude='source')
        >>> table4
        +-----+-----+--------+
        | foo | bar | source |
        +=====+=====+========+
        |   2 | 'b' |      1 |
        +-----+-----+--------+
        |   2 | 'c' |      2 |
        +-----+-----+--------+

    If `presorted` is True, it is assumed that the data are already sorted by
    the given key, and the `buffersize`, `tempdir` and `cache` arguments are
    ignored. Otherwise, the data are sorted, see also the discussion of the
    `buffersize`, `tempdir` and `cache` arguments under the
    :func:`petl.transform.sorts.sort` function.
    
    """
    
    return ConflictsView(table, key, missing=missing, exclude=exclude,
                         include=include, presorted=presorted,
                         buffersize=buffersize, tempdir=tempdir, cache=cache)


Table.conflicts = conflicts


class ConflictsView(Table):
    
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
    if exclude and not isinstance(exclude, (list, tuple)):
        exclude = (exclude,)
    if include and not isinstance(include, (list, tuple)):
        include = (include,)

    # exclude overrides include
    if include and exclude:
        include = None
        
    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        return
    flds = list(map(text_type, hdr))
    yield tuple(hdr)

    # convert field selection into field indices
    indices = asindices(hdr, key)
                    
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


def distinct(table, key=None, count=None, presorted=False, buffersize=None,
             tempdir=None, cache=True):
    """
    Return only distinct rows in the table.

    If the `count` argument is not None, it will be used as the name for an
    additional field, and the values of the field will be the number of
    duplicate rows.

    If the `key` keyword argument is passed, the comparison is done on the
    given key instead of the full row.

    See also :func:`petl.transform.dedup.duplicates`,
    :func:`petl.transform.dedup.unique`,
    :func:`petl.transform.reductions.groupselectfirst`,
    :func:`petl.transform.reductions.groupselectlast`.

    """

    return DistinctView(table, key=key, count=count, presorted=presorted,
                        buffersize=buffersize, tempdir=tempdir, cache=cache)


Table.distinct = distinct


class DistinctView(Table):
    def __init__(self, table, key=None, count=None, presorted=False,
                 buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.table = table
        else:
            self.table = sort(table, key=key, buffersize=buffersize,
                              tempdir=tempdir, cache=cache)
        self.key = key
        self.count = count

    def __iter__(self):
        it = iter(self.table)
        try:
            hdr = next(it)
        except StopIteration:
            return

        # convert field selection into field indices
        if self.key is None:
            indices = range(len(hdr))
        else:
            indices = asindices(hdr, self.key)

        # now use field indices to construct a _getkey function
        # N.B., this may raise an exception on short rows, depending on
        # the field selection
        getkey = operator.itemgetter(*indices)

        INIT = object()
        if self.count:
            hdr = tuple(hdr) + (self.count,)
            yield hdr
            previous = INIT
            n_dup = 1
            for row in it:
                if previous is INIT:
                    previous = row
                else:
                    kprev = getkey(previous)
                    kcurr = getkey(row)
                    if kprev == kcurr:
                        n_dup += 1
                    else:
                        yield tuple(previous) + (n_dup,)
                        n_dup = 1
                        previous = row
            # deal with last row
            yield tuple(previous) + (n_dup,)
        else:
            yield tuple(hdr)
            previous_keys = INIT
            for row in it:
                keys = getkey(row)
                if keys != previous_keys:
                    yield tuple(row)
                previous_keys = keys


def isunique(table, field):
    """
    Return True if there are no duplicate values for the given field(s),
    otherwise False. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b'],
        ...           ['b', 2],
        ...           ['c', 3, True]]
        >>> etl.isunique(table1, 'foo')
        False
        >>> etl.isunique(table1, 'bar')
        True

    The `field` argument can be a single field name or index (starting from
    zero) or a tuple of field names and/or indexes.

    """

    vals = set()
    for v in itervalues(table, field):
        if v in vals:
            return False
        else:
            vals.add(v)
    return True


Table.isunique = isunique
