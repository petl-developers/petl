__author__ = 'aliman'


import cPickle as pickle
from tempfile import NamedTemporaryFile
import operator
import itertools


from petl.util import RowContainer, asindices, shortlistmergesorted, \
    heapqmergesorted, sortable_itemgetter


import logging
logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


def sort(table, key=None, reverse=False, buffersize=None, tempdir=None,
         cache=True):
    """
    Sort the table. Field names or indices (from zero) can be used to specify
    the key. E.g.::

        >>> from petl import sort, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'C'   | 2     |
        +-------+-------+
        | 'A'   | 9     |
        +-------+-------+
        | 'A'   | 6     |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+

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

        >>> # sorting by compound key is supported
        ... table3 = sort(table1, key=['foo', 'bar'])
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

        >>> # if no key is specified, the default is a lexical sort
        ... table4 = sort(table1)
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

    If `buffersize` is `None`, the value of `petl.transform.sorts.defaultbuffersize`
    will be used. By default this is set to 100000 rows, but can be changed, e.g.::

        >>> import petl.transform.sorts
        >>> petl.transform.sorts.defaultbuffersize = 500000

    If `petl.transform.sorts.defaultbuffersize` is set to `None`, this forces all
    sorting to be done entirely in memory.

    .. versionchanged:: 0.16

    By default the results of the sort will be cached, and so a second pass over
    the sorted table will yield rows from the cache and will not repeat the
    sort operation. To turn off caching, set the `cache` argument to `False`.

    """

    return SortView(table, key=key, reverse=reverse, buffersize=buffersize,
                    tempdir=tempdir, cache=cache)


def iterchunk(f):
    # reopen so iterators from file cache are independent
    with open(f.name, 'rb') as f:
        try:
            while True:
                yield pickle.load(f)
        except EOFError:
            pass

# non-independent version of iteration from file cache which doesn't depend
# on named temporary files
#def iterchunk(f):
#    debug('seek(0): %r', f)
#    f.seek(0)
#    try:
#        while True:
#            yield pickle.load(f)
#    except EOFError:
#        pass


def _mergesorted(key=None, reverse=False, *iterables):

    # N.B., I've used heapq for normal merge sort and shortlist merge sort for reverse
    # merge sort because I've assumed that heapq.merge is faster and so is preferable
    # but it doesn't support reverse sorting so the shortlist merge sort has to
    # be used for reverse sorting. Some casual profiling suggests there isn't much
    # between the two in terms of speed, but might be worth profiling more carefully

    if reverse:
        return shortlistmergesorted(key, True, *iterables)
    else:
        return heapqmergesorted(key, *iterables)


defaultbuffersize = 100000


class SortView(RowContainer):

    def __init__(self, source, key=None, reverse=False, buffersize=None,
                 tempdir=None, cache=True):
        self.source = source
        self.key = key
        self.reverse = reverse
        if buffersize is None:
            self.buffersize = defaultbuffersize
        else:
            self.buffersize = buffersize
        self.tempdir = tempdir
        self.cache = cache
        self._fldcache = None
        self._memcache = None
        self._filecache = None
        self._getkey = None

    def clearcache(self):
        self._clearcache()

    def _clearcache(self):
        self._fldcache = None
        self._memcache = None
        self._filecache = None
        self._getkey = None

    def __iter__(self):
        source = self.source
        key = self.key
        reverse = self.reverse
        if self.cache and self._memcache is not None:
            return self._iterfrommemcache()
        elif self.cache and self._filecache is not None:
            return self._iterfromfilecache()
        else:
            return self._iternocache(source, key, reverse)

    def _iterfrommemcache(self):
        debug('iterate from mem cache')
        yield tuple(self._fldcache)
        for row in self._memcache:
            yield tuple(row)

    def _iterfromfilecache(self):
        debug('iterate from file cache: %r', [f for f in self._filecache])
        yield tuple(self._fldcache)
        chunkiters = [iterchunk(f) for f in self._filecache]
        for row in _mergesorted(self._getkey, self.reverse, *chunkiters):
            yield tuple(row)

    def _iternocache(self, source, key, reverse):
        debug('iterate without cache')
        self._clearcache()
        it = iter(source)

        flds = it.next()
        yield tuple(flds)

        if key is not None:
            # convert field selection into field indices
            indices = asindices(flds, key)
        else:
            indices = range(len(flds))
        # now use field indices to construct a _getkey function
        # N.B., this will probably raise an exception on short rows
        getkey = sortable_itemgetter(*indices)

        # initialise the first chunk
        rows = list(itertools.islice(it, 0, self.buffersize))
        rows.sort(key=getkey, reverse=reverse)

        # have we exhausted the source iterator?
        if self.buffersize is None or len(rows) < self.buffersize:

            if self.cache:
                debug('caching mem')
                self._fldcache = flds
                self._memcache = rows
                self._getkey = getkey # actually not needed to iterate from memcache

            for row in rows:
                yield tuple(row)

        else:

            chunkfiles = []

            while rows:

                # dump the chunk
                f = NamedTemporaryFile(dir=self.tempdir)
                for row in rows:
                    pickle.dump(row, f, protocol=-1)
                f.flush()
                # N.B., do not close the file! Closing will delete
                # the file, and we might want to keep it around
                # if it can be cached. We'll let garbage collection
                # deal with this, i.e., when no references to the
                # chunk files exist any more, garbage collection
                # should be an implicit close, which will cause file
                # deletion.
                chunkfiles.append(f)

                # grab the next chunk
                rows = list(itertools.islice(it, 0, self.buffersize))
                rows.sort(key=getkey, reverse=reverse)

            if self.cache:
                debug('caching files %r', chunkfiles)
                self._fldcache = flds
                self._filecache = chunkfiles
                self._getkey = getkey

            chunkiters = [iterchunk(f) for f in chunkfiles]
            for row in _mergesorted(getkey, reverse, *chunkiters):
                yield tuple(row)


def mergesort(*tables, **kwargs):
    """
    Combine multiple input tables into one sorted output table. E.g.::

        >>> from petl import mergesort, look
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 9     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'A'   | 6     |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+

        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'B'   | 3     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'A'   | 10    |
        +-------+-------+
        | 'F'   | 4     |
        +-------+-------+

        >>> table3 = mergesort(table1, table2, key='foo')
        >>> look(table3)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'A'   | 9     |
        +-------+-------+
        | 'A'   | 6     |
        +-------+-------+
        | 'A'   | 10    |
        +-------+-------+
        | 'B'   | 3     |
        +-------+-------+
        | 'C'   | 2     |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'D'   | 10    |
        +-------+-------+
        | 'F'   | 1     |
        +-------+-------+
        | 'F'   | 4     |
        +-------+-------+

    If the input tables are already sorted by the given key, give ``presorted=True``
    as a keyword argument.

    This function is equivalent to concatenating the input tables using :func:`cat`
    then sorting, however this function will typically be more efficient,
    especially if the input tables are presorted.

    Keyword arguments:

        - `key` - field name or tuple of fields to sort by (defaults to `None` - lexical sort)
        - `reverse` - `True` if sort in reverse (descending) order (defaults to `False`)
        - `presorted` - `True` if inputs are already sorted by the given key (defaults to `False`)
        - `missing` - value to fill with when input tables have different fields (defaults to `None`)
        - `header` - specify a fixed header for the output table
        - `buffersize` - limit the number of rows in memory per input table when inputs are not presorted

    .. versionadded:: 0.9

    """

    return MergeSortView(tables, **kwargs)


class MergeSortView(RowContainer):

    def __init__(self, tables, key=None, reverse=False, presorted=False,
                 missing=None, header=None, buffersize=None, tempdir=None, cache=True):
        self.key = key
        if presorted:
            self.tables = tables
        else:
            self.tables = [sort(t, key=key, reverse=reverse, buffersize=buffersize, tempdir=tempdir, cache=cache) for t in tables]
        self.missing = missing
        self.header = header
        self.reverse = reverse

    def __iter__(self):
        return itermergesort(self.tables, self.key, self.header, self.missing, self.reverse)


def itermergesort(sources, key, header, missing, reverse):

    # first need to standardise headers of all input tables
    # borrow this from itercat - TODO remove code smells

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

    def _standardisedata(it, flds, outflds):
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

    # wrap all iterators to standardise fields
    sits = [_standardisedata(it, flds, outflds) for flds, it in zip(source_flds_lists, its)]

    # now determine key function
    getkey = None
    if key is not None:
        # convert field selection into field indices
        indices = asindices(outflds, key)
        # now use field indices to construct a _getkey function
        # N.B., this will probably raise an exception on short rows
        getkey = operator.itemgetter(*indices)

    # OK, do the merge sort
    for row in shortlistmergesorted(getkey, reverse, *sits):
        yield row


