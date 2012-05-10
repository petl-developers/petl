"""
A tentative module for pushing data through branching pipelines.

"""


import csv
from tempfile import NamedTemporaryFile
from operator import itemgetter
from itertools import islice
from collections import defaultdict
import cPickle as pickle

from petl.util import asindices, HybridRow, shortlistmergesorted
import petl.transform


class PipelineComponent(object):

    def __init__(self):
        self.default_receivers = list()
        self.keyed_receivers = defaultdict(list)

    def pipe(self, *args):
        assert 1 <= len(args) <= 2, '1 or 2 arguments expected'
        if len(args) == 1:
            receiver = args[0]
            self.default_receivers.append(receiver)
            return receiver
        elif len(args) == 2:
            key, receiver = args
            self.keyed_receivers[key].append(receiver)
            return receiver

    def __or__(self, other):
        if isinstance(other, tuple):
            return self.pipe(*other)
        else:
            return self.pipe(other)

    def _connect_receivers(self, fields):
        default_connections = [r.connect(fields) for r in self.default_receivers]
        keyed_connections = dict()
        for k in self.keyed_receivers:
            keyed_connections[k] = [r.connect(fields) for r in self.keyed_receivers[k]]
        return default_connections, keyed_connections
            
    def push(self, source, limit=None):
        it = iter(source)
        fields = it.next()
        c = self.connect(fields)
        for row in islice(it, limit):
            c.accept(tuple(row))
        c.close()


class PipelineConnection(object):

    def __init__(self, default_connections, keyed_connections, fields):
        self.default_connections = default_connections
        self.keyed_connections = keyed_connections
        self.fields = fields

    def close(self):
        for c in self.default_connections:
            c.close()
        for k in self.keyed_connections:
            for c in self.keyed_connections[k]:
                c.close()

    def broadcast(self, *args):
        assert 1 <= len(args) <= 2, 'expected 1 or 2 arguments'
        if len(args) == 1:
            row = args[0]
            for c in self.default_connections:
                c.accept(tuple(row))
        elif len(args) == 2:
            key, row = args
            if key in self.keyed_connections:
                for c in self.keyed_connections[key]:
                    c.accept(tuple(row))


def tocsv(filename, dialect=csv.excel, **kwargs):
    """
    Push rows to a CSV file. E.g.::

        >>> from petl.push import tocsv
        >>> p = tocsv('example.csv')
        >>> p.push(sometable)

    """

    return ToCsvComponent(filename, dialect, **kwargs)


def totsv(filename, dialect=csv.excel_tab, **kwargs):
    """
    Push rows to a tab-delimited file. E.g.::

        >>> from petl.push import totsv
        >>> p = totsv('example.tsv')
        >>> p.push(sometable)

    """

    return ToCsvComponent(filename, dialect, **kwargs)


class ToCsvComponent(PipelineComponent):

    def __init__(self, filename, dialect, **kwargs):
        super(ToCsvComponent, self).__init__()
        self.filename = filename
        self.dialect = dialect
        self.kwargs = kwargs

    def connect(self, fields):
        default_connections, keyed_connections = self._connect_receivers(fields)
        return ToCsvConnection(default_connections, keyed_connections, fields, 
                               self.filename, self.dialect, self.kwargs)


class ToCsvConnection(PipelineConnection):

    def __init__(self, default_connections, keyed_connections, fields, filename, dialect, kwargs):
        super(ToCsvConnection, self).__init__(default_connections, keyed_connections, fields)
        self.file = open(filename, 'wb')
        self.writer = csv.writer(self.file, dialect=dialect, **kwargs)
        self.writer.writerow(fields)

    def accept(self, row):
        self.writer.writerow(row)
        # forward rows on the default pipe (behave like tee)
        self.broadcast(row)

    def close(self):
        self.file.flush()
        self.file.close()
        super(ToCsvConnection, self).close()


def topickle(filename, protocol=-1):
    """
    Push rows to a pickle file. E.g.::

        >>> from petl.push import topickle
        >>> p = topickle('example.pickle')
        >>> p.push(sometable)

    """

    return ToPickleComponent(filename, protocol)


class ToPickleComponent(PipelineComponent):

    def __init__(self, filename, protocol):
        super(ToPickleComponent, self).__init__()
        self.filename = filename
        self.protocol = protocol

    def connect(self, fields):
        default_connections, keyed_connections = self._connect_receivers(fields)
        return ToPickleConnection(default_connections, keyed_connections, fields, 
                                  self.filename, self.protocol)


class ToPickleConnection(PipelineConnection):

    def __init__(self, default_connections, keyed_connections, fields, filename, protocol):
        super(ToPickleConnection, self).__init__(default_connections, keyed_connections, fields)
        self.file = open(filename, 'wb')
        self.protocol = protocol
        pickle.dump(fields, self.file, self.protocol)

    def accept(self, row):
        pickle.dump(row, self.file, self.protocol)
        # forward rows on the default pipe (behave like tee)
        self.broadcast(row)

    def close(self):
        self.file.flush()
        self.file.close()
        super(ToPickleConnection, self).close()


def partition(discriminator):
    """
    Partition rows based on values of a field or results of applying a
    function on the row. E.g.::

        >>> from petl.push import partition, tocsv
        >>> p = partition('fruit')
        >>> p.pipe('orange', tocsv('oranges.csv'))
        >>> p.pipe('banana', tocsv('bananas.csv'))
        >>> p.push(sometable)

    In the example above, rows where the value of the 'fruit' field
    equals 'orange' are piped to the 'oranges.csv' file, and rows
    where the 'fruit' field equals 'banana' are piped to the
    'bananas.csv' file.

    """

    return PartitionComponent(discriminator)


class PartitionComponent(PipelineComponent):

    def __init__(self, discriminator):
        super(PartitionComponent, self).__init__()
        self.discriminator = discriminator

    def connect(self, fields):
        default_connections, keyed_connections = self._connect_receivers(fields)
        return PartitionConnection(default_connections, keyed_connections, fields, self.discriminator)


class PartitionConnection(PipelineConnection):

    def __init__(self, default_connections, keyed_connections, fields, discriminator):
        super(PartitionConnection, self).__init__(default_connections, keyed_connections, fields)
        if callable(discriminator):
            self.discriminator = discriminator
        else: # assume field or fields
            self.discriminator = itemgetter(*asindices(fields, discriminator))

    def accept(self, row):
        row = HybridRow(row, self.fields)
        key = self.discriminator(row)
        self.broadcast(key, row)


def sort(key=None, reverse=False, buffersize=None):
    """
    Sort rows based on some key field or fields. E.g.::

        >>> from petl.push import sort, tocsv
        >>> p = sort('foo')
        >>> p.pipe(tocsv('sorted_by_foo.csv'))
        >>> p.push(sometable)

    """

    return SortComponent(key=key, reverse=reverse, buffersize=buffersize)


class SortComponent(PipelineComponent):

    def __init__(self, key=None, reverse=False, buffersize=None):
        super(SortComponent, self).__init__()
        self.key = key
        self.reverse = reverse
        self.buffersize = buffersize

    def connect(self, fields):
        default_connections, keyed_connections = self._connect_receivers(fields)
        return SortConnection(default_connections, keyed_connections, fields, 
                              self.key, self.reverse, self.buffersize)


class SortConnection(PipelineConnection):

    def __init__(self, default_connections, keyed_connections, fields, key, reverse, buffersize):
        super(SortConnection, self).__init__(default_connections, keyed_connections, fields)

        self.getkey = None
        if key is not None:
            # convert field selection into field indices
            indices = asindices(fields, key)
            # now use field indices to construct a _getkey function
            # N.B., this will probably raise an exception on short rows
            self.getkey = itemgetter(*indices)

        self.reverse = reverse

        if buffersize is None:
            self.buffersize = petl.transform.defaultbuffersize
        else:
            self.buffersize = buffersize

        self.cache = list()
        self.chunkfiles = list()

    def accept(self, row):
        row = tuple(row)
        if len(self.cache) < self.buffersize:
            self.cache.append(row)
        else:
            # sort and dump the chunk
            self.cache.sort(key=self.getkey, reverse=self.reverse)
            f = NamedTemporaryFile() # TODO need not be named
            for r in self.cache:
                pickle.dump(r, f, protocol=-1)
            f.flush()
            f.seek(0)
            self.chunkfiles.append(f)
            self.cache = [row]
        
    def close(self):
        # sort anything remaining in the cache
        self.cache.sort(key=self.getkey, reverse=self.reverse)
        if self.chunkfiles:
            chunkiters = [iterchunk(f) for f in self.chunkfiles]
            chunkiters.append(self.cache) # make sure any left in cache are included
            for row in shortlistmergesorted(self.getkey, self.reverse, *chunkiters):
                self.broadcast(row)
        else:
            for row in self.cache:
                self.broadcast(row)
        super(SortConnection, self).close()
    

def iterchunk(f):
    try:
        while True:
            yield pickle.load(f)
    except EOFError:
        pass


def duplicates(key):
    """
    Report rows with duplicate key values. E.g.::

        >>> from petl.push import duplicates, tocsv
        >>> p = duplicates('foo')
        >>> p.pipe(tocsv('foo_dups.csv'))
        >>> p.pipe('remainder', tocsv('foo_uniq.csv'))
        >>> p.push(sometable)

    N.B., assumes data are already sorted by the given key.

    """

    return DuplicatesComponent(key)


class DuplicatesComponent(PipelineComponent):

    def __init__(self, key):
        super(DuplicatesComponent, self).__init__()
        self.key = key

    def connect(self, fields):
        default_connections, keyed_connections = self._connect_receivers(fields)
        return DuplicatesConnection(default_connections, keyed_connections, fields, self.key)


class DuplicatesConnection(PipelineConnection):

    def __init__(self, default_connections, keyed_connections, fields, key):
        super(DuplicatesConnection, self).__init__(default_connections, keyed_connections, fields)

        # convert field selection into field indices
        indices = asindices(fields, key)
        
        # now use field indices to construct a _getkey function
        # N.B., this may raise an exception on short rows, depending on
        # the field selection
        self.getkey = itemgetter(*indices)

        # initial state
        self.previous = None
        self.previous_is_duplicate = False

        # convert field selection into field indices
        indices = asindices(fields, key)
        
        # now use field indices to construct a _getkey function
        # N.B., this may raise an exception on short rows, depending on
        # the field selection
        self.getkey = itemgetter(*indices)

        # initial state
        self.previous = None
        self.previous_is_duplicate = False
        
    def _broadcast_duplicate(self, row):
        self.broadcast(row)

    def _broadcast_unique(self, row):
        self.broadcast('remainder', row)

    def accept(self, row):
        
        if self.previous is None:
            self.previous = row
        else:
            # TODO repeat calculation of key could be removed?
            kprev = self.getkey(self.previous)
            kcurr = self.getkey(row)
            if kprev == kcurr:
                if not self.previous_is_duplicate:
                    self._broadcast_duplicate(self.previous)
                self.previous_is_duplicate = True
                self._broadcast_duplicate(row)
            else:
                if not self.previous_is_duplicate:
                    # forward unique row
                    self._broadcast_unique(self.previous)

                # reset
                self.previous_is_duplicate = False
            self.previous = row

    def close(self):
        if not self.previous_is_duplicate:
            # forward unique row
            self._broadcast_unique(self.previous)
        super(DuplicatesConnection, self).close()
        

def unique(key):
    """
    Report rows with unique key values. E.g.::

        >>> from petl.push import unique, tocsv
        >>> p = unique('foo')
        >>> p.pipe(tocsv('foo_uniq.csv'))
        >>> p.pipe('remainder', tocsv('foo_dups.csv'))
        >>> p.push(sometable)

    N.B., assumes data are already sorted by the given key. See also
    :func:`duplicates`.

    """

    return UniqueComponent(key)


class UniqueComponent(DuplicatesComponent):

    def __init__(self, key):
        super(UniqueComponent, self).__init__(key)

    def connect(self, fields):
        default_connections, keyed_connections = self._connect_receivers(fields)
        return UniqueConnection(default_connections, keyed_connections, fields, self.key)


class UniqueConnection(DuplicatesConnection):

    def __init__(self, default_connections, keyed_connections, fields, key):
        super(UniqueConnection, self).__init__(default_connections, keyed_connections, fields, key)

    def _broadcast_duplicate(self, row):
        self.broadcast('remainder', row)

    def _broadcast_unique(self, row):
        self.broadcast(row) # unique on default pipe


def diff():
    """
    Find rows that differ between two tables. E.g.::

        >>> from petl.push import diff, tocsv
        >>> p = diff()
        >>> p.pipe('+', tocsv('added.csv'))
        >>> p.pipe('-', tocsv('subtracted.csv'))
        >>> p.pipe(tocsv('common.csv'))
        >>> p.push(sometable, someothertable)
    
    """

    return DiffComponent()


class DiffComponent(PipelineComponent):

    def __init__(self):
        super(DiffComponent, self).__init__()

    def push(self, ta, tb, limit=None):
        ita = iter(ta) 
        itb = iter(tb)
        aflds = [str(f) for f in ita.next()]
        itb.next() # ignore b fields

        default_connections, keyed_connections = self._connect_receivers(aflds)
        def _broadcast(*args):
            if len(args) == 1:
                for c in default_connections:
                    c.accept(args[0])
            else:
                key, row = args
                if key in keyed_connections:
                    for c in keyed_connections[key]:
                        c.accept(row)
        
        try:
            a = tuple(ita.next())
        except StopIteration:
            # a is empty, everything in b is added
            for b in itb:
                _broadcast('+', b)
        else:
            try:
                b = tuple(itb.next())
            except StopIteration:
                # b is empty, everything in a is subtracted
                _broadcast('-', a)
                for a in ita:
                    _broadcast('-', a)
            else:
                while a is not None and b is not None:
                    if b is None or a < b:
                        _broadcast('-', a)
                        # advance a
                        try:
                            a = tuple(ita.next())
                        except StopIteration:
                            a = None
                    elif a == b:
                        _broadcast(a) # default channel
                        # advance both
                        try:
                            a = tuple(ita.next())
                        except StopIteration:
                            a = None
                        try:
                            b = tuple(itb.next())
                        except StopIteration:
                            b = None
                    else:
                        _broadcast('+', b)
                        # advance b
                        try:
                            b = tuple(itb.next())
                        except StopIteration:
                            b = None



# TODO standard components (one in, one out)...
# totext
# tosqlite3
# todb
# toxml
# tojson
# todicts
# tolist
# rename
# setheader
# extendheader
# pushheader
# skip
# skipcomments
# rowslice
# head
# tail
# cut
# cutout
# select
# selectop
# selecteq
# selectne
# selectlt
# selectle
# selectgt
# selectge
# selectrangeopen
# selectrangeopenleft
# selectrangeopenright
# selectrangeclosed
# selectin
# selectnotin
# selectis
# selectisnot
# selectre
# rowselect
# rowlenselect
# fieldselect
# replace
# replaceall
# convert
# convertall
# fieldconvert
# convertnumbers
# resub
# extend
# capture
# split
# unpack
# fieldmap
# rowmap
# rowmapmany
# sort
# aggregate
# rangeaggregate
# rangecounts
# rowreduce
# rangerowreduce
# mergereduce
# melt
# recast
# transpose
# pivot
# 


# TODO branching components (one in, many out)...
# conflicts (default pipe is conflicts, 'remainder' is the rest)
#

# TODO special components (many in)...
# cat (no point?)
# joins
# complement (default pipe is complement, 'remainder' is the rest)
# recordcomplement
# recorddiff
# intersection
# mergesort
# merge
# 
