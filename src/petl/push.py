"""
A tentative module for pushing data through branching pipelines.

"""


import csv
from petl.util import asindices, HybridRow
from operator import itemgetter
from itertools import islice
from collections import defaultdict


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


def partition(discriminator):
    """
    TODO doc me

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
        if key in self.keyed_connections:
            for c in self.keyed_connections[key]:
                c.accept(tuple(row))


def tocsv(filename, dialect=csv.excel, **kwargs):
    """
    TODO doc me

    """

    return ToCsvComponent(filename, dialect, **kwargs)


class ToCsvComponent(PipelineComponent):

    def __init__(self, filename, dialect, **kwargs):
        super(ToCsvComponent, self).__init__()
        self.filename = filename
        self.dialect = dialect
        self.kwargs = kwargs

    def pipe(self, *args):
        raise Exception('Component does not support pipe.')

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

    def close(self):
        self.file.flush()
        self.file.close()
        super(ToCsvConnection, self).close()


# TODO standard components (one in, one out)...
# totsv
# topickle
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

# make to... work like tee, i.e., pass on rows to default receivers
#

# TODO branching components (one in, many out)...
# duplicates (default pipe is duplicates, 'pass' is the rest)
# conflicts (default pipe is conflicts, 'pass' is the rest)
#

# TODO special components (many in)...
# cat (no point?)
# joins
# complement (default pipe is complement, 'pass' is the rest)
# diff (default pipe is rows in common, '+', '-')
# recordcomplement
# recorddiff
# intersection
# mergesort
# merge
# 
