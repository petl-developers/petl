"""
A tentative module for branching pipelines.

"""


import csv
from petl.util import asindices
from operator import itemgetter
from itertools import islice
from collections import defaultdict


class PipelineComponent(object):

    def __init__(self):
        self.default_receivers = list()
        self.receivers = defaultdict(list)

    def pipe(self, *args):
        assert 1 <= len(args) <= 2, '1 or 2 arguments expected'
        if len(args) == 1:
            self.default_receivers.append(args[0])
            return args[0]
        elif len(args) == 2:
            self.receivers[args[0]].append(args[1])
            return args[1]
            

def pipeline(source):
    """
    TODO doc me

    """

    return SourceComponent(source)


class SourceComponent(PipelineComponent):

    def __init__(self, source):
        super(SourceComponent, self).__init__()
        self.source = source

    def run(self, limit=None):
        it = iter(self.source)
        fields = it.next()
        # only default receivers get connected
        connections = [r.connect(fields) for r in self.default_receivers]
        for row in islice(it, limit):
            for c in connections:
                c.accept(tuple(row))
        for c in connections:
            c.close()


def partition(discriminant):
    """
    TODO doc me

    """

    return PartitionComponent(discriminant)


class PartitionComponent(PipelineComponent):

    def __init__(self, discriminant):
        super(PartitionComponent, self).__init__()
        self.discriminant = discriminant

    def connect(self, fields):
        default_connections = [r.connect(fields) for r in self.default_receivers]
        connections = dict()
        for k in self.receivers:
            connections[k] = [r.connect(fields) for r in self.receivers[k]]
        return PartitionConnection(default_connections, connections, fields, self.discriminant)


class PartitionConnection(object):

    def __init__(self, default_connections, connections, fields, discriminant):
        self.default_connections = default_connections
        self.connections = connections
        self.fields = fields
        if callable(discriminant):
            self.discriminant = discriminant
        else: # assume field or fields
            self.discriminant = itemgetter(*asindices(fields, discriminant))

    def accept(self, row):
        key = self.discriminant(row)
        if key in self.connections:
            for c in self.connections[key]:
                c.accept(tuple(row))

    def close(self):
        for c in self.default_connections:
            c.close()
        for k in self.connections:
            for c in self.connections[k]:
                c.close()


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

    def connect(self, fields):
        default_connections = [r.connect(fields) for r in self.default_receivers]
        connections = dict()
        for k in self.receivers:
            connections[k] = [r.connect(fields) for r in self.receivers[k]]
        return ToCsvConnection(default_connections, connections, fields, 
                               self.filename, self.dialect, self.kwargs)


class ToCsvConnection(object):

    def __init__(self, default_connections, connections, fields, filename, dialect, kwargs):
        self.default_connections = default_connections
        self.connections = connections
        self.fields = fields
        self.file = open(filename, 'wb')
        self.writer = csv.writer(self.file, dialect=dialect, **kwargs)
        self.writer.writerow(fields)

    def accept(self, row):
        self.writer.writerow(row)
        for c in self.default_connections:
            c.accept(tuple(row))

    def close(self):
        self.file.flush()
        self.file.close()
        for c in self.default_connections:
            c.close()
        for k in self.connections:
            for c in self.connections[k]:
                c.close()
