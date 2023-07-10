# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
import csv
import cStringIO


# internal dependencies
from petl.util.base import Table, data
from petl.io.base import getcodec


def fromcsv_impl(source, **kwargs):
    return CSVView(source, **kwargs)


class CSVView(Table):

    def __init__(self, source=None, encoding=None, errors='strict', header=None, **csvargs):
            self.source = source
            self.encoding = encoding
            self.errors = errors
            self.csvargs = csvargs
            self.header = header

    def __iter__(self):
        if self.header is not None:
          yield tuple(self.header)

        # determine encoding
        codec = getcodec(self.encoding)

        # ascii
        if codec.name == 'ascii':
            # bypass encoding
            with self.source.open('rU') as csvfile:
                reader = csv.reader(csvfile, **self.csvargs)
                for row in reader:
                    yield tuple(row)

        # non-ascii
        else:
            with self.source.open('rb') as buf:
                reader = UnicodeReader(buf, encoding=self.encoding,
                                       errors=self.errors, **self.csvargs)
                for row in reader:
                    yield tuple(row)


def tocsv_impl(table, source, **kwargs):
    _writecsv(table, source=source, mode='wb', **kwargs)


def appendcsv_impl(table, source, **kwargs):
    _writecsv(table, source=source, mode='ab', **kwargs)


def _writecsv(table, source, mode, write_header, encoding, errors, **csvargs):
    rows = table if write_header else data(table)
    with source.open(mode) as buf:

        # determine encoding
        codec = getcodec(encoding)

        # ascii
        if codec.name == 'ascii':
            # bypass encoding
            writer = csv.writer(buf, **csvargs)

        # non-ascii
        else:
            writer = UnicodeWriter(buf, encoding=encoding, errors=errors,
                                   **csvargs)

        for row in rows:
            writer.writerow(row)


def teecsv_impl(table, source, **kwargs):
    return TeeCSVView(table, source=source, **kwargs)


class TeeCSVView(Table):
    def __init__(self, table, source=None, encoding=None,
                 errors='strict', write_header=True, **csvargs):
        self.table = table
        self.source = source
        self.encoding = encoding
        self.errors = errors
        self.write_header = write_header
        self.csvargs = csvargs

    def __iter__(self):
        with self.source.open('wb') as buf:

            # determine encoding
            codec = getcodec(self.encoding)

            # ascii
            if codec.name == 'ascii':
                # bypass encoding
                writer = csv.writer(buf, **self.csvargs)

            # non-ascii
            else:
                writer = UnicodeWriter(buf, encoding=self.encoding,
                                       errors=self.errors,
                                       **self.csvargs)

            it = iter(self.table)

            # deal with header row
            try:
                hdr = next(it)
            except StopIteration:
                return
            if self.write_header:
                writer.writerow(hdr)
            # N.B., always yield header, even if we don't write it
            yield tuple(hdr)

            # data rows
            for row in it:
                writer.writerow(row)
                yield tuple(row)


# Additional classes for Unicode CSV support in PY2
# taken from the original csv module docs
# http://docs.python.org/2/library/csv.html#examples


class UTF8Recoder:

    def __init__(self, buf, encoding, errors):
        codec = getcodec(encoding)
        self.reader = codec.streamreader(buf, errors=errors)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode('utf-8')


class UnicodeReader:

    def __init__(self, f, encoding=None, errors='strict', **csvargs):
        f = UTF8Recoder(f, encoding=encoding, errors=errors)
        self.reader = csv.reader(f, **csvargs)

    def next(self):
        row = self.reader.next()
        return [unicode(s, 'utf-8') if isinstance(s, basestring) else s 
                    for s in row]

    def __iter__(self):
        return self


class UnicodeWriter:

    def __init__(self, buf, encoding=None, errors='strict', **csvargs):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, **csvargs)
        self.stream = buf
        codec = getcodec(encoding)
        self.encoder = codec.incrementalencoder(errors)

    def writerow(self, row):
        self.writer.writerow(
            [unicode(s).encode('utf-8') if s is not None else None
             for s in row]
        )
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode('utf-8')
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
