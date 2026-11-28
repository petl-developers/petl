# -*- coding: utf-8 -*-
import io
import csv
import logging


from petl.util.base import Table, data


logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


def fromcsv_impl(source, **kwargs):
    return CSVView(source, **kwargs)


def _strip_nulls(lines):
    """Yield physical lines as if NULs had been removed before line splitting."""
    pending = ''
    for line in lines:
        line = line.replace('\x00', '')
        if pending:
            if line == '\n':
                line = pending + line
            else:
                yield pending
            pending = ''
        if line.endswith('\r'):
            # Removing NULs may join this CR to the next line's LF.
            pending = line
        elif line:
            yield line
    if pending:
        yield pending


class CSVView(Table):

    def __init__(self, source, encoding, errors, header, strip_nulls=False, **csvargs):
        self.source = source
        self.encoding = encoding
        self.errors = errors
        self.csvargs = csvargs
        self._header = header
        self.strip_nulls = strip_nulls

    def __iter__(self):
        if self._header is not None:
            yield tuple(self._header)
        with self.source.open('rb') as buf:
            csvfile = io.TextIOWrapper(buf, encoding=self.encoding,
                                       errors=self.errors, newline='')
            try:
                lines = csvfile
                if self.strip_nulls:
                    lines = _strip_nulls(csvfile)
                reader = csv.reader(lines, **self.csvargs)
                for row in reader:
                    yield tuple(row)
            finally:
                csvfile.detach()


def tocsv_impl(table, source, **kwargs):
    _writecsv(table, source=source, mode='wb', **kwargs)


def appendcsv_impl(table, source, **kwargs):
    _writecsv(table, source=source, mode='ab', **kwargs)


def _writecsv(table, source, mode, write_header, encoding, errors, **csvargs):
    rows = table if write_header else data(table)
    with source.open(mode) as buf:
        # wrap buffer for text IO
        csvfile = io.TextIOWrapper(buf, encoding=encoding, errors=errors,
                                   newline='')
        try:
            writer = csv.writer(csvfile, **csvargs)
            for row in rows:
                writer.writerow(row)
            csvfile.flush()
        finally:
            csvfile.detach()


def teecsv_impl(table, source, **kwargs):
    return TeeCSVView(table, source=source, **kwargs)


class TeeCSVView(Table):

    def __init__(self, table, source=None, encoding=None,
                 errors='strict', write_header=True, **csvargs):
        self.table = table
        self.source = source
        self.write_header = write_header
        self.encoding = encoding
        self.errors = errors
        self.csvargs = csvargs

    def __iter__(self):
        with self.source.open('wb') as buf:
            # wrap buffer for text IO
            csvfile = io.TextIOWrapper(buf, encoding=self.encoding,
                                       errors=self.errors, newline='')
            try:
                writer = csv.writer(csvfile, **self.csvargs)
                it = iter(self.table)
                try:
                    hdr = next(it)
                except StopIteration:
                    return
                if self.write_header:
                    writer.writerow(hdr)
                yield tuple(hdr)
                for row in it:
                    writer.writerow(row)
                    yield tuple(row)
                csvfile.flush()
            finally:
                csvfile.detach()
