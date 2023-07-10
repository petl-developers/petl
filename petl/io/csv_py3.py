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


class CSVView(Table):

    def __init__(self, source, encoding, errors, header, **csvargs):
        self.source = source
        self.encoding = encoding
        self.errors = errors
        self.csvargs = csvargs
        self.header = header

    def __iter__(self):
        if self.header is not None:
            yield tuple(self.header)
        with self.source.open('rb') as buf:
            csvfile = io.TextIOWrapper(buf, encoding=self.encoding,
                                       errors=self.errors, newline='')
            try:
                reader = csv.reader(csvfile, **self.csvargs)
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
