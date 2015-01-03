from __future__ import division, print_function, absolute_import, \
    unicode_literals


import io
import csv


from ..util import RowContainer, data


def fromcsv_impl(source, **csvargs):
    return CSVView(source, encoding='ascii', **csvargs)


def fromucsv_impl(source, encoding='utf-8', **csvargs):
    return CSVView(source, encoding=encoding, **csvargs)


class CSVView(RowContainer):

    def __init__(self, source, encoding, **csvargs):
            self.source = source
            self.encoding = encoding
            self.csvargs = csvargs

    def __iter__(self):
        with self.source.open_('rb') as buffer:
            csvfile = io.TextIOWrapper(buffer, encoding=self.encoding,
                                       newline='')
            reader = csv.reader(csvfile, **self.csvargs)
            for row in reader:
                yield tuple(row)


def tocsv_impl(table, source, write_header=True, **csvargs):
    _writecsv(table, source=source, mode='wb',
              encoding='ascii', write_header=write_header,
              **csvargs)


def appendcsv_impl(table, source, write_header=False, **csvargs):
    _writecsv(table, source=source, mode='ab',
              encoding='ascii', write_header=write_header,
              **csvargs)


def toucsv_impl(table, source, encoding='utf-8', write_header=True, **csvargs):
    _writecsv(table, source=source, mode='wb', encoding=encoding,
              write_header=write_header, **csvargs)


def appenducsv_impl(table, source, encoding='utf-8', write_header=False,
                    **csvargs):
    _writecsv(table, source=source, mode='ab', encoding=encoding,
              write_header=write_header, **csvargs)


def _writecsv(table, source, mode, write_header, encoding, **csvargs):
    rows = table if write_header else data(table)
    with source.open_(mode) as buffer:
        # wrap buffer for text IO
        csvfile = io.TextIOWrapper(buffer, encoding=encoding,
                                   newline='', write_through=True)
        writer = csv.writer(csvfile, **csvargs)
        for row in rows:
            writer.writerow(row)


def teecsv_impl(table, source, **csvargs):
    return TeeCSVContainer(table, source=source, encoding='ascii', **csvargs)


def teeucsv_impl(table, source, encoding='utf-8', **csvargs):
    return TeeCSVContainer(table, source=source, encoding=encoding, **csvargs)


class TeeCSVContainer(RowContainer):
    def __init__(self, table, source=None, encoding='utf-8', **csvargs):
        self.table = table
        self.source = source
        self.encoding = encoding
        self.csvargs = csvargs

    def __iter__(self):
        with self.source.open_('wb') as buffer:
            # wrap buffer for text IO
            csvfile = io.TextIOWrapper(buffer, encoding=self.encoding,
                                       newline='', write_through=True)
            writer = csv.writer(csvfile, **self.csvargs)
            for row in self.table:
                writer.writerow(row)
                yield row
