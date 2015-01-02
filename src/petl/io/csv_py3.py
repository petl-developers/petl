from __future__ import division, print_function, absolute_import, \
    unicode_literals


import io
import csv


from ..util import RowContainer


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
