from __future__ import absolute_import, print_function, division, \
    unicode_literals


# standard library dependencies
import csv
import codecs
import io
from ..compat import StringIO, text_type, next, PY2


# internal dependencies
from ..util import RowContainer, data
from .sources import read_source_from_arg, write_source_from_arg


def fromcsv(source=None, dialect='excel', **kwargs):
    """
    Wrapper for the standard :func:`csv.reader` function. Returns a table
    providing access to the data in the given delimited file. E.g.::

        >>> TODO

    The `filename` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.reader`. So, e.g., to override the
    delimiter from the default CSV dialect, provide the `delimiter` keyword
    argument.

    Note that all data values are strings, and any intended numeric values will
    need to be converted, see also :func:`convert`.

    """

    source = read_source_from_arg(source)
    return CSVView(source=source, dialect=dialect, **kwargs)


class CSVView(RowContainer):

    def __init__(self, source=None, dialect='excel', **kwargs):
            self.source = source
            self.dialect = dialect
            self.kwargs = kwargs

    def __iter__(self):
        if PY2:
            with self.source.open_('rU') as csvfile:
                reader = csv.reader(csvfile, dialect=self.dialect,
                                    **self.kwargs)
                for row in reader:
                    yield tuple(row)

        else:
            with self.source.open_('rb') as buffer:
                csvfile = io.TextIOWrapper(buffer, encoding='ascii',
                                           newline='')
                reader = csv.reader(csvfile, dialect=self.dialect,
                                    **self.kwargs)
                for row in reader:
                    yield tuple(row)


def fromtsv(source=None, dialect='excel-tab', **kwargs):
    """
    Convenience function, as :func:`fromcsv` but with different default dialect
    (tab delimited).

    .. versionadded:: 0.9

    """

    return fromcsv(source, dialect=dialect, **kwargs)


def tocsv(table, source=None, dialect='excel', write_header=True, **kwargs):
    """
    Write the table to a CSV file. E.g.::

        >>> TODO

    The `source` argument is the path of the delimited file, and the optional
    `write_header` argument specifies whether to include the field names in the
    delimited file.  All other keyword arguments are passed to
    :func:`csv.writer`. So, e.g., to override the delimiter from the default
    CSV dialect, provide the `delimiter` keyword argument.

    Note that if a file already exists at the given location, it will be
    overwritten.

    """

    _writecsv(table, source=source, mode='wb', write_header=write_header,
              dialect=dialect, **kwargs)


def appendcsv(table, source=None, dialect='excel', write_header=False,
              **kwargs):
    """
    Append data rows to an existing CSV file. E.g.::

        >>> TODO

    The `source` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.writer`. So, e.g., to override the
    delimiter from the default CSV dialect, provide the `delimiter` keyword
    argument.

    Note that no attempt is made to check that the fields or row lengths are
    consistent with the existing data, the data rows from the table are simply
    appended to the file. See also the :func:`cat` function.

    """

    _writecsv(table, source=source, mode='ab', write_header=write_header,
              dialect=dialect, **kwargs)


def _writecsv(table, source, mode, write_header, **kwargs):

    source = write_source_from_arg(source)
    rows = table if write_header else data(table)

    with source.open_(mode) as csvfile:
        if PY2:
            # write directly to binary file
            pass
        else:
            # wrap buffer for text IO
            csvfile = io.TextIOWrapper(csvfile, encoding='ascii',
                                       newline='', write_through=True)
        writer = csv.writer(csvfile, **kwargs)
        for row in rows:
            writer.writerow(row)


def totsv(table, source=None, dialect='excel-tab', write_header=True,
          **kwargs):
    """
    Convenience function, as :func:`tocsv` but with different default dialect
    (tab delimited).

    """

    return tocsv(table, source=source, dialect=dialect,
                 write_header=write_header, **kwargs)


def appendtsv(table, source=None, dialect='excel-tab', **kwargs):
    """
    Convenience function, as :func:`appendcsv` but with different default
    dialect (tab delimited).

    """

    return appendcsv(table, source=source, dialect=dialect, **kwargs)


# Additional classes for Unicode CSV support
# taken from the original csv module docs
# http://docs.python.org/2/library/csv.html#examples
# TODO review this is appropriate for PY3


# class UTF8Recoder:
#     """
#     Iterator that reads an encoded stream and reencodes the input to UTF-8.
#     """
#
#     def __init__(self, f, encoding):
#         self.reader = codecs.getreader(encoding)(f)
#
#     def __iter__(self):
#         return self
#
#     def next(self):
#         return next(self.reader).encode('utf-8')
#
#     __next__ = next
#
#
# class UnicodeReader:
#     """
#     A CSV reader which will iterate over lines in the CSV file "f",
#     which is encoded in the given encoding.
#     """
#
#     def __init__(self, f, dialect='excel', encoding='utf-8', **kwds):
#         f = UTF8Recoder(f, encoding)
#         self.reader = csv.reader(f, dialect=dialect, **kwds)
#
#     def __iter__(self):
#         return self
#
#     def next(self):
#         row = next(self.reader)
#         return [text_type(s, "utf-8") for s in row]
#
#     __next__ = next
#
#
# class UnicodeWriter:
#     """
#     A CSV writer which will write rows to CSV file "f",
#     which is encoded in the given encoding.
#     """
#
#     def __init__(self, f, dialect='excel', encoding="utf-8", **kwds):
#         # Redirect output to a queue
#         self.queue = StringIO()
#         self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
#         self.stream = f
#         self.encoder = codecs.getincrementalencoder(encoding)()
#
#     def writerow(self, row):
#         self.writer.writerow([text_type(s).encode("utf-8") for s in row])
#         # Fetch UTF-8 output from the queue ...
#         data = self.queue.getvalue()
#         data = data.decode("utf-8")
#         # ... and reencode it into the target encoding
#         data = self.encoder.encode(data)
#         # write to the target stream
#         self.stream.write(data)
#         # empty queue
#         self.queue.truncate(0)
#
#     def writerows(self, rows):
#         for row in rows:
#             self.writerow(row)
#
#
# def fromucsv(source=None, dialect='excel', encoding='utf-8', **kwargs):
#     """
#     Returns a table containing unicode data extracted from a delimited file via
#     the given encoding. Like :func:`fromcsv` but accepts an additional
#     ``encoding`` argument which should be one of the Python supported encodings.
#     See also :mod:`codecs`.
#
#     .. versionadded:: 0.19
#     """
#     source = read_source_from_arg(source)
#     return UnicodeCSVView(source=source, dialect=dialect, encoding=encoding,
#                           **kwargs)
#
#
# class UnicodeCSVView(RowContainer):
#     def __init__(self, source=None, dialect='excel', encoding='utf-8',
#                  **kwargs):
#         self.source = source
#         self.dialect = dialect
#         self.encoding = encoding
#         self.kwargs = kwargs
#
#     def __iter__(self):
#         with self.source.open_('rb') as f:
#             reader = UnicodeReader(f, dialect=self.dialect,
#                                    encoding=self.encoding, **self.kwargs)
#             for row in reader:
#                 yield tuple(row)
#
#
# def toucsv(table, source=None, dialect='excel', encoding='utf-8',
#            write_header=True, **kwargs):
#     """
#     Write the table to a CSV file via the given encoding. Like :func:`tocsv` but
#     accepts an additional ``encoding`` argument which should be one of the
#     Python supported encodings. See also :mod:`codecs`.
#
#     .. versionadded:: 0.19
#     """
#     source = write_source_from_arg(source)
#     with source.open_('wb') as f:
#         writer = UnicodeWriter(f, dialect=dialect, encoding=encoding, **kwargs)
#         # User specified no header
#         if not write_header:
#             for row in data(table):
#                 writer.writerow(row)
#         # Default behavior, write the header
#         else:
#             for row in table:
#                 writer.writerow(row)
#
#
# def appenducsv(table, source=None, dialect='excel', encoding='utf-8',
#                **kwargs):
#     """
#     Append the table to a CSV file via the given encoding. Like
#     :func:`appendcsv` but accepts an additional ``encoding`` argument which
#     should be one of the Python supported encodings. See also :mod:`codecs`.
#
#     .. versionadded:: 0.19
#     """
#     source = write_source_from_arg(source)
#     with source.open_('ab') as f:
#         writer = UnicodeWriter(f, dialect=dialect, encoding=encoding, **kwargs)
#         for row in data(table):
#             writer.writerow(row)
#
#
# def fromutsv(source=None, dialect='excel-tab', **kwargs):
#     """
#     Convenience function, as :func:`fromucsv` but with different default dialect
#     (tab delimited).
#
#     .. versionadded:: 0.19
#
#     """
#
#     return fromucsv(source=source, dialect=dialect, **kwargs)
#
#
# def toutsv(table, source=None, dialect='excel-tab', write_header=True,
#            **kwargs):
#     """
#     Convenience function, as :func:`toucsv` but with different default dialect
#     (tab delimited).
#
#     .. versionadded:: 0.19
#
#     """
#
#     return toucsv(table, source=source, dialect=dialect,
#                   write_header=write_header, **kwargs)
#
#
# def appendutsv(table, source=None, dialect='excel-tab', **kwargs):
#     """
#     Convenience function, as :func:`appenducsv` but with different default
#     dialect (tab delimited).
#
#     .. versionadded:: 0.19
#
#     """
#
#     return appenducsv(table, source=source, dialect=dialect, **kwargs)
#
#
# def teecsv(table, source=None, dialect='excel', write_header=True, **kwargs):
#     """
#     Return a table that writes rows to a CSV file as they are iterated over.
#
#     .. versionadded:: 0.25
#
#     """
#
#     return teeucsv(table, source=source, dialect=dialect,
#                    write_header=write_header, encoding='ascii', **kwargs)
#     # return TeeCSVContainer(table, source=source, dialect=dialect,
#     #                        write_header=write_header, **kwargs)
#
#
# # class TeeCSVContainer(RowContainer):
# #     def __init__(self, table, source=None, dialect='excel',
# #                  write_header=True, **kwargs):
# #         self.table = table
# #         self.source = source
# #         self.dialect = dialect
# #         self.write_header = write_header
# #         self.kwargs = kwargs
# #
# #     def __iter__(self):
# #         source = write_source_from_arg(self.source)
# #         with source.open_('wb') as f:
# #             writer = csv.writer(f, dialect=self.dialect, **self.kwargs)
# #             # User specified no header
# #             if not self.write_header:
# #                 for row in data(self.table):
# #                     writer.writerow(row)
# #                     yield row
# #             # Default behavior, write the header
# #             else:
# #                 for row in self.table:
# #                     writer.writerow(row)
# #                     yield row
#
#
# def teetsv(table, source=None, dialect='excel-tab', write_header=True,
#            **kwargs):
#     """
#     Convenience function, as :func:`teecsv` but with different default dialect
#     (tab delimited).
#
#     .. versionadded:: 0.25
#
#     """
#
#     return teecsv(table, source=source, dialect=dialect,
#                   write_header=write_header, **kwargs)
#
#
# def teeucsv(table, source=None, dialect='excel', encoding='utf-8',
#             write_header=True, **kwargs):
#     """
#     Return a table that writes rows to a Unicode CSV file as they are iterated
#     over.
#
#     .. versionadded:: 0.25
#
#     """
#
#     return TeeUCSVContainer(table, source=source, dialect=dialect,
#                             encoding=encoding, write_header=write_header,
#                             **kwargs)
#
#
# class TeeUCSVContainer(RowContainer):
#     def __init__(self, table, source=None, dialect='excel', encoding='utf-8',
#                  write_header=True, **kwargs):
#         self.table = table
#         self.source = source
#         self.dialect = dialect
#         self.encoding = encoding
#         self.write_header = write_header
#         self.kwargs = kwargs
#
#     def __iter__(self):
#         source = write_source_from_arg(self.source)
#         with source.open_('wb') as f:
#             writer = UnicodeWriter(f, dialect=self.dialect,
#                                    encoding=self.encoding, **self.kwargs)
#             # User specified no header
#             if not self.write_header:
#                 for row in data(self.table):
#                     writer.writerow(row)
#                     yield row
#             # Default behavior, write the header
#             else:
#                 for row in self.table:
#                     writer.writerow(row)
#                     yield row
#
#
# def teeutsv(table, source=None, dialect='excel-tab',
#             encoding='utf-8', write_header=True, **kwargs):
#     """
#     Convenience function, as :func:`teeucsv` but with different default dialect
#     (tab delimited).
#
#     .. versionadded:: 0.25
#
#     """
#
#     return teeucsv(table, source=source, dialect=dialect, encoding=encoding,
#                    write_header=write_header, **kwargs)
