from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


# standard library dependencies
import csv
import codecs
import cStringIO


# internal dependencies
from petl.util import RowContainer, data
from petl.io.sources import read_source_from_arg, write_source_from_arg


def fromcsv(source=None, dialect=csv.excel, **kwargs):
    """
    Wrapper for the standard :func:`csv.reader` function. Returns a table
    providing access to the data in the given delimited file. E.g.::

        >>> import csv
        >>> # set up a CSV file to demonstrate with
        ... with open('test.csv', 'wb') as f:
        ...     writer = csv.writer(f)
        ...     writer.writerow(['foo', 'bar'])
        ...     writer.writerow(['a', 1])
        ...     writer.writerow(['b', 2])
        ...     writer.writerow(['c', 2])
        ...
        >>> # now demonstrate the use of petl.fromcsv
        ... from petl import fromcsv, look
        >>> testcsv = fromcsv('test.csv')
        >>> look(testcsv)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+

    The `filename` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.reader`. So, e.g., to override the
    delimiter from the default CSV dialect, provide the `delimiter` keyword
    argument.

    Note that all data values are strings, and any intended numeric values will
    need to be converted, see also :func:`convert`.

    Supports transparent reading from URLs, ``.gz`` and ``.bz2`` files.

    """

    source = read_source_from_arg(source)
    return CSVView(source=source, dialect=dialect, **kwargs)


class CSVView(RowContainer):

    def __init__(self, source=None, dialect=csv.excel, **kwargs):
        self.source = source
        self.dialect = dialect
        self.kwargs = kwargs

    def __iter__(self):
        with self.source.open_('rU') as f:
            reader = csv.reader(f, dialect=self.dialect, **self.kwargs)
            for row in reader:
                yield tuple(row)


def tocsv(table, source=None, dialect=csv.excel, write_header=True, **kwargs):
    """
    Write the table to a CSV file. E.g.::

        >>> from petl import tocsv, look
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+

        >>> tocsv(table, 'test.csv')
        >>> # look what it did
        ... from petl import fromcsv
        >>> look(fromcsv('test.csv'))
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+

    The `filename` argument is the path of the delimited file, and the optional
    `write_header` argument specifies whether to include the field names in the
    delimited file.  All other keyword arguments are passed to
    :func:`csv.writer`. So, e.g., to override the delimiter from the default
    CSV dialect, provide the `delimiter` keyword argument.

    Note that if a file already exists at the given location, it will be
    overwritten.

    Supports transparent writing to ``.gz`` and ``.bz2`` files.

    """

    source = write_source_from_arg(source)
    with source.open_('wb') as f:
        writer = csv.writer(f, dialect=dialect, **kwargs)
        # User specified no header
        if not write_header:
            for row in data(table):
                writer.writerow(row)
       # Default behavior, write the header
        else:
            for row in table:
                writer.writerow(row)


def appendcsv(table, source=None, dialect=csv.excel, **kwargs):
    """
    Append data rows to an existing CSV file. E.g.::

        >>> # look at an existing CSV file
        ... from petl import look, fromcsv
        >>> testcsv = fromcsv('test.csv')
        >>> look(testcsv)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+

        >>> # append some data
        ... look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'd'   | 7     |
        +-------+-------+
        | 'e'   | 42    |
        +-------+-------+
        | 'f'   | 12    |
        +-------+-------+

        >>> from petl import appendcsv
        >>> appendcsv(table, 'test.csv')
        >>> # look what it did
        ... look(testcsv)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+
        | 'd'   | '7'   |
        +-------+-------+
        | 'e'   | '42'  |
        +-------+-------+
        | 'f'   | '12'  |
        +-------+-------+

    The `filename` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.writer`. So, e.g., to override the
    delimiter from the default CSV dialect, provide the `delimiter` keyword
    argument.

    Note that no attempt is made to check that the fields or row lengths are
    consistent with the existing data, the data rows from the table are simply
    appended to the file. See also the :func:`cat` function.

    Supports transparent writing to ``.gz`` and ``.bz2`` files.

    """

    source = write_source_from_arg(source)
    with source.open_('ab') as f:
        writer = csv.writer(f, dialect=dialect, **kwargs)
        for row in data(table):
            writer.writerow(row)


def fromtsv(source=None, dialect=csv.excel_tab, **kwargs):
    """
    Convenience function, as :func:`fromcsv` but with different default dialect
    (tab delimited).

    Supports transparent reading from URLs, ``.gz`` and ``.bz2`` files.

    .. versionadded:: 0.9

    """

    return fromcsv(source, dialect=dialect, **kwargs)


def totsv(table, source=None, dialect=csv.excel_tab, write_header=True,
          **kwargs):
    """
    Convenience function, as :func:`tocsv` but with different default dialect
    (tab delimited).

    Supports transparent writing to ``.gz`` and ``.bz2`` files.

    .. versionadded:: 0.9

    """

    return tocsv(table, source=source, dialect=dialect,
                 write_header=write_header, **kwargs)


def appendtsv(table, source=None, dialect=csv.excel_tab, **kwargs):
    """
    Convenience function, as :func:`appendcsv` but with different default
    dialect (tab delimited).

    Supports transparent writing to ``.gz`` and ``.bz2`` files.

    .. versionadded:: 0.9

    """

    return appendcsv(table, source=source, dialect=dialect, **kwargs)



#   Additional classes for Unicode CSV support
#   taken from the original csv module docs
#   http://docs.python.org/2/library/csv.html#examples


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([unicode(s).encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def fromucsv(source=None, dialect=csv.excel, encoding='utf-8', **kwargs):
    """
    Returns a table containing unicode data extracted from a delimited file via
    the given encoding. Like :func:`fromcsv` but accepts an additional
    ``encoding`` argument which should be one of the Python supported encodings.
    See also :mod:`codecs`.

    .. versionadded:: 0.19
    """
    source = read_source_from_arg(source)
    return UnicodeCSVView(source=source, dialect=dialect, encoding=encoding,
                          **kwargs)


class UnicodeCSVView(RowContainer):

    def __init__(self, source=None, dialect=csv.excel, encoding='utf-8',
                 **kwargs):
        self.source = source
        self.dialect = dialect
        self.encoding = encoding
        self.kwargs = kwargs

    def __iter__(self):
        with self.source.open_('rb') as f:
            reader = UnicodeReader(f, dialect=self.dialect,
                                   encoding=self.encoding, **self.kwargs)
            for row in reader:
                yield tuple(row)


def toucsv(table, source=None, dialect=csv.excel, encoding='utf-8',
           write_header=True, **kwargs):
    """
    Write the table to a CSV file via the given encoding. Like :func:`tocsv` but
    accepts an additional ``encoding`` argument which should be one of the
    Python supported encodings. See also :mod:`codecs`.

    .. versionadded:: 0.19
    """
    source = write_source_from_arg(source)
    with source.open_('wb') as f:
        writer = UnicodeWriter(f, dialect=dialect, encoding=encoding, **kwargs)
        # User specified no header
        if not write_header:
            for row in data(table):
                writer.writerow(row)
        # Default behavior, write the header
        else:
            for row in table:
                writer.writerow(row)


def appenducsv(table, source=None, dialect=csv.excel, encoding='utf-8',
               **kwargs):
    """
    Append the table to a CSV file via the given encoding. Like
    :func:`appendcsv` but accepts an additional ``encoding`` argument which
    should be one of the Python supported encodings. See also :mod:`codecs`.

    .. versionadded:: 0.19
    """
    source = write_source_from_arg(source)
    with source.open_('ab') as f:
        writer = UnicodeWriter(f, dialect=dialect, encoding=encoding, **kwargs)
        for row in data(table):
            writer.writerow(row)


def fromutsv(source=None, dialect=csv.excel_tab, **kwargs):
    """
    Convenience function, as :func:`fromucsv` but with different default dialect
    (tab delimited).

    .. versionadded:: 0.19

    """

    return fromucsv(source, dialect=dialect, **kwargs)


def toutsv(table, source=None, dialect=csv.excel_tab, write_header=True,
           **kwargs):
    """
    Convenience function, as :func:`toucsv` but with different default dialect
    (tab delimited).

    .. versionadded:: 0.19

    """

    return toucsv(table, source=source, dialect=dialect,
                  write_header=write_header, **kwargs)


def appendutsv(table, source=None, dialect=csv.excel_tab, **kwargs):
    """
    Convenience function, as :func:`appenducsv` but with different default
    dialect (tab delimited).

    .. versionadded:: 0.19

    """

    return appenducsv(table, source=source, dialect=dialect, **kwargs)


def teecsv(table, source=None, dialect=csv.excel, write_header=True, **kwargs):
    """
    Return a table that writes rows to a CSV file as they are iterated over.

    .. versionadded:: 0.25

    """

    return TeeCSVContainer(table, source=source, dialect=dialect,
                           write_header=write_header, **kwargs)


class TeeCSVContainer(RowContainer):

    def __init__(self, table, source=None, dialect=csv.excel,
                 write_header=True, **kwargs):
        self.table = table
        self.source = source
        self.dialect = dialect
        self.write_header = write_header
        self.kwargs = kwargs

    def __iter__(self):
        source = write_source_from_arg(self.source)
        with source.open_('wb') as f:
            writer = csv.writer(f, dialect=self.dialect, **self.kwargs)
            # User specified no header
            if not self.write_header:
                for row in data(self.table):
                    writer.writerow(row)
                    yield row
           # Default behavior, write the header
            else:
                for row in self.table:
                    writer.writerow(row)
                    yield row


def teetsv(table, source=None, dialect=csv.excel_tab, write_header=True,
           **kwargs):
    """
    Convenience function, as :func:`teecsv` but with different default dialect
    (tab delimited).

    .. versionadded:: 0.25

    """

    return TeeCSVContainer(table, source=source, dialect=dialect,
                           write_header=write_header, **kwargs)


def teeucsv(table, source=None, dialect=csv.excel, encoding='utf-8',
            write_header=True, **kwargs):
    """
    Return a table that writes rows to a Unicode CSV file as they are iterated
    over.

    .. versionadded:: 0.25

    """

    return TeeUCSVContainer(table, source=source, dialect=dialect,
                            encoding=encoding, write_header=write_header,
                            **kwargs)


class TeeUCSVContainer(RowContainer):

    def __init__(self, table, source=None, dialect=csv.excel, encoding='utf-8',
                 write_header=True, **kwargs):
        self.table = table
        self.source = source
        self.dialect = dialect
        self.encoding = encoding
        self.write_header = write_header
        self.kwargs = kwargs

    def __iter__(self):
        source = write_source_from_arg(self.source)
        with source.open_('wb') as f:
            writer = UnicodeWriter(f, dialect=self.dialect,
                                   encoding=self.encoding, **self.kwargs)
            # User specified no header
            if not self.write_header:
                for row in data(self.table):
                    writer.writerow(row)
                    yield row
            # Default behavior, write the header
            else:
                for row in self.table:
                    writer.writerow(row)
                    yield row


def teeutsv(table, source=None, dialect=csv.excel_tab,
            encoding='utf-8', write_header=True, **kwargs):
    """
    Convenience function, as :func:`teeucsv` but with different default dialect
    (tab delimited).

    .. versionadded:: 0.25

    """

    return TeeUCSVContainer(table, source=source, dialect=dialect,
                            encoding=encoding, write_header=write_header,
                            **kwargs)


