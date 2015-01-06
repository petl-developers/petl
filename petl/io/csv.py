from __future__ import absolute_import, print_function, division, \
    unicode_literals


# standard library dependencies
from petl.compat import PY2


# internal dependencies
from petl.util.base import Table
from petl.io.sources import read_source_from_arg, write_source_from_arg
if PY2:
    from petl.io.csv_py2 import fromcsv_impl, fromucsv_impl, tocsv_impl, \
        toucsv_impl, appendcsv_impl, appenducsv_impl, teecsv_impl, teeucsv_impl
else:
    from petl.io.csv_py3 import fromcsv_impl, fromucsv_impl, tocsv_impl, \
        toucsv_impl, appendcsv_impl, appenducsv_impl, teecsv_impl, teeucsv_impl


def fromcsv(source=None, dialect='excel', **kwargs):
    """Extract a table from a delimited file. E.g.::

        >>> # set up a CSV file to demonstrate with
        ... table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> import csv
        >>> with open('example.csv', 'w') as f:
        ...     writer = csv.writer(f)
        ...     writer.writerows(table1)
        ...
        >>> # now demonstrate the use of fromcsv()
        ... from petl import fromcsv, look
        >>> table2 = fromcsv('example.csv')
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+

    The `source` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.reader`. So, e.g., to override the
    delimiter from the default CSV dialect, provide the `delimiter` keyword
    argument.

    Note that all data values are strings, and any intended numeric values will
    need to be converted, see also :func:`convert`.

    Under Python 3 this function is equivalent to :func:`fromucsv` with
    ``encoding='ascii'``.

    """

    source = read_source_from_arg(source)
    return fromcsv_impl(source=source, dialect=dialect, **kwargs)


def fromtsv(source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`fromcsv` but with different default
    dialect (tab delimited).

    """

    return fromcsv(source, dialect=dialect, **kwargs)


def fromucsv(source=None, encoding='utf-8', dialect='excel', **kwargs):
    """Extract a table from a delimited file with the given text encoding. Like
    :func:`fromcsv` but accepts an additional ``encoding`` argument which
    should be one of the Python supported encodings.

    """

    source = read_source_from_arg(source)
    return fromucsv_impl(source, encoding=encoding, dialect=dialect, **kwargs)


def fromutsv(source=None, encoding='utf-8', dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`fromucsv` but with different default
    dialect (tab delimited).

    """

    return fromucsv(source=source, encoding=encoding, dialect=dialect, **kwargs)


def tocsv(table, source=None, dialect='excel', write_header=True, **kwargs):
    """Write the table to a CSV file. E.g.::

        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> from petl import tocsv
        >>> tocsv(table1, 'example.csv')
        >>> # look what it did
        ... print(open('example.csv').read())
        foo,bar
        a,1
        b,2
        c,2


    The `source` argument is the path of the delimited file, and the optional
    `write_header` argument specifies whether to include the field names in the
    delimited file.  All other keyword arguments are passed to
    :func:`csv.writer`. So, e.g., to override the delimiter from the default
    CSV dialect, provide the `delimiter` keyword argument.

    Note that if a file already exists at the given location, it will be
    overwritten.

    """

    source = write_source_from_arg(source)
    tocsv_impl(table, source=source, dialect=dialect,
               write_header=write_header, **kwargs)


Table.tocsv = tocsv


def appendcsv(table, source=None, dialect='excel', write_header=False,
              **kwargs):
    """Append data rows to an existing CSV file. As :func:`tocsv` but the
    file is opened in append mode and the table header is not written by
    default.

    Note that no attempt is made to check that the fields or row lengths are
    consistent with the existing data, the data rows from the table are simply
    appended to the file.

    """

    source = write_source_from_arg(source)
    appendcsv_impl(table, source=source, dialect=dialect,
                   write_header=write_header, **kwargs)


Table.appendcsv = appendcsv


def totsv(table, source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`tocsv` but with different default dialect
    (tab delimited).

    """

    return tocsv(table, source=source, dialect=dialect, **kwargs)


Table.totsv = totsv


def appendtsv(table, source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`appendcsv` but with different default
    dialect (tab delimited).

    """

    return appendcsv(table, source=source, dialect=dialect, **kwargs)


Table.appendtsv = appendtsv


def toucsv(table, source=None, dialect='excel', encoding='utf-8',
           write_header=True, **kwargs):
    """Write the table to a delimited file using the given text encoding. Like
    :func:`tocsv` but accepts an additional ``encoding`` argument which
    should be one of the Python supported encodings.

    """

    source = write_source_from_arg(source)
    toucsv_impl(table, source=source, dialect=dialect, encoding=encoding,
                write_header=write_header, **kwargs)


Table.toucsv = toucsv


def appenducsv(table, source=None, dialect='excel', encoding='utf-8',
               write_header=False, **kwargs):
    """Append the table to a delimited file using the given text encoding. Like
    :func:`appendcsv` but accepts an additional ``encoding`` argument which
    should be one of the Python supported encodings.

    """

    source = write_source_from_arg(source)
    appenducsv_impl(table, source=source, dialect=dialect, encoding=encoding,
                    write_header=write_header, **kwargs)


Table.appenducsv = appenducsv


def toutsv(table, source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`toucsv` but with different default
    dialect (tab delimited).

    """

    return toucsv(table, source=source, dialect=dialect, **kwargs)


Table.toutsv = toutsv


def appendutsv(table, source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`appenducsv` but with different default
    dialect (tab delimited).

    """

    return appenducsv(table, source=source, dialect=dialect, **kwargs)


Table.appendutsv = appendutsv


def teecsv(table, source=None, write_header=True, dialect='excel', **kwargs):
    """Returns a table that writes rows to a CSV file as they are iterated over.

    """

    source = write_source_from_arg(source)
    return teecsv_impl(table, source=source, write_header=write_header,
                       dialect=dialect, **kwargs)


Table.teecsv = teecsv


def teetsv(table, source=None, write_header=True, dialect='excel-tab',
           **kwargs):
    """Convenience function, as :func:`teecsv` but with different default
    dialect (tab delimited).

    """

    return teecsv(table, source=source, write_header=write_header,
                  dialect=dialect, **kwargs)


Table.teetsv = teetsv


def teeucsv(table, source=None, write_header=True, dialect='excel',
            encoding='utf-8', **kwargs):
    """Returns a table that writes rows to a Unicode CSV file as they are
    iterated over.

    """

    source = write_source_from_arg(source)
    return teeucsv_impl(table, source=source, encoding=encoding,
                        write_header=write_header, dialect=dialect, **kwargs)


Table.teeucsv = teeucsv


def teeutsv(table, source=None, write_header=True, dialect='excel-tab',
            encoding='utf-8', **kwargs):
    """Convenience function, as :func:`teeucsv` but with different default
    dialect (tab delimited).

    """

    return teeucsv(table, source=source, write_header=write_header,
                   dialect=dialect, encoding=encoding, **kwargs)


Table.teeutsv = teeutsv
