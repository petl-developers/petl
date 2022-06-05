# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
from petl.compat import PY2


# internal dependencies
from petl.util.base import Table
from petl.io.sources import read_source_from_arg, write_source_from_arg
if PY2:
    from petl.io.csv_py2 import fromcsv_impl, tocsv_impl, appendcsv_impl, \
        teecsv_impl
else:
    from petl.io.csv_py3 import fromcsv_impl, tocsv_impl, appendcsv_impl, \
        teecsv_impl


def fromcsv(source=None, encoding=None, errors='strict', header=None, 
            **csvargs):
    """
    Extract a table from a delimited file. E.g.::

        >>> import petl as etl
        >>> import csv
        >>> # set up a CSV file to demonstrate with
        ... table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> with open('example.csv', 'w') as f:
        ...     writer = csv.writer(f)
        ...     writer.writerows(table1)
        ...
        >>> # now demonstrate the use of fromcsv()
        ... table2 = etl.fromcsv('example.csv')
        >>> table2
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' | '1' |
        +-----+-----+
        | 'b' | '2' |
        +-----+-----+
        | 'c' | '2' |
        +-----+-----+

    The `source` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.reader`. So, e.g., to override the
    delimiter from the default CSV dialect, provide the `delimiter` keyword
    argument.

    Note that all data values are strings, and any intended numeric values will
    need to be converted, see also :func:`petl.transform.conversions.convert`.

    """

    source = read_source_from_arg(source)
    csvargs.setdefault('dialect', 'excel')
    return fromcsv_impl(source=source, encoding=encoding, errors=errors, 
                        header=header, **csvargs)


def fromtsv(source=None, encoding=None, errors='strict', header=None, 
            **csvargs):
    """
    Convenience function, as :func:`petl.io.csv.fromcsv` but with different
    default dialect (tab delimited).

    """

    csvargs.setdefault('dialect', 'excel-tab')
    return fromcsv(source, encoding=encoding, errors=errors, header=header, **csvargs)


def tocsv(table, source=None, encoding=None, errors='strict', write_header=True,
          **csvargs):
    """
    Write the table to a CSV file. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> etl.tocsv(table1, 'example.csv')
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
    csvargs.setdefault('dialect', 'excel')
    tocsv_impl(table, source=source, encoding=encoding, errors=errors,
               write_header=write_header, **csvargs)


Table.tocsv = tocsv


def appendcsv(table, source=None, encoding=None, errors='strict',
              write_header=False, **csvargs):
    """
    Append data rows to an existing CSV file. As :func:`petl.io.csv.tocsv`
    but the file is opened in append mode and the table header is not written by
    default.

    Note that no attempt is made to check that the fields or row lengths are
    consistent with the existing data, the data rows from the table are simply
    appended to the file.

    """

    source = write_source_from_arg(source, mode='ab')
    csvargs.setdefault('dialect', 'excel')
    appendcsv_impl(table, source=source, encoding=encoding, errors=errors,
                   write_header=write_header, **csvargs)


Table.appendcsv = appendcsv


def totsv(table, source=None, encoding=None, errors='strict',
          write_header=True, **csvargs):
    """
    Convenience function, as :func:`petl.io.csv.tocsv` but with different
    default dialect (tab delimited).

    """

    csvargs.setdefault('dialect', 'excel-tab')
    return tocsv(table, source=source, encoding=encoding, errors=errors,
                 write_header=write_header, **csvargs)


Table.totsv = totsv


def appendtsv(table, source=None, encoding=None, errors='strict',
              write_header=False, **csvargs):
    """
    Convenience function, as :func:`petl.io.csv.appendcsv` but with different
    default dialect (tab delimited).

    """

    csvargs.setdefault('dialect', 'excel-tab')
    return appendcsv(table, source=source, encoding=encoding, errors=errors,
                     write_header=write_header, **csvargs)


Table.appendtsv = appendtsv


def teecsv(table, source=None, encoding=None, errors='strict', write_header=True,
           **csvargs):
    """
    Returns a table that writes rows to a CSV file as they are iterated over.

    """

    source = write_source_from_arg(source)
    csvargs.setdefault('dialect', 'excel')
    return teecsv_impl(table, source=source, encoding=encoding,
                       errors=errors, write_header=write_header,
                       **csvargs)


Table.teecsv = teecsv


def teetsv(table, source=None, encoding=None, errors='strict', write_header=True,
           **csvargs):
    """
    Convenience function, as :func:`petl.io.csv.teecsv` but with different
    default dialect (tab delimited).

    """

    csvargs.setdefault('dialect', 'excel-tab')
    return teecsv(table, source=source, encoding=encoding, errors=errors,
                  write_header=write_header, **csvargs)


Table.teetsv = teetsv
