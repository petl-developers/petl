from __future__ import absolute_import, print_function, division, \
    unicode_literals


# standard library dependencies
from ..compat import PY2


# internal dependencies
from .sources import read_source_from_arg, write_source_from_arg
if PY2:
    from .csv_py2 import fromcsv_impl, fromucsv_impl, tocsv_impl, \
        toucsv_impl, appendcsv_impl, appenducsv_impl, teecsv_impl, teeucsv_impl
else:
    from .csv_py3 import fromcsv_impl, fromucsv_impl, tocsv_impl, \
        toucsv_impl, appendcsv_impl, appenducsv_impl, teecsv_impl, teeucsv_impl


def fromcsv(source=None, dialect='excel', **kwargs):
    """Extract a table from a delimited file. E.g.::

        >>> TODO

    The `filename` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.reader`. So, e.g., to override the
    delimiter from the default CSV dialect, provide the `delimiter` keyword
    argument.

    Note that all data values are strings, and any intended numeric values will
    need to be converted, see also :func:`convert`.

    Note that under Python 3 this function is equivalent to :func:`fromucsv`
    with ``encoding='ascii'``.

    """

    source = read_source_from_arg(source)
    return fromcsv_impl(source=source, dialect=dialect, **kwargs)


def fromtsv(source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`fromcsv` but with different default
    dialect (tab delimited).

    .. versionadded:: 0.9

    """

    return fromcsv(source, dialect=dialect, **kwargs)


def fromucsv(source=None, encoding='utf-8', dialect='excel', **kwargs):
    """Returns a table containing unicode data extracted from a delimited file
    via the given encoding. Like :func:`fromcsv` but accepts an additional
    ``encoding`` argument which should be one of the Python supported encodings.

    """
    source = read_source_from_arg(source)
    return fromucsv_impl(source, encoding=encoding, dialect=dialect, **kwargs)


def fromutsv(source=None, encoding='utf-8', dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`fromucsv` but with different default
    dialect (tab delimited).

    .. versionadded:: 0.19

    """

    return fromucsv(source=source, encoding=encoding, dialect=dialect, **kwargs)


def tocsv(table, source=None, dialect='excel', write_header=True, **kwargs):
    """Write the table to a CSV file. E.g.::

        >>> TODO

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


def appendcsv(table, source=None, dialect='excel', write_header=False,
              **kwargs):
    """Append data rows to an existing CSV file. E.g.::

        >>> TODO

    The `source` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.writer`. So, e.g., to override the
    delimiter from the default CSV dialect, provide the `delimiter` keyword
    argument.

    Note that no attempt is made to check that the fields or row lengths are
    consistent with the existing data, the data rows from the table are simply
    appended to the file. See also the :func:`cat` function.

    """

    source = write_source_from_arg(source)
    appendcsv_impl(table, source=source, dialect=dialect,
                   write_header=write_header, **kwargs)


def totsv(table, source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`tocsv` but with different default dialect
    (tab delimited).

    """

    return tocsv(table, source=source, dialect=dialect, **kwargs)


def appendtsv(table, source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`appendcsv` but with different default
    dialect (tab delimited).

    """

    return appendcsv(table, source=source, dialect=dialect, **kwargs)


def toucsv(table, source=None, dialect='excel', encoding='utf-8',
           write_header=True, **kwargs):
    """Write the table to a CSV file via the given encoding. Like :func:`tocsv`
    but accepts an additional ``encoding`` argument which should be one of the
    Python supported encodings.

    """

    source = write_source_from_arg(source)
    toucsv_impl(table, source=source, dialect=dialect, encoding=encoding,
                write_header=write_header, **kwargs)


def appenducsv(table, source=None, dialect='excel', encoding='utf-8',
               write_header=False, **kwargs):
    """Append the table to a CSV file via the given encoding. Like
    :func:`appendcsv` but accepts an additional ``encoding`` argument which
    should be one of the Python supported encodings.

    """

    source = write_source_from_arg(source)
    appenducsv_impl(table, source=source, dialect=dialect, encoding=encoding,
                    write_header=write_header, **kwargs)


def toutsv(table, source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`toucsv` but with different default
    dialect (tab delimited).

    """

    return toucsv(table, source=source, dialect=dialect, **kwargs)


def appendutsv(table, source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`appenducsv` but with different default
    dialect (tab delimited).

    """

    return appenducsv(table, source=source, dialect=dialect, **kwargs)


def teecsv(table, source=None, dialect='excel', **kwargs):
    """Return a table that writes rows to a CSV file as they are iterated over.

    """

    source = write_source_from_arg(source)
    return teecsv_impl(table, source=source, dialect=dialect, **kwargs)


def teetsv(table, source=None, dialect='excel-tab', **kwargs):
    """Convenience function, as :func:`teecsv` but with different default
    dialect (tab delimited).

    """

    return teecsv(table, source=source, dialect=dialect, **kwargs)


def teeucsv(table, source=None, dialect='excel', encoding='utf-8',
            **kwargs):
    """Return a table that writes rows to a Unicode CSV file as they are iterated
    over.

    """

    source = write_source_from_arg(source)
    return teeucsv_impl(table, source=source, encoding=encoding,
                        dialect=dialect, **kwargs)


def teeutsv(table, source=None, dialect='excel-tab',
            encoding='utf-8', **kwargs):
    """Convenience function, as :func:`teeucsv` but with different default
    dialect (tab delimited).

    """

    return teeucsv(table, source=source, dialect=dialect, encoding=encoding,
                   **kwargs)


# TODO support write_header argument in tees