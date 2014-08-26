from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


# standard library dependencies
import json
from json.encoder import JSONEncoder


# internal dependencies
from petl.util import data, RowContainer
from petl.util import dicts as asdicts
from petl.io.sources import read_source_from_arg, write_source_from_arg


def fromjson(source, *args, **kwargs):
    """
    Extract data from a JSON file. The file must contain a JSON array as the top
    level object, and each member of the array will be treated as a row of data.
    E.g.::

        >>> from petl import fromjson, look
        >>> data = '[{"foo": "a", "bar": 1}, {"foo": "b", "bar": 2}, {"foo": "c", "bar": 2}]'
        >>> with open('example1.json', 'w') as f:
        ...     f.write(data)
        ...
        >>> table1 = fromjson('example1.json')
        >>> look(table1)
        +--------+--------+
        | u'foo' | u'bar' |
        +========+========+
        | u'a'   | 1      |
        +--------+--------+
        | u'b'   | 2      |
        +--------+--------+
        | u'c'   | 2      |
        +--------+--------+

    If your JSON file does not fit this structure, you will need to parse it
    via :func:`json.load` and select the array to treat as the data, see also
    :func:`fromdicts`.

    Supports transparent reading from URLs, ``.gz`` and ``.bz2`` files.

    .. versionadded:: 0.5

    """

    source = read_source_from_arg(source)
    return JsonView(source, *args, **kwargs)


class JsonView(RowContainer):

    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.args = args
        self.kwargs = kwargs
        self.missing = kwargs.pop('missing', None)
        self.header = kwargs.pop('header', None)

    def __iter__(self):
        with self.source.open_('rb') as f:
            result = json.load(f, *self.args, **self.kwargs)
            if self.header is None:
                # determine fields
                header = list()
                for o in result:
                    if hasattr(o, 'keys'):
                        header.extend(k for k in o.keys() if k not in header)
            else:
                header = self.header
            yield tuple(header)
            # output data rows
            for o in result:
                row = tuple(o[f] if f in o else None for f in header)
                yield row


def fromdicts(dicts, header=None):
    """
    View a sequence of Python :class:`dict` as a table. E.g.::

        >>> from petl import fromdicts, look
        >>> dicts = [{"foo": "a", "bar": 1}, {"foo": "b", "bar": 2}, {"foo": "c", "bar": 2}]
        >>> table = fromdicts(dicts)
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

    See also :func:`fromjson`.

    .. versionadded:: 0.5

    """

    return DictsView(dicts, header=header)


class DictsView(RowContainer):

    def __init__(self, dicts, header=None):
        self.dicts = dicts
        self.header = header

    def __iter__(self):
        result = self.dicts
        if self.header is None:
            # determine fields
            header = list()
            for o in result:
                if hasattr(o, 'keys'):
                    header.extend(k for k in o.keys() if k not in header)
        else:
            header = self.header
        yield tuple(header)
        # output data rows
        for o in result:
            row = tuple(o[f] if f in o else None for f in header)
            yield row


def tojson(table, source=None, prefix=None, suffix=None, *args, **kwargs):
    """
    Write a table in JSON format, with rows output as JSON objects. E.g.::

        >>> from petl import tojson, look
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

        >>> tojson(table, 'example.json')
        >>> # check what it did
        ... with open('example.json') as f:
        ...     print f.read()
        ...
        [{"foo": "a", "bar": 1}, {"foo": "b", "bar": 2}, {"foo": "c", "bar": 2}]

    Note that this is currently not streaming, all data is loaded into memory
    before being written to the file.

    Supports transparent writing to ``.gz`` and ``.bz2`` files.

    .. versionadded:: 0.5

    """

    encoder = JSONEncoder(*args, **kwargs)
    source = write_source_from_arg(source)
    with source.open_('wb') as f:
        if prefix is not None:
            f.write(prefix)
        for chunk in encoder.iterencode(list(asdicts(table))):
            f.write(chunk)
        if suffix is not None:
            f.write(suffix)


def tojsonarrays(table, source=None, prefix=None, suffix=None,
                 output_header=False, *args, **kwargs):
    """
    Write a table in JSON format, with rows output as JSON arrays. E.g.::

        >>> from petl import tojsonarrays, look
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

        >>> tojsonarrays(table, 'example.json')
        >>> # check what it did
        ... with open('example.json') as f:
        ...     print f.read()
        ...
        [["a", 1], ["b", 2], ["c", 2]]

    Note that this is currently not streaming, all data is loaded into memory
    before being written to the file.

    Supports transparent writing to ``.gz`` and ``.bz2`` files.

    .. versionadded:: 0.11

    """

    encoder = JSONEncoder(*args, **kwargs)
    source = write_source_from_arg(source)
    if output_header:
        obj = list(table)
    else:
        obj = list(data(table))
    with source.open_('wb') as f:
        if prefix is not None:
            f.write(prefix)
        for chunk in encoder.iterencode(obj):
            f.write(chunk)
        if suffix is not None:
            f.write(suffix)
