# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
from petl.compat import pickle, next


# internal dependencies
from petl.util.base import Table
from petl.io.sources import read_source_from_arg, write_source_from_arg


def frompickle(source=None):
    """
    Extract a table From data pickled in the given file. The rows in the
    table should have been pickled to the file one at a time. E.g.::

        >>> import petl as etl
        >>> import pickle
        >>> # set up a file to demonstrate with
        ... with open('example.p', 'wb') as f:
        ...     pickle.dump(['foo', 'bar'], f)
        ...     pickle.dump(['a', 1], f)
        ...     pickle.dump(['b', 2], f)
        ...     pickle.dump(['c', 2.5], f)
        ...
        >>> # now demonstrate the use of frompickle()
        ... table1 = etl.frompickle('example.p')
        >>> table1
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' | 2.5 |
        +-----+-----+


    """

    source = read_source_from_arg(source)
    return PickleView(source)


class PickleView(Table):

    def __init__(self, source):
        self.source = source

    def __iter__(self):
        with self.source.open('rb') as f:
            try:
                while True:
                    yield tuple(pickle.load(f))
            except EOFError:
                pass


def topickle(table, source=None, protocol=-1, write_header=True):
    """
    Write the table to a pickle file. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> etl.topickle(table1, 'example.p')
        >>> # look what it did
        ... table2 = etl.frompickle('example.p')
        >>> table2
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' |   2 |
        +-----+-----+

    Note that if a file already exists at the given location, it will be
    overwritten.

    The pickle file format preserves type information, i.e., reading and writing
    is round-trippable for tables with non-string data values.

    """

    _writepickle(table, source=source, mode='wb', protocol=protocol,
                 write_header=write_header)


Table.topickle = topickle


def appendpickle(table, source=None, protocol=-1, write_header=False):
    """
    Append data to an existing pickle file. I.e.,
    as :func:`petl.io.pickle.topickle` but the file is opened in append mode.

    Note that no attempt is made to check that the fields or row lengths are
    consistent with the existing data, the data rows from the table are simply
    appended to the file.

    """

    _writepickle(table, source=source, mode='ab', protocol=protocol,
                 write_header=write_header)


Table.appendpickle = appendpickle


def _writepickle(table, source, mode, protocol, write_header):
    source = write_source_from_arg(source, mode)
    with source.open(mode) as f:
        it = iter(table)
        try:
            hdr = next(it)
        except StopIteration:
            return
        if write_header:
            pickle.dump(hdr, f, protocol)
        for row in it:
            pickle.dump(row, f, protocol)


def teepickle(table, source=None, protocol=-1, write_header=True):
    """
    Return a table that writes rows to a pickle file as they are iterated
    over.

    """

    return TeePickleView(table, source=source, protocol=protocol,
                         write_header=write_header)


Table.teepickle = teepickle


class TeePickleView(Table):

    def __init__(self, table, source=None, protocol=-1, write_header=True):
        self.table = table
        self.source = source
        self.protocol = protocol
        self.write_header = write_header

    def __iter__(self):
        protocol = self.protocol
        source = write_source_from_arg(self.source)
        with source.open('wb') as f:
            it = iter(self.table)
            try:
                hdr = next(it)
            except StopIteration:
                return
            if self.write_header:
                pickle.dump(hdr, f, protocol)
            yield tuple(hdr)
            for row in it:
                pickle.dump(row, f, protocol)
                yield tuple(row)
