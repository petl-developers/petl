from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


# standard library dependencies
import cPickle as pickle


# internal dependencies
from petl.util import RowContainer, data
from petl.io.sources import read_source_from_arg, write_source_from_arg


def frompickle(source=None):
    """
    Returns a table providing access to the data pickled in the given file. The
    rows in the table should have been pickled to the file one at a time. E.g.::

        >>> import pickle
        >>> # set up a file to demonstrate with
        ... with open('test.dat', 'wb') as f:
        ...     pickle.dump(['foo', 'bar'], f)
        ...     pickle.dump(['a', 1], f)
        ...     pickle.dump(['b', 2], f)
        ...     pickle.dump(['c', 2.5], f)
        ...
        >>> # now demonstrate the use of petl.frompickle
        ... from petl import frompickle, look
        >>> testdat = frompickle('test.dat')
        >>> look(testdat)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2.5   |
        +-------+-------+

    Supports transparent reading from URLs, ``.gz`` and ``.bz2`` files.

    """

    source = read_source_from_arg(source)
    return PickleView(source)


class PickleView(RowContainer):

    def __init__(self, source):
        self.source = source

    def __iter__(self):
        with self.source.open_('rb') as f:
            try:
                while True:
                    yield tuple(pickle.load(f))
            except EOFError:
                pass


def topickle(table, source=None, protocol=-1):
    """
    Write the table to a pickle file. E.g.::

        >>> from petl import topickle, look
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

        >>> topickle(table, 'test.dat')
        >>> # look what it did
        ... from petl import frompickle
        >>> look(frompickle('test.dat'))
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+

    Note that if a file already exists at the given location, it will be
    overwritten.

    The pickle file format preserves type information, i.e., reading and writing
    is round-trippable.

    Supports transparent writing to ``.gz`` and ``.bz2`` files.

    """

    source = write_source_from_arg(source)
    with source.open_('wb') as f:
        for row in table:
            pickle.dump(row, f, protocol)


def appendpickle(table, source=None, protocol=-1):
    """
    Append data to an existing pickle file. E.g.::

        >>> from petl import look, frompickle
        >>> # inspect an existing pickle file
        ... testdat = frompickle('test.dat')
        >>> look(testdat)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+

        >>> # append some data
        ... from petl import appendpickle
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'd'   | 7     |
        +-------+-------+
        | 'e'   | 42    |
        +-------+-------+
        | 'f'   | 12    |
        +-------+-------+

        >>> appendpickle(table, 'test.dat')
        >>> # look what it did
        ... look(testdat)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        | 'd'   | 7     |
        +-------+-------+
        | 'e'   | 42    |
        +-------+-------+
        | 'f'   | 12    |
        +-------+-------+

    Note that no attempt is made to check that the fields or row lengths are
    consistent with the existing data, the data rows from the table are simply
    appended to the file. See also the :func:`cat` function.

    Supports transparent writing to ``.gz`` and ``.bz2`` files.

    """

    source = write_source_from_arg(source)
    with source.open_('ab') as f:
        for row in data(table):
            pickle.dump(row, f, protocol)


def teepickle(table, source=None, protocol=-1):
    """
    Return a table that writes rows to a pickle file as they are iterated over.

    .. versionadded:: 0.25

    """

    return TeePickleContainer(table, source=source, protocol=protocol)


class TeePickleContainer(RowContainer):

    def __init__(self, table, source=None, protocol=-1):
        self.table = table
        self.source = source
        self.protocol = protocol

    def __iter__(self):
        protocol = self.protocol
        source = write_source_from_arg(self.source)
        with source.open_('wb') as f:
            for row in self.table:
                pickle.dump(row, f, protocol)
                yield row
