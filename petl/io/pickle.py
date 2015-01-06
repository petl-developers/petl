from __future__ import absolute_import, print_function, division, \
    unicode_literals


# standard library dependencies
from petl.compat import pickle, next


# internal dependencies
from petl.util import RowContainer
from petl.io.sources import read_source_from_arg, write_source_from_arg


def frompickle(source=None):
    """Extract a table From data pickled in the given file. The rows in the
    table should have been pickled to the file one at a time. E.g.::

        >>> # set up a file to demonstrate with
        ... import pickle
        >>> with open('example.p', 'wb') as f:
        ...     pickle.dump(['foo', 'bar'], f)
        ...     pickle.dump(['a', 1], f)
        ...     pickle.dump(['b', 2], f)
        ...     pickle.dump(['c', 2.5], f)
        ...
        >>> # now demonstrate the use of petl.frompickle
        ... from petl import frompickle, look
        >>> table1 = frompickle('example.p')
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   |     1 |
        +-------+-------+
        | 'b'   |     2 |
        +-------+-------+
        | 'c'   |   2.5 |
        +-------+-------+

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


def topickle(table, source=None, protocol=-1, write_header=True):
    """Write the table to a pickle file. E.g.::

        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> from petl import topickle, look
        >>> topickle(table1, 'example.p')
        >>> # look what it did
        ... from petl import frompickle
        >>> table2 = frompickle('example.p')
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   |     1 |
        +-------+-------+
        | 'b'   |     2 |
        +-------+-------+
        | 'c'   |     2 |
        +-------+-------+

    Note that if a file already exists at the given location, it will be
    overwritten.

    The pickle file format preserves type information, i.e., reading and writing
    is round-trippable.

    """

    _writepickle(table, source=source, mode='wb', protocol=protocol,
                 write_header=write_header)


def appendpickle(table, source=None, protocol=-1, write_header=False):
    """Append data to an existing pickle file. I.e., as :func:`topickle`
    but the file is opened in append mode.

    Note that no attempt is made to check that the fields or row lengths are
    consistent with the existing data, the data rows from the table are simply
    appended to the file.

    """

    _writepickle(table, source=source, mode='ab', protocol=protocol,
                 write_header=write_header)


def _writepickle(table, source, mode, protocol, write_header):
    source = write_source_from_arg(source)
    with source.open_(mode) as f:
        it = iter(table)
        hdr = next(it)
        if write_header:
            pickle.dump(hdr, f, protocol)
        for row in it:
            pickle.dump(row, f, protocol)


def teepickle(table, source=None, protocol=-1, write_header=True):
    """Return a table that writes rows to a pickle file as they are iterated
    over.

    """

    return TeePickleContainer(table, source=source, protocol=protocol,
                              write_header=write_header)


class TeePickleContainer(RowContainer):

    def __init__(self, table, source=None, protocol=-1, write_header=True):
        self.table = table
        self.source = source
        self.protocol = protocol
        self.write_header = write_header

    def __iter__(self):
        protocol = self.protocol
        source = write_source_from_arg(self.source)
        with source.open_('wb') as f:
            it = iter(self.table)
            hdr = next(it)
            if self.write_header:
                pickle.dump(hdr, f, protocol)
            yield hdr
            for row in it:
                pickle.dump(row, f, protocol)
                yield row
