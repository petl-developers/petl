# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from contextlib import contextmanager
from petl.compat import string_types


from petl.errors import ArgumentError
from petl.util.base import Table, iterpeek, data
from petl.io.numpy import infer_dtype


def fromhdf5(source, where=None, name=None, condition=None,
             condvars=None, start=None, stop=None, step=None):
    """
    Provides access to an HDF5 table. E.g.::

        >>> import petl as etl
        >>>
        >>> # set up a new hdf5 table to demonstrate with
        >>> class FooBar(tables.IsDescription): # doctest: +SKIP
        ...     foo = tables.Int32Col(pos=0) # doctest: +SKIP
        ...     bar = tables.StringCol(6, pos=2) # doctest: +SKIP
        >>> #
        >>> def setup_hdf5_table():
        ...     import tables
        ...     h5file = tables.open_file('example.h5', mode='w',
        ...                               title='Example file')
        ...     h5file.create_group('/', 'testgroup', 'Test Group')
        ...     h5table = h5file.create_table('/testgroup', 'testtable', FooBar,
        ...                                   'Test Table')
        ...     # load some data into the table
        ...     table1 = (('foo', 'bar'),
        ...               (1, b'asdfgh'),
        ...               (2, b'qwerty'),
        ...               (3, b'zxcvbn'))
        ...     for row in table1[1:]:
        ...         for i, f in enumerate(table1[0]):
        ...             h5table.row[f] = row[i]
        ...         h5table.row.append()
        ...     h5file.flush()
        ...     h5file.close()
        >>>
        >>> setup_hdf5_table() # doctest: +SKIP
        >>>
        >>> # now demonstrate use of fromhdf5
        >>> table1 = etl.fromhdf5('example.h5', '/testgroup', 'testtable') # doctest: +SKIP
        >>> table1 # doctest: +SKIP
        +-----+-----------+
        | foo | bar       |
        +=====+===========+
        |   1 | b'asdfgh' |
        +-----+-----------+
        |   2 | b'qwerty' |
        +-----+-----------+
        |   3 | b'zxcvbn' |
        +-----+-----------+

        >>> # alternatively just specify path to table node
        ... table1 = etl.fromhdf5('example.h5', '/testgroup/testtable') # doctest: +SKIP
        >>> # ...or use an existing tables.File object
        ... h5file = tables.open_file('example.h5') # doctest: +SKIP
        >>> table1 = etl.fromhdf5(h5file, '/testgroup/testtable') # doctest: +SKIP
        >>> # ...or use an existing tables.Table object
        ... h5tbl = h5file.get_node('/testgroup/testtable') # doctest: +SKIP
        >>> table1 = etl.fromhdf5(h5tbl) # doctest: +SKIP
        >>> # use a condition to filter data
        ... table2 = etl.fromhdf5(h5tbl, condition='foo < 3') # doctest: +SKIP
        >>> table2 # doctest: +SKIP
        +-----+-----------+
        | foo | bar       |
        +=====+===========+
        |   1 | b'asdfgh' |
        +-----+-----------+
        |   2 | b'qwerty' |
        +-----+-----------+

        >>> h5file.close() # doctest: +SKIP

    """

    return HDF5View(source, where=where, name=name,
                    condition=condition, condvars=condvars,
                    start=start, stop=stop, step=step)


class HDF5View(Table):

    def __init__(self, source, where=None, name=None, condition=None,
                 condvars=None, start=None, stop=None, step=None):
        self.source = source
        self.where = where
        self.name = name
        self.condition = condition
        self.condvars = condvars
        self.start = start
        self.stop = stop
        self.step = step

    def __iter__(self):
        return iterhdf5(self.source, self.where, self.name, self.condition,
                        self.condvars, self.start, self.stop, self.step)


@contextmanager
def _get_hdf5_table(source, where, name, mode='r'):
    import tables

    needs_closing = False
    h5file = None

    # allow for polymorphic args
    if isinstance(source, tables.Table):

        # source is a table
        h5tbl = source

    elif isinstance(source, string_types):

        # assume source is the name of an HDF5 file, try to open it
        h5file = tables.open_file(source, mode=mode)
        needs_closing = True
        h5tbl = h5file.get_node(where, name=name)

    elif isinstance(source, tables.File):

        # source is an HDF5 file object
        h5file = source
        h5tbl = h5file.get_node(where, name=name)

    else:

        # invalid source
        raise ArgumentError('invalid source argument, expected file name or '
                            'tables.File or tables.Table object, found: %r'
                            % source)

    try:
        yield h5tbl
    finally:
        # tidy up
        if needs_closing:
            h5file.close()


@contextmanager
def _get_hdf5_file(source, mode='r'):
    import tables

    needs_closing = False

    # allow for polymorphic args
    if isinstance(source, string_types):

        # assume source is the name of an HDF5 file, try to open it
        h5file = tables.open_file(source, mode=mode)
        needs_closing = True

    elif isinstance(source, tables.File):

        # source is an HDF5 file object
        h5file = source

    else:

        # invalid source
        raise ArgumentError('invalid source argument, expected file name or '
                            'tables.File object, found: %r' % source)

    try:
        yield h5file
    finally:
        if needs_closing:
            h5file.close()


def iterhdf5(source, where, name, condition, condvars, start, stop, step):

    with _get_hdf5_table(source, where, name) as h5tbl:

        # header row
        hdr = tuple(h5tbl.colnames)
        yield hdr

        # determine how to iterate over the table
        if condition is not None:
            it = h5tbl.where(condition, condvars=condvars,
                             start=start, stop=stop, step=step)

        else:
            it = h5tbl.iterrows(start=start, stop=stop, step=step)

        # data rows
        for row in it:
            yield row[:]  # access row as a tuple


def fromhdf5sorted(source, where=None, name=None, sortby=None, checkCSI=False,
                   start=None, stop=None, step=None):
    """
    Provides access to an HDF5 table, sorted by an indexed column, e.g.::

        >>> import petl as etl
        >>>
        >>> # set up a new hdf5 table to demonstrate with
        >>> class FooBar(tables.IsDescription): # doctest: +SKIP
        ...     foo = tables.Int32Col(pos=0) # doctest: +SKIP
        ...     bar = tables.StringCol(6, pos=2) # doctest: +SKIP
        >>>
        >>> def setup_hdf5_index():
        ...     import tables
        ...     h5file = tables.open_file('example.h5', mode='w',
        ...                               title='Example file')
        ...     h5file.create_group('/', 'testgroup', 'Test Group')
        ...     h5table = h5file.create_table('/testgroup', 'testtable', FooBar,
        ...                                   'Test Table')
        ...     # load some data into the table
        ...     table1 = (('foo', 'bar'),
        ...               (1, b'asdfgh'),
        ...               (2, b'qwerty'),
        ...               (3, b'zxcvbn'))
        ...     for row in table1[1:]:
        ...         for i, f in enumerate(table1[0]):
        ...             h5table.row[f] = row[i]
        ...         h5table.row.append()
        ...     h5table.cols.foo.create_csindex()  # CS index is required
        ...     h5file.flush()
        ...     h5file.close()
        >>>
        >>> setup_hdf5_index() # doctest: +SKIP
        >>>
        ... # access the data, sorted by the indexed column
        ... table2 = etl.fromhdf5sorted('example.h5', '/testgroup', 'testtable', sortby='foo') # doctest: +SKIP
        >>> table2 # doctest: +SKIP
        +-----+-----------+
        | foo | bar       |
        +=====+===========+
        |   1 | b'zxcvbn' |
        +-----+-----------+
        |   2 | b'qwerty' |
        +-----+-----------+
        |   3 | b'asdfgh' |
        +-----+-----------+

    """

    assert sortby is not None, 'no column specified to sort by'
    return HDF5SortedView(source, where=where, name=name,
                          sortby=sortby, checkCSI=checkCSI,
                          start=start, stop=stop, step=step)


class HDF5SortedView(Table):

    def __init__(self, source, where=None, name=None, sortby=None,
                 checkCSI=False, start=None, stop=None, step=None):
        self.source = source
        self.where = where
        self.name = name
        self.sortby = sortby
        self.checkCSI = checkCSI
        self.start = start
        self.stop = stop
        self.step = step

    def __iter__(self):
        return iterhdf5sorted(self.source, self.where, self.name, self.sortby,
                              self.checkCSI, self.start, self.stop, self.step)


def iterhdf5sorted(source, where, name, sortby, checkCSI, start, stop, step):

    with _get_hdf5_table(source, where, name) as h5tbl:

        # header row
        hdr = tuple(h5tbl.colnames)
        yield hdr

        it = h5tbl.itersorted(sortby,
                              checkCSI=checkCSI,
                              start=start,
                              stop=stop,
                              step=step)
        for row in it:
            yield row[:]  # access row as a tuple


def tohdf5(table, source, where=None, name=None, create=False, drop=False,
           description=None, title='', filters=None, expectedrows=10000,
           chunkshape=None, byteorder=None, createparents=False,
           sample=1000):
    """
    Write to an HDF5 table. If `create` is `False`, assumes the table
    already exists, and attempts to truncate it before loading. If `create`
    is `True`, a new table will be created, and if `drop` is True,
    any existing table will be dropped first. If `description` is `None`,
    the description will be guessed. E.g.::

        >>> import petl as etl
        >>> table1 = (('foo', 'bar'),
        ...           (1, b'asdfgh'),
        ...           (2, b'qwerty'),
        ...           (3, b'zxcvbn'))
        >>> etl.tohdf5(table1, 'example.h5', '/testgroup', 'testtable',
        ...            drop=True, create=True, createparents=True) # doctest: +SKIP
        >>> etl.fromhdf5('example.h5', '/testgroup', 'testtable') # doctest: +SKIP
        +-----+-----------+
        | foo | bar       |
        +=====+===========+
        |   1 | b'asdfgh' |
        +-----+-----------+
        |   2 | b'qwerty' |
        +-----+-----------+
        |   3 | b'zxcvbn' |
        +-----+-----------+

    """

    import tables
    it = iter(table)

    if create:
        with _get_hdf5_file(source, mode='a') as h5file:

            if drop:
                try:
                    h5file.get_node(where, name)
                except tables.NoSuchNodeError:
                    pass
                else:
                    h5file.remove_node(where, name)

            # determine datatype
            if description is None:
                peek, it = iterpeek(it, sample)
                # use a numpy dtype
                description = infer_dtype(peek)

            # create the table
            h5file.create_table(where, name, description,
                                title=title,
                                filters=filters,
                                expectedrows=expectedrows,
                                chunkshape=chunkshape,
                                byteorder=byteorder,
                                createparents=createparents)

    with _get_hdf5_table(source, where, name, mode='a') as h5table:

        # truncate the existing table
        h5table.truncate(0)

        # load the data
        _insert(it, h5table)


Table.tohdf5 = tohdf5


def appendhdf5(table, source, where=None, name=None):
    """
    As :func:`petl.io.hdf5.tohdf5` but don't truncate the target table before
    loading.

    """

    with _get_hdf5_table(source, where, name, mode='a') as h5table:

        # load the data
        _insert(table, h5table)


Table.appendhdf5 = appendhdf5


def _insert(table, h5table):
    it = data(table)  # don't need header
    for row in it:
        for i, f in enumerate(h5table.colnames):
            # depends on order of fields being the same in input table
            # and hd5 table, but field names don't need to match
            h5table.row[f] = row[i]
        h5table.row.append()
    h5table.flush()
