# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


from itertools import chain
from tempfile import NamedTemporaryFile

import pytest

from petl.test.helpers import ieq
from petl.transform.sorts import sort
import petl as etl
from petl.io.pytables import fromhdf5, fromhdf5sorted, tohdf5, appendhdf5


try:
    # noinspection PyUnresolvedReferences
    import tables
except ImportError as e:
    pytest.skip('SKIP pytables tests: %s' % e, allow_module_level=True)
else:

    class FooBar(tables.IsDescription):
        foo = tables.Int32Col(pos=0)
        bar = tables.StringCol(6, pos=2)

    def test_fromhdf5():

        f = NamedTemporaryFile()

        # set up a new hdf5 table to work with
        h5file = tables.open_file(f.name, mode='w', title='Test file')
        h5file.create_group('/', 'testgroup', 'Test Group')
        h5table = h5file.create_table('/testgroup', 'testtable', FooBar,
                                      'Test Table')

        # load some data into the table
        table1 = (('foo', 'bar'),
                  (1, b'asdfgh'),
                  (2, b'qwerty'),
                  (3, b'zxcvbn'))
        for row in table1[1:]:
            for i, fld in enumerate(table1[0]):
                h5table.row[fld] = row[i]
            h5table.row.append()
        h5file.flush()
        h5file.close()

        # verify we can get the data back out
        table2a = fromhdf5(f.name, '/testgroup', 'testtable')
        ieq(table1, table2a)
        ieq(table1, table2a)

        # verify we can get the data back out
        table2b = fromhdf5(f.name, '/testgroup/testtable')
        ieq(table1, table2b)
        ieq(table1, table2b)

        # verify using an existing tables.File object
        h5file = tables.open_file(f.name)
        table3 = fromhdf5(h5file, '/testgroup/testtable')
        ieq(table1, table3)

        # verify using an existing tables.Table object
        h5tbl = h5file.get_node('/testgroup/testtable')
        table4 = fromhdf5(h5tbl)
        ieq(table1, table4)

        # verify using a condition to filter data
        table5 = fromhdf5(h5tbl, condition="(foo < 3)")
        ieq(table1[:3], table5)

        # clean up
        h5file.close()

    def test_fromhdf5sorted():

        f = NamedTemporaryFile()

        # set up a new hdf5 table to work with
        h5file = tables.open_file(f.name, mode='w', title='Test file')
        h5file.create_group('/', 'testgroup', 'Test Group')
        h5table = h5file.create_table('/testgroup', 'testtable', FooBar,
                                      'Test Table')

        # load some data into the table
        table1 = (('foo', 'bar'),
                  (3, b'asdfgh'),
                  (2, b'qwerty'),
                  (1, b'zxcvbn'))
        for row in table1[1:]:
            for i, f in enumerate(table1[0]):
                h5table.row[f] = row[i]
            h5table.row.append()
        h5table.cols.foo.create_csindex()
        h5file.flush()

        # verify we can get the data back out
        table2 = fromhdf5sorted(h5table, sortby='foo')
        ieq(sort(table1, 'foo'), table2)
        ieq(sort(table1, 'foo'), table2)

        # clean up
        h5file.close()

    def test_tohdf5():

        f = NamedTemporaryFile()

        # set up a new hdf5 table to work with
        h5file = tables.open_file(f.name, mode="w", title="Test file")
        h5file.create_group('/', 'testgroup', 'Test Group')
        h5file.create_table('/testgroup', 'testtable', FooBar, 'Test Table')
        h5file.flush()
        h5file.close()

        # load some data via tohdf5
        table1 = (('foo', 'bar'),
                  (1, b'asdfgh'),
                  (2, b'qwerty'),
                  (3, b'zxcvbn'))

        tohdf5(table1, f.name, '/testgroup', 'testtable')
        ieq(table1, fromhdf5(f.name, '/testgroup', 'testtable'))

        tohdf5(table1, f.name, '/testgroup/testtable')
        ieq(table1, fromhdf5(f.name, '/testgroup/testtable'))

        h5file = tables.open_file(f.name, mode="a")
        tohdf5(table1, h5file, '/testgroup/testtable')
        ieq(table1, fromhdf5(h5file, '/testgroup/testtable'))

        h5table = h5file.get_node('/testgroup/testtable')
        tohdf5(table1, h5table)
        ieq(table1, fromhdf5(h5table))

        # clean up
        h5file.close()

    def test_tohdf5_create():

        table1 = (('foo', 'bar'),
                  (1, b'asdfgh'),
                  (2, b'qwerty'),
                  (3, b'zxcvbn'))

        f = NamedTemporaryFile()

        # test creation with defined datatype
        tohdf5(table1, f.name, '/testgroup', 'testtable', create=True,
               drop=True, description=FooBar, createparents=True)
        ieq(table1, fromhdf5(f.name, '/testgroup', 'testtable'))

        # test dynamically determined datatype
        tohdf5(table1, f.name, '/testgroup', 'testtable2', create=True,
               drop=True, createparents=True)
        ieq(table1, fromhdf5(f.name, '/testgroup', 'testtable2'))

    def test_appendhdf5():

        f = NamedTemporaryFile()

        # set up a new hdf5 table to work with
        h5file = tables.open_file(f.name, mode="w", title="Test file")
        h5file.create_group('/', 'testgroup', 'Test Group')
        h5file.create_table('/testgroup', 'testtable', FooBar, 'Test Table')
        h5file.flush()
        h5file.close()

        # load some initial data via tohdf5()
        table1 = (('foo', 'bar'),
                  (1, b'asdfgh'),
                  (2, b'qwerty'),
                  (3, b'zxcvbn'))
        tohdf5(table1, f.name, '/testgroup', 'testtable')
        ieq(table1, fromhdf5(f.name, '/testgroup', 'testtable'))

        # append some more data
        appendhdf5(table1, f.name, '/testgroup', 'testtable')
        ieq(chain(table1, table1[1:]), fromhdf5(f.name, '/testgroup',
                                                'testtable'))

    def test_integration():

        f = NamedTemporaryFile()

        # set up a new hdf5 table to work with
        h5file = tables.open_file(f.name, mode="w", title="Test file")
        h5file.create_group('/', 'testgroup', 'Test Group')
        h5file.create_table('/testgroup', 'testtable', FooBar, 'Test Table')
        h5file.flush()
        h5file.close()

        # load some initial data via tohdf5()
        table1 = etl.wrap((('foo', 'bar'),
                           (1, b'asdfgh'),
                           (2, b'qwerty'),
                           (3, b'zxcvbn')))
        table1.tohdf5(f.name, '/testgroup', 'testtable')
        ieq(table1, etl.fromhdf5(f.name, '/testgroup', 'testtable'))

        # append some more data
        table1.appendhdf5(f.name, '/testgroup', 'testtable')
        ieq(chain(table1, table1[1:]), etl.fromhdf5(f.name, '/testgroup',
                                                    'testtable'))
