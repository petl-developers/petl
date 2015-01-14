# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# fromhdf5()
############

import petl as etl
import tables
# set up a new hdf5 table to demonstrate with
h5file = tables.openFile('example.h5', mode='w', title='Example file')
h5file.createGroup('/', 'testgroup', 'Test Group')
class FooBar(tables.IsDescription):
    foo = tables.Int32Col(pos=0)
    bar = tables.StringCol(6, pos=2)

h5table = h5file.createTable('/testgroup', 'testtable', FooBar, 'Test Table')
# load some data into the table
table1 = (('foo', 'bar'),
          (1, b'asdfgh'),
          (2, b'qwerty'),
          (3, b'zxcvbn'))

for row in table1[1:]:
    for i, f in enumerate(table1[0]):
        h5table.row[f] = row[i]
    h5table.row.append()

h5file.flush()
h5file.close()
#
# now demonstrate use of fromhdf5
table1 = etl.fromhdf5('example.h5', '/testgroup', 'testtable')
table1
# alternatively just specify path to table node
table1 = etl.fromhdf5('example.h5', '/testgroup/testtable')
# ...or use an existing tables.File object
h5file = tables.openFile('example.h5')
table1 = etl.fromhdf5(h5file, '/testgroup/testtable')
# ...or use an existing tables.Table object
h5tbl = h5file.getNode('/testgroup/testtable')
table1 = etl.fromhdf5(h5tbl)
# use a condition to filter data
table2 = etl.fromhdf5(h5tbl, condition='foo < 3')
table2
h5file.close()


# fromhdf5sorted()
##################

import petl as etl
import tables
# set up a new hdf5 table to demonstrate with
h5file = tables.openFile('example.h5', mode='w', title='Test file')
h5file.createGroup('/', 'testgroup', 'Test Group')
class FooBar(tables.IsDescription):
    foo = tables.Int32Col(pos=0)
    bar = tables.StringCol(6, pos=2)

h5table = h5file.createTable('/testgroup', 'testtable', FooBar, 'Test Table')
# load some data into the table
table1 = (('foo', 'bar'),
          (3, b'asdfgh'),
          (2, b'qwerty'),
          (1, b'zxcvbn'))
for row in table1[1:]:
    for i, f in enumerate(table1[0]):
        h5table.row[f] = row[i]
    h5table.row.append()

h5table.cols.foo.createCSIndex()  # CS index is required
h5file.flush()
h5file.close()
#
# access the data, sorted by the indexed column
table2 = etl.fromhdf5sorted('example.h5', '/testgroup', 'testtable',
                            sortby='foo')
table2


# tohdf5()
##########

import petl as etl
table1 = (('foo', 'bar'),
          (1, b'asdfgh'),
          (2, b'qwerty'),
          (3, b'zxcvbn'))
etl.tohdf5(table1, 'example.h5', '/testgroup', 'testtable',
           drop=True, create=True, createparents=True)
etl.fromhdf5('example.h5', '/testgroup', 'testtable')
