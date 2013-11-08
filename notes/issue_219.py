# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

# see http://pynash.org/2013/03/06/timing-and-profiling.html for setup of profiling magics

# <codecell>

import petl; print petl.VERSION
from petl.fluent import etl
import psycopg2
from collections import OrderedDict

# <codecell>

tbl_dummy_data = etl().dummytable(1000000)
tbl_dummy_data.look()

# <codecell>

%memit -r 1 print tbl_dummy_data.nrows()

# <codecell>

connection = psycopg2.connect(dbname='petl', user='petl', password='petl', host='localhost')

# <codecell>

%memit -r 1 tbl_dummy_data.progress(100000).todb(connection, 'issue_219')

# <codecell>

%memit print etl.fromdb(connection, 'select * from issue_219').look()

# <codecell>

# use server-side cursor to avoid loading whole table into memory
cursor = connection.cursor(name='cursor1')

# <codecell>

cursor.execute('select * from issue_219')

# <codecell>

print cursor.description

# <codecell>

cursor.fetchone()

# <codecell>

print cursor.description

# <codecell>

%memit print etl.fromdb(cursor, 'select * from issue_219').look()

# <codecell>

from collections import OrderedDict
mappings = OrderedDict([('A', lambda r: r['foo'] * 2),
                        ('B', lambda r: r['bar'][:5]),
                        ('C', lambda r: r['foo'] * r['baz'])])
tbl_transformed = (etl
    .fromdb(connection, 'select * from issue_219')
    .fieldmap(mappings)
)
tbl_transformed.look()

# <codecell>

%memit -r 1 tbl_transformed.progress(100000).todb(connection, 'issue_219_transformed')

# <codecell>

etl.fromdb(connection, 'select * from issue_219_transformed').look()

