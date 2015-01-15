# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# fromtextindex()
#################

import petl as etl
import os
# set up an index and load some documents via the Whoosh API
from whoosh.index import create_in
from whoosh.fields import *
schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT)
dirname = 'example.whoosh'
if not os.path.exists(dirname):
    os.mkdir(dirname)

index = create_in(dirname, schema)
writer = index.writer()
writer.add_document(title=u"First document", path=u"/a",
                    content=u"This is the first document we've added!")
writer.add_document(title=u"Second document", path=u"/b",
                    content=u"The second one is even more interesting!")
writer.commit()
# extract documents as a table
table = etl.fromtextindex(dirname)
table


# totextindex()
###############

import petl as etl
import datetime
import os
# here is the table we want to load into an index
table = (('f0', 'f1', 'f2', 'f3', 'f4'),
         ('AAA', 12, 4.3, True, datetime.datetime.now()),
         ('BBB', 6, 3.4, False, datetime.datetime(1900, 1, 31)),
         ('CCC', 42, 7.8, True, datetime.datetime(2100, 12, 25)))
# define a schema for the index
from whoosh.fields import *
schema = Schema(f0=TEXT(stored=True),
                f1=NUMERIC(int, stored=True),
                f2=NUMERIC(float, stored=True),
                f3=BOOLEAN(stored=True),
                f4=DATETIME(stored=True))
# load index
dirname = 'example.whoosh'
if not os.path.exists(dirname):
    os.mkdir(dirname)

etl.totextindex(table, dirname, schema=schema)


# searchtextindex()
###################

import petl as etl
import os
# set up an index and load some documents via the Whoosh API
from whoosh.index import create_in
from whoosh.fields import *
schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT)
dirname = 'example.whoosh'
if not os.path.exists(dirname):
    os.mkdir(dirname)

index = create_in('example.whoosh', schema)
writer = index.writer()
writer.add_document(title=u"Oranges", path=u"/a",
                    content=u"This is the first document we've added!")
writer.add_document(title=u"Apples", path=u"/b",
                    content=u"The second document is even more "
                            u"interesting!")
writer.commit()
# demonstrate the use of searchtextindex()
table1 = etl.searchtextindex('example.whoosh', 'oranges')
table1
table2 = etl.searchtextindex('example.whoosh', 'doc*')
table2
