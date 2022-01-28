# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import os
import tempfile

import pytest

from petl.test.helpers import ieq
import petl as etl
from petl.io.whoosh import fromtextindex, totextindex, appendtextindex, \
    searchtextindex


try:
    # noinspection PyUnresolvedReferences
    import whoosh
except ImportError as e:
    pytest.skip('SKIP whoosh tests: %s' % e, allow_module_level=True)
else:

    from whoosh.index import create_in
    from whoosh.fields import *
    import datetime

    def test_fromindex_dirname():

        dirname = tempfile.mkdtemp()

        schema = Schema(title=TEXT(stored=True), path=ID(stored=True),
                        content=TEXT)

        ix = create_in(dirname, schema)
        writer = ix.writer()
        writer.add_document(title=u"First document", path=u"/a",
                            content=u"This is the first document we've added!")
        writer.add_document(title=u"Second document", path=u"/b",
                            content=u"The second one is even more interesting!")
        writer.commit()

        # N.B., fields get sorted
        expect = ((u'path', u'title'),
                  (u'/a', u'First document'),
                  (u'/b', u'Second document'))
        actual = fromtextindex(dirname)
        ieq(expect, actual)

    def test_fromindex_index():

        dirname = tempfile.mkdtemp()

        schema = Schema(title=TEXT(stored=True), path=ID(stored=True),
                        content=TEXT)

        ix = create_in(dirname, schema)
        writer = ix.writer()
        writer.add_document(title=u"First document", path=u"/a",
                            content=u"This is the first document we've added!")
        writer.add_document(title=u"Second document", path=u"/b",
                            content=u"The second one is even more interesting!")
        writer.commit()

        # N.B., fields get sorted
        expect = ((u'path', u'title'),
                  (u'/a', u'First document'),
                  (u'/b', u'Second document'))
        actual = fromtextindex(ix)
        ieq(expect, actual)

    def test_fromindex_docnum_field():

        dirname = tempfile.mkdtemp()
        schema = Schema(title=TEXT(stored=True), path=ID(stored=True),
                        content=TEXT)

        ix = create_in(dirname, schema)
        writer = ix.writer()
        writer.add_document(title=u"First document", path=u"/a",
                            content=u"This is the first document we've added!")
        writer.add_document(title=u"Second document", path=u"/b",
                            content=u"The second one is even more interesting!")
        writer.commit()

        # N.B., fields get sorted
        expect = ((u'docnum', u'path', u'title'),
                  (0, u'/a', u'First document'),
                  (1, u'/b', u'Second document'))
        actual = fromtextindex(dirname, docnum_field='docnum')
        ieq(expect, actual)

    def test_toindex_dirname():

        dirname = tempfile.mkdtemp()

        # name fields in ascending order as whoosh sorts fields on the way out
        tbl = (('f0', 'f1', 'f2', 'f3', 'f4'),
               (u'AAA', 12, 4.3, True, datetime.datetime.now()),
               (u'BBB', 6, 3.4, False, datetime.datetime(1900, 1, 31)),
               (u'CCC', 42, 7.8, True, datetime.datetime(2100, 12, 25)))

        schema = Schema(f0=TEXT(stored=True),
                        f1=NUMERIC(int, stored=True),
                        f2=NUMERIC(float, stored=True),
                        f3=BOOLEAN(stored=True),
                        f4=DATETIME(stored=True))

        totextindex(tbl, dirname, schema=schema)

        actual = fromtextindex(dirname)
        ieq(tbl, actual)

    def test_toindex_index():

        dirname = tempfile.mkdtemp()

        # name fields in ascending order as whoosh sorts fields on the way out
        tbl = (('f0', 'f1', 'f2', 'f3', 'f4'),
               (u'AAA', 12, 4.3, True, datetime.datetime.now()),
               (u'BBB', 6, 3.4, False, datetime.datetime(1900, 1, 31)),
               (u'CCC', 42, 7.8, True, datetime.datetime(2100, 12, 25)))

        schema = Schema(f0=TEXT(stored=True),
                        f1=NUMERIC(int, stored=True),
                        f2=NUMERIC(float, stored=True),
                        f3=BOOLEAN(stored=True),
                        f4=DATETIME(stored=True))
        index = create_in(dirname, schema)

        totextindex(tbl, index)

        actual = fromtextindex(index)
        ieq(tbl, actual)

    def test_appendindex_dirname():

        dirname = tempfile.mkdtemp()

        # name fields in ascending order as whoosh sorts fields on the way out
        tbl = (('f0', 'f1', 'f2', 'f3', 'f4'),
               (u'AAA', 12, 4.3, True, datetime.datetime.now()),
               (u'BBB', 6, 3.4, False, datetime.datetime(1900, 1, 31)),
               (u'CCC', 42, 7.8, True, datetime.datetime(2100, 12, 25)))

        schema = Schema(f0=TEXT(stored=True),
                        f1=NUMERIC(int, stored=True),
                        f2=NUMERIC(float, stored=True),
                        f3=BOOLEAN(stored=True),
                        f4=DATETIME(stored=True))

        totextindex(tbl, dirname, schema=schema)
        appendtextindex(tbl, dirname)

        actual = fromtextindex(dirname)
        expect = tbl + tbl[1:]
        ieq(expect, actual)

    def test_appendindex_index():

        dirname = tempfile.mkdtemp()

        # name fields in ascending order as whoosh sorts fields on the way out
        tbl = (('f0', 'f1', 'f2', 'f3', 'f4'),
               (u'AAA', 12, 4.3, True, datetime.datetime.now()),
               (u'BBB', 6, 3.4, False, datetime.datetime(1900, 1, 31)),
               (u'CCC', 42, 7.8, True, datetime.datetime(2100, 12, 25)))

        schema = Schema(f0=TEXT(stored=True),
                        f1=NUMERIC(int, stored=True),
                        f2=NUMERIC(float, stored=True),
                        f3=BOOLEAN(stored=True),
                        f4=DATETIME(stored=True))
        index = create_in(dirname, schema)

        totextindex(tbl, index)
        appendtextindex(tbl, index)

        actual = fromtextindex(index)
        expect = tbl + tbl[1:]
        ieq(expect, actual)

    def test_searchindex():

        dirname = tempfile.mkdtemp()
        schema = Schema(title=TEXT(stored=True), path=ID(stored=True),
                        content=TEXT)

        ix = create_in(dirname, schema)
        writer = ix.writer()
        writer.add_document(title=u"Oranges", path=u"/a",
                            content=u"This is the first document we've added!")
        writer.add_document(title=u"Apples", path=u"/b",
                            content=u"The second document is even more "
                                    u"interesting!")
        writer.commit()

        # N.B., fields get sorted
        expect = ((u'path', u'title'),
                  (u'/a', u'Oranges'))
        # N.B., by default whoosh does not do stemming
        actual = searchtextindex(dirname, 'oranges')
        ieq(expect, actual)
        actual = searchtextindex(dirname, 'add*')
        ieq(expect, actual)

        expect = ((u'path', u'title'),
                  (u'/a', u'Oranges'),
                  (u'/b', u'Apples'))
        actual = searchtextindex(dirname, 'doc*')
        ieq(expect, actual)

    def test_integration():

        dirname = tempfile.mkdtemp()
        schema = Schema(title=TEXT(stored=True), path=ID(stored=True),
                        content=TEXT)

        ix = create_in(dirname, schema)
        writer = ix.writer()
        writer.add_document(title=u"First document", path=u"/a",
                            content=u"This is the first document we've added!")
        writer.add_document(title=u"Second document", path=u"/b",
                            content=u"The second one is even more interesting!")
        writer.commit()

        # N.B., fields get sorted
        expect = ((u'path', u'title'),
                  (u'/a', u'first document'),
                  (u'/b', u'second document'))
        actual = etl.fromtextindex(dirname).convert('title', 'lower')
        ieq(expect, actual)

    # TODO test_searchindexpage
    # TODO test_searchindex_multifield_query
    # TODO test_searchindex_nontext_query
