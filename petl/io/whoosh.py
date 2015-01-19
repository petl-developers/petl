# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import operator
from petl.compat import string_types, izip


from petl.errors import ArgumentError
from petl.util.base import Table, dicts


def fromtextindex(index_or_dirname, indexname=None, docnum_field=None):
    """
    Extract all documents from a Whoosh index. E.g.::

        >>> import petl as etl
        >>> import os
        >>> # set up an index and load some documents via the Whoosh API
        ... from whoosh.index import create_in
        >>> from whoosh.fields import *
        >>> schema = Schema(title=TEXT(stored=True), path=ID(stored=True),
        ...                 content=TEXT)
        >>> dirname = 'example.whoosh'
        >>> if not os.path.exists(dirname):
        ...     os.mkdir(dirname)
        ...
        >>> index = create_in(dirname, schema)
        >>> writer = index.writer()
        >>> writer.add_document(title=u"First document", path=u"/a",
        ...                     content=u"This is the first document we've added!")
        >>> writer.add_document(title=u"Second document", path=u"/b",
        ...                     content=u"The second one is even more interesting!")
        >>> writer.commit()
        >>> # extract documents as a table
        ... table = etl.fromtextindex(dirname)
        >>> table
        +------+-------------------+
        | path | title             |
        +======+===================+
        | '/a' | 'First document'  |
        +------+-------------------+
        | '/b' | 'Second document' |
        +------+-------------------+

    Keyword arguments:
    
    index_or_dirname
        Either an instance of `whoosh.index.Index` or a string containing the
        directory path where the index is stored.
    indexname
        String containing the name of the index, if multiple indexes are stored
        in the same directory.
    docnum_field
        If not None, an extra field will be added to the output table containing
        the internal document number stored in the index. The name of the field
        will be the value of this argument.

    """

    return TextIndexView(index_or_dirname, indexname=indexname,
                         docnum_field=docnum_field)


class TextIndexView(Table):

    def __init__(self, index_or_dirname, indexname=None, docnum_field=None):
        self.index_or_dirname = index_or_dirname
        self.indexname = indexname
        self.docnum_field = docnum_field

    def __iter__(self):
        return itertextindex(self.index_or_dirname, self.indexname,
                             self.docnum_field)


def itertextindex(index_or_dirname, indexname, docnum_field):
    import whoosh.index

    if isinstance(index_or_dirname, string_types):
        dirname = index_or_dirname
        index = whoosh.index.open_dir(dirname, indexname=indexname,
                                      readonly=True)
        needs_closing = True
    elif isinstance(index_or_dirname, whoosh.index.Index):
        index = index_or_dirname
        needs_closing = False
    else:
        raise ArgumentError('expected string or index, found %r'
                            % index_or_dirname)

    try:

        if docnum_field is None:

            # figure out the field names
            hdr = tuple(index.schema.stored_names())
            yield hdr

            # yield all documents
            astuple = operator.itemgetter(*index.schema.stored_names())
            for _, stored_fields_dict in index.reader().iter_docs():
                yield astuple(stored_fields_dict)

        else:

            # figure out the field names
            hdr = (docnum_field,) + tuple(index.schema.stored_names())
            yield hdr

            # yield all documents
            astuple = operator.itemgetter(*index.schema.stored_names())
            for docnum, stored_fields_dict in index.reader().iter_docs():
                yield (docnum,) + astuple(stored_fields_dict)

    except:
        raise

    finally:
        if needs_closing:
            # close the index if we're the ones who opened it
            index.close()


def totextindex(table, index_or_dirname, schema=None, indexname=None,
                merge=False, optimize=False):
    """
    Load all rows from `table` into a Whoosh index. N.B., this will clear any
    existing data in the index before loading. E.g.::

        >>> import petl as etl
        >>> import datetime
        >>> import os
        >>> # here is the table we want to load into an index
        ... table = (('f0', 'f1', 'f2', 'f3', 'f4'),
        ...          ('AAA', 12, 4.3, True, datetime.datetime.now()),
        ...          ('BBB', 6, 3.4, False, datetime.datetime(1900, 1, 31)),
        ...          ('CCC', 42, 7.8, True, datetime.datetime(2100, 12, 25)))
        >>> # define a schema for the index
        ... from whoosh.fields import *
        >>> schema = Schema(f0=TEXT(stored=True),
        ...                 f1=NUMERIC(int, stored=True),
        ...                 f2=NUMERIC(float, stored=True),
        ...                 f3=BOOLEAN(stored=True),
        ...                 f4=DATETIME(stored=True))
        >>> # load index
        ... dirname = 'example.whoosh'
        >>> if not os.path.exists(dirname):
        ...     os.mkdir(dirname)
        ...
        >>> etl.totextindex(table, dirname, schema=schema)

    Keyword arguments:

    table
        A table container with the data to be loaded.
    index_or_dirname
        Either an instance of `whoosh.index.Index` or a string containing the
        directory path where the index is to be stored.
    schema
        Index schema to use if creating the index.
    indexname
        String containing the name of the index, if multiple indexes are stored
        in the same directory.
    merge
        Merge small segments during commit?
    optimize
        Merge all segments together?

    """
    import whoosh.index
    import whoosh.writing

    # deal with polymorphic argument
    if isinstance(index_or_dirname, string_types):
        dirname = index_or_dirname
        index = whoosh.index.create_in(dirname, schema,
                                       indexname=indexname)
        needs_closing = True
    elif isinstance(index_or_dirname, whoosh.index.Index):
        index = index_or_dirname
        needs_closing = False
    else:
        raise ArgumentError('expected string or index, found %r'
                            % index_or_dirname)

    writer = index.writer()
    try:

        for d in dicts(table):
            writer.add_document(**d)
        writer.commit(merge=merge, optimize=optimize,
                      mergetype=whoosh.writing.CLEAR)

    except:
        writer.cancel()
        raise

    finally:
        if needs_closing:
            index.close()


def appendtextindex(table, index_or_dirname, indexname=None, merge=True,
                    optimize=False):
    """
    Load all rows from `table` into a Whoosh index, adding them to any existing
    data in the index.

    Keyword arguments:

    table
        A table container with the data to be loaded.
    index_or_dirname
        Either an instance of `whoosh.index.Index` or a string containing the
        directory path where the index is to be stored.
    indexname
        String containing the name of the index, if multiple indexes are stored
        in the same directory.
    merge
        Merge small segments during commit?
    optimize
        Merge all segments together?

    """
    import whoosh.index

    # deal with polymorphic argument
    if isinstance(index_or_dirname, string_types):
        dirname = index_or_dirname
        index = whoosh.index.open_dir(dirname, indexname=indexname,
                                      readonly=False)
        needs_closing = True
    elif isinstance(index_or_dirname, whoosh.index.Index):
        index = index_or_dirname
        needs_closing = False
    else:
        raise ArgumentError('expected string or index, found %r'
                            % index_or_dirname)

    writer = index.writer()
    try:

        for d in dicts(table):
            writer.add_document(**d)
        writer.commit(merge=merge, optimize=optimize)

    except Exception:
        writer.cancel()
        raise

    finally:
        if needs_closing:
            index.close()


def searchtextindex(index_or_dirname, query, limit=10, indexname=None,
                    docnum_field=None, score_field=None, fieldboosts=None,
                    search_kwargs=None):
    """
    Search a Whoosh index using a query. E.g.::

        >>> import petl as etl
        >>> import os
        >>> # set up an index and load some documents via the Whoosh API
        ... from whoosh.index import create_in
        >>> from whoosh.fields import *
        >>> schema = Schema(title=TEXT(stored=True), path=ID(stored=True),
        ...                            content=TEXT)
        >>> dirname = 'example.whoosh'
        >>> if not os.path.exists(dirname):
        ...     os.mkdir(dirname)
        ...
        >>> index = create_in('example.whoosh', schema)
        >>> writer = index.writer()
        >>> writer.add_document(title=u"Oranges", path=u"/a",
        ...                     content=u"This is the first document we've added!")
        >>> writer.add_document(title=u"Apples", path=u"/b",
        ...                     content=u"The second document is even more "
        ...                             u"interesting!")
        >>> writer.commit()
        >>> # demonstrate the use of searchtextindex()
        ... table1 = etl.searchtextindex('example.whoosh', 'oranges')
        >>> table1
        +------+-----------+
        | path | title     |
        +======+===========+
        | '/a' | 'Oranges' |
        +------+-----------+

        >>> table2 = etl.searchtextindex('example.whoosh', 'doc*')
        >>> table2
        +------+-----------+
        | path | title     |
        +======+===========+
        | '/a' | 'Oranges' |
        +------+-----------+
        | '/b' | 'Apples'  |
        +------+-----------+

    Keyword arguments:

    index_or_dirname
        Either an instance of `whoosh.index.Index` or a string containing the
        directory path where the index is to be stored.
    query
        Either a string or an instance of `whoosh.query.Query`. If a string,
        it will be parsed as a multi-field query, i.e., any terms not bound
        to a specific field will match **any** field.
    limit
        Return at most `limit` results.
    indexname
        String containing the name of the index, if multiple indexes are stored
        in the same directory.
    docnum_field
        If not None, an extra field will be added to the output table containing
        the internal document number stored in the index. The name of the field
        will be the value of this argument.
    score_field
        If not None, an extra field will be added to the output table containing
        the score of the result. The name of the field will be the value of this
        argument.
    fieldboosts
        An optional dictionary mapping field names to boosts.
    search_kwargs
        Any extra keyword arguments to be passed through to the Whoosh
        `search()` method.

    """

    return SearchTextIndexView(index_or_dirname, query, limit=limit,
                               indexname=indexname, docnum_field=docnum_field,
                               score_field=score_field, fieldboosts=fieldboosts,
                               search_kwargs=search_kwargs)


def searchtextindexpage(index_or_dirname, query, pagenum, pagelen=10,
                        indexname=None, docnum_field=None, score_field=None,
                        fieldboosts=None, search_kwargs=None):
    """
    Search an index using a query, returning a result page.

    Keyword arguments:

    index_or_dirname
        Either an instance of `whoosh.index.Index` or a string containing the
        directory path where the index is to be stored.
    query
        Either a string or an instance of `whoosh.query.Query`. If a string,
        it will be parsed as a multi-field query, i.e., any terms not bound
        to a specific field will match **any** field.
    pagenum
        Number of the page to return (e.g., 1 = first page).
    pagelen
        Number of results per page.
    indexname
        String containing the name of the index, if multiple indexes are stored
        in the same directory.
    docnum_field
        If not None, an extra field will be added to the output table containing
        the internal document number stored in the index. The name of the field
        will be the value of this argument.
    score_field
        If not None, an extra field will be added to the output table containing
        the score of the result. The name of the field will be the value of this
        argument.
    fieldboosts
        An optional dictionary mapping field names to boosts.
    search_kwargs
        Any extra keyword arguments to be passed through to the Whoosh
        `search()` method.

    """

    return SearchTextIndexView(index_or_dirname, query, pagenum=pagenum,
                               pagelen=pagelen, indexname=indexname,
                               docnum_field=docnum_field,
                               score_field=score_field, fieldboosts=fieldboosts,
                               search_kwargs=search_kwargs)


class SearchTextIndexView(Table):

    def __init__(self, index_or_dirname, query, limit=None, pagenum=None,
                 pagelen=None, indexname=None, docnum_field=None,
                 score_field=None, fieldboosts=None, search_kwargs=None):
        self._index_or_dirname = index_or_dirname
        self._query = query
        self._limit = limit
        self._pagenum = pagenum
        self._pagelen = pagelen
        self._indexname = indexname
        self._docnum_field = docnum_field
        self._score_field = score_field
        self._fieldboosts = fieldboosts
        self._search_kwargs = search_kwargs

    def __iter__(self):
        return itersearchindex(self._index_or_dirname, self._query,
                               self._limit, self._pagenum, self._pagelen,
                               self._indexname, self._docnum_field,
                               self._score_field, self._fieldboosts,
                               self._search_kwargs)


def itersearchindex(index_or_dirname, query, limit, pagenum, pagelen, indexname,
                    docnum_field, score_field, fieldboosts, search_kwargs):
    import whoosh.index
    import whoosh.query
    import whoosh.qparser

    if not search_kwargs:
        search_kwargs = dict()

    if isinstance(index_or_dirname, string_types):
        dirname = index_or_dirname
        index = whoosh.index.open_dir(dirname,
                                      indexname=indexname,
                                      readonly=True)
        needs_closing = True
    elif isinstance(index_or_dirname, whoosh.index.Index):
        index = index_or_dirname
        needs_closing = False
    else:
        raise ArgumentError('expected string or index, found %r'
                            % index_or_dirname)

    try:

        # figure out header
        hdr = tuple()
        if docnum_field is not None:
            hdr += (docnum_field,)
        if score_field is not None:
            hdr += (score_field,)
        stored_names = tuple(index.schema.stored_names())
        hdr += stored_names
        yield hdr

        # parse the query
        if isinstance(query, string_types):
            # search all fields by default
            parser = whoosh.qparser.MultifieldParser(
                index.schema.names(),
                index.schema,
                fieldboosts=fieldboosts
            )
            query = parser.parse(query)
        elif isinstance(query, whoosh.query.Query):
            pass
        else:
            raise ArgumentError(
                'expected string or whoosh.query.Query, found %r' % query
            )

        # make a function to turn docs into tuples
        astuple = operator.itemgetter(*index.schema.stored_names())

        with index.searcher() as searcher:
            if limit is not None:
                results = searcher.search(query, limit=limit,
                                          **search_kwargs)
            else:
                results = searcher.search_page(query, pagenum,
                                               pagelen=pagelen,
                                               **search_kwargs)

            if docnum_field is None and score_field is None:

                for doc in results:
                    yield astuple(doc)

            else:

                for (docnum, score), doc in izip(results.items(), results):
                    row = tuple()
                    if docnum_field is not None:
                        row += (docnum,)
                    if score_field is not None:
                        row += (score,)
                    row += astuple(doc)
                    yield row

    except:
        raise

    finally:
        if needs_closing:
            # close the index if we're the ones who opened it
            index.close()


# TODO guess schema