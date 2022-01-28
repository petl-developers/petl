# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
import logging
from petl.compat import next, text_type, string_types


# internal dependencies
from petl.errors import ArgumentError
from petl.util.base import Table
from petl.io.db_utils import _is_dbapi_connection, _is_dbapi_cursor, \
    _is_sqlalchemy_connection, _is_sqlalchemy_engine, _is_sqlalchemy_session, \
    _is_clikchouse_dbapi_connection, _quote, _placeholders
from petl.io.db_create import drop_table, create_table


logger = logging.getLogger(__name__)
debug = logger.debug
warning = logger.warning


def fromdb(dbo, query, *args, **kwargs):
    """Provides access to data from any DB-API 2.0 connection via a given query.
    E.g., using :mod:`sqlite3`::

        >>> import petl as etl
        >>> import sqlite3
        >>> connection = sqlite3.connect('example.db')
        >>> table = etl.fromdb(connection, 'SELECT * FROM example')

    E.g., using :mod:`psycopg2` (assuming you've installed it first)::

        >>> import petl as etl
        >>> import psycopg2
        >>> connection = psycopg2.connect('dbname=example user=postgres')
        >>> table = etl.fromdb(connection, 'SELECT * FROM example')

    E.g., using :mod:`pymysql` (assuming you've installed it first)::

        >>> import petl as etl
        >>> import pymysql
        >>> connection = pymysql.connect(password='moonpie', database='thangs')
        >>> table = etl.fromdb(connection, 'SELECT * FROM example')

    The `dbo` argument may also be a function that creates a cursor. N.B., each
    call to the function should return a new cursor. E.g.::

        >>> import petl as etl
        >>> import psycopg2
        >>> connection = psycopg2.connect('dbname=example user=postgres')
        >>> mkcursor = lambda: connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        >>> table = etl.fromdb(mkcursor, 'SELECT * FROM example')

    The parameter `dbo` may also be an SQLAlchemy engine, session or
    connection object.

    The parameter `dbo` may also be a string, in which case it is interpreted as
    the name of a file containing an :mod:`sqlite3` database.

    Note that the default behaviour of most database servers and clients is for
    the entire result set for each query to be sent from the server to the
    client. If your query returns a large result set this can result in
    significant memory usage at the client. Some databases support server-side
    cursors which provide a means for client libraries to fetch result sets
    incrementally, reducing memory usage at the client.

    To use a server-side cursor with a PostgreSQL database, e.g.::

        >>> import petl as etl
        >>> import psycopg2
        >>> connection = psycopg2.connect('dbname=example user=postgres')
        >>> table = etl.fromdb(lambda: connection.cursor(name='arbitrary'),
        ...                    'SELECT * FROM example')

    For more information on server-side cursors see the following links:

        * http://initd.org/psycopg/docs/usage.html#server-side-cursors
        * http://mysql-python.sourceforge.net/MySQLdb.html#using-and-extending

    """

    # convenience for working with sqlite3
    if isinstance(dbo, string_types):
        import sqlite3
        dbo = sqlite3.connect(dbo)

    return DbView(dbo, query, *args, **kwargs)


class DbView(Table):

    def __init__(self, dbo, query, *args, **kwargs):
        self.dbo = dbo
        self.query = query
        self.args = args
        self.kwargs = kwargs

    def __iter__(self):

        # does it quack like a standard DB-API 2.0 connection?
        if _is_dbapi_connection(self.dbo):
            debug('assuming %r is standard DB-API 2.0 connection', self.dbo)
            _iter = _iter_dbapi_connection

        # does it quack like a standard DB-API 2.0 cursor?
        elif _is_dbapi_cursor(self.dbo):
            debug('assuming %r is standard DB-API 2.0 cursor')
            warning('using a DB-API cursor with fromdb() is not recommended '
                    'and may lead to unexpected results, a DB-API connection '
                    'is better')
            _iter = _iter_dbapi_cursor

        # does it quack like an SQLAlchemy engine?
        elif _is_sqlalchemy_engine(self.dbo):
            debug('assuming %r instance of sqlalchemy.engine.base.Engine',
                  self.dbo)
            _iter = _iter_sqlalchemy_engine

        # does it quack like an SQLAlchemy session?
        elif _is_sqlalchemy_session(self.dbo):
            debug('assuming %r instance of sqlalchemy.orm.session.Session',
                  self.dbo)
            _iter = _iter_sqlalchemy_session

        # does it quack like an SQLAlchemy connection?
        elif _is_sqlalchemy_connection(self.dbo):
            debug('assuming %r instance of sqlalchemy.engine.base.Connection',
                  self.dbo)
            _iter = _iter_sqlalchemy_connection

        elif callable(self.dbo):
            debug('assuming %r is a function returning a cursor', self.dbo)
            _iter = _iter_dbapi_mkcurs

        # some other sort of duck...
        else:
            raise ArgumentError('unsupported database object type: %r' % self.dbo)

        return _iter(self.dbo, self.query, *self.args, **self.kwargs)


def _iter_dbapi_mkcurs(mkcurs, query, *args, **kwargs):
    cursor = mkcurs()
    try:
        for row in _iter_dbapi_cursor(cursor, query, *args, **kwargs):
            yield row
    finally:
        cursor.close()


def _iter_dbapi_connection(connection, query, *args, **kwargs):
    cursor = connection.cursor()
    try:
        for row in _iter_dbapi_cursor(cursor, query, *args, **kwargs):
            yield row
    finally:
        cursor.close()


def _iter_dbapi_cursor(cursor, query, *args, **kwargs):
    cursor.execute(query, *args, **kwargs)
    # fetch one row before iterating, to force population of cursor.description
    # which may be postponed if using server-side cursors
    # not all database drivers populate cursor after execute so we call fetchall
    try:
        it = iter(cursor)
    except TypeError:
        it = iter(cursor.fetchall())
    try:
        first_row = next(it)
    except StopIteration:
        first_row = None
    # fields should be available now
    hdr = [d[0] for d in cursor.description]
    yield tuple(hdr)
    if first_row is None:
        return
    yield first_row
    for row in it:
        yield row  # don't wrap, return whatever the database engine returns


def _iter_sqlalchemy_engine(engine, query, *args, **kwargs):
    connection = engine.connect()
    for row in _iter_sqlalchemy_connection(connection, query, *args, **kwargs):
        yield row
    connection.close()


def _iter_sqlalchemy_connection(connection, query, *args, **kwargs):
    debug('connection: %r', connection)
    results = connection.execute(query, *args, **kwargs)
    hdr = results.keys()
    yield tuple(hdr)
    for row in results:
        yield row


def _iter_sqlalchemy_session(session, query, *args, **kwargs):
    results = session.execute(query, *args, **kwargs)
    hdr = results.keys()
    yield tuple(hdr)
    for row in results:
        yield row


def todb(table, dbo, tablename, schema=None, commit=True,
         create=False, drop=False, constraints=True, metadata=None,
         dialect=None, sample=1000):
    """
    Load data into an existing database table via a DB-API 2.0
    connection or cursor. Note that the database table will be truncated,
    i.e., all existing rows will be deleted prior to inserting the new data.
    E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> # using sqlite3
        ... import sqlite3
        >>> connection = sqlite3.connect('example.db')
        >>> # assuming table "foobar" already exists in the database
        ... etl.todb(table, connection, 'foobar')
        >>> # using psycopg2
        >>> import psycopg2
        >>> connection = psycopg2.connect('dbname=example user=postgres')
        >>> # assuming table "foobar" already exists in the database
        ... etl.todb(table, connection, 'foobar')
        >>> # using pymysql
        >>> import pymysql
        >>> connection = pymysql.connect(password='moonpie', database='thangs')
        >>> # tell MySQL to use standard quote character
        ... connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        >>> # load data, assuming table "foobar" already exists in the database
        ... etl.todb(table, connection, 'foobar')

    N.B., for MySQL the statement ``SET SQL_MODE=ANSI_QUOTES`` is required to
    ensure MySQL uses SQL-92 standard quote characters.

    A cursor can also be provided instead of a connection, e.g.::

        >>> import psycopg2
        >>> connection = psycopg2.connect('dbname=example user=postgres')
        >>> cursor = connection.cursor()
        >>> etl.todb(table, cursor, 'foobar')

    The parameter `dbo` may also be an SQLAlchemy engine, session or
    connection object.

    The parameter `dbo` may also be a string, in which case it is interpreted
    as the name of a file containing an :mod:`sqlite3` database.

    If ``create=True`` this function will attempt to automatically create a
    database table before loading the data. This functionality requires
    `SQLAlchemy <http://www.sqlalchemy.org/>`_ to be installed.

    **Keyword arguments:**

    table : table container
        Table data to load
    dbo : database object
        DB-API 2.0 connection, callable returning a DB-API 2.0 cursor, or
        SQLAlchemy connection, engine or session
    tablename : string
        Name of the table in the database
    schema : string
        Name of the database schema to find the table in
    commit : bool
        If True commit the changes
    create : bool
        If True attempt to create the table before loading, inferring types
        from a sample of the data (requires SQLAlchemy)
    drop : bool
        If True attempt to drop the table before recreating (only relevant if
        create=True)
    constraints : bool
        If True use length and nullable constraints (only relevant if
        create=True)
    metadata : sqlalchemy.MetaData
        Custom table metadata (only relevant if create=True)
    dialect : string
        One of {'access', 'sybase', 'sqlite', 'informix', 'firebird', 'mysql',
        'oracle', 'maxdb', 'postgresql', 'mssql'} (only relevant if
        create=True)
    sample : int
        Number of rows to sample when inferring types etc. Set to 0 to use the
        whole table (only relevant if create=True)

    .. note::

        This function is in principle compatible with any DB-API 2.0
        compliant database driver. However, at the time of writing some DB-API
        2.0 implementations, including cx_Oracle and MySQL's
        Connector/Python, are not compatible with this function, because they
        only accept a list argument to the cursor.executemany() function
        called internally by :mod:`petl`. This can be worked around by
        proxying the cursor objects, e.g.::

            >>> import cx_Oracle
            >>> connection = cx_Oracle.Connection(...)
            >>> class CursorProxy(object):
            ...     def __init__(self, cursor):
            ...         self._cursor = cursor
            ...     def executemany(self, statement, parameters, **kwargs):
            ...         # convert parameters to a list
            ...         parameters = list(parameters)
            ...         # pass through to proxied cursor
            ...         return self._cursor.executemany(statement, parameters, **kwargs)
            ...     def __getattr__(self, item):
            ...         return getattr(self._cursor, item)
            ...
            >>> def get_cursor():
            ...     return CursorProxy(connection.cursor())
            ...
            >>> import petl as etl
            >>> etl.todb(tbl, get_cursor, ...)

        Note however that this does imply loading the entire table into
        memory as a list prior to inserting into the database.

    """

    needs_closing = False

    # convenience for working with sqlite3
    if isinstance(dbo, string_types):
        import sqlite3
        dbo = sqlite3.connect(dbo)
        needs_closing = True

    try:
        if create:
            if drop:
                drop_table(dbo, tablename, schema=schema, commit=commit)
            create_table(table, dbo, tablename, schema=schema, commit=commit,
                         constraints=constraints, metadata=metadata,
                         dialect=dialect, sample=sample)
        _todb(table, dbo, tablename, schema=schema, commit=commit,
              truncate=True)

    finally:
        if needs_closing:
            dbo.close()


Table.todb = todb


def _todb(table, dbo, tablename, schema=None, commit=True, truncate=False):

    # need to deal with polymorphic dbo argument
    # what sort of duck is it?

    if _is_clikchouse_dbapi_connection(dbo):
        debug('assuming %r is clickhosue DB-API 2.0 connection', dbo)
        _todb_clikchouse_dbapi_connection(table, dbo, tablename, schema=schema,
                               commit=commit, truncate=truncate)

    # does it quack like a standard DB-API 2.0 connection?
    elif _is_dbapi_connection(dbo):
        debug('assuming %r is standard DB-API 2.0 connection', dbo)
        _todb_dbapi_connection(table, dbo, tablename, schema=schema,
                               commit=commit, truncate=truncate)

    # does it quack like a standard DB-API 2.0 cursor?
    elif _is_dbapi_cursor(dbo):
        debug('assuming %r is standard DB-API 2.0 cursor')
        _todb_dbapi_cursor(table, dbo, tablename, schema=schema, commit=commit,
                           truncate=truncate)

    # does it quack like an SQLAlchemy engine?
    elif _is_sqlalchemy_engine(dbo):
        debug('assuming %r instance of sqlalchemy.engine.base.Engine', dbo)
        _todb_sqlalchemy_engine(table, dbo, tablename, schema=schema,
                                commit=commit, truncate=truncate)

    # does it quack like an SQLAlchemy session?
    elif _is_sqlalchemy_session(dbo):
        debug('assuming %r instance of sqlalchemy.orm.session.Session', dbo)
        _todb_sqlalchemy_session(table, dbo, tablename, schema=schema,
                                 commit=commit, truncate=truncate)

    # does it quack like an SQLAlchemy connection?
    elif _is_sqlalchemy_connection(dbo):
        debug('assuming %r instance of sqlalchemy.engine.base.Connection', dbo)
        _todb_sqlalchemy_connection(table, dbo, tablename, schema=schema,
                                    commit=commit, truncate=truncate)

    elif callable(dbo):
        debug('assuming %r is a function returning standard DB-API 2.0 cursor '
              'objects', dbo)
        _todb_dbapi_mkcurs(table, dbo, tablename, schema=schema, commit=commit,
                           truncate=truncate)

    # some other sort of duck...
    else:
        raise ArgumentError('unsupported database object type: %r' % dbo)


SQL_TRUNCATE_QUERY = 'DELETE FROM %s'
SQL_INSERT_QUERY = 'INSERT INTO %s (%s) VALUES (%s)'


def _todb_dbapi_connection(table, connection, tablename, schema=None,
                           commit=True, truncate=False):

    # sanitise table name
    tablename = _quote(tablename)
    if schema is not None:
        tablename = _quote(schema) + '.' + tablename
    debug('tablename: %r', tablename)

    # sanitise field names
    it = iter(table)
    hdr = next(it)
    flds = list(map(text_type, hdr))
    colnames = [_quote(n) for n in flds]
    debug('column names: %r', colnames)

    # determine paramstyle and build placeholders string
    placeholders = _placeholders(connection, colnames)
    debug('placeholders: %r', placeholders)

    # get a cursor
    cursor = connection.cursor()

    if truncate:
        # TRUNCATE is not supported in some databases and causing locks with
        # MySQL used via SQLAlchemy, fall back to DELETE FROM for now
        truncatequery = SQL_TRUNCATE_QUERY % tablename
        debug('truncate the table via query %r', truncatequery)
        cursor.execute(truncatequery)
        # just in case, close and resurrect cursor
        cursor.close()
        cursor = connection.cursor()

    insertcolnames = ', '.join(colnames)
    insertquery = SQL_INSERT_QUERY % (tablename, insertcolnames, placeholders)
    debug('insert data via query %r' % insertquery)
    cursor.executemany(insertquery, it)

    # finish up
    debug('close the cursor')
    cursor.close()

    if commit:
        debug('commit transaction')
        connection.commit()


def _todb_clikchouse_dbapi_connection(table, connection, tablename, schema=None,
                           commit=True, truncate=False):

    # sanitise table name
    tablename = _quote(tablename)
    if schema is not None:
        tablename = _quote(schema) + '.' + tablename
    debug('tablename: %r', tablename)

    # sanitise field names
    it = iter(table)
    hdr = next(it)
    flds = list(map(text_type, hdr))
    colnames = [_quote(n) for n in flds]
    debug('column names: %r', colnames)

    # determine paramstyle and build placeholders string
    placeholders = _placeholders(connection, colnames)
    debug('placeholders: %r', placeholders)

    # get a cursor
    cursor = connection.cursor()

    if truncate:
        # TRUNCATE is not supported in some databases and causing locks with
        # MySQL used via SQLAlchemy, fall back to DELETE FROM for now
        truncatequery = 'TRUNCATE TABLE IF EXISTS %s' % tablename
        debug('truncate the table via query %r', truncatequery)
        cursor.execute(truncatequery)
        # just in case, close and resurrect cursor
        cursor.close()
        cursor = connection.cursor()

    insertcolnames = ', '.join(colnames)
    insertquery = 'INSERT INTO %s (%s) VALUES' % (tablename, insertcolnames)
    debug('insert data via query %r' % insertquery)
    cursor.executemany(insertquery, it)

    # finish up
    debug('close the cursor')
    cursor.close()

    if commit:
        debug('commit transaction')
        connection.commit()


def _todb_dbapi_mkcurs(table, mkcurs, tablename, schema=None, commit=True,
                       truncate=False):

    # sanitise table name
    tablename = _quote(tablename)
    if schema is not None:
        tablename = _quote(schema) + '.' + tablename
    debug('tablename: %r', tablename)

    # sanitise field names
    it = iter(table)
    hdr = next(it)
    flds = list(map(text_type, hdr))
    colnames = [_quote(n) for n in flds]
    debug('column names: %r', colnames)

    debug('obtain cursor and connection')
    cursor = mkcurs()
    # N.B., we depend on this optional DB-API 2.0 attribute being implemented
    assert hasattr(cursor, 'connection'), \
        'could not obtain connection via cursor'
    connection = cursor.connection

    # determine paramstyle and build placeholders string
    placeholders = _placeholders(connection, colnames)
    debug('placeholders: %r', placeholders)

    if truncate:
        # TRUNCATE is not supported in some databases and causing locks with
        # MySQL used via SQLAlchemy, fall back to DELETE FROM for now
        truncatequery = SQL_TRUNCATE_QUERY % tablename
        debug('truncate the table via query %r', truncatequery)
        cursor.execute(truncatequery)
        # N.B., may be server-side cursor, need to resurrect
        cursor.close()
        cursor = mkcurs()

    insertcolnames = ', '.join(colnames)
    insertquery = SQL_INSERT_QUERY % (tablename, insertcolnames, placeholders)
    debug('insert data via query %r' % insertquery)
    cursor.executemany(insertquery, it)
    cursor.close()

    if commit:
        debug('commit transaction')
        connection.commit()


def _todb_dbapi_cursor(table, cursor, tablename, schema=None, commit=True,
                       truncate=False):

    # sanitise table name
    tablename = _quote(tablename)
    if schema is not None:
        tablename = _quote(schema) + '.' + tablename
    debug('tablename: %r', tablename)

    # sanitise field names
    it = iter(table)
    hdr = next(it)
    flds = list(map(text_type, hdr))
    colnames = [_quote(n) for n in flds]
    debug('column names: %r', colnames)

    debug('obtain connection via cursor')
    # N.B., we depend on this optional DB-API 2.0 attribute being implemented
    assert hasattr(cursor, 'connection'), \
        'could not obtain connection via cursor'
    connection = cursor.connection

    # determine paramstyle and build placeholders string
    placeholders = _placeholders(connection, colnames)
    debug('placeholders: %r', placeholders)

    if truncate:
        # TRUNCATE is not supported in some databases and causing locks with
        # MySQL used via SQLAlchemy, fall back to DELETE FROM for now
        truncatequery = SQL_TRUNCATE_QUERY % tablename
        debug('truncate the table via query %r', truncatequery)
        cursor.execute(truncatequery)

    insertcolnames = ', '.join(colnames)
    insertquery = SQL_INSERT_QUERY % (tablename, insertcolnames, placeholders)
    debug('insert data via query %r' % insertquery)
    cursor.executemany(insertquery, it)

    # N.B., don't close the cursor, leave that to the application

    if commit:
        debug('commit transaction')
        connection.commit()


def _todb_sqlalchemy_engine(table, engine, tablename, schema=None, commit=True,
                            truncate=False):

    _todb_sqlalchemy_connection(table, engine.connect(), tablename,
                                schema=schema, commit=commit, truncate=truncate)


def _todb_sqlalchemy_connection(table, connection, tablename, schema=None,
                                commit=True, truncate=False):

    debug('connection: %r', connection)

    # sanitise table name
    tablename = _quote(tablename)
    if schema is not None:
        tablename = _quote(schema) + '.' + tablename
    debug('tablename: %r', tablename)

    # sanitise field names
    it = iter(table)
    hdr = next(it)
    flds = list(map(text_type, hdr))
    colnames = [_quote(n) for n in flds]
    debug('column names: %r', colnames)

    # N.B., we need to obtain a reference to the underlying DB-API connection so
    # we can import the module and determine the paramstyle
    proxied_raw_connection = connection.connection
    actual_raw_connection = proxied_raw_connection.connection

    # determine paramstyle and build placeholders string
    placeholders = _placeholders(actual_raw_connection, colnames)
    debug('placeholders: %r', placeholders)

    if commit:
        debug('begin transaction')
        trans = connection.begin()

    if truncate:
        # TRUNCATE is not supported in some databases and causing locks with
        # MySQL used via SQLAlchemy, fall back to DELETE FROM for now
        truncatequery = SQL_TRUNCATE_QUERY % tablename
        debug('truncate the table via query %r', truncatequery)
        connection.execute(truncatequery)

    insertcolnames = ', '.join(colnames)
    insertquery = SQL_INSERT_QUERY % (tablename, insertcolnames, placeholders)
    debug('insert data via query %r' % insertquery)
    for row in it:
        connection.execute(insertquery, row)

    # finish up

    if commit:
        debug('commit transaction')
        trans.commit()

    # N.B., don't close connection, leave that to the application


def _todb_sqlalchemy_session(table, session, tablename, schema=None,
                             commit=True, truncate=False):

    _todb_sqlalchemy_connection(table, session.connection(), tablename,
                                schema=schema, commit=commit,
                                truncate=truncate)


def appenddb(table, dbo, tablename, schema=None, commit=True):
    """
    Load data into an existing database table via a DB-API 2.0
    connection or cursor. As :func:`petl.io.db.todb` except that the database
    table will be appended, i.e., the new data will be inserted into the
    table, and any existing rows will remain.

    """

    needs_closing = False

    # convenience for working with sqlite3
    if isinstance(dbo, string_types):
        import sqlite3
        dbo = sqlite3.connect(dbo)
        needs_closing = True

    try:
        _todb(table, dbo, tablename, schema=schema, commit=commit,
              truncate=False)

    finally:
        if needs_closing:
            dbo.close()


Table.appenddb = appenddb
