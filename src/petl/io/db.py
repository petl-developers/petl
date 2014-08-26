from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


# standard library dependencies
import logging


# internal dependencies
from petl.util import RowContainer


logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


def fromdb(dbo, query, *args, **kwargs):
    """
    Provides access to data from any DB-API 2.0 connection via a given query.
    E.g., using `sqlite3`::

        >>> import sqlite3
        >>> from petl import look, fromdb
        >>> connection = sqlite3.connect('test.db')
        >>> table = fromdb(connection, 'select * from foobar')
        >>> look(table)

    E.g., using `psycopg2` (assuming you've installed it first)::

        >>> import psycopg2
        >>> from petl import look, fromdb
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> table = fromdb(connection, 'select * from test')
        >>> look(table)

    E.g., using `MySQLdb` (assuming you've installed it first)::

        >>> import MySQLdb
        >>> from petl import look, fromdb
        >>> connection = MySQLdb.connect(passwd="moonpie", db="thangs")
        >>> table = fromdb(connection, 'select * from test')
        >>> look(table)

    .. versionchanged:: 0.10.2

    The first argument may also be a function that creates a cursor. E.g.::

        >>> import psycopg2
        >>> from petl import look, fromdb
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> mkcursor = lambda: connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        >>> table = fromdb(mkcursor, 'select * from test')
        >>> look(table)

    N.B., each call to the function should return a new cursor.

    .. versionchanged:: 0.18

    Added support for server-side cursors.

    Note that the default behaviour of most database servers and clients is for
    the entire result set for each query to be sent from the server to the
    client. If your query returns a large result set this can result in
    significant memory usage at the client. Some databases support server-side
    cursors which provide a means for client libraries to fetch result sets
    incrementally, reducing memory usage at the client.

    To use a server-side cursor with a PostgreSQL database, e.g.::

        >>> import psycopg2
        >>> from petl import look, fromdb
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> table = fromdb(lambda: connection.cursor(name='arbitrary'), 'select * from test')
        >>> look(table)

    To use a server-side cursor with a MySQL database, e.g.::

        >>> import MySQLdb
        >>> from petl import look, fromdb
        >>> connection = MySQLdb.connect(passwd="moonpie", db="thangs")
        >>> table = fromdb(lambda: connection.cursor(MySQLdb.cursors.SSCursor), 'select * from test')
        >>> look(table)

    For more information on server-side cursors see the following links:

        * http://initd.org/psycopg/docs/usage.html#server-side-cursors
        * http://mysql-python.sourceforge.net/MySQLdb.html#using-and-extending

    """

    return DbView(dbo, query, *args, **kwargs)


def _is_dbapi_connection(dbo):
    return _hasmethod(dbo, 'cursor')


def _is_dbapi_cursor(dbo):
    return _hasmethods(dbo, 'execute', 'executemany', 'fetchone', 'fetchmany',
                       'fetchall')


def _is_sqlalchemy_engine(dbo):
    return (_hasmethods(dbo, 'execute', 'contextual_connect', 'raw_connection')
            and _hasprop(dbo, 'driver'))


def _is_sqlalchemy_session(dbo):
    return _hasmethods(dbo, 'execute', 'connection', 'get_bind')


def _is_sqlalchemy_connection(dbo):
    # N.B., this are not completely selective conditions, this test needs
    # to be applied after ruling out DB-API cursor
    return _hasmethod(dbo, 'execute') and _hasprop(dbo, 'connection')


class DbView(RowContainer):

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
            raise Exception('unsupported database object type: %r' % self.dbo)

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
    first_row = cursor.fetchone()
    # fields should be available now
    fields = [d[0] for d in cursor.description]
    yield tuple(fields)
    if first_row is None:
        raise StopIteration
    yield first_row
    for row in cursor:
        yield row # don't wrap, return whatever the database engine returns


def _iter_sqlalchemy_engine(engine, query, *args, **kwargs):
    return _iter_sqlalchemy_connection(engine.contextual_connect(), query,
                                       *args, **kwargs)


def _iter_sqlalchemy_connection(connection, query, *args, **kwargs):
    debug('connection: %r', connection)
    results = connection.execute(query, *args, **kwargs)
    fields = results.keys()
    yield tuple(fields)
    for row in results:
        yield row


def _iter_sqlalchemy_session(session, query, *args, **kwargs):
    results = session.execute(query, *args, **kwargs)
    fields = results.keys()
    yield tuple(fields)
    for row in results:
        yield row


def todb(table, dbo, tablename, schema=None, commit=True):
    """
    Load data into an existing database table via a DB-API 2.0
    connection or cursor. Note that the database table will be truncated,
    i.e., all existing rows will be deleted prior to inserting the new data.
    E.g.::

        >>> from petl import look, todb
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

    ... using :mod:`sqlite3`::

        >>> import sqlite3
        >>> connection = sqlite3.connect('test.db')
        >>> # assuming table "foobar" already exists in the database
        ... todb(table, connection, 'foobar')

    ... using :mod:`psycopg2`::

        >>> import psycopg2
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> # assuming table "foobar" already exists in the database
        ... todb(table, connection, 'foobar')

    ... using :mod:`MySQLdb`::

        >>> import MySQLdb
        >>> connection = MySQLdb.connect(passwd="moonpie", db="thangs")
        >>> # tell MySQL to use standard quote character
        ... connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        >>> # load data, assuming table "foobar" already exists in the database
        ... todb(table, connection, 'foobar')

    N.B., for MySQL the statement ``SET SQL_MODE=ANSI_QUOTES`` is required to
    ensure MySQL uses SQL-92 standard quote characters.

    .. versionchanged:: 0.10.2

    A cursor can also be provided instead of a connection, e.g.::

        >>> import psycopg2
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> cursor = connection.cursor()
        >>> todb(table, cursor, 'foobar')

    """

    _todb(table, dbo, tablename, schema=schema, commit=commit, truncate=True)


def _hasmethod(o, n):
    return hasattr(o, n) and callable(getattr(o, n))


def _hasmethods(o, *l):
    return all(_hasmethod(o, n) for n in l)


def _hasprop(o, n):
    return hasattr(o, n) and not callable(getattr(o, n))


def _todb(table, dbo, tablename, schema=None, commit=True, truncate=False):

    # need to deal with polymorphic dbo argument
    # what sort of duck is it?

    # does it quack like a standard DB-API 2.0 connection?
    if _is_dbapi_connection(dbo):
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
        raise Exception('unsupported database object type: %r' % dbo)


SQL_TRUNCATE_QUERY = u'DELETE FROM %s'
SQL_INSERT_QUERY = u'INSERT INTO %s (%s) VALUES (%s)'


def _todb_dbapi_connection(table, connection, tablename, schema=None,
                           commit=True, truncate=False):

    # sanitise table name
    tablename = _quote(tablename)
    if schema is not None:
        tablename = _quote(schema) + '.' + tablename
    debug('tablename: %r', tablename)

    # sanitise field names
    it = iter(table)
    fields = it.next()
    fieldnames = map(str, fields)
    colnames = [_quote(n) for n in fieldnames]
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

#    insertquery = 'INSERT INTO %s VALUES (%s)' % (tablename, placeholders)
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


def _todb_dbapi_mkcurs(table, mkcurs, tablename, schema=None, commit=True,
                       truncate=False):

    # sanitise table name
    tablename = _quote(tablename)
    if schema is not None:
        tablename = _quote(schema) + '.' + tablename
    debug('tablename: %r', tablename)

    # sanitise field names
    it = iter(table)
    fields = it.next()
    fieldnames = map(str, fields)
    colnames = [_quote(n) for n in fieldnames]
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

#    insertquery = 'INSERT INTO %s VALUES (%s)' % (tablename, placeholders)
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
    fields = it.next()
    fieldnames = map(str, fields)
    colnames = [_quote(n) for n in fieldnames]
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

#    insertquery = 'INSERT INTO %s VALUES (%s)' % (tablename, placeholders)
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

    _todb_sqlalchemy_connection(table, engine.contextual_connect(), tablename,
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
    fields = it.next()
    fieldnames = map(str, fields)
    colnames = [_quote(n) for n in fieldnames]
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

#    insertquery = 'INSERT INTO %s VALUES (%s)' % (tablename, placeholders)
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
    connection or cursor. Note that the database table will be appended,
    i.e., the new data will be inserted into the table, and any existing
    rows will remain. E.g.::

        >>> from petl import look, appenddb
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

    ... using :mod:`sqlite3`::

        >>> import sqlite3
        >>> connection = sqlite3.connect('test.db')
        >>> # assuming table "foobar" already exists in the database
        ... appenddb(table, connection, 'foobar')

    ... using :mod:`psycopg2`::

        >>> import psycopg2
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> # assuming table "foobar" already exists in the database
        ... appenddb(table, connection, 'foobar')

    ... using :mod:`MySQLdb`::

        >>> import MySQLdb
        >>> connection = MySQLdb.connect(passwd="moonpie", db="thangs")
        >>> # tell MySQL to use standard quote character
        ... connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        >>> # load data, appending rows to table "foobar"
        ... appenddb(table, connection, 'foobar')

    N.B., for MySQL the statement ``SET SQL_MODE=ANSI_QUOTES`` is required to
    ensure MySQL uses SQL-92 standard quote characters.

    .. versionchanged:: 0.10.2

    A cursor can also be provided instead of a connection, e.g.::

        >>> import psycopg2
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> cursor = connection.cursor()
        >>> appenddb(table, cursor, 'foobar')

    """

    _todb(table, dbo, tablename, schema=schema, commit=commit, truncate=False)


# default DB quote char per SQL-92
quotechar = '"'


def _quote(s):
    # crude way to sanitise table and field names
    # conform with the SQL-92 standard. See http://stackoverflow.com/a/214344
    return quotechar + s.replace(quotechar, quotechar+quotechar) + quotechar


def _placeholders(connection, names):
    # discover the paramstyle
    if connection is None:
        # default to using question mark
        debug('connection is None, default to using qmark paramstyle')
        placeholders = ', '.join(['?'] * len(names))
    else:
        mod = __import__(connection.__class__.__module__)

        if not hasattr(mod, 'paramstyle'):
            debug('module %r from connection %r has no attribute paramstyle, '
                  'defaulting to qmark' , mod, connection)
            # default to using question mark
            placeholders = ', '.join(['?'] * len(names))

        elif mod.paramstyle == 'qmark':
            debug('found paramstyle qmark')
            placeholders = ', '.join(['?'] * len(names))

        elif mod.paramstyle in ('format', 'pyformat'):
            debug('found paramstyle pyformat')
            placeholders = ', '.join(['%s'] * len(names))

        elif mod.paramstyle == 'numeric':
            debug('found paramstyle numeric')
            placeholders = ', '.join([':' + str(i + 1)
                                      for i in range(len(names))])

        else:
            debug('found unexpected paramstyle %r, defaulting to qmark',
                  mod.paramstyle)
            placeholders = ', '.join(['?'] * len(names))

    return placeholders



