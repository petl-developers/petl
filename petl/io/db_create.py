# -*- coding: utf-8 -*-
"""
Module providing some convenience functions for working with SQL databases.
SQLAlchemy is required, try ``apt-get install python-sqlalchemy``
or ``pip install SQLAlchemy``.

Acknowledgments: much of the code of this module is based on the ``csvsql``
utility in the `csvkit <https://github.com/onyxfish/csvkit>`_ package.

"""


import datetime
import logging
from petl.compat import long, text_type


from petl.errors import ArgumentError
from petl.util.materialise import columns
from petl.transform.basics import head
from petl.io.db_utils import _is_dbapi_connection, _is_dbapi_cursor, \
    _is_sqlalchemy_engine, _is_sqlalchemy_session, _is_sqlalchemy_connection,\
    _quote


logger = logging.getLogger(__name__)
debug = logger.debug


DIALECTS = {
    'access': 'access.base',
    'firebird': 'firebird.kinterbasdb',
    'informix': 'informix.informixdb',
    'maxdb': 'maxdb.sapdb',
    'mssql': 'mssql.pyodbc',
    'mysql': 'mysql.mysqlconnector',
    'oracle': 'oracle.cx_oracle',
    'postgresql': 'postgresql.psycopg2',
    'sqlite': 'sqlite.pysqlite',
    'sybase': 'sybase.pyodbc'
}


SQL_INTEGER_MAX = 2147483647
SQL_INTEGER_MIN = -2147483647
NULL_COLUMN_MAX_LENGTH = 32


def make_sqlalchemy_column(col, colname, constraints=True):
    """
    Infer an appropriate SQLAlchemy column type based on a sequence of values.

    Keyword arguments:

    col : sequence
        A sequence of values to use to infer type, length etc.
    colname : string
        Name of column
    constraints : bool
        If True use length and nullable constraints

    """

    import sqlalchemy

    col_not_none = [v for v in col if v is not None]
    sql_column_kwargs = {}
    sql_type_kwargs = {}

    if len(col_not_none) == 0:
        sql_column_type = sqlalchemy.String
        if constraints:
            sql_type_kwargs['length'] = NULL_COLUMN_MAX_LENGTH

    elif all(isinstance(v, bool) for v in col_not_none):
        sql_column_type = sqlalchemy.Boolean

    elif all(isinstance(v, int) for v in col_not_none):
        if max(col_not_none) > SQL_INTEGER_MAX \
                or min(col_not_none) < SQL_INTEGER_MIN:
            sql_column_type = sqlalchemy.BigInteger
        else:
            sql_column_type = sqlalchemy.Integer

    elif all(isinstance(v, long) for v in col_not_none):
        sql_column_type = sqlalchemy.BigInteger

    elif all(isinstance(v, (int, long)) for v in col_not_none):
        sql_column_type = sqlalchemy.BigInteger

    elif all(isinstance(v, (int, long, float)) for v in col_not_none):
        sql_column_type = sqlalchemy.Float

    elif all(isinstance(v, datetime.datetime) for v in col_not_none):
        sql_column_type = sqlalchemy.DateTime

    elif all(isinstance(v, datetime.date) for v in col_not_none):
        sql_column_type = sqlalchemy.Date

    elif all(isinstance(v, datetime.time) for v in col_not_none):
        sql_column_type = sqlalchemy.Time

    else:
        sql_column_type = sqlalchemy.String
        if constraints:
            sql_type_kwargs['length'] = max([len(text_type(v)) for v in col])

    if constraints:
        sql_column_kwargs['nullable'] = len(col_not_none) < len(col)

    return sqlalchemy.Column(colname, sql_column_type(**sql_type_kwargs),
                             **sql_column_kwargs)


def make_sqlalchemy_table(table, tablename, schema=None, constraints=True,
                          metadata=None):
    """
    Create an SQLAlchemy table definition based on data in `table`.

    Keyword arguments:

    table : table container
        Table data to use to infer types etc.
    tablename : text
        Name of the table
    schema : text
        Name of the database schema to create the table in
    constraints : bool
        If True use length and nullable constraints
    metadata : sqlalchemy.MetaData
        Custom table metadata

    """

    import sqlalchemy

    if not metadata:
        metadata = sqlalchemy.MetaData()

    sql_table = sqlalchemy.Table(tablename, metadata, schema=schema)
    cols = columns(table)
    flds = list(cols.keys())
    for f in flds:
        sql_column = make_sqlalchemy_column(cols[f], f,
                                            constraints=constraints)
        sql_table.append_column(sql_column)

    return sql_table


def make_create_table_statement(table, tablename, schema=None,
                                constraints=True, metadata=None, dialect=None):
    """
    Generate a CREATE TABLE statement based on data in `table`.

    Keyword arguments:

    table : table container
        Table data to use to infer types etc.
    tablename : text
        Name of the table
    schema : text
        Name of the database schema to create the table in
    constraints : bool
        If True use length and nullable constraints
    metadata : sqlalchemy.MetaData
        Custom table metadata
    dialect : text
        One of {'access', 'sybase', 'sqlite', 'informix', 'firebird', 'mysql',
        'oracle', 'maxdb', 'postgresql', 'mssql'}

    """

    import sqlalchemy
    sql_table = make_sqlalchemy_table(table, tablename, schema=schema,
                                      constraints=constraints,
                                      metadata=metadata)

    if dialect:
        module = __import__('sqlalchemy.dialects.%s' % DIALECTS[dialect],
                            fromlist=['dialect'])
        sql_dialect = module.dialect()
    else:
        sql_dialect = None

    return text_type(sqlalchemy.schema.CreateTable(sql_table)
                     .compile(dialect=sql_dialect)).strip()


def create_table(table, dbo, tablename, schema=None, commit=True,
                 constraints=True, metadata=None, dialect=None, sample=1000):
    """
    Create a database table based on a sample of data in the given `table`.

    Keyword arguments:

    table : table container
        Table data to load
    dbo : database object
        DB-API 2.0 connection, callable returning a DB-API 2.0 cursor, or
        SQLAlchemy connection, engine or session
    tablename : text
        Name of the table
    schema : text
        Name of the database schema to create the table in
    commit : bool
        If True commit the changes
    constraints : bool
        If True use length and nullable constraints
    metadata : sqlalchemy.MetaData
        Custom table metadata
    dialect : text
        One of {'access', 'sybase', 'sqlite', 'informix', 'firebird', 'mysql',
        'oracle', 'maxdb', 'postgresql', 'mssql'}
    sample : int
        Number of rows to sample when inferring types etc., set to 0 to use
        the whole table

    """

    if sample > 0:
        table = head(table, sample)
    sql = make_create_table_statement(table, tablename, schema=schema,
                                      constraints=constraints,
                                      metadata=metadata, dialect=dialect)
    _execute(sql, dbo, commit=commit)


def drop_table(dbo, tablename, schema=None, commit=True):
    """
    Drop a database table.

    Keyword arguments:

    dbo : database object
        DB-API 2.0 connection, callable returning a DB-API 2.0 cursor, or
        SQLAlchemy connection, engine or session
    tablename : text
        Name of the table
    schema : text
        Name of the database schema the table is in
    commit : bool
        If True commit the changes

    """

    # sanitise table name
    tablename = _quote(tablename)
    if schema is not None:
        tablename = _quote(schema) + '.' + tablename

    sql = u'DROP TABLE %s' % tablename
    _execute(sql, dbo, commit)


def _execute(sql, dbo, commit):

    debug(sql)

    # need to deal with polymorphic dbo argument
    # what sort of duck is it?

    # does it quack like a standard DB-API 2.0 connection?
    if _is_dbapi_connection(dbo):
        debug('assuming %r is standard DB-API 2.0 connection', dbo)
        _execute_dbapi_connection(sql, dbo, commit)

    # does it quack like a standard DB-API 2.0 cursor?
    elif _is_dbapi_cursor(dbo):
        debug('assuming %r is standard DB-API 2.0 cursor')
        _execute_dbapi_cursor(sql, dbo, commit)

    # does it quack like an SQLAlchemy engine?
    elif _is_sqlalchemy_engine(dbo):
        debug('assuming %r is an instance of sqlalchemy.engine.base.Engine',
              dbo)
        _execute_sqlalchemy_engine(sql, dbo, commit)

    # does it quack like an SQLAlchemy session?
    elif _is_sqlalchemy_session(dbo):
        debug('assuming %r is an instance of sqlalchemy.orm.session.Session',
              dbo)
        _execute_sqlalchemy_session(sql, dbo, commit)

    # does it quack like an SQLAlchemy connection?
    elif _is_sqlalchemy_connection(dbo):
        debug('assuming %r is an instance of '
              'sqlalchemy.engine.base.Connection',
              dbo)
        _execute_sqlalchemy_connection(sql, dbo, commit)

    elif callable(dbo):
        debug('assuming %r is a function returning standard DB-API 2.0 cursor '
              'objects', dbo)
        _execute_dbapi_mkcurs(sql, dbo, commit)

    # some other sort of duck...
    else:
        raise ArgumentError('unsupported database object type: %r' % dbo)


def _execute_dbapi_connection(sql, connection, commit):

    debug('obtain a cursor')
    cursor = connection.cursor()

    debug('execute SQL')
    cursor.execute(sql)

    debug('close the cursor')
    cursor.close()

    if commit:
        debug('commit transaction')
        connection.commit()


def _execute_dbapi_mkcurs(sql, mkcurs, commit):

    debug('obtain a cursor')
    cursor = mkcurs()

    debug('execute SQL')
    cursor.execute(sql)

    debug('close the cursor')
    cursor.close()

    if commit:
        debug('commit transaction')
        # N.B., we depend on this optional DB-API 2.0 attribute being
        # implemented
        assert hasattr(cursor, 'connection'), \
            'could not obtain connection via cursor'
        connection = cursor.connection
        connection.commit()


def _execute_dbapi_cursor(sql, cursor, commit):

    debug('execute SQL')
    cursor.execute(sql)

    # don't close the cursor, leave that to the application

    if commit:
        debug('commit transaction')
        # N.B., we depend on this optional DB-API 2.0 attribute being
        # implemented
        assert hasattr(cursor, 'connection'),\
            'could not obtain connection via cursor'
        connection = cursor.connection
        connection.commit()


def _execute_sqlalchemy_connection(sql, connection, commit):

    if commit:
        debug('begin transaction')
        trans = connection.begin()

    debug('execute SQL')
    connection.execute(sql)

    if commit:
        debug('commit transaction')
        trans.commit()

    # N.B., don't close connection, leave that to the application


def _execute_sqlalchemy_engine(sql, engine, commit):
    _execute_sqlalchemy_connection(sql, engine.connect(), commit)


def _execute_sqlalchemy_session(sql, session, commit):
    _execute_sqlalchemy_connection(sql, session.connection(), commit)
