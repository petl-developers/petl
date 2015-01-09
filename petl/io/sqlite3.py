from __future__ import absolute_import, print_function, division


# standard library dependencies
import sqlite3
from petl.compat import string_types, next


# internal dependencies
from petl.util.base import Table


quotechar = '"'


def _quote(s):
    # crude way to sanitise table and field names
    # conform with the SQL-92 standard. See http://stackoverflow.com/a/214344
    return quotechar + s.replace(quotechar, quotechar+quotechar) + quotechar


def fromsqlite3(source, query, *args, **kwargs):
    """
    Extract data from an :mod:`sqlite3` database file via a given query. E.g.::

        >>> import petl as etl
        >>> import sqlite3
        >>> # set up a database to demonstrate with
        ... data = [['a', 1],
        ...         ['b', 2],
        ...         ['c', 2.0]]
        >>> connection = sqlite3.connect('example.db')
        >>> c = connection.cursor()
        >>> _ = c.execute('drop table if exists foobar')
        >>> _ = c.execute('create table foobar (foo, bar)')
        >>> for row in data:
        ...     _ = c.execute('insert into foobar values (?, ?)', row)
        ...
        >>> connection.commit()
        >>> c.close()
        >>> # now demonstrate the petl.fromsqlite3 function
        ... table = etl.fromsqlite3('example.db', 'select * from foobar')
        >>> table
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' | 2.0 |
        +-----+-----+

    Either a database file name or a connection object can be given as the
    first argument.

    """

    return Sqlite3View(source, query, *args, **kwargs)


class Sqlite3View(Table):

    def __init__(self, source, query, *args, **kwargs):
        self.source = source
        self.query = query
        self.args = args
        self.kwargs = kwargs
        # setup the connection
        if isinstance(self.source, string_types):
            self.connection = sqlite3.connect(self.source)
            self.connection.row_factory = sqlite3.Row
        elif isinstance(self.source, sqlite3.Connection):
            self.connection = self.source
        else:
            raise Exception('source argument must be filename or connection; '
                            'found %r' % self.source)

    def __iter__(self):

        cursor = self.connection.cursor()
        cursor.execute(self.query, *self.args, **self.kwargs)
        fields = [d[0] for d in cursor.description]
        yield tuple(fields)
        for row in cursor:
            yield row  # don't wrap

        # tidy up
        cursor.close()


def tosqlite3(table, filename_or_connection, tablename, create=False,
              commit=True):
    """
    Load data into a table in an :mod:`sqlite3` database. Note that if
    the database table exists, it will be truncated, i.e., all
    existing rows will be deleted prior to inserting the new
    data. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> _ = etl.tosqlite3(table1, 'example.db', 'foobar', create=True)
        >>> # look what it did
        ... table2 = etl.fromsqlite3('example.db', 'select * from foobar')
        >>> table2
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' |   2 |
        +-----+-----+

    If the table does not exist and ``create=True`` then a table will be created
    using the field names in the table header. However, note that no type
    specifications will be included in the table creation statement and so
    column type affinities may be inappropriate.

    Either a database file name or a connection object can be given as the
    second argument.

    """

    return _tosqlite3(table, filename_or_connection, tablename, create=create,
                      commit=commit, truncate=True)


Table.tosqlite3 = tosqlite3


def _tosqlite3(table, filename_or_connection, tablename, create=False,
               commit=True, truncate=False):

    if isinstance(filename_or_connection, string_types):
        conn = sqlite3.connect(filename_or_connection)
    elif isinstance(filename_or_connection, sqlite3.Connection):
        conn = filename_or_connection
    else:
        raise Exception('filename_or_connection argument must be filename or '
                        'connection; found %r' % filename_or_connection)

    tablename = _quote(tablename)
    it = iter(table)
    fields = next(it)
    fieldnames = map(str, fields)
    colnames = [_quote(n) for n in fieldnames]

    cursor = conn.cursor()

    if create:  # force table creation
        cursor.execute(u'DROP TABLE IF EXISTS %s' % tablename)
        cursor.execute(u'CREATE TABLE %s (%s)'
                       % (tablename, ', '.join(colnames)))

    if truncate:
        # truncate table
        cursor.execute(u'DELETE FROM %s' % tablename)

    # insert rows
    placeholders = ', '.join(['?'] * len(colnames))
    insertquery = u'INSERT INTO %s VALUES (%s);' % (tablename, placeholders)
    cursor.executemany(insertquery, it)

    # tidy up
    cursor.close()
    if commit:
        conn.commit()

    return conn  # in case people want to re-use it or close it


def appendsqlite3(table, filename_or_connection, tablename, commit=True):
    """
    Load data into a table in an :mod:`sqlite3` database. As
    :func:`tosqlite3` except the table must exist and is not truncated before
    loading.

    Either a database file name or a connection object can be given as the
    second argument.

    """

    return _tosqlite3(table, filename_or_connection, tablename, create=False,
                      commit=commit, truncate=False)


Table.appendsqlite3 = appendsqlite3
