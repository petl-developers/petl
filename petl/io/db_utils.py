# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import logging
from contextlib import contextmanager


from petl.compat import callable, string_types


logger = logging.getLogger(__name__)
debug = logger.debug


def _is_dbapi_connection(dbo):
    return _hasmethod(dbo, 'cursor')


def _is_clikchouse_dbapi_connection(dbo):
    return 'clickhouse_driver' in str(type(dbo))

    
def _is_dbapi_cursor(dbo):
    return _hasmethods(dbo, 'execute', 'executemany', 'fetchone', 'fetchmany',
                       'fetchall')


def _is_sqlalchemy_engine(dbo):
    return (_hasmethods(dbo, 'connect', 'raw_connection')
            and _hasprop(dbo, 'driver'))


def _is_sqlalchemy_session(dbo):
    return _hasmethods(dbo, 'execute', 'connection', 'get_bind')


def _is_sqlalchemy_connection(dbo):
    # N.B., this are not completely selective conditions, this test needs
    # to be applied after ruling out DB-API cursor
    return _hasmethod(dbo, 'execute') and _hasprop(dbo, 'connection')


def _hasmethod(o, n):
    return hasattr(o, n) and callable(getattr(o, n))


def _hasmethods(o, *l):
    return all(_hasmethod(o, n) for n in l)


def _hasprop(o, n):
    return hasattr(o, n) and not callable(getattr(o, n))


def _execute_sqlalchemy(connection, query, *args, **kwargs):
    # Raw SQL keeps the underlying driver's parameter style. SQLAlchemy
    # expressions still use execute(), including their named bind parameters.
    if not _hasmethod(connection, 'exec_driver_sql'):
        return connection.execute(query, *args, **kwargs)

    # Older execute() accepted keywords, scalar arguments and flat lists.
    # Modern execution APIs take one parameter mapping or sequence instead.
    parameters = args if len(args) > 1 else args[0] if args else kwargs
    if isinstance(query, string_types):
        if isinstance(parameters, list):
            if not parameters or not (isinstance(parameters[0], (tuple, list))
                                      or hasattr(parameters[0], 'keys')):
                parameters = tuple(parameters)
        elif (parameters is not None and not isinstance(parameters, tuple)
              and not hasattr(parameters, 'keys')):
            parameters = (parameters,)
        return connection.exec_driver_sql(query, parameters)
    return connection.execute(query, parameters)


@contextmanager
def _sqlalchemy_transaction(dbo, commit):
    if not commit:
        yield
        return

    if _is_sqlalchemy_session(dbo):
        transaction = dbo
    else:
        # SQLAlchemy 1.4 future mode and 2.x can autobegin on a prior read.
        transaction = (dbo.get_transaction()
                       if _hasmethod(dbo, 'get_transaction') else None)
        if transaction is None:
            transaction = dbo.begin()
    try:
        yield
    except Exception:
        transaction.rollback()
        raise
    else:
        transaction.commit()


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
                  'defaulting to qmark', mod, connection)
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

        elif mod.paramstyle == 'named':
            debug('found paramstyle named')
            placeholders = ', '.join([':%s' % name
                                      for name in names])

        else:
            debug('found unexpected paramstyle %r, defaulting to qmark',
                  mod.paramstyle)
            placeholders = ', '.join(['?'] * len(names))

    return placeholders
