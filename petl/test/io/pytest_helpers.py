# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import os
import pytest


def get_from_env(prefix, name, current, default):
    if current != default:
        return current
    varname = "{}_{}".format(prefix.upper(), name.upper())
    return os.getenv(varname, default)


def get_db_args(*prefixes):
    host = '127.0.0.1'
    user = 'petl'
    pwrd = 'test'
    base = 'petl'
    for prefix in prefixes:
        host = get_from_env(prefix, 'HOST', host, '127.0.0.1')
        user = get_from_env(prefix, 'USER', user, 'petl')
        pwrd = get_from_env(prefix, 'PASSWORD', pwrd, 'test')
        base = get_from_env(prefix, 'DATABASE', base, 'petl')
    return host, user, pwrd, base


def get_mysql_args(*prefixes):
    prefixes += ('PYMYSQL', 'MYSQL',)
    return get_db_args(*prefixes)


def get_pg_args(*prefixes):
    prefixes += ('PSYCOPG2', 'POSTGRESQL', 'POSTGRES', 'PG',)
    return get_db_args(*prefixes)


def require_package(module_name):
    return pytest.importorskip(module_name)


def skip_unless_connect(connect_callable, reason_prefix):
    try:
        return connect_callable()
    except Exception as e:
        pytest.skip('%s: %s' % (reason_prefix, e))
