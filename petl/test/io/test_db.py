# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import sqlite3
from tempfile import NamedTemporaryFile
from petl.compat import next


from petl.test.helpers import ieq, eq_
from petl.io.db import fromdb, todb, appenddb


# N.B., this file only tests the DB-related functions using sqlite3,
# as anything else requires database connection configuration. See
# docs/dbtests.py for a script to exercise the DB-related functions with
# MySQL and Postgres.


def test_fromdb():

    # initial data
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(':memory:')
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    actual = fromdb(connection, 'select * from foobar')
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2.0))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice

    # test iterators are isolated
    i1 = iter(actual)
    i2 = iter(actual)
    eq_(('foo', 'bar'), next(i1))
    eq_(('a', 1), next(i1))
    eq_(('foo', 'bar'), next(i2))
    eq_(('b', 2), next(i1))


def test_fromdb_mkcursor():

    # initial data
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(':memory:')
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    mkcursor = lambda: connection.cursor()
    actual = fromdb(mkcursor, 'select * from foobar')
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2.0))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice

    # test iterators are isolated
    i1 = iter(actual)
    i2 = iter(actual)
    eq_(('foo', 'bar'), next(i1))
    eq_(('a', 1), next(i1))
    eq_(('foo', 'bar'), next(i2))
    eq_(('b', 2), next(i1))


def test_fromdb_withargs():

    # initial data
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(':memory:')
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    actual = fromdb(
        connection,
        'select * from foobar where bar > ? and bar < ?',
        (1, 3)
    )
    expect = (('foo', 'bar'),
              ('b', 2),
              ('c', 2.0))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


def test_todb_appenddb():

    f = NamedTemporaryFile(delete=False)
    conn = sqlite3.connect(f.name)
    conn.execute('create table foobar (foo, bar)')
    conn.commit()

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    todb(table, conn, 'foobar')

    # check what it did
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)

    # try appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appenddb(table2, conn, 'foobar')

    # check what it did
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    ieq(expect, actual)


def test_todb_appenddb_cursor():

    f = NamedTemporaryFile(delete=False)
    conn = sqlite3.connect(f.name)
    conn.execute('create table foobar (foo, bar)')
    conn.commit()

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    cursor = conn.cursor()
    todb(table, cursor, 'foobar')

    # check what it did
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)

    # try appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appenddb(table2, cursor, 'foobar')

    # check what it did
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    ieq(expect, actual)
