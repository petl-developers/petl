# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import sqlite3


from petl.test.helpers import ieq
from petl.io.db import fromdb, todb, appenddb


def test_fromsqlite3():

    # initial data
    f = NamedTemporaryFile(delete=False)
    f.close()
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(f.name)
    c = connection.cursor()
    c.execute('CREATE TABLE foobar (foo, bar)')
    for row in data:
        c.execute('INSERT INTO foobar VALUES (?, ?)', row)
    connection.commit()
    c.close()
    connection.close()

    # test the function
    actual = fromdb(f.name, 'SELECT * FROM foobar')
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2.0))

    ieq(expect, actual, cast=tuple)
    ieq(expect, actual, cast=tuple)  # verify can iterate twice


def test_fromsqlite3_connection():

    # initial data
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(':memory:')
    c = connection.cursor()
    c.execute('CREATE TABLE foobar (foo, bar)')
    for row in data:
        c.execute('INSERT INTO foobar VALUES (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    actual = fromdb(connection, 'SELECT * FROM foobar')
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2.0))

    ieq(expect, actual, cast=tuple)
    ieq(expect, actual, cast=tuple)  # verify can iterate twice


def test_fromsqlite3_withargs():

    # initial data
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(':memory:')
    c = connection.cursor()
    c.execute('CREATE TABLE foobar (foo, bar)')
    for row in data:
        c.execute('INSERT INTO foobar VALUES (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    actual = fromdb(
        connection,
        'SELECT * FROM foobar WHERE bar > ? AND bar < ?',
        (1, 3)
    )
    expect = (('foo', 'bar'),
              ('b', 2),
              ('c', 2.0))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_tosqlite3_appendsqlite3():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    f.close()
    conn = sqlite3.connect(f.name)
    conn.execute('CREATE TABLE foobar (foo TEXT, bar INT)')
    conn.close()
    todb(table, f.name, 'foobar')

    # check what it did
    conn = sqlite3.connect(f.name)
    actual = conn.execute('SELECT * FROM foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appenddb(table2, f.name, 'foobar')

    # check what it did
    conn = sqlite3.connect(f.name)
    actual = conn.execute('SELECT * FROM foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    ieq(expect, actual)


def test_tosqlite3_appendsqlite3_connection():

    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE foobar (foo TEXT, bar INT)')

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    todb(table, conn, 'foobar')

    # check what it did
    actual = conn.execute('SELECT * FROM foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appenddb(table2, conn, 'foobar')

    # check what it did
    actual = conn.execute('SELECT * FROM foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    ieq(expect, actual)


def test_tosqlite3_identifiers():

    # exercise function
    table = (('foo foo', 'bar.baz.spong`'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    f.close()
    conn = sqlite3.connect(f.name)
    conn.execute('CREATE TABLE "foo "" bar`" '
                 '("foo foo" TEXT, "bar.baz.spong`" INT)')
    conn.close()
    todb(table, f.name, 'foo " bar`')

    # check what it did
    conn = sqlite3.connect(f.name)
    actual = conn.execute('SELECT * FROM `foo " bar```')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)


# TODO test uneven rows
