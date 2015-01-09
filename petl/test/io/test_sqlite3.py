from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import sqlite3


from petl.test.helpers import ieq
from petl.io.sqlite3 import fromsqlite3, tosqlite3, appendsqlite3


def test_fromsqlite3():

    # initial data
    f = NamedTemporaryFile(delete=False)
    data = (('a', 1),
            ('b', 2),
            ('c', 2.0))
    connection = sqlite3.connect(f.name)
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()
    connection.close()

    # test the function
    actual = fromsqlite3(f.name, 'select * from foobar')
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
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    actual = fromsqlite3(connection, 'select * from foobar')
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
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()

    # test the function
    actual = fromsqlite3(
        connection,
        'select * from foobar where bar > ? and bar < ?',
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
    tosqlite3(table, f.name, 'foobar', create=True)

    # check what it did
    conn = sqlite3.connect(f.name)
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendsqlite3(table2, f.name, 'foobar')

    # check what it did
    conn = sqlite3.connect(f.name)
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    ieq(expect, actual)


def test_tosqlite3_appendsqlite3_connection():

    conn = sqlite3.connect(':memory:')

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    tosqlite3(table, conn, 'foobar', create=True)

    # check what it did
    actual = conn.execute('select * from foobar')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendsqlite3(table2, conn, 'foobar')

    # check what it did
    actual = conn.execute('select * from foobar')
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
    tosqlite3(table, f.name, 'foo " bar`', create=True)

    # check what it did
    conn = sqlite3.connect(f.name)
    actual = conn.execute('select * from `foo " bar```')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)


# TODO test uneven rows
