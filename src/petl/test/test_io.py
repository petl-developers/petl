"""
TODO doc me

"""

from tempfile import NamedTemporaryFile
import csv
import cPickle as pickle

from petl import fromcsv, frompickle, fromsqlite3, adler32sum, crc32sum, fromdb
import sqlite3


def assertequal(expect, actual):
    assert expect == actual, (expect, actual)


def iassertequal(expect, actual):
    for e, a in zip(expect, actual):
        assert tuple(e) == tuple(a), (e, a)


def test_fromcsv():
    """Test the fromcsv function."""
    
    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f, delimiter='\t')
    table = [['foo', 'bar'],
             ['a', 1],
             ['b', 2],
             ['c', 2]]
    for row in table:
        writer.writerow(row)
    f.close()
    
    actual = fromcsv(f.name, delimiter='\t')
    expect = [['foo', 'bar'],
              ['a', '1'],
              ['b', '2'],
              ['c', '2']]
    iassertequal(expect, actual)
    iassertequal(expect, actual) # verify can iterate twice
    
    
def test_fromcsv_cachetag():
    """Test the cachetag method on tables returned by fromcsv."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f)
    table = [['foo', 'bar'],
             ['a', 1],
             ['b', 2],
             ['c', 2]]
    for row in table:
        writer.writerow(row)
    f.close()

    # cachetag with initial data
    tbl = fromcsv(f.name)
    tag1 = tbl.cachetag()
    
    # make a change
    with open(f.name, 'wb') as f:
        writer = csv.writer(f)
        rows = [['foo', 'bar'],
                ['d', 3],
#                ['e', 5],
                ['f', 4]]
        for row in rows:
            writer.writerow(row)

    # check cachetag has changed
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)
    

def test_fromcsv_cachetag_strict():
    """Test the cachetag method on tables returned by fromcsv."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f)
    table = [['foo', 'bar'],
             ['a', 1],
             ['b', 2],
             ['c', 2]]
    for row in table:
        writer.writerow(row)
    f.close()

    # cachetag with initial data
    tbl = fromcsv(f.name, checksumfun=adler32sum)
    tag1 = tbl.cachetag()
    
    # make a change, preserving file size
    with open(f.name, 'wb') as f:
        writer = csv.writer(f)
        rows = [['foo', 'bar'],
                ['d', 3],
                ['e', 5],
                ['f', 4]]
        for row in rows:
            writer.writerow(row)

    # check cachetag has changed
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)
    

def test_frompickle():
    """Test the frompickle function."""
    
    f = NamedTemporaryFile(delete=False)
    table = [['foo', 'bar'],
             ['a', 1],
             ['b', 2],
             ['c', 2]]
    for row in table:
        pickle.dump(row, f)
    f.close()
    
    actual = frompickle(f.name)
    iassertequal(table, actual)
    iassertequal(table, actual) # verify can iterate twice
    
    
def test_frompickle_cachetag():
    """Test the cachetag method on tables returned by frompickle."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    table = [['foo', 'bar'],
             ['a', 1],
             ['b', 2],
             ['c', 2]]
    for row in table:
        pickle.dump(row, f)
    f.close()

    # cachetag with initial data
    tbl = frompickle(f.name)
    tag1 = tbl.cachetag()
    
    # make a change
    with open(f.name, 'wb') as f:
        rows = [['foo', 'bar'],
                ['d', 3],
#                ['e', 5],
                ['f', 4]]
        for row in rows:
            pickle.dump(row, f)

    # check cachetag has changed
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)
    

def test_frompickle_cachetag_strict():
    """Test the cachetag method on tables returned by frompickle."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    table = [['foo', 'bar'],
             ['a', 1],
             ['b', 2],
             ['c', 2]]
    for row in table:
        pickle.dump(row, f)
    f.close()

    # cachetag with initial data
    tbl = frompickle(f.name, checksumfun=crc32sum)
    tag1 = tbl.cachetag()
    
    # make a change, preserving file size
    with open(f.name, 'wb') as f:
        rows = [['foo', 'bar'],
                ['d', 3],
                ['e', 5],
                ['f', 4]]
        for row in rows:
            pickle.dump(row, f)

    # check cachetag has changed
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)
    

def test_fromsqlite3():
    """Test the fromsqlite3 function."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    data = [['a', 1],
            ['b', 2],
            ['c', 2.0]]
    connection = sqlite3.connect(f.name)
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()
    
    # test the function
    actual = fromsqlite3(f.name, 'select * from foobar')
    expect = [['foo', 'bar'],
              ['a', 1],
              ['b', 2],
              ['c', 2.0]]
    iassertequal(expect, actual)
    iassertequal(expect, actual) # verify can iterate twice


def test_fromsqlite3_cachetag():
    """Test the fromsqlite3 cachetag function."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    data = [['a', 1],
            ['b', 2],
            ['c', 2.0]]
    connection = sqlite3.connect(f.name)
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()
    
    # test the function
    tbl = fromsqlite3(f.name, 'select * from foobar')
    tag1 = tbl.cachetag()
    
    # update the data
    modata = [['d', 1],
              ['e', 2],
              ['f', 2.0]]
    c = connection.cursor()
    for i in range(100):
        for row in modata:
            c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()
    
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)

    
def test_fromsqlite3_cachetag_strict():
    """Test the fromsqlite3 cachetag function under strict conditions."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    data = [['a', 1],
            ['b', 2],
            ['c', 2.0]]
    connection = sqlite3.connect(f.name)
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()
    
    # test the function
    tbl = fromsqlite3(f.name, 'select * from foobar', checksumfun=adler32sum)
    tag1 = tbl.cachetag()
    
    # update the data
    connection.execute('update foobar set bar = ? where foo = ?', (42, 'a'))
    connection.commit()
    
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)
    
    
def test_fromdb():
    """Test the fromdb function."""
    
    # initial data
    data = [['a', 1],
            ['b', 2],
            ['c', 2.0]]
    connection = sqlite3.connect(':memory:')
    c = connection.cursor()
    c.execute('create table foobar (foo, bar)')
    for row in data:
        c.execute('insert into foobar values (?, ?)', row)
    connection.commit()
    c.close()
    
    # test the function
    actual = fromdb(connection, 'select * from foobar')
    expect = [['foo', 'bar'],
              ['a', 1],
              ['b', 2],
              ['c', 2.0]]
    iassertequal(expect, actual)
    iassertequal(expect, actual) # verify can iterate twice



    
