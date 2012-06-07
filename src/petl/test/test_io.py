"""
Tests for the petl.io module.

"""

from tempfile import NamedTemporaryFile
import csv
import cPickle as pickle
import sqlite3
from nose.tools import eq_

from petl import fromcsv, frompickle, fromsqlite3, adler32sum, crc32sum, fromdb, \
                tocsv, topickle, appendcsv, appendpickle, tosqlite3, appendsqlite3, \
                todb, appenddb, fromtext, totext, fromxml, fromjson, fromdicts, \
                tojson, fromtsv, totsv, appendtsv
                

from petl.testutils import ieq, assertequal
import json
import gzip
import os
from petl.io import FileSource


def test_fromcsv():
    """Test the fromcsv function."""
    
    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f, delimiter='\t')
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        writer.writerow(row)
    f.close()
    
    actual = fromcsv(f.name, delimiter='\t')
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    
    
def test_fromcsv_cachetag():
    """Test the cachetag method on tables returned by fromcsv."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f)
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        writer.writerow(row)
    f.close()

    # cachetag with initial data
    tbl = fromcsv(f.name)
    tag1 = tbl.cachetag()
    
    # make a change
    with open(f.name, 'wb') as o:
        writer = csv.writer(o)
        rows = (('foo', 'bar'),
                ('d', 3),
#                ('e', 5),
                ('f', 4))
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
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        writer.writerow(row)
    f.close()

    # cachetag with initial data
    tbl = fromcsv(FileSource(f.name, checksumfun=adler32sum))
    tag1 = tbl.cachetag()
    
    # make a change, preserving file size
    with open(f.name, 'wb') as o:
        writer = csv.writer(o)
        rows = (('foo', 'bar'),
                ('d', 3),
                ('e', 5),
                ('f', 4))
        for row in rows:
            writer.writerow(row)

    # check cachetag has changed
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)
    

def test_fromtsv():
    
    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f, delimiter='\t')
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        writer.writerow(row)
    f.close()
    
    actual = fromtsv(f.name)
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    
    
def test_frompickle():
    """Test the frompickle function."""
    
    f = NamedTemporaryFile(delete=False)
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        pickle.dump(row, f)
    f.close()
    
    actual = frompickle(f.name)
    ieq(table, actual)
    ieq(table, actual) # verify can iterate twice
    
    
def test_frompickle_cachetag():
    """Test the cachetag method on tables returned by frompickle."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        pickle.dump(row, f)
    f.close()

    # cachetag with initial data
    tbl = frompickle(f.name)
    tag1 = tbl.cachetag()
    
    # make a change
    with open(f.name, 'wb') as o:
        rows = (('foo', 'bar'),
                ('d', 3),
#                ('e', 5),
                ('f', 4))
        for row in rows:
            pickle.dump(row, o)

    # check cachetag has changed
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)
    

def test_frompickle_cachetag_strict():
    """Test the cachetag method on tables returned by frompickle."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        pickle.dump(row, f)
    f.close()

    # cachetag with initial data
    tbl = frompickle(FileSource(f.name, checksumfun=crc32sum))
    tag1 = tbl.cachetag()
    
    # make a change, preserving file size
    with open(f.name, 'wb') as o:
        rows = (('foo', 'bar'),
                ('d', 3),
                ('e', 5),
                ('f', 4))
        for row in rows:
            pickle.dump(row, o)

    # check cachetag has changed
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)
    

def test_fromsqlite3():
    """Test the fromsqlite3 function."""
    
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
    print list(actual)
    ieq(expect, actual, cast=tuple)
    ieq(expect, actual, cast=tuple) # verify can iterate twice


def test_fromsqlite3_cachetag():
    """Test the fromsqlite3 cachetag function."""
    
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
    
    # test the function
    tbl = fromsqlite3(f.name, 'select * from foobar')
    tag1 = tbl.cachetag()
    
    # update the data
    modata = (('d', 1),
              ('e', 2),
              ('f', 2.0))
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
    ieq(expect, actual) # verify can iterate twice

    # test iterators are isolated
    i1 = iter(actual)
    i2 = iter(actual)
    eq_(('foo', 'bar'), i1.next())
    eq_(('a', 1), i1.next())
    eq_(('foo', 'bar'), i2.next())
    eq_(('b', 2), i1.next())


def test_fromtext():
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    f.write('foo\tbar\n')
    f.write('a\t1\n')
    f.write('b\t2\n')
    f.write('c\t3\n')
    f.close()
    
    actual = fromtext(f.name)
    expect = (('lines',),
              ('foo\tbar',),
              ('a\t1',),
              ('b\t2',),
              ('c\t3',))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


def test_fromxml():
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    data = """<table>
    <tr>
        <td>foo</td><td>bar</td>
    </tr>
    <tr>
        <td>a</td><td>1</td>
    </tr>
    <tr>
        <td>b</td><td>2</td>
    </tr>
    <tr>
        <td>c</td><td>2</td>
    </tr>
</table>"""
    f.write(data)
    f.close()
    
    actual = fromxml(f.name, 'tr', 'td')
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


def test_fromxml_2():
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    data = """<table>
    <tr>
        <td v='foo'/><td v='bar'/>
    </tr>
    <tr>
        <td v='a'/><td v='1'/>
    </tr>
    <tr>
        <td v='b'/><td v='2'/>
    </tr>
    <tr>
        <td v='c'/><td v='2'/>
    </tr>
</table>"""
    f.write(data)
    f.close()
    
    actual = fromxml(f.name, 'tr', 'td', 'v')
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


def test_fromxml_3():
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    data = """<table>
    <row>
        <foo>a</foo><baz><bar v='1'/></baz>
    </row>
    <row>
        <foo>b</foo><baz><bar v='2'/></baz>
    </row>
    <row>
        <foo>c</foo><baz><bar v='2'/></baz>
    </row>
</table>"""
    f.write(data)
    f.close()
    
    actual = fromxml(f.name, 'row', {'foo': 'foo', 'bar': ('baz/bar', 'v')})
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


def test_fromxml_4():
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    data = """<table>
    <row>
        <foo>a</foo><baz><bar>1</bar><bar>3</bar></baz>
    </row>
    <row>
        <foo>b</foo><baz><bar>2</bar></baz>
    </row>
    <row>
        <foo>c</foo><baz><bar>2</bar></baz>
    </row>
</table>"""
    f.write(data)
    f.close()
    
    actual = fromxml(f.name, 'row', {'foo': 'foo', 'bar': './/bar'})
    expect = (('foo', 'bar'),
              ('a', ('1', '3')),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


def test_fromxml_5():
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    data = """<table>
    <row>
        <foo>a</foo><baz><bar v='1'/><bar v='3'/></baz>
    </row>
    <row>
        <foo>b</foo><baz><bar v='2'/></baz>
    </row>
    <row>
        <foo>c</foo><baz><bar v='2'/></baz>
    </row>
</table>"""
    f.write(data)
    f.close()
    
    actual = fromxml(f.name, 'row', {'foo': 'foo', 'bar': ('baz/bar', 'v')})
    expect = (('foo', 'bar'),
              ('a', ('1', '3')),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


def test_fromjson_1():
    
    f = NamedTemporaryFile(delete=False)
    data = '[{"foo": "a", "bar": 1}, {"foo": "b", "bar": 2}, {"foo": "c", "bar": 2}]'
    f.write(data)
    f.close()
    
    actual = fromjson(f.name)
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    

def test_fromjson_2():
    
    f = NamedTemporaryFile(delete=False)
    data = '[{"foo": "a", "bar": 1}, {"foo": "b"}, {"foo": "c", "bar": 2, "baz": true}]'
    f.write(data)
    f.close()
    
    actual = fromjson(f.name)
    expect = (('foo', 'bar', 'baz'),
              ('a', 1, None),
              ('b', None, None),
              ('c', 2, True))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    

def test_fromjson_3():
    
    f = NamedTemporaryFile(delete=False)
    data = '[{"foo": "a", "bar": 1}, {"foo": "b"}, {"foo": "c", "bar": 2, "baz": true}]'
    f.write(data)
    f.close()
    
    actual = fromjson(f.name, header=['foo', 'bar'])
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', None),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    

def test_fromdicts_1():
    
    data = [{"foo": "a", "bar": 1}, {"foo": "b", "bar": 2}, {"foo": "c", "bar": 2}]
    actual = fromdicts(data)
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    

def test_fromdicts_2():
    
    data = [{"foo": "a", "bar": 1}, {"foo": "b"}, {"foo": "c", "bar": 2, "baz": True}]
    actual = fromdicts(data)
    expect = (('foo', 'bar', 'baz'),
              ('a', 1, None),
              ('b', None, None),
              ('c', 2, True))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    

def test_fromdicts_3():
    
    data = [{"foo": "a", "bar": 1}, {"foo": "b"}, {"foo": "c", "bar": 2, "baz": True}]
    actual = fromdicts(data, header=['foo', 'bar'])
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', None),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    

def test_tocsv_appendcsv():
    """Test the tocsv and appendcsv function."""
    
    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    tocsv(table, f.name, delimiter='\t')
    
    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2']]
        ieq(expect, actual)
    
    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendcsv(table2, f.name, delimiter='\t') 

    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2'],
                  ['d', '7'],
                  ['e', '9'],
                  ['f', '1']]
        ieq(expect, actual)
    
        
def test_totsv_appendtsv():
    
    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    totsv(table, f.name)
    
    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2']]
        ieq(expect, actual)
    
    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendtsv(table2, f.name) 

    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2'],
                  ['d', '7'],
                  ['e', '9'],
                  ['f', '1']]
        ieq(expect, actual)
    
    
def test_topickle_appendpickle():
    """Test the topickle and appendpickle functions."""
    
    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    topickle(table, f.name)
    
    def picklereader(file):
        try:
            while True:
                yield pickle.load(file)
        except EOFError:
            pass

    # check what it did
    with open(f.name, 'rb') as o:
        actual = picklereader(o)
        ieq(table, actual)
    
    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendpickle(table2, f.name) 

    # check what it did
    with open(f.name, 'rb') as o:
        actual = picklereader(o)
        expect = (('foo', 'bar'),
                  ('a', 1),
                  ('b', 2),
                  ('c', 2),
                  ('d', 7),
                  ('e', 9),
                  ('f', 1))
        ieq(expect, actual)
    
        
def test_tosqlite3_appendsqlite3():
    """Test the tosqlite3 and appendsqlite3 functions."""
    
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
    
        
def test_tosqlite3_identifiers():
    """Test the tosqlite3 function with funky table and field names."""
    
    # exercise function
    table = (('foo foo', 'bar.baz.spong"'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    tosqlite3(table, f.name, 'foo bar"', create=True)
    
    # check what it did
    conn = sqlite3.connect(f.name)
    actual = conn.execute('select * from "foo bar"')
    expect = (('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)


# TODO test uneven rows
    
    
def test_todb_appenddb():
    """Test the todb and appenddb functions."""
    
    f = NamedTemporaryFile(delete=False)
    conn = sqlite3.connect(f.name)
    conn.execute('create table foobar (foo, bar)')
    conn.commit()

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    todb(table, conn, 'foobar', truncate=False) # sqlite doesn't support TRUNCATE
    
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
    
        
def test_totext():
    
    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    prologue = """{| class="wikitable"
|-
! foo
! bar
"""
    template = """|-
| {foo}
| {bar}
"""
    epilogue = "|}"
    totext(table, f.name, template, prologue, epilogue)
    
    # check what it did
    with open(f.name, 'rb') as o:
        actual = o.read()
        expect = """{| class="wikitable"
|-
! foo
! bar
|-
| a
| 1
|-
| b
| 2
|-
| c
| 2
|}"""
        assertequal(expect, actual)
    
    
def test_tojson():
    
    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    tojson(table, f.name)
    result = json.load(f)
    assert len(result) == 3
    assert result[0]['foo'] == 'a'
    assert result[0]['bar'] == 1
    assert result[1]['foo'] == 'b'
    assert result[1]['bar'] == 2
    assert result[2]['foo'] == 'c'
    assert result[2]['bar'] == 2
    

def test_fromcsv_gz():
    """Test the fromcsv function on a gzipped file."""
    
    f = NamedTemporaryFile(delete=False)
    f.close()
    fn = f.name + '.gz'
    os.rename(f.name, fn)

    fz = gzip.open(fn, 'wb')
    writer = csv.writer(fz, delimiter='\t')
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        writer.writerow(row)
    fz.close()
    
    actual = fromcsv(fn, delimiter='\t')
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    
    
def test_tocsv_appendcsv_gz():
    """Test the tocsv and appendcsv function."""
    
    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    fn = f.name + '.gz'
    f.close()
    tocsv(table, fn, delimiter='\t')
    
    # check what it did
    with gzip.open(fn, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2']]
        ieq(expect, actual)
    
    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendcsv(table2, fn, delimiter='\t') 

    # check what it did
    with gzip.open(fn, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2'],
                  ['d', '7'],
                  ['e', '9'],
                  ['f', '1']]
        ieq(expect, actual)
    
        
def test_fromtext_gz():
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    f.close()
    fn = f.name + '.gz'
    os.rename(f.name, fn)
    with gzip.open(fn, 'wb') as f:
        f.write('foo\tbar\n')
        f.write('a\t1\n')
        f.write('b\t2\n')
        f.write('c\t3\n')
    
    actual = fromtext(fn)
    expect = (('lines',),
              ('foo\tbar',),
              ('a\t1',),
              ('b\t2',),
              ('c\t3',))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


def test_totext_gz():
    
    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    fn = f.name + '.gz'
    f.close()
    os.rename(f.name, fn)
    prologue = """{| class="wikitable"
|-
! foo
! bar
"""
    template = """|-
| {foo}
| {bar}
"""
    epilogue = "|}"
    totext(table, fn, template, prologue, epilogue)
    
    # check what it did
    with gzip.open(fn, 'rb') as o:
        actual = o.read()
        expect = """{| class="wikitable"
|-
! foo
! bar
|-
| a
| 1
|-
| b
| 2
|-
| c
| 2
|}"""
        assertequal(expect, actual)
    
    
