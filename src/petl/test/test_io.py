# -*- coding: utf-8 -*-


"""
Tests for the petl.io module.

"""


from tempfile import NamedTemporaryFile
import csv
import cPickle as pickle
import sqlite3
from nose.tools import eq_
import json
import gzip
import os


from petl import fromcsv, frompickle, fromsqlite3, fromdb, \
    tocsv, topickle, appendcsv, appendpickle, tosqlite3, appendsqlite3, \
    todb, appenddb, fromtext, totext, fromxml, fromjson, fromdicts, \
    tojson, fromtsv, totsv, appendtsv, tojsonarrays, tohtml, nrows, StringSource, PopenSource, cut, ZipSource
from petl.testutils import ieq


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


def test_fromsqlite3_connection():
    """Test the fromsqlite3 function."""
    
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
    print list(actual)
    ieq(expect, actual, cast=tuple)
    ieq(expect, actual, cast=tuple) # verify can iterate twice


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
    actual = fromsqlite3(connection, 'select * from foobar where bar > ? and bar < ?', (1, 3))
    expect = (('foo', 'bar'),
              ('b', 2),
              ('c', 2.0))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


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
    ieq(expect, actual) # verify can iterate twice

    # test iterators are isolated
    i1 = iter(actual)
    i2 = iter(actual)
    eq_(('foo', 'bar'), i1.next())
    eq_(('a', 1), i1.next())
    eq_(('foo', 'bar'), i2.next())
    eq_(('b', 2), i1.next())


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
    actual = fromdb(connection, 'select * from foobar where bar > ? and bar < ?', (1, 3))
    expect = (('foo', 'bar'),
              ('b', 2),
              ('c', 2.0))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


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
    
    print open(f.name).read()
    actual = fromxml(f.name, 'tr', 'td', 'v')
    print actual
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
    
    def picklereader(fl):
        try:
            while True:
                yield pickle.load(fl)
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
        eq_(expect, actual)
    
    
def test_tohtml():
    
    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', (1, 2)),
             ('c', False))

    f = NamedTemporaryFile(delete=False)
    tohtml(table, f.name, lineterminator='\n')
    
    # check what it did
    with open(f.name, 'rb') as o:
        actual = o.read()
        expect = """<table class='petl'>
<thead>
<tr>
<th>foo</th>
<th>bar</th>
</tr>
</thead>
<tbody>
<tr>
<td>a</td>
<td style='text-align: right'>1</td>
</tr>
<tr>
<td>b</td>
<td>(1, 2)</td>
</tr>
<tr>
<td>c</td>
<td>False</td>
</tr>
</tbody>
</table>
"""
        eq_(expect, actual)
    
    
def test_tohtml_caption():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', (1, 2)))
    f = NamedTemporaryFile(delete=False)
    tohtml(table, f.name, caption='my table', lineterminator='\n')

    # check what it did
    with open(f.name, 'rb') as o:
        actual = o.read()
        expect = """<table class='petl'>
<caption>my table</caption>
<thead>
<tr>
<th>foo</th>
<th>bar</th>
</tr>
</thead>
<tbody>
<tr>
<td>a</td>
<td style='text-align: right'>1</td>
</tr>
<tr>
<td>b</td>
<td>(1, 2)</td>
</tr>
</tbody>
</table>
"""
        eq_(expect, actual)


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
    

def test_tojsonarrays():
    
    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    tojsonarrays(table, f.name)
    result = json.load(f)
    assert len(result) == 3
    assert result[0][0] == 'a'
    assert result[0][1] == 1
    assert result[1][0] == 'b'
    assert result[1][1] == 2
    assert result[2][0] == 'c'
    assert result[2][1] == 2
    

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
    o = gzip.open(fn, 'rb')
    try:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2']]
        ieq(expect, actual)
    finally:
        o.close()

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendcsv(table2, fn, delimiter='\t') 

    # check what it did
    o = gzip.open(fn, 'rb')
    try:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2'],
                  ['d', '7'],
                  ['e', '9'],
                  ['f', '1']]
        ieq(expect, actual)
    finally:
        o.close()
        
def test_fromtext_gz():
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    f.close()
    fn = f.name + '.gz'
    os.rename(f.name, fn)
    f = gzip.open(fn, 'wb')
    try:
        f.write('foo\tbar\n')
        f.write('a\t1\n')
        f.write('b\t2\n')
        f.write('c\t3\n')
    finally:
        f.close()

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
    o = gzip.open(fn, 'rb')
    try:
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
        eq_(expect, actual)
    finally:
        o.close()
    
    
def test_StringSource():
    
    table1 = (('foo', 'bar'),
             ('a', '1'),
             ('b', '2'),
             ('c', '2'))

    # test writing to a string buffer
    ss = StringSource()
    tocsv(table1, ss)
    expect = "foo,bar\r\na,1\r\nb,2\r\nc,2\r\n"
    actual = ss.getvalue()
    eq_(expect, actual)

    # test reading from a string buffer
    table2 = fromcsv(StringSource(actual))
    ieq(table1, table2)
    ieq(table1, table2)

    # test appending
    appendcsv(table1, ss)
    actual = ss.getvalue()
    expect = "foo,bar\r\na,1\r\nb,2\r\nc,2\r\na,1\r\nb,2\r\nc,2\r\n"
    eq_(expect, actual)


def test_fromxml_url():

    tbl = fromxml('http://feeds.bbci.co.uk/news/rss.xml', './/item', 'title')
    print tbl
    assert nrows(tbl) > 0


def test_PopenSource():

    expect = (('foo', 'bar'),
              ('a', '1'))
    actual = fromcsv(PopenSource(r'echo -e foo bar\\na 1', shell=True, executable='/bin/bash'), delimiter=' ')
    ieq(expect, actual)


def test_issue_231():

    table = [['foo', 'bar'], ['a', '1'], ['b', '2']]
    t = cut(table, 'foo')
    totsv(t, 'tmp/issue_231.tsv')
    u = fromtsv('tmp/issue_231.tsv')
    ieq(t, u)
    tocsv(t, 'tmp/issue_231.csv')
    u = fromcsv('tmp/issue_231.csv')
    ieq(t, u)
    topickle(t, 'tmp/issue_231.pickle')
    u = frompickle('tmp/issue_231.pickle')
    ieq(t, u)


import zipfile


def test_ZipSource():

    # setup
    table = [('foo', 'bar'), ('a', '1'), ('b', '2')]
    totsv(table, 'tmp/issue_241.tsv')
    z = zipfile.ZipFile('tmp/issue_241.zip', mode='w')
    z.write('tmp/issue_241.tsv', 'data.tsv')
    z.close()

    # test
    actual = fromtsv(ZipSource('tmp/issue_241.zip', 'data.tsv'))
    ieq(table, actual)
