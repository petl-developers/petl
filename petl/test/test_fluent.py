from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import csv
from petl.compat import PY2


import petl as etl
from petl.test.helpers import ieq, eq_


def test_basics():
    
    t1 = (('foo', 'bar'),
          ('A', 1),
          ('B', 2))
    w1 = etl.wrap(t1)
    
    eq_(('foo', 'bar'), w1.header())
    eq_(etl.header(w1), w1.header())
    ieq((('A', 1), ('B', 2)), w1.data())
    ieq(etl.data(w1), w1.data())
    
    w2 = w1.cut('bar', 'foo')
    expect2 = (('bar', 'foo'),
               (1, 'A'),
               (2, 'B'))
    ieq(expect2, w2)
    ieq(etl.cut(w1, 'bar', 'foo'), w2)
    
    w3 = w1.cut('bar', 'foo').cut('foo', 'bar')
    ieq(t1, w3)
    
    
def test_staticmethods():
    
    data = [b'foo,bar',
            b'a,1',
            b'b,2',
            b'c,2']
    f = NamedTemporaryFile(mode='wb', delete=False)
    f.write(b'\n'.join(data))
    f.close()

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    actual = etl.fromcsv(f.name, encoding='ascii')
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_container():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    actual = etl.wrap(table)[0]
    expect = ('foo', 'bar')
    eq_(expect, actual)
    actual = etl.wrap(table)['bar']
    expect = (1, 2, 2)
    ieq(expect, actual)
    actual = len(etl.wrap(table))
    expect = 4
    eq_(expect, actual)
    
    
def test_values_container_convenience_methods():
    table = etl.wrap((('foo', 'bar'),
                      ('a', 1),
                      ('b', 2),
                      ('c', 2)))
    
    actual = table.values('foo').set()
    expect = {'a', 'b', 'c'}
    eq_(expect, actual)
    
    actual = table.values('foo').list()
    expect = ['a', 'b', 'c']
    eq_(expect, actual)
    
    actual = table.values('foo').tuple()
    expect = ('a', 'b', 'c')
    eq_(expect, actual)
    
    actual = table.values('bar').sum()
    expect = 5
    eq_(expect, actual)
    
    actual = table.data().dict()
    expect = {'a': 1, 'b': 2, 'c': 2}
    eq_(expect, actual)
    
    
def test_empty():

    actual = (
        etl
        .empty()
        .addcolumn('foo', ['a', 'b', 'c'])
        .addcolumn('bar', [1, 2, 2])
    )
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)


def test_wrap_tuple_return():
    tablea = etl.wrap((('foo', 'bar'),
                       ('A', 1),
                       ('C', 7)))
    tableb = etl.wrap((('foo', 'bar'),
                       ('B', 5),
                       ('C', 7)))

    added, removed = tablea.diff(tableb)
    eq_(('foo', 'bar'), added.header())
    eq_(('foo', 'bar'), removed.header())
    ieq(etl.data(added), added.data())
    ieq(etl.data(removed), removed.data())
