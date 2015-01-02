"""
Tests for the petl.fluent module.

"""

from __future__ import absolute_import, print_function, division, \
    unicode_literals


from tempfile import NamedTemporaryFile
import csv
from petl.compat import PY3


import petl
import petl.fluent as etl
from petl.testutils import ieq, eq_


def test_basics():
    
    t1 = (('foo', 'bar'),
          ('A', 1),
          ('B', 2))
    w1 = etl.wrap(t1)
    
    eq_(('foo', 'bar'), w1.header())
    eq_(petl.header(w1), w1.header())
    ieq((('A', 1), ('B', 2)), w1.data())
    ieq(petl.data(w1), w1.data())
    
    w2 = w1.cut('bar', 'foo')
    expect2 = (('bar', 'foo'),
               (1, 'A'),
               (2, 'B'))
    ieq(expect2, w2)
    ieq(petl.cut(w1, 'bar', 'foo'), w2)
    
    w3 = w1.cut('bar', 'foo').cut('foo', 'bar')
    ieq(t1, w3)
    
    
def test_staticmethods():
    
    if PY3:
        mode = 'w'
    else:
        mode = 'wb'
    f = NamedTemporaryFile(mode=mode, delete=False)
    writer = csv.writer(f, delimiter='\t')
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        writer.writerow(row)
    f.close()
    
    actual = etl.fromcsv(f.name, delimiter='\t')
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
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
    expect = set(['a', 'b', 'c'])
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
    ieq(petl.data(added), added.data())
    ieq(petl.data(removed), removed.data())
