"""
Tests for the petl.fluent module.

"""

from tempfile import NamedTemporaryFile
import csv
from nose.tools import eq_


import petl
from petl.interactive import etl
from petl.testutils import ieq


def test_basics():
    
    t1 = (('foo', 'bar'),
         ('A', 1),
         ('B', 2))
    w1 = etl(t1)
    
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
    
    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f, delimiter='\t')
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        writer.writerow(row)
    f.close()
    
    actual = etl().fromcsv(f.name, delimiter='\t')
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice
    
    
def test_container():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    actual = etl(table)[0]
    expect = ('foo', 'bar')
    eq_(expect, actual)
    actual = len(etl(table))
    expect = 4
    eq_(expect, actual)
    
    
    
    
