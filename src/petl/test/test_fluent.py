"""
Tests for the petl.fluent module.

"""


from nose.tools import eq_


import petl
from ..fluent import FluentWrapper
from ..testutils import ieq


def test_basics():
    
    t1 = (('foo', 'bar'),
         ('A', 1),
         ('B', 2))
    w1 = FluentWrapper(t1)
    
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