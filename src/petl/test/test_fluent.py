"""
Tests for the petl.fluent module.

"""


from nose.tools import eq_
from petl.fluent import FluentWrapper
import petl
from petl.testutils import iassertequal


def test_basics():
    
    t1 = (('foo', 'bar'),
         ('A', 1),
         ('B', 2))
    w1 = FluentWrapper(t1)
    
    eq_(('foo', 'bar'), w1.header())
    eq_(petl.header(w1), w1.header())
    iassertequal((('A', 1), ('B', 2)), w1.data())
    iassertequal(petl.data(w1), w1.data())
    
    w2 = w1.cut('bar', 'foo')
    expect2 = (('bar', 'foo'),
               (1, 'A'),
               (2, 'B'))
    iassertequal(expect2, w2)
    iassertequal(petl.cut(w1, 'bar', 'foo'), w2)
    
    w3 = w1.cut('bar', 'foo').cut('foo', 'bar')
    iassertequal(t1, w3)