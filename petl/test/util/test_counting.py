from __future__ import absolute_import, print_function, division


from petl.compat import PY2
from petl.test.helpers import ieq, eq_
from petl.util.counting import valuecount, valuecounter, valuecounts, \
    rowlengths, typecounts, parsecounts, stringpatterns, nrows


def test_nrows():
    table = (('foo', 'bar'), ('a', 1), ('b',))
    actual = nrows(table)
    expect = 2
    eq_(expect, actual)


def test_valuecount():

    table = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 7))
    n, f = valuecount(table, 'foo', 'b')
    eq_(2, n)
    eq_(2./3, f)


def test_valuecounter():

    table = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 7))
    actual = valuecounter(table, 'foo')
    expect = {'b': 2, 'a': 1}
    eq_(expect, actual)


def test_valuecounter_shortrows():

    table = (('foo', 'bar'), ('a', 7), ('b',), ('b', 7))
    actual = valuecounter(table, 'foo')
    expect = {'b': 2, 'a': 1}
    eq_(expect, actual)
    actual = valuecounter(table, 'bar')
    expect = {7: 2, None: 1}
    eq_(expect, actual)
    actual = valuecounter(table, 'foo', 'bar')
    expect = {('a', 7): 1, ('b', None): 1, ('b', 7): 1}
    eq_(expect, actual)


def test_valuecounts():

    table = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 7))
    actual = valuecounts(table, 'foo')
    expect = (('foo', 'count', 'frequency'), ('b', 2, 2./3), ('a', 1, 1./3))
    ieq(expect, actual)
    ieq(expect, actual)


def test_valuecounts_shortrows():

    table = (('foo', 'bar'),
             ('a', True),
             ('x', True),
             ('b',),
             ('b', True),
             ('c', False),
             ('z', False))
    actual = valuecounts(table, 'bar')
    expect = (('bar', 'count', 'frequency'),
              (True, 3, 3./6),
              (False, 2, 2./6),
              (None, 1, 1./6))
    ieq(expect, actual)
    ieq(expect, actual)


def test_valuecounts_multifields():

    table = (('foo', 'bar', 'baz'),
             ('a', True, .12),
             ('a', True, .17),
             ('b', False, .34),
             ('b', False, .44),
             ('b',),
             ('b', False, .56))
    actual = valuecounts(table, 'foo', 'bar')
    expect = (('foo', 'bar', 'count', 'frequency'),
              ('b', False, 3, 3./6),
              ('a', True, 2, 2./6),
              ('b', None, 1, 1./6))
    ieq(expect, actual)
    ieq(expect, actual)


def test_rowlengths():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '3', '7.8', True),
             ('D', 'xyz', 9.0),
             ('E', None),
             ('F', 9))
    actual = rowlengths(table)
    expect = (('length', 'count'), (3, 3), (2, 2), (4, 1))
    ieq(expect, actual)


def test_typecounts():

    table = (('foo', 'bar', 'baz'),
             (b'A', 1, 2.),
             (b'B', u'2', 3.4),
             (u'B', u'3', 7.8, True),
             (b'D', u'xyz', 9.0),
             (b'E', 42))

    actual = typecounts(table, 'foo')
    if PY2:
        expect = (('type', 'count', 'frequency'),
                  ('str', 4, 4./5),
                  ('unicode', 1, 1./5))
    else:
        expect = (('type', 'count', 'frequency'),
                  ('bytes', 4, 4./5),
                  ('str', 1, 1./5))
    ieq(expect, actual)

    actual = typecounts(table, 'bar')
    if PY2:
        expect = (('type', 'count', 'frequency'),
                  ('unicode', 3, 3./5),
                  ('int', 2, 2./5))
    else:
        expect = (('type', 'count', 'frequency'),
                  ('str', 3, 3./5),
                  ('int', 2, 2./5))
    ieq(expect, actual)

    actual = typecounts(table, 'baz')
    expect = (('type', 'count', 'frequency'),
              ('float', 4, 4./5),
              ('NoneType', 1, 1./5))
    ieq(expect, actual)


def test_parsecounts():

    table = (('foo', 'bar', 'baz'),
             ('A', 'aaa', 2),
             ('B', '2', '3.4'),
             ('B', '3', '7.8', True),
             ('D', '3.7', 9.0),
             ('E', 42))

    actual = parsecounts(table, 'bar')
    expect = (('type', 'count', 'errors'), ('float', 3, 1), ('int', 2, 2))
    ieq(expect, actual)


def test_stringpatterns():

    table = (('foo', 'bar'),
             ('Mr. Foo', '123-1254'),
             ('Mrs. Bar', '234-1123'),
             ('Mr. Spo', '123-1254'),
             ('Mr. Baz', '321 1434'),
             ('Mrs. Baz', '321 1434'),
             ('Mr. Quux', '123-1254-XX'))

    actual = stringpatterns(table, 'foo')
    expect = (('pattern', 'count', 'frequency'),
              ('Aa. Aaa', 3, 3./6),
              ('Aaa. Aaa', 2, 2./6),
              ('Aa. Aaaa', 1, 1./6))
    ieq(expect, actual)

    actual = stringpatterns(table, 'bar')
    expect = (('pattern', 'count', 'frequency'),
              ('999-9999', 3, 3./6),
              ('999 9999', 2, 2./6),
              ('999-9999-AA', 1, 1./6))
    ieq(expect, actual)
