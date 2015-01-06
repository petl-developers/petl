from __future__ import absolute_import, print_function, division


from petl.test.helpers import eq_
from petl.util.vis import look, see, lookstr


def test_look():

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(look(table))
    expect = """+-------+-------+
| 'foo' | 'bar' |
+=======+=======+
| 'a'   |     1 |
+-------+-------+
| 'b'   |     2 |
+-------+-------+
"""
    eq_(expect, actual)


def test_look_irregular_rows():

    table = (('foo', 'bar'), ('a',), ('b', 2, True))
    actual = repr(look(table))
    expect = """+-------+-------+------+
| 'foo' | 'bar' |      |
+=======+=======+======+
| 'a'   |       |      |
+-------+-------+------+
| 'b'   |     2 | True |
+-------+-------+------+
"""
    eq_(expect, actual)


def test_look_bool():

    table = (('foo', 'bar'), ('a', True), ('b', False))
    actual = repr(look(table))
    expect = """+-------+-------+
| 'foo' | 'bar' |
+=======+=======+
| 'a'   | True  |
+-------+-------+
| 'b'   | False |
+-------+-------+
"""
    eq_(expect, actual)


def test_look_style_simple():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(look(table, style='simple'))
    expect = """=====  =====
'foo'  'bar'
=====  =====
'a'        1
'b'        2
=====  =====
"""
    eq_(expect, actual)
    look.default_style = 'simple'
    actual = repr(look(table))
    eq_(expect, actual)
    look.default_style = 'grid'


def test_look_style_minimal():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(look(table, style='minimal'))
    expect = """'foo'  'bar'
'a'        1
'b'        2
"""
    eq_(expect, actual)
    look.default_style = 'minimal'
    actual = repr(look(table))
    eq_(expect, actual)
    look.default_style = 'grid'


def test_see():

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(see(table))
    expect = """'foo': 'a', 'b'
'bar': 1, 2
"""
    eq_(expect, actual)


def test_see_duplicateheader():

    table = (('foo', 'bar', 'foo'), ('a', 1, 'a_prime'), ('b', 2, 'b_prime'))
    actual = repr(see(table))
    expect = """'foo': 'a', 'b'
'bar': 1, 2
'foo': 'a_prime', 'b_prime'
"""
    eq_(expect, actual)


def test_lookstr():

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(lookstr(table))
    expect = """+-----+-----+
| foo | bar |
+=====+=====+
| a   |   1 |
+-----+-----+
| b   |   2 |
+-----+-----+
"""
    eq_(expect, actual)
