from __future__ import absolute_import, print_function, division


import logging

import pytest

from petl.test.helpers import eq_
import petl as etl
from petl.util.vis import look, see, lookstr


logger = logging.getLogger(__name__)
debug = logger.debug


def test_look():

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(look(table))
    expect = """+-----+-----+
| foo | bar |
+=====+=====+
| 'a' |   1 |
+-----+-----+
| 'b' |   2 |
+-----+-----+
"""
    eq_(expect, actual)


def test_look_irregular_rows():

    table = (('foo', 'bar'), ('a',), ('b', 2, True))
    actual = repr(look(table))
    expect = """+-----+-----+------+
| foo | bar |      |
+=====+=====+======+
| 'a' |     |      |
+-----+-----+------+
| 'b' |   2 | True |
+-----+-----+------+
"""
    eq_(expect, actual)


def test_look_index_header():

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(look(table, index_header=True))
    expect = """+-------+-------+
| 0|foo | 1|bar |
+=======+=======+
| 'a'   |     1 |
+-------+-------+
| 'b'   |     2 |
+-------+-------+
"""
    eq_(expect, actual)


def test_look_bool():

    table = (('foo', 'bar'), ('a', True), ('b', False))
    actual = repr(look(table))
    expect = """+-----+-------+
| foo | bar   |
+=====+=======+
| 'a' | True  |
+-----+-------+
| 'b' | False |
+-----+-------+
"""
    eq_(expect, actual)


def test_look_truncate():

    table = (('foo', 'bar'), ('abcd', 1234), ('bcde', 2345))

    actual = repr(look(table, truncate=3))
    expect = """+-----+-----+
| foo | bar |
+=====+=====+
| 'ab | 123 |
+-----+-----+
| 'bc | 234 |
+-----+-----+
"""
    eq_(expect, actual)

    actual = repr(look(table, truncate=3, vrepr=str))
    expect = """+-----+-----+
| foo | bar |
+=====+=====+
| abc | 123 |
+-----+-----+
| bcd | 234 |
+-----+-----+
"""
    eq_(expect, actual)


def test_look_width():

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(look(table, width=10))
    expect = ("+-----+---\n"
              "| foo | ba\n"
              "+=====+===\n"
              "| 'a' |   \n"
              "+-----+---\n"
              "| 'b' |   \n"
              "+-----+---\n")
    eq_(expect, actual)


def test_look_style_simple():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(look(table, style='simple'))
    expect = """===  ===
foo  bar
===  ===
'a'    1
'b'    2
===  ===
"""
    eq_(expect, actual)
    etl.config.look_style = 'simple'
    actual = repr(look(table))
    eq_(expect, actual)
    etl.config.look_style = 'grid'


def test_look_style_minimal():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(look(table, style='minimal'))
    expect = """foo  bar
'a'    1
'b'    2
"""
    eq_(expect, actual)
    etl.config.look_style = 'minimal'
    actual = repr(look(table))
    eq_(expect, actual)
    etl.config.look_style = 'grid'


def test_see():

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(see(table))
    expect = """foo: 'a', 'b'
bar: 1, 2
"""
    eq_(expect, actual)


def test_see_index_header():

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(see(table, index_header=True))
    expect = """0|foo: 'a', 'b'
1|bar: 1, 2
"""
    eq_(expect, actual)


def test_see_duplicateheader():

    table = (('foo', 'bar', 'foo'), ('a', 1, 'a_prime'), ('b', 2, 'b_prime'))
    actual = repr(see(table))
    expect = """foo: 'a', 'b'
bar: 1, 2
foo: 'a_prime', 'b_prime'
"""
    eq_(expect, actual)


@pytest.mark.parametrize('header', [
    ('foo', 'bar'), ('foo', 'foo'), ('foo', '0'), (1, 0),
])
@pytest.mark.parametrize('index_header', [False, True])
@pytest.mark.parametrize('limit', [None, 2])
def test_see_irregular_rows(header, index_header, limit):
    table = (header, ('a',), ('b', 2), (), ('c', 3))
    fields = list(map(str, header))
    if index_header:
        fields = [str(i) + '|' + field for i, field in enumerate(fields)]
    if limit is None:
        expect = (fields[0] + ": 'a', 'b', , 'c'\n" +
                  fields[1] + ": , 2, , 3\n")
    else:
        expect = (fields[0] + ": 'a', 'b'...\n" +
                  fields[1] + ": , 2...\n")
    view = see(table, index_header=index_header, limit=limit)
    eq_(expect, repr(view))
    eq_(expect, repr(view))


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


def test_look_headerless():
    table = []
    actual = repr(look(table))
    expect = ""
    eq_(expect, actual)
