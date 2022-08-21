# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

from collections import OrderedDict
from tempfile import NamedTemporaryFile
import json

import pytest

from petl.test.helpers import ieq
from petl import fromjson, fromdicts, tojson, tojsonarrays


def test_fromjson_1():

    f = NamedTemporaryFile(delete=False, mode='w')
    data = '[{"foo": "a", "bar": 1}, ' \
           '{"foo": "b", "bar": 2}, ' \
           '{"foo": "c", "bar": 2}]'
    f.write(data)
    f.close()

    actual = fromjson(f.name, header=['foo', 'bar'])
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromjson_2():

    f = NamedTemporaryFile(delete=False, mode='w')
    data = '[{"foo": "a", "bar": 1}, ' \
           '{"foo": "b"}, ' \
           '{"foo": "c", "bar": 2, "baz": true}]'
    f.write(data)
    f.close()

    actual = fromjson(f.name, header=['bar', 'baz', 'foo'])
    expect = (('bar', 'baz', 'foo'),
              (1, None, 'a'),
              (None, None, 'b'),
              (2, True, 'c'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromjson_3():

    f = NamedTemporaryFile(delete=False, mode='w')
    data = '[{"foo": "a", "bar": 1}, ' \
           '{"foo": "b"}, ' \
           '{"foo": "c", "bar": 2, "baz": true}]'
    f.write(data)
    f.close()

    actual = fromjson(f.name, header=['foo', 'bar'])
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', None),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromdicts_1():

    data = [{'foo': 'a', 'bar': 1},
            {'foo': 'b', 'bar': 2},
            {'foo': 'c', 'bar': 2}]
    actual = fromdicts(data, header=['foo', 'bar'])
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromdicts_2():

    data = [{'foo': 'a', 'bar': 1},
            {'foo': 'b'},
            {'foo': 'c', 'bar': 2, 'baz': True}]
    actual = fromdicts(data, header=['bar', 'baz', 'foo'])
    expect = (('bar', 'baz', 'foo'),
              (1, None, 'a'),
              (None, None, 'b'),
              (2, True, 'c'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromdicts_3():

    data = [{'foo': 'a', 'bar': 1},
            {'foo': 'b'},
            {'foo': 'c', 'bar': 2, 'baz': True}]
    actual = fromdicts(data, header=['foo', 'bar'])
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', None),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromdicts_onepass():

    # check that fromdicts() only makes a single pass through the data
    data = iter([{'foo': 'a', 'bar': 1},
                 {'foo': 'b', 'bar': 2},
                 {'foo': 'c', 'bar': 2}])
    actual = fromdicts(data, header=['foo', 'bar'])
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)


def test_fromdicts_ordered():
    data = [OrderedDict([('foo', 'a'), ('bar', 1)]),
            OrderedDict([('foo', 'b')]),
            OrderedDict([('foo', 'c'), ('bar', 2), ('baz', True)])]
    actual = fromdicts(data)
    # N.B., fields come out in original order
    expect = (('foo', 'bar', 'baz'),
              ('a', 1, None),
              ('b', None, None),
              ('c', 2, True))
    ieq(expect, actual)


def test_fromdicts_missing():
    data = [OrderedDict([('foo', 'a'), ('bar', 1)]),
            OrderedDict([('foo', 'b')]),
            OrderedDict([('foo', 'c'), ('bar', 2), ('baz', True)])]
    actual = fromdicts(data, missing="x")
    expect = (('foo', 'bar', 'baz'),
              ('a', 1, "x"),
              ('b', "x", "x"),
              ('c', 2, True))
    ieq(expect, actual)


def test_tojson():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False, mode='r')
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
    f = NamedTemporaryFile(delete=False, mode='r')
    tojsonarrays(table, f.name)
    result = json.load(f)
    assert len(result) == 3
    assert result[0][0] == 'a'
    assert result[0][1] == 1
    assert result[1][0] == 'b'
    assert result[1][1] == 2
    assert result[2][0] == 'c'
    assert result[2][1] == 2


def test_fromdicts_header_does_not_raise():
    data = [{'foo': 'a', 'bar': 1},
            {'foo': 'b', 'bar': 2},
            {'foo': 'c', 'bar': 2}]
    actual = fromdicts(data)
    assert actual.header()


def test_fromdicts_header_list():
    data = [OrderedDict([('foo', 'a'), ('bar', 1)]),
        OrderedDict([('foo', 'b'), ('bar', 2)]),
        OrderedDict([('foo', 'c'), ('bar', 2)])]
    actual = fromdicts(data)
    header = actual.header()
    assert header == ('foo', 'bar')
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)


@pytest.fixture
def dicts_generator():
    def generator():
        yield OrderedDict([('foo', 'a'), ('bar', 1)])
        yield OrderedDict([('foo', 'b'), ('bar', 2)])
        yield OrderedDict([('foo', 'c'), ('bar', 2)])
    return generator()


def test_fromdicts_generator_single(dicts_generator):
    actual = fromdicts(dicts_generator)
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)


def test_fromdicts_generator_twice(dicts_generator):
    actual = fromdicts(dicts_generator)
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)


def test_fromdicts_generator_header(dicts_generator):
    actual = fromdicts(dicts_generator)
    header = actual.header()
    assert header == ('foo', 'bar')
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)


def test_fromdicts_generator_random_access():
    def generator():
        for i in range(5):
            yield OrderedDict([('n', i), ('foo', 100*i), ('bar', 200*i)])

    actual = fromdicts(generator(), sample=3)
    assert actual.header() == ('n', 'foo', 'bar')
    # first pass
    it1 = iter(actual)
    first_row1 = next(it1)
    first_row2 = next(it1)
    # second pass
    it2 = iter(actual)
    second_row1 = next(it2)
    second_row2 = next(it2)
    assert first_row1 == second_row1
    assert first_row2 == second_row2
    # reverse order
    second_row3 = next(it2)
    first_row3 = next(it1)
    assert second_row3 == first_row3
    ieq(actual, actual)
    assert actual.header() == ('n', 'foo', 'bar')
    assert len(actual) == 6


def test_fromdicts_generator_missing():
    def generator():
        yield OrderedDict([('foo', 'a'), ('bar', 1)])
        yield OrderedDict([('foo', 'b'), ('bar', 2)])
        yield OrderedDict([('foo', 'c'), ('baz', 2)])
    actual = fromdicts(generator(), missing="x")
    expect = (('foo', 'bar', 'baz'),
              ('a', 1, "x"),
              ('b', 2, "x"),
              ('c', "x", 2))
    ieq(expect, actual)
