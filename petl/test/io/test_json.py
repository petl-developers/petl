# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import json


from petl.test.helpers import ieq
from petl.io.json import fromjson, fromdicts, tojson, tojsonarrays


def test_fromjson_1():

    f = NamedTemporaryFile(delete=False, mode='w')
    data = '[{"foo": "a", "bar": 1}, ' \
           '{"foo": "b", "bar": 2}, ' \
           '{"foo": "c", "bar": 2}]'
    f.write(data)
    f.close()

    actual = fromjson(f.name)
    # N.B., fields come out in sorted order
    expect = (('bar', 'foo'),
              (1, 'a'),
              (2, 'b'),
              (2, 'c'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromjson_2():

    f = NamedTemporaryFile(delete=False, mode='w')
    data = '[{"foo": "a", "bar": 1}, ' \
           '{"foo": "b"}, ' \
           '{"foo": "c", "bar": 2, "baz": true}]'
    f.write(data)
    f.close()

    actual = fromjson(f.name)
    # N.B., fields come out in sorted order
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
    actual = fromdicts(data)
    # N.B., fields come out in sorted order
    expect = (('bar', 'foo'),
              (1, 'a'),
              (2, 'b'),
              (2, 'c'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromdicts_2():

    data = [{'foo': 'a', 'bar': 1},
            {'foo': 'b'},
            {'foo': 'c', 'bar': 2, 'baz': True}]
    actual = fromdicts(data)
    # N.B., fields come out in sorted order
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
