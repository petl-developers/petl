# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

from tempfile import NamedTemporaryFile
import json

from petl import fromjson, tojson
from petl.test.helpers import ieq


def test_fromjson_1():
    f = NamedTemporaryFile(delete=False, mode='w')
    data = '{"name": "Gilbert", "wins": [["straight", "7S"], ["one pair", "10H"]]}\n' \
           '{"name": "Alexa", "wins": [["two pair", "4S"], ["two pair", "9S"]]}\n' \
           '{"name": "May", "wins": []}\n' \
           '{"name": "Deloise", "wins": [["three of a kind", "5S"]]}'

    f.write(data)
    f.close()

    actual = fromjson(f.name, header=['name', 'wins'], lines=True)

    expect = (('name', 'wins'),
              ('Gilbert', [["straight", "7S"], ["one pair", "10H"]]),
              ('Alexa', [["two pair", "4S"], ["two pair", "9S"]]),
              ('May', []),
              ('Deloise', [["three of a kind", "5S"]]))

    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromjson_2():
    f = NamedTemporaryFile(delete=False, mode='w')
    data = '{"foo": "bar1", "baz": 1}\n' \
           '{"foo": "bar2", "baz": 2}\n' \
           '{"foo": "bar3", "baz": 3}\n' \
           '{"foo": "bar4", "baz": 4}\n'

    f.write(data)
    f.close()

    actual = fromjson(f.name, header=['foo', 'baz'], lines=True)

    expect = (('foo', 'baz'),
              ('bar1', 1),
              ('bar2', 2),
              ('bar3', 3),
              ('bar4', 4))

    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_tojson_1():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False, mode='r')
    tojson(table, f.name, lines=True)
    result = []
    for line in f:
        result.append(json.loads(line))
    assert len(result) == 3
    assert result[0]['foo'] == 'a'
    assert result[0]['bar'] == 1
    assert result[1]['foo'] == 'b'
    assert result[1]['bar'] == 2
    assert result[2]['foo'] == 'c'
    assert result[2]['bar'] == 2


def test_tojson_2():
    table = [['name', 'wins'],
             ['Gilbert', [['straight', '7S'], ['one pair', '10H']]],
             ['Alexa', [['two pair', '4S'], ['two pair', '9S']]],
             ['May', []],
             ['Deloise', [['three of a kind', '5S']]]]
    f = NamedTemporaryFile(delete=False, mode='r')
    tojson(table, f.name, lines=True)
    result = []
    for line in f:
        result.append(json.loads(line))
    assert len(result) == 4
    assert result[0]['name'] == 'Gilbert'
    assert result[0]['wins'] == [['straight', '7S'], ['one pair', '10H']]
    assert result[1]['name'] == 'Alexa'
    assert result[1]['wins'] == [['two pair', '4S'], ['two pair', '9S']]
    assert result[2]['name'] == 'May'
    assert result[2]['wins'] == []
    assert result[3]['name'] == 'Deloise'
    assert result[3]['wins'] == [['three of a kind', '5S']]
