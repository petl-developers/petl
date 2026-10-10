# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

from collections import OrderedDict
from tempfile import NamedTemporaryFile
from decimal import Decimal
import json

import pytest

from petl import fromjson, tojson
from petl.io.json import iterjlines
from petl.test.helpers import ieq


@pytest.mark.parametrize('lines', [False, True])
@pytest.mark.parametrize('header', [None, ['amount', 'count', 'constant']])
def test_fromjson_decoder_options(tmpdir, lines, header):
    records = [('{"amount": 0.12345678901234567890123456789, '
                '"count": 9007199254740993, "constant": NaN}'),
               '{"amount": -1.25, "count": 0, "constant": Infinity}']
    text = '\n'.join(records) if lines else '[' + ','.join(records) + ']'
    path = tmpdir.join('numbers.json')
    path.write(text)
    actual = fromjson(str(path), lines=lines, header=header,
                      parse_float=Decimal, parse_int=str,
                      parse_constant=lambda value: None)
    expected = [('amount', 'count', 'constant'),
                (Decimal('0.12345678901234567890123456789'),
                 '9007199254740993', None),
                (Decimal('-1.25'), '0', None)]
    for _ in range(2):
        rows = list(actual)
        if header is None:
            assert sorted(rows[0]) == sorted(expected[0])
            assert all(len(row) == len(rows[0]) for row in rows[1:])
            assert [dict(zip(rows[0], row)) for row in rows[1:]] == [
                dict(zip(expected[0], row)) for row in expected[1:]]
        else:
            assert rows == expected


@pytest.mark.parametrize('header', [None, ['value']])
def test_fromjson_lines_decoder_once_per_iteration(tmpdir, header):
    calls = []

    def parse_int(value):
        calls.append(value)
        return len(calls)

    path = tmpdir.join('counter.jsonl')
    path.write('{"value": 10}\n{"value": 20}\n')
    table = fromjson(str(path), lines=True, header=header, parse_int=parse_int)
    for traversal in range(2):
        rows = iter(table)
        assert calls == ['10', '20'] * traversal
        assert next(rows) == ('value',)
        assert len(calls) == 2 * traversal + (1 if header is None else 0)
        assert next(rows) == (2 * traversal + 1,)
        assert len(calls) == 2 * traversal + 1
        assert next(rows) == (2 * traversal + 2,)
        with pytest.raises(StopIteration):
            next(rows)
        assert calls == ['10', '20'] * (traversal + 1)


@pytest.mark.parametrize('header', [None, ['first']])
def test_fromjson_lines_stateful_schema_hook(tmpdir, header):
    calls = []

    def schema_hook(record):
        calls.append(record['value'])
        field = 'first' if len(calls) == 1 else 'second'
        return {field: record['value']}

    path = tmpdir.join('schema.jsonl')
    path.write('{"value": 10}\n{"value": 20}\n')
    rows = iter(fromjson(str(path), lines=True, header=header,
                         object_hook=schema_hook, missing='missing'))
    assert calls == []
    assert next(rows) == ('first',)
    assert next(rows) == (10,)
    assert next(rows) == ('missing',)
    with pytest.raises(StopIteration):
        next(rows)
    assert calls == [10, 20]


@pytest.mark.parametrize('lines', [False, True])
@pytest.mark.parametrize('header', [None, ['value']])
def test_fromjson_finite_decoder_callback(tmpdir, lines, header):
    values = iter(['first', 'second'])

    def parse_int(value):
        return next(values)

    records = ['{"value": 10}', '{"value": 20}']
    text = '\n'.join(records) if lines else '[' + ','.join(records) + ']'
    path = tmpdir.join('finite.json')
    path.write(text)
    rows = iter(fromjson(str(path), lines=lines, header=header,
                         parse_int=parse_int))
    assert next(rows) == ('value',)
    assert next(rows) == ('first',)
    assert next(rows) == ('second',)
    with pytest.raises(StopIteration):
        next(rows)


@pytest.mark.parametrize('header', [None, ['value']])
def test_fromjson_lines_default_iteration(tmpdir, header):
    path = tmpdir.join('default.jsonl')
    path.write('{"value": 10}\n{"value": 20}\n')
    table = fromjson(str(path), lines=True, header=header)
    for _ in range(2):
        rows = iter(table)
        assert next(rows) == ('value',)
        assert next(rows) == (10,)
        assert next(rows) == (20,)
        with pytest.raises(StopIteration):
            next(rows)


@pytest.mark.parametrize('header', [None, ['value']])
def test_fromjson_lines_empty_decoder_iteration(tmpdir, header):
    calls = []

    def parse_int(value):
        calls.append(value)
        return int(value)

    path = tmpdir.join('empty.jsonl')
    path.write('')
    table = fromjson(str(path), lines=True, header=header, parse_int=parse_int)
    for _ in range(2):
        rows = iter(table)
        assert next(rows) == (() if header is None else ('value',))
        with pytest.raises(StopIteration):
            next(rows)
    assert calls == []


@pytest.mark.parametrize('header', [None, ['value']])
@pytest.mark.parametrize('first_valid', [False, True])
def test_fromjson_lines_malformed_decoder_timing(tmpdir, header, first_valid):
    path = tmpdir.join('malformed.jsonl')
    path.write(('{"value": 10}\n' if first_valid else '') + 'not-json\n')
    rows = iter(fromjson(str(path), lines=True, header=header))
    if first_valid or header is not None:
        assert next(rows) == ('value',)
    if first_valid:
        assert next(rows) == (10,)
    with pytest.raises(ValueError):
        next(rows)
    with pytest.raises(StopIteration):
        next(rows)


@pytest.mark.parametrize('header', [None, ['value']])
def test_fromjson_lines_decoder_error_timing(tmpdir, header):
    error = ValueError('decoder callback failed')

    def parse_int(value):
        raise error

    path = tmpdir.join('error.jsonl')
    path.write('{"value": 10}\n')
    rows = iter(fromjson(str(path), lines=True, header=header,
                         parse_int=parse_int))
    if header is not None:
        assert next(rows) == ('value',)
    with pytest.raises(ValueError) as raised:
        next(rows)
    assert raised.value is error


@pytest.mark.parametrize('header', [None, ['NAME', 'EXTRA']])
@pytest.mark.parametrize('custom_decoder', [False, True])
@pytest.mark.parametrize('reverse_order', [False, True])
def test_fromjson_lines_object_hook(tmpdir, header, custom_decoder,
                                   reverse_order):
    def uppercase_keys(record):
        values = {key.upper(): value for key, value in record.items()}
        if reverse_order:
            return OrderedDict(sorted(values.items()))
        return values

    class UppercaseDecoder(json.JSONDecoder):
        def __init__(self, *args, **kwargs):
            kwargs['object_hook'] = uppercase_keys
            json.JSONDecoder.__init__(self, *args, **kwargs)

    options = ({'cls': UppercaseDecoder} if custom_decoder else
               {'object_hook': uppercase_keys})
    path = tmpdir.join('objects.jsonl')
    path.write('{"name": "first", "extra": {"nested": 1}}\n'
               '{"name": "second"}\n')
    actual = fromjson(str(path), lines=True, header=header, missing='NA',
                      **options)
    expected = [('NAME', 'EXTRA'), ('first', {'NESTED': 1}), ('second', 'NA')]
    for _ in range(2):
        rows = list(actual)
        if header is None:
            assert sorted(rows[0]) == sorted(expected[0])
            assert all(len(row) == len(rows[0]) for row in rows[1:])
            assert [dict(zip(rows[0], row)) for row in rows[1:]] == [
                dict(zip(expected[0], row)) for row in expected[1:]]
        else:
            assert rows == expected


@pytest.mark.parametrize('header', [None, ['value']])
def test_fromjson_lines_invalid_decoder_option(tmpdir, header):
    path = tmpdir.join('options.jsonl')
    path.write('{"value": 1}\n')
    with pytest.raises(TypeError):
        list(fromjson(str(path), lines=True, header=header,
                      unknown_decoder_option=True))


@pytest.mark.parametrize('lines', [False, True])
@pytest.mark.parametrize('header', [None, ['value']])
@pytest.mark.parametrize('option', ['f', 'args', 'kwargs'])
def test_fromjson_custom_decoder_keyword_names(tmpdir, lines, header, option):
    class OptionDecoder(json.JSONDecoder):
        def __init__(self, **kwargs):
            prefix = kwargs.pop(option)
            kwargs['parse_int'] = lambda value: prefix + value
            json.JSONDecoder.__init__(self, **kwargs)

    records = ['{"value": 1}', '{"value": 2}']
    text = '\n'.join(records) if lines else '[' + ','.join(records) + ']'
    path = tmpdir.join('custom-options.json')
    path.write(text)
    options = {'cls': OptionDecoder, option: 'decoded-'}
    actual = fromjson(str(path), lines=lines, header=header, **options)
    expected = [('value',), ('decoded-1',), ('decoded-2',)]
    assert actual.list() == expected
    assert actual.list() == expected


def test_iterjlines_three_argument_call():
    actual = iterjlines(['{"value": 1}\n', '{"value": 2}\n'], None, None)
    assert list(actual) == [('value',), (1,), (2,)]


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
