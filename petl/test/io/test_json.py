# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

from collections import OrderedDict
from tempfile import NamedTemporaryFile
import json

import pytest

from petl.test.helpers import ieq
from petl import dummytable, fromjson, fromdicts, tojson, tojsonarrays


@pytest.mark.parametrize('kind', ['list', 'iterator', 'generator'])
@pytest.mark.parametrize('sample', [0, 1, 2])
def test_fromdicts_header_sample(kind, sample):
    records = [OrderedDict([('id', '001')]),
               OrderedDict([('id', '002'), ('later', 'value')])]
    if kind == 'iterator':
        source = iter(records)
    elif kind == 'generator':
        source = (record for record in records)
    else:
        source = records
    expected = {
        0: [(), (), ()],
        1: [('id',), ('001',), ('002',)],
        2: [('id', 'later'), ('001', None), ('002', 'value')],
    }[sample]
    actual = fromdicts(source, sample=sample)
    ieq(expected, actual)
    if kind != 'iterator':
        ieq(expected, actual)


@pytest.mark.parametrize('kind', ['list', 'iterator', 'generator'])
def test_fromdicts_single_sample_empty(kind):
    source = []
    if kind == 'iterator':
        source = iter(source)
    elif kind == 'generator':
        source = (record for record in source)
    actual = fromdicts(source, sample=1)
    ieq([()], actual)
    if kind != 'iterator':
        ieq([()], actual)


@pytest.mark.parametrize('records, expected', [
    ([], [()]),
    ([{'id': '001'}, {'id': '002', 'later': 'value'}],
     [('id',), ('001',), ('002',)]),
])
def test_fromjson_single_sample(tmpdir, records, expected):
    path = tmpdir.join('sample.json')
    path.write(json.dumps(records))
    actual = fromjson(str(path), sample=1)
    ieq(expected, actual)
    ieq(expected, actual)


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


def _run_python_with_stdin(code, input_data):
    import subprocess
    import sys

    proc = subprocess.Popen(
        [sys.executable, '-c', code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = proc.communicate(input_data)
    assert proc.returncode == 0, stderr.decode('utf-8')
    return stdout


def test_tojson_fromcsv_stdin_subprocess():
    csv_data = b'foo,bar\nx,1\ny,2\n'
    code = 'import petl as etl; etl.fromcsv().tojson(sort_keys=True)'

    stdout = _run_python_with_stdin(code, csv_data)

    assert json.loads(stdout.decode('utf-8')) == [
        {'bar': '1', 'foo': 'x'},
        {'bar': '2', 'foo': 'y'},
    ]


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


def test_tojsonarrays_fromcsv_stdin_subprocess():
    csv_data = b'foo,bar\nx,1\ny,2\n'
    code = 'import petl as etl; etl.fromcsv().tojsonarrays()'

    stdout = _run_python_with_stdin(code, csv_data)

    assert json.loads(stdout.decode('utf-8')) == [
        ['x', '1'],
        ['y', '2'],
    ]


def test_tojsonarrays_header_fromcsv_stdin_subprocess():
    csv_data = b'foo,bar\nx,1\ny,2\n'
    code = (
        'import petl as etl; '
        'etl.fromcsv().tojsonarrays(output_header=True)'
    )

    stdout = _run_python_with_stdin(code, csv_data)

    assert json.loads(stdout.decode('utf-8')) == [
        ['foo', 'bar'],
        ['x', '1'],
        ['y', '2'],
    ]


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


def test_fromdicts_dicts_method():
    # .dicts() must reach the inherited Table.dicts(), not the input data (#643)
    dummy = dummytable(numrows=3, seed=42)
    data = list(dummy.dicts())
    actual = fromdicts(dummy.dicts())
    assert list(actual.dicts()) == data


def test_fromdicts_dicts_attribute_reads_as_data():
    data = [{'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': 2}]
    actual = fromdicts(data)
    assert actual.dicts == data
    assert list(actual.dicts) == data
    assert len(actual.dicts) == 2
    assert actual.dicts[1] == data[1]
    assert repr(actual.dicts) == repr(data)


def test_fromdicts_dicts_attribute_assignment():
    actual = fromdicts([{'foo': 'a'}])
    actual.dicts = [{'foo': 'z'}]
    assert list(actual.dicts) == [{'foo': 'z'}]
    ieq((('foo',), ('z',)), actual)


def test_fromdicts_generator_dicts_method(dicts_generator):
    actual = fromdicts(dicts_generator)
    expect = [{'foo': 'a', 'bar': 1},
              {'foo': 'b', 'bar': 2},
              {'foo': 'c', 'bar': 2}]
    assert list(actual.dicts()) == expect


def test_fromdicts_generator_dicts_attribute_is_consumable(dicts_generator):
    # the generator view's .dicts is a one-shot iterator, as it was before
    actual = fromdicts(dicts_generator)
    it = actual.dicts
    assert next(it) == {'foo': 'a', 'bar': 1}
    assert len(list(it)) == 2
    assert list(actual.dicts) == []
