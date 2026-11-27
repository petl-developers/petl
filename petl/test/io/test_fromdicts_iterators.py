import pytest

import petl as etl
from petl import config


@pytest.mark.parametrize('factory', [iter, lambda rows: map(dict, rows),
                                     lambda rows: (row for row in rows)])
@pytest.mark.parametrize('header', [None, ['name', 'value']])
def test_fromdicts_iterators_survive_header_inspection(factory, header, tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'temp_dir', str(tmp_path))
    rows = [{'name': 'a', 'value': 2}, {'name': 'b', 'value': 3}]
    view = etl.fromdicts(factory(rows), header=header)
    expected = [('name', 'value'), ('a', 2), ('b', 3)]

    assert etl.header(view) == expected[0]
    assert list(view) == expected
    assert list(view) == expected


def test_fromdicts_repeatable_input_remains_live():
    rows = [{'value': 2}]
    view = etl.fromdicts(rows)
    assert list(view) == [('value',), (2,)]

    rows.append({'value': 3})

    assert list(view) == [('value',), (2,), (3,)]


@pytest.mark.parametrize('factory', [iter, lambda rows: map(dict, rows),
                                     lambda rows: (row for row in rows)])
@pytest.mark.parametrize('header', [None, ['value']])
def test_fromdicts_iterators_retain_unpicklable_values(factory, header, tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'temp_dir', str(tmp_path))

    def value():
        return 'retained'

    view = etl.fromdicts(factory([{'value': value}]), header=header)
    expected = [('value',), (value,)]

    assert list(view) == expected
    assert list(view) == expected


def test_fromdicts_cache_recovers_from_partial_pickle_writes(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'temp_dir', str(tmp_path))

    def value():
        return 'retained'

    # The large first cell is written before pickling the local function fails.
    payload = b'x' * 131072
    rows = [{'value': 1}, {'value': (payload, value)}, {'value': 2}]
    view = etl.fromdicts(iter(rows), header=['value'])
    expected = [('value',), (1,), ((payload, value),), (2,)]
    first = iter(view)
    assert next(first) == expected[0]
    assert next(first) == expected[1]
    assert next(first) == expected[2]

    # The second reader replays both kinds of cached row before finishing.
    assert list(view) == expected
    assert list(first) == expected[3:]
    assert list(view) == expected


def test_fromdicts_iterators_retain_open_file_values(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'temp_dir', str(tmp_path))
    with (tmp_path / 'value.txt').open('w+') as value:
        view = etl.fromdicts(iter([{'value': value}]), header=['value'])
        expected = [('value',), (value,)]

        assert list(view) == expected
        assert list(view) == expected
        assert not value.closed
