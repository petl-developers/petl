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
