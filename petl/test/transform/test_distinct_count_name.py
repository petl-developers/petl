import pytest

import petl as etl


@pytest.mark.parametrize('name', ['', 0, False, 'count'])
@pytest.mark.parametrize('presorted', [False, True])
def test_distinct_uses_explicit_count_name(name, presorted):
    table = [('key', 'value'), ('a', 2), ('a', 3), ('b', 4)]
    result = etl.distinct(table, key='key', count=name, presorted=presorted)
    expected = [('key', 'value', name), ('a', 2, 2), ('b', 4, 1)]

    assert list(result) == expected
    # A second read must retain both the field name and the counts.
    assert list(result) == expected


def test_empty_count_name_on_header_only_input():
    assert list(etl.distinct([('key',)], count='')) == [('key', '')]


def test_omitted_count_keeps_original_columns():
    assert list(etl.distinct([('key',), ('a',), ('a',)])) == [('key',), ('a',)]
