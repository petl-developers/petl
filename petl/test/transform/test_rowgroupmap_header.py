import pytest

import petl as etl
from petl.errors import ArgumentError


@pytest.mark.parametrize('presorted', [False, True])
@pytest.mark.parametrize('one_shot', [False, True])
def test_rowgroupmap_can_reuse_the_source_header(presorted, one_shot):
    table = [('group', 'amount'), ('a', 12), ('a', 24), ('b', 36)]
    source = iter(table) if one_shot else table
    calls = []

    def mapper(key, rows):
        calls.append(key)
        for row in rows:
            yield row[0], row[1] * 2

    result = etl.rowgroupmap(source, 'group', mapper, presorted=presorted)
    assert list(iter(result)) == [('group', 'amount'), ('a', 24), ('a', 48), ('b', 72)]
    assert calls == ['a', 'b']


def test_rowgroupmap_keeps_an_explicit_header():
    table = [('group', 'amount'), ('a', 12), ('a', 24)]
    result = etl.rowgroupmap(table, 'group', lambda key, rows: [(sum(row[1] for row in rows),)],
                            header=['total'])
    assert list(result) == [('total',), (36,)]


def test_rowgroupmap_keeps_a_header_only_table():
    assert list(etl.rowgroupmap([('group', 'amount')], 'group', lambda key, rows: rows)) == [
        ('group', 'amount')]


@pytest.mark.parametrize('one_shot', [False, True])
def test_rowgroupmap_reports_missing_header_for_empty_presorted_input(one_shot):
    source = iter([]) if one_shot else []
    with pytest.raises(ArgumentError, match='header.*empty table'):
        list(etl.rowgroupmap(source, 'group', lambda key, rows: rows, presorted=True))
