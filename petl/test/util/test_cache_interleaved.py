import pytest

import petl as etl


@pytest.mark.parametrize('limit', [None, 2, 3, 10])
def test_interleaved_reads_do_not_duplicate_cached_rows(limit):
    table = [('id',), (1,), (2,), (3,)]
    view = etl.wrap(table).cache(n=limit)
    first = iter(view)
    second = iter(view)

    assert next(first) == table[0]
    assert next(second) == table[0]
    assert next(second) == table[1]
    assert list(first) == table[1:]
    assert list(second) == table[2:]
    assert list(view) == table
    assert list(view) == table


def test_completed_second_reader_keeps_suspended_reader_independent():
    table = [('id',), (1,), (2,)]
    view = etl.wrap(table).cache()
    first = iter(view)
    assert next(first) == table[0]

    assert list(view) == table
    assert list(first) == table[1:]
    assert list(view) == table
    view.clearcache()
    assert list(view) == table
