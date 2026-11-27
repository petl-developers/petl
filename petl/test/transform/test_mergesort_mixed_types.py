import pytest

import petl as etl


@pytest.mark.parametrize('reverse', [False, True])
@pytest.mark.parametrize('presorted', [False, True])
def test_unkeyed_mergesort_matches_sort_for_mixed_values(reverse, presorted):
    tables = [[('value', 'label'), (None, 'none'), (2, 'two'), ('z', 'zee')],
              [('value', 'label'), (1, 'one'), ('a', 'aye')]]
    expected_rows = [(None, 'none'), (1, 'one'), (2, 'two'), ('a', 'aye'), ('z', 'zee')]
    if reverse:
        expected_rows.reverse()
    expected = [('value', 'label')] + expected_rows
    if presorted:
        tables = [etl.sort(table, reverse=reverse) for table in tables]

    result = etl.mergesort(*tables, reverse=reverse, presorted=presorted)

    assert list(result) == expected
    assert list(result) == expected


def test_unkeyed_mergesort_with_empty_output_header():
    tables = [[('id',), (1,)], [('id',), (2,)]]

    assert list(etl.mergesort(*tables, header=[], presorted=True)) == [(), (), ()]
