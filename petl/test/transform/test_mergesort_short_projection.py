import pytest

import petl as etl


@pytest.mark.parametrize('presorted', [False, True])
def test_mergesort_pads_short_rows_when_omitting_source_fields(presorted):
    first = [('id', 'unused', 'value'), (1, 'ignored')]
    second = [('id', 'value', 'unused'), (2, 'two', 'ignored')]
    result = etl.mergesort(first, second, key='id', header=['id', 'value'],
                          missing='missing', presorted=presorted)
    expected = [('id', 'value'), (1, 'missing'), (2, 'two')]

    assert list(result) == expected
    assert list(result) == expected


def test_mergesort_short_projection_can_reorder_and_add_fields():
    first = [('id', 'unused', 'value'), (1, 'ignored')]
    second = [('value', 'id'), ('two', 2)]

    result = etl.mergesort(first, second, key='id', header=['value', 'new', 'id'])

    assert list(result) == [('value', 'new', 'id'), (None, None, 1), ('two', None, 2)]


def test_mergesort_projection_of_complete_rows_is_unchanged():
    table = [('id', 'value', 'unused'), (1, 'one', 'ignored')]

    assert list(etl.mergesort(table, key='id', header=['value', 'id'])) == [
        ('value', 'id'), ('one', 1)]
