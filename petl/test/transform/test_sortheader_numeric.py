import pytest

import petl as etl


@pytest.mark.parametrize('reverse', [False, True])
def test_sortheader_keeps_numeric_labels_with_their_values(reverse):
    table = [(2, 0, 1), ('two', 'zero', 'one')]
    expected = [(0, 1, 2), ('zero', 'one', 'two')]
    if reverse:
        expected = [tuple(reversed(row)) for row in expected]

    result = etl.sortheader(table, reverse=reverse)

    assert list(result) == expected
    assert list(result) == expected


def test_sortheader_large_numeric_labels_and_short_rows():
    table = [(30, 10, 20), ('thirty', 'ten')]

    assert list(etl.sortheader(table, missing='missing')) == [
        (10, 20, 30), ('ten', 'missing', 'thirty')]


def test_sortheader_preserves_duplicate_label_order():
    table = [('b', 'a', 'a'), (3, 1, 2)]

    assert list(etl.sortheader(table)) == [('a', 'a', 'b'), (1, 2, 3)]
