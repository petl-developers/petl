import pytest

import petl as etl
from petl.errors import FieldSelectionError


@pytest.mark.parametrize('index', [-1, 1])
def test_conversion_index_uses_header_position_on_uneven_rows(index):
    table = [('id', 'value'), (1, 2), (3,), (4, 5, 999)]

    result = etl.convert(table, index, lambda value: value * 10)

    assert list(result) == [('id', 'value'), (1, 20), (3,), (4, 50, 999)]


def test_negative_index_with_record_and_where():
    table = [('id', 'value'), (1, 2), (3, 4)]

    result = etl.convert(table, -1, lambda value, row: value + row.id,
                         pass_row=True, where=lambda row: row.id == 1)

    assert list(result) == [('id', 'value'), (1, 3), (3, 4)]


@pytest.mark.parametrize('index', [-3, 2])
def test_conversion_rejects_indexes_outside_header(index):
    table = [('id', 'value'), (1, 2, 999)]

    with pytest.raises(FieldSelectionError):
        list(etl.convert(table, index, int))
