from decimal import Decimal

import pytest

import petl as etl

pytest.importorskip('fastavro')


@pytest.mark.parametrize('values', [[12, None, 24], [None, 'alpha'], [None],
                                  [Decimal('1.25'), None, Decimal('2.5')],
                                  [[12, None], None, [24]]])
def test_inferred_array_items_round_trip_nulls(tmp_path, values):
    path = str(tmp_path / 'arrays.avro')
    table = [('values',), (values,)]
    etl.toavro(table, path)
    assert list(etl.fromavro(path)) == table


def test_null_item_support_survives_later_nonnull_samples(tmp_path):
    path = str(tmp_path / 'arrays.avro')
    table = [('values',), ([12, None],), ([24, 36],)]
    etl.toavro(table, path)
    assert list(etl.fromavro(path)) == table


def test_regular_and_empty_arrays_keep_their_values(tmp_path):
    path = str(tmp_path / 'arrays.avro')
    table = [('values',), ([],), ([12, 24],)]
    etl.toavro(table, path)
    assert list(etl.fromavro(path)) == table
