from datetime import datetime, time, timezone

import pytest

import petl as etl

pytest.importorskip('fastavro')


@pytest.mark.parametrize('value', [time(12, 34, 56, 123456), time(0, 0, 0, 1),
                                 datetime(2026, 11, 25, 12, 34, 56, 123456, tzinfo=timezone.utc),
                                 datetime(1960, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc)])
def test_inferred_temporal_fields_round_trip_microseconds(tmp_path, value):
    path = str(tmp_path / 'temporal.avro')
    table = [('value',), (value,)]
    etl.toavro(table, path)
    assert list(etl.fromavro(path)) == table


def test_explicit_millisecond_schema_remains_respected(tmp_path):
    path = str(tmp_path / 'explicit.avro')
    schema = {'type': 'record', 'name': 'clock', 'fields': [
        {'name': 'value', 'type': {'type': 'int', 'logicalType': 'time-millis'}}]}
    etl.toavro([('value',), (time(12, 0, 0, 123456),)], path, schema=schema)
    assert list(etl.fromavro(path))[1] == (time(12, 0, 0, 123000),)
