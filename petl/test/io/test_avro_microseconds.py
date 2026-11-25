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


@pytest.mark.parametrize('avro_type, logical_type, value', [
    ('int', 'time-millis', time(12, 0, 0, 123456)),
    ('long', 'timestamp-millis', datetime(2026, 11, 25, 12, 0, 0, 123456, tzinfo=timezone.utc)),
])
def test_explicit_millisecond_schema_remains_respected(tmp_path, avro_type, logical_type, value):
    path = str(tmp_path / 'explicit.avro')
    schema = {'type': 'record', 'name': 'clock', 'fields': [
        {'name': 'value', 'type': {'type': avro_type, 'logicalType': logical_type}}]}
    etl.toavro([('value',), (value,)], path, schema=schema)
    assert list(etl.fromavro(path))[1] == (value.replace(microsecond=123000),)


@pytest.mark.parametrize('value', [
    time(12, 0, 0, 123456),
    datetime(2026, 11, 25, 12, 0, 0, 123456, tzinfo=timezone.utc),
])
def test_microseconds_first_seen_after_the_sample_are_preserved(tmp_path, value):
    path = str(tmp_path / 'late.avro')
    table = [('value',), (value.replace(microsecond=0),), (value,)]
    etl.toavro(table, path, sample=1)
    assert list(etl.fromavro(path)) == table


@pytest.mark.parametrize('avro_type, logical_type, value', [
    ('int', 'time-millis', time(12, 0, 0, 123456)),
    ('long', 'timestamp-millis', datetime(2026, 11, 25, 12, 0, 0, 123456, tzinfo=timezone.utc)),
])
def test_appending_to_millisecond_files_keeps_the_stored_schema(tmp_path, avro_type, logical_type, value):
    path = str(tmp_path / 'existing.avro')
    field_type = {'type': avro_type, 'logicalType': logical_type}
    schema = {'type': 'record', 'name': 'clock', 'fields': [
        {'name': 'value', 'type': field_type}]}
    etl.toavro([('value',), (value,)], path, schema=schema)
    etl.appendavro([('value',), (value,)], path)
    loaded = etl.fromavro(path)
    expected = value.replace(microsecond=123000)
    assert list(loaded) == [('value',), (expected,), (expected,)]
    assert loaded.get_avro_schema()['fields'][0]['type'] == field_type
