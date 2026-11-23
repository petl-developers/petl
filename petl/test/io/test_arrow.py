from __future__ import absolute_import, division, print_function  # noqa: UP010

import datetime
import decimal
import io

import pytest

from petl.errors import ArgumentError
from petl.io.arrow import fromarrow, toarrow
from petl.io.sources import MemorySource, URLSource
from petl.test.helpers import ieq
from petl.util.base import Table

try:
    import pyarrow  # noqa: F401  # pylint: disable=unused-import
except ImportError as e:
    pytest.skip('SKIP arrow tests: {}'.format(e), allow_module_level=True)  # noqa: UP032


table1 = (('foo', 'bar'),
          ('a', 1),
          ('b', 2),
          ('c', 2))


class _NonSeekableBytesIO(io.RawIOBase):
    """Non-seekable byte stream emulating HTTP/URL responses."""

    def __init__(self, data):
        self._inner = io.BytesIO(data)

    def readable(self):
        return True

    def read(self, size=-1):
        return self._inner.read(size)

    def readall(self):
        return self._inner.read()

    def seekable(self):
        return False


class _NonSeekableSource(object):  # pylint: disable=too-few-public-methods,useless-object-inheritance

    def __init__(self, data):
        self._data = data

    def open(self, _mode):
        return _NonSeekableBytesIO(self._data)


def test_toarrow_parquet(tmp_path):
    f = str(tmp_path / 'example.parquet')
    toarrow(table1, f)
    ieq(table1, fromarrow(f))


def test_toarrow_feather(tmp_path):
    from pyarrow import feather  # pylint: disable=import-outside-toplevel

    f = str(tmp_path / 'example.feather')
    toarrow(table1, f, format='feather')
    ieq(table1, fromarrow(f, format='feather'))
    tbl = feather.read_table(f)
    assert tbl.column('foo').to_pylist() == ['a', 'b', 'c']
    assert tbl.column('bar').to_pylist() == [1, 2, 2]


def test_toarrow_ipc(tmp_path):
    f = str(tmp_path / 'example.ipc')
    toarrow(table1, f, format='ipc')
    ieq(table1, fromarrow(f, format='ipc'))


def test_fromarrow_autodetect_format():
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    from pyarrow import ipc  # pylint: disable=import-outside-toplevel

    # Parquet auto-detection
    pq_sink = MemorySource()
    toarrow(table1, pq_sink, format='parquet')
    ieq(table1, fromarrow(MemorySource(pq_sink.getvalue())))

    # Feather / IPC file format auto-detection
    feather_sink = MemorySource()
    toarrow(table1, feather_sink, format='feather')
    ieq(table1, fromarrow(MemorySource(feather_sink.getvalue())))

    # IPC stream format auto-detection
    stream_buf = io.BytesIO()
    data = pa.Table.from_pydict({'foo': ['a', 'b'], 'bar': [1, 2]})
    with ipc.new_stream(stream_buf, data.schema) as writer:
        writer.write_table(data)
    ieq((('foo', 'bar'), ('a', 1), ('b', 2)),
        fromarrow(MemorySource(stream_buf.getvalue())))


def test_fromarrow_ipc_stream_seekable(tmp_path):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    from pyarrow import ipc  # pylint: disable=import-outside-toplevel

    f = str(tmp_path / 'stream.ipc')
    data = pa.Table.from_pydict({'foo': ['a', 'b'], 'bar': [1, 2]})
    with open(f, 'wb') as fh, ipc.new_stream(fh, data.schema) as writer:
        writer.write_table(data)
    ieq((('foo', 'bar'), ('a', 1), ('b', 2)),
        fromarrow(f, format='ipc'))


def test_fromarrow_non_seekable_streams(monkeypatch):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    from pyarrow import ipc  # pylint: disable=import-outside-toplevel
    import petl.io.sources as sources_mod  # pylint: disable=import-outside-toplevel

    # Non-seekable Parquet (explicit and auto-detected)
    pq_sink = MemorySource()
    toarrow(table1, pq_sink, format='parquet')
    pq_bytes = pq_sink.getvalue()
    ieq(table1, fromarrow(_NonSeekableSource(pq_bytes), format='parquet'))
    ieq(table1, fromarrow(_NonSeekableSource(pq_bytes)))

    # Non-seekable Feather / IPC file (explicit and auto-detected)
    feather_sink = MemorySource()
    toarrow(table1, feather_sink, format='feather')
    feather_bytes = feather_sink.getvalue()
    ieq(table1, fromarrow(_NonSeekableSource(feather_bytes), format='feather'))
    ieq(table1, fromarrow(_NonSeekableSource(feather_bytes)))

    # Non-seekable IPC stream (explicit and auto-detected)
    stream_buf = io.BytesIO()
    data = pa.Table.from_pydict({'foo': ['a'], 'bar': [1]})
    with ipc.new_stream(stream_buf, data.schema) as writer:
        writer.write_table(data)
    stream_bytes = stream_buf.getvalue()
    ieq((('foo', 'bar'), ('a', 1)),
        fromarrow(_NonSeekableSource(stream_bytes), format='ipc'))
    ieq((('foo', 'bar'), ('a', 1)),
        fromarrow(_NonSeekableSource(stream_bytes)))

    # URLSource integration with non-seekable HTTP response streams
    monkeypatch.setattr(
        sources_mod, 'urlopen', lambda _url: _NonSeekableBytesIO(pq_bytes)
    )
    ieq(table1, fromarrow(URLSource('http://example.com/data.parquet')))

    monkeypatch.setattr(
        sources_mod,
        'urlopen',
        lambda _url: _NonSeekableBytesIO(feather_bytes),
    )
    ieq(table1, fromarrow(URLSource('http://example.com/data.feather')))

    monkeypatch.setattr(
        sources_mod,
        'urlopen',
        lambda _url: _NonSeekableBytesIO(stream_bytes),
    )
    ieq((('foo', 'bar'), ('a', 1)),
        fromarrow(URLSource('http://example.com/data.ipc')))


def test_fromarrow_columns(tmp_path):
    f = str(tmp_path / 'cols.parquet')
    toarrow(table1, f)
    ieq((('bar',), (1,), (2,), (2,)), fromarrow(f, columns=['bar']))
    ieq((('bar', 'foo'), (1, 'a'), (2, 'b'), (2, 'c')),
        fromarrow(f, columns=['bar', 'foo']))
    ieq(((), (), (), ()), fromarrow(f, columns=[]))
    ieq((('bar',), (1,), (2,), (2,)),
        fromarrow(f, columns=(c for c in ['bar'])))
    ieq(((), (), (), ()), fromarrow(f, columns=iter([])))

    f_ipc = str(tmp_path / 'cols.ipc')
    toarrow(table1, f_ipc, format='ipc')
    ieq(((), (), (), ()), fromarrow(f_ipc, format='ipc', columns=[]))


def test_toarrow_duplicate_headers(tmp_path):
    tbl = (('foo', 'foo'),
           ('a', 1),
           ('b', 2))

    f_pq = str(tmp_path / 'dup.parquet')
    toarrow(tbl, f_pq)
    ieq(tbl, fromarrow(f_pq))

    f_ipc = str(tmp_path / 'dup.ipc')
    toarrow(tbl, f_ipc, format='ipc')
    ieq(tbl, fromarrow(f_ipc, format='ipc'))


def test_toarrow_zero_columns(tmp_path):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    f = str(tmp_path / 'zero.parquet')
    with pytest.raises(ArgumentError):
        toarrow(((), ()), f)
    with pytest.raises(ArgumentError):
        toarrow((('foo',), ('a',)), f, schema=pa.schema([]))


def test_toarrow_schema(tmp_path):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    f = str(tmp_path / 'schema.parquet')
    schema = pa.schema([('foo', pa.string()), ('bar', pa.int32())])
    toarrow(table1, f, schema=schema)
    ieq(table1, fromarrow(f))

    f_pairs = str(tmp_path / 'schema_pairs.parquet')
    toarrow(table1, f_pairs, schema=[('foo', pa.string()), ('bar', pa.int32())])
    ieq(table1, fromarrow(f_pairs))


def test_toarrow_python_types_roundtrip(tmp_path):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    tbl = (
        ('d', 't', 'dt', 'arr', 'obj', 'dec', 'flt'),
        (
            datetime.date(2026, 1, 15),
            datetime.time(14, 30, 15),
            datetime.datetime(2026, 1, 15, 14, 30, 15),
            ['x', 'y'],
            {'k': 1, 'v': 'ok'},
            decimal.Decimal('12.34'),
            3.14,
        ),
        (
            datetime.date(2026, 11, 21),
            datetime.time(8, 0, 0),
            datetime.datetime(2026, 11, 21, 8, 0, 0),
            [],
            {'k': 2, 'v': 'done'},
            decimal.Decimal('-99.95'),
            -0.5,
        ),
    )

    # Inferred schema round-trip
    f_inferred = str(tmp_path / 'types_inferred.parquet')
    toarrow(tbl, f_inferred)
    ieq(tbl, fromarrow(f_inferred))

    # Explicit schema round-trip
    schema = pa.schema([
        pa.field('d', pa.date32()),
        pa.field('t', pa.time64('us')),
        pa.field('dt', pa.timestamp('us')),
        pa.field('arr', pa.list_(pa.string())),
        pa.field('obj', pa.struct([('k', pa.int64()), ('v', pa.string())])),
        pa.field('dec', pa.decimal128(10, 2)),
        pa.field('flt', pa.float64()),
    ])
    f_explicit = str(tmp_path / 'types_explicit.parquet')
    toarrow(tbl, f_explicit, schema=schema)
    ieq(tbl, fromarrow(f_explicit))


def test_arrow_options(tmp_path):
    f = str(tmp_path / 'opts.parquet')
    toarrow(table1, f, options={'compression': 'snappy'})
    ieq(table1, fromarrow(f, options={'batch_size': 2}))

    with pytest.raises(ArgumentError):
        toarrow(table1, f, options='invalid')
    with pytest.raises(ArgumentError):
        fromarrow(f, options=['invalid'])


def test_toarrow_empty(tmp_path):
    f = str(tmp_path / 'empty.parquet')
    toarrow((('foo', 'bar'),), f)
    assert list(fromarrow(f)) == [('foo', 'bar')]


def test_toarrow_nulls_and_late_typed_values(tmp_path):
    f = str(tmp_path / 'nulls.parquet')
    tbl = (('foo', 'bar'), ('a', None), ('b', 2))
    toarrow(tbl, f)
    ieq(tbl, fromarrow(f))

    # Columns that are all None in the first 1,024 rows but numeric later
    f_late = str(tmp_path / 'late_types.parquet')
    late_tbl = (
        [('id', 'num', 'flt', 'all_null')]
        + [(i, None, None, None) for i in range(1100)]
        + [(1100, 42, 3.5, None)]
    )
    toarrow(late_tbl, f_late)
    ieq(late_tbl, fromarrow(f_late))


def test_toarrow_schema_mismatch_and_unknown_column(tmp_path):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    f = str(tmp_path / 'sample.parquet')
    toarrow(table1, f)

    with pytest.raises(ArgumentError):
        list(fromarrow(f, columns=['nonexistent']))

    with pytest.raises(ArgumentError):
        toarrow(table1, str(tmp_path / 'bad_count.parquet'),
                schema=[('foo', pa.string())])

    with pytest.raises(ArgumentError):
        toarrow(table1, str(tmp_path / 'bad_name.parquet'),
                schema=[('foo', pa.string()), ('wrong', pa.int64())])


def test_toarrow_ragged_rows(tmp_path):
    f = str(tmp_path / 'ragged.parquet')
    toarrow((('foo', 'bar'), ('a',), ('b', 2)), f)
    ieq((('foo', 'bar'), ('a', None), ('b', 2)), fromarrow(f))
    with pytest.raises(ArgumentError):
        toarrow((('foo', 'bar'), ('a', 1, 2)), f)


def test_fromarrow_missing_file(tmp_path):
    f = str(tmp_path / 'nope.parquet')
    with pytest.raises(OSError):
        list(fromarrow(f))


def test_toarrow_returns_table(tmp_path):
    f = str(tmp_path / 'ret.parquet')
    result = toarrow(table1, f)
    ieq(table1, result)


def test_toarrow_batching(tmp_path):
    f = str(tmp_path / 'many.parquet')
    tbl = [('foo', 'bar')] + [('x', i) for i in range(5000)]
    toarrow(tbl, f)
    ieq(tbl, fromarrow(f))


def test_toarrow_chunk_schema_stability(tmp_path):
    f = str(tmp_path / 'stable.parquet')
    tbl = [('foo', 'bar')] + [(None, 'x')] * 1100 + [('a', 'y')]
    toarrow(tbl, f)
    ieq(tbl, fromarrow(f))


def test_table_methods(tmp_path):
    import petl as etl  # pylint: disable=import-outside-toplevel

    f = str(tmp_path / 'method.parquet')
    etl.wrap(table1).toarrow(f)
    ieq(table1, etl.fromarrow(f))
    assert not hasattr(Table, 'fromarrow')
    assert callable(etl.fromarrow)


def test_pathlike_source_and_target(tmp_path):
    f = tmp_path / 'pathlike.parquet'
    toarrow(table1, f)
    ieq(table1, fromarrow(f))


def test_unsupported_format(tmp_path):
    f = str(tmp_path / 'data.orc')
    with pytest.raises(ArgumentError):
        toarrow(table1, f, format='orc')
    with pytest.raises(ArgumentError):
        fromarrow(f, format='orc')


def test_toarrow_memory_source():
    import pyarrow.parquet as pq  # pylint: disable=import-outside-toplevel

    sink = MemorySource()
    toarrow(table1, sink)
    actual = pq.read_table(io.BytesIO(sink.getvalue()))
    assert actual.column('foo').to_pylist() == ['a', 'b', 'c']
    ieq(table1, fromarrow(MemorySource(sink.getvalue())))
