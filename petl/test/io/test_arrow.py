from __future__ import absolute_import, division, print_function  # noqa: UP010

import io

import pytest

from petl.errors import ArgumentError
from petl.io.arrow import fromarrow, toarrow
from petl.io.sources import MemorySource
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


def test_toarrow_parquet(tmp_path):
    f = str(tmp_path / 'example.parquet')
    toarrow(table1, f)
    actual = fromarrow(f)
    ieq(table1, actual)
    ieq(table1, actual)


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


def test_fromarrow_ipc_stream_seekable(tmp_path):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    from pyarrow import ipc  # pylint: disable=import-outside-toplevel

    f = str(tmp_path / 'stream.ipc')
    data = pa.Table.from_pydict({'foo': ['a', 'b'], 'bar': [1, 2]})
    with open(f, 'wb') as fh, ipc.new_stream(fh, data.schema) as writer:
        writer.write_table(data)
    ieq((('foo', 'bar'), ('a', 1), ('b', 2)),
        fromarrow(f, format='ipc'))


def test_fromarrow_ipc_non_seekable(tmp_path):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    from pyarrow import ipc  # pylint: disable=import-outside-toplevel

    f = str(tmp_path / 'stream-ns.ipc')
    data = pa.Table.from_pydict({'foo': ['a'], 'bar': [1]})
    with open(f, 'wb') as fh, ipc.new_stream(fh, data.schema) as writer:
        writer.write_table(data)
    with open(f, 'rb') as fh:
        raw = fh.read()

    class NonSeekable:
        closed = False

        def __init__(self, inner):
            self.inner = inner

        def read(self, size=-1):
            return self.inner.read(size)

        def seekable(self):
            return False

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    class Source:  # pylint: disable=too-few-public-methods

        def open(self, _mode):
            return NonSeekable(io.BytesIO(raw))

    ieq((('foo', 'bar'), ('a', 1)), fromarrow(Source(), format='ipc'))


def test_fromarrow_columns(tmp_path):
    f = str(tmp_path / 'cols.parquet')
    toarrow(table1, f)
    actual = fromarrow(f, columns=['bar'])
    ieq((('bar',), (1,), (2,), (2,)), actual)


def test_toarrow_schema(tmp_path):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    f = str(tmp_path / 'schema.parquet')
    schema = pa.schema([('foo', pa.string()), ('bar', pa.int32())])
    toarrow(table1, f, schema=schema)
    ieq(table1, fromarrow(f))


def test_toarrow_empty(tmp_path):
    f = str(tmp_path / 'empty.parquet')
    toarrow((('foo', 'bar'),), f)
    assert list(fromarrow(f)) == [('foo', 'bar')]


def test_toarrow_nulls(tmp_path):
    f = str(tmp_path / 'nulls.parquet')
    tbl = (('foo', 'bar'), ('a', None), ('b', 2))
    toarrow(tbl, f)
    ieq(tbl, fromarrow(f))


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
