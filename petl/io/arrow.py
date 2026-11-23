"""Module for reading and writing `Arrow <https://arrow.apache.org/>`_ files.

Supports Parquet and Feather/IPC single-file formats through ``pyarrow``.

The ``pyarrow`` package is an optional dependency and is imported lazily, so
the rest of ``petl`` keeps working when it is not installed.

.. versionadded:: 1.8.0

"""
from __future__ import absolute_import, division, print_function  # noqa: UP010

import io
import os
from collections.abc import Mapping
from itertools import chain, islice

from petl.errors import ArgumentError
from petl.io.sources import read_source_from_arg, write_source_from_arg
from petl.util.base import Table, header

__all__ = (
    'fromarrow', 'toarrow',
)

# region API


def fromarrow(
    source,
    format=None,  # pylint: disable=redefined-builtin
    columns=None,
    options=None
):
    """Extract a table from a Parquet or Arrow IPC/Feather file.

    The `source` argument can be a string path, an `os.PathLike`, a
    :class:`petl.io.sources.URLSource`, a :class:`petl.io.sources.MemorySource`
    or any petl source object.

    The `format` argument selects the file format. When omitted (``None``),
    the format is auto-detected from the file header bytes. Supported values
    are ``'parquet'``, ``'feather'`` and ``'ipc'``.

    The `columns` argument (list of strings, optional) selects a subset of
    the columns to extract.

    The `options` argument (dict, optional) provides keyword arguments
    forwarded to the underlying reader, e.g.
    ``{'batch_size': 512}`` for `pyarrow.parquet.ParquetFile.iter_batches()`.

    This function is not attached to the :class:`petl.util.base.Table` class,
    because its first argument is the source. Use it as a module level or
    package level function instead.

    Example usage::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> sink = etl.MemorySource()
        >>> _ = etl.toarrow(table1, sink)  # doctest: +SKIP
        >>> src = etl.MemorySource(sink.getvalue())
        >>> table2 = etl.fromarrow(src)  # doctest: +SKIP
        >>> table2  # doctest: +SKIP
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' |   2 |
        +-----+-----+

    .. versionadded:: 1.8.0

    """
    if format is not None:
        _check_format(format)
    _check_options(options)
    if isinstance(source, os.PathLike):
        source = os.fspath(source)
    return ArrowView(source, format=format, columns=columns, options=options)


def toarrow(
    table,
    target,
    format='parquet',  # pylint: disable=redefined-builtin
    schema=None,
    options=None
):
    """Write a table to a Parquet or Arrow IPC/Feather file.

    The `target` argument can be a string path, an `os.PathLike`, a
    :class:`petl.io.sources.MemorySource` or any petl source object.

    The `format` argument selects the file format and defaults to
    ``'parquet'``. Supported values are ``'parquet'``, ``'feather'`` and
    ``'ipc'``.

    The `schema` argument (`pyarrow.Schema` or sequence of `(name, type)`
    pairs, optional) defines the output column names and types. Field names
    and order must match the table header, and the table must contain at
    least one column. When omitted, types are inferred from the data,
    falling back to ``pyarrow.string()`` for columns whose values are all
    ``None``.

    When ``schema`` is provided, the table is consumed in batches so memory
    usage does not grow with the number of rows. When omitted, rows are
    buffered until at least one non-``None`` value is observed for each
    column (which scans the entire table if a column is all ``None``).

    The `options` argument (dict, optional) provides keyword arguments
    forwarded to the underlying writer, e.g.
    ``{'compression': 'snappy'}`` for `pyarrow.parquet.ParquetWriter` or
    `pyarrow.ipc.new_file`.

    Example usage::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> sink = etl.MemorySource()
        >>> _ = etl.toarrow(table1, sink)  # doctest: +SKIP
        >>> _ = etl.wrap(table1).toarrow(sink)  # doctest: +SKIP

    Providing an explicit schema::

        >>> import pyarrow as pa  # doctest: +SKIP
        >>> schema = pa.schema([
        ...     pa.field('foo', pa.string()),
        ...     pa.field('bar', pa.int32()),
        ... ])  # doctest: +SKIP
        >>> _ = etl.toarrow(table1, sink, schema=schema)  # doctest: +SKIP

    .. versionadded:: 1.8.0

    """
    _check_format(format)
    _check_options(options)
    if isinstance(target, os.PathLike):
        target = os.fspath(target)
    arrow_schema, record_batches = _prepare_batches(table, schema)
    _write_file(target, format, arrow_schema, record_batches, options)
    return table


# endregion API

# region Implementation


SUPPORTED_FORMATS = ('parquet', 'feather', 'ipc')
PARQUET_MAGIC = b'PAR1'
ARROW_FILE_MAGIC = b'ARROW1'
BATCH_SIZE = 1024


class ArrowView(Table):

    def __init__(
        self,
        source,
        format=None,  # pylint: disable=redefined-builtin
        columns=None,
        options=None
    ):
        self._source = source
        self._format = format
        self._columns = columns
        self._options = options

    def __iter__(self):
        return _iter_file(
            self._source,
            self._format,
            columns=self._columns,
            options=self._options,
        )


class _PrefixedStream(io.RawIOBase):
    """Non-seekable byte stream prepending already-peeked header bytes."""

    def __init__(self, prefix, stream):
        self._prefix = prefix
        self._offset = 0
        self._stream = stream

    def readable(self):
        return True

    def read(self, size=-1):
        if self._offset < len(self._prefix):
            if size is None or size < 0:
                head = self._prefix[self._offset:]
                self._offset = len(self._prefix)
                return head + self._stream.read()
            end = min(self._offset + size, len(self._prefix))
            head = self._prefix[self._offset:end]
            self._offset = end
            remaining = size - len(head)
            if remaining > 0:
                return head + self._stream.read(remaining)
            return head
        return self._stream.read(size)

    def readall(self):
        return self.read(-1)


def _check_format(fmt):
    if fmt not in SUPPORTED_FORMATS:
        msg = 'unsupported format {!r}; expected one of {!r}'
        raise ArgumentError(msg.format(fmt, SUPPORTED_FORMATS))


def _check_options(options):
    if options is not None and not isinstance(options, Mapping):
        msg = 'expected options to be a dict or mapping, found {!r}'
        raise ArgumentError(msg.format(type(options).__name__))


def _iter_file(source, fmt, columns=None, options=None):
    opts = dict(options) if options is not None else {}
    source2 = read_source_from_arg(source)
    with source2.open('rb') as source_file:
        reader, resolved_fmt = _open_reader(source_file, fmt, opts)
        schema = getattr(reader, 'schema_arrow', reader.schema)
        names = list(schema.names)
        if columns is None:
            out_names = tuple(names)
            indices = list(range(len(names)))
        else:
            out_names = tuple(columns)
            indices = []
            for col in out_names:
                try:
                    indices.append(names.index(col))
                except ValueError:
                    msg = 'column {!r} not found in file header {!r}'
                    raise ArgumentError(msg.format(col, names))
        yield out_names
        if resolved_fmt == 'parquet':
            if columns is None:
                proj = None
                batch_indices = range(len(names))
            elif len(out_names) == 0:
                proj = [names[0]] if names else []
                batch_indices = ()
            else:
                proj = []
                for col in out_names:
                    if col not in proj:
                        proj.append(col)
                batch_indices = [proj.index(col) for col in out_names]
            for batch in reader.iter_batches(columns=proj, **opts):
                for row in _batch_rows(batch, batch_indices):
                    yield row
        else:
            for batch in _iter_ipc_batches(reader):
                for row in _batch_rows(batch, indices):
                    yield row


def _detect_format(head):
    if head.startswith(PARQUET_MAGIC):
        return 'parquet'
    if head.startswith(ARROW_FILE_MAGIC):
        return 'feather'
    return 'ipc'


def _open_reader(source_file, fmt, opts):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    import pyarrow.parquet as pq  # pylint: disable=import-outside-toplevel
    from pyarrow import ipc  # pylint: disable=import-outside-toplevel

    seekable = getattr(source_file, 'seekable', lambda: False)()
    if not seekable:
        head = source_file.read(8)
        resolved_fmt = _detect_format(head) if fmt is None else fmt
        if resolved_fmt == 'parquet':
            buf = io.BytesIO(head + source_file.read())
            return pq.ParquetFile(buf), resolved_fmt
        ipc_opts = opts if resolved_fmt != 'parquet' else {}
        if head.startswith(ARROW_FILE_MAGIC):
            buf = io.BytesIO(head + source_file.read())
            return ipc.open_file(buf, **ipc_opts), resolved_fmt
        stream = _PrefixedStream(head, source_file)
        return ipc.open_stream(stream, **ipc_opts), resolved_fmt

    if fmt is None:
        pos = source_file.tell()
        head = source_file.read(8)
        source_file.seek(pos)
        resolved_fmt = _detect_format(head)
    else:
        resolved_fmt = fmt

    if resolved_fmt == 'parquet':
        return pq.ParquetFile(source_file), resolved_fmt

    ipc_opts = opts if resolved_fmt != 'parquet' else {}
    try:
        return ipc.open_file(source_file, **ipc_opts), resolved_fmt
    except (pa.lib.ArrowException, OSError):
        source_file.seek(0)
        return ipc.open_stream(source_file, **ipc_opts), resolved_fmt


def _iter_ipc_batches(reader):
    if hasattr(reader, 'num_record_batches'):
        for index in range(reader.num_record_batches):
            yield reader.get_batch(index)
    else:
        while True:
            try:
                yield reader.read_next_batch()
            except StopIteration:
                return


def _batch_rows(batch, indices):
    indices = list(indices)
    if not indices:
        return (() for _ in range(batch.num_rows))
    values = [batch.column(index).to_pylist() for index in indices]
    return zip(*values)


def _prepare_batches(table, schema):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    it = iter(table)
    names = header(it)
    if not names:
        raise ArgumentError('cannot write a table with zero columns')
    padded = (_pad_row(row, len(names)) for row in it)
    if schema is not None:
        arrow_schema = (
            schema if isinstance(schema, pa.Schema) else pa.schema(schema)
        )
        if len(arrow_schema) == 0:
            raise ArgumentError('cannot write a table with zero columns')
        if list(arrow_schema.names) != list(names):
            msg = 'schema field names {!r} do not match table header {!r}'
            raise ArgumentError(
                msg.format(list(arrow_schema.names), list(names))
            )
        rows = padded
    else:
        sample = []
        unresolved = set(range(len(names)))
        while unresolved:
            chunk = list(islice(padded, BATCH_SIZE))
            if not chunk:
                break
            sample.extend(chunk)
            for row in chunk:
                for idx in list(unresolved):
                    if row[idx] is not None:
                        unresolved.remove(idx)
        arrow_schema = pa.schema(_infer_fields(names, sample))
        rows = chain(sample, padded)
    return arrow_schema, _record_batches(rows, arrow_schema)


def _pad_row(row, size):
    if len(row) == size:
        return row
    if len(row) > size:
        raise ArgumentError(
            'expected {} values in row, found {}'.format(  # noqa: UP032
                size, len(row)
            )
        )
    return tuple(row) + (None,) * (size - len(row))


def _infer_fields(names, sample):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    fields = []
    for index, name in enumerate(names):
        values = [
            row[index] for row in sample
            if index < len(row) and row[index] is not None
        ]
        # A column that is all None cannot use the null type in Parquet;
        # fall back to string when no non-null values exist.
        field_type = pa.array(values).type if values else pa.string()
        fields.append(pa.field(name, field_type))
    return fields


def _record_batches(rows, schema):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    while True:
        chunk = list(islice(rows, BATCH_SIZE))
        if not chunk:
            return
        col_values = list(zip(*chunk))
        arrays = [
            pa.array(values, type=field.type)
            for values, field in zip(col_values, schema)
        ]
        yield pa.RecordBatch.from_arrays(arrays, schema=schema)


def _write_file(target, fmt, arrow_schema, record_batches, options):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    import pyarrow.parquet as pq  # pylint: disable=import-outside-toplevel
    from pyarrow import ipc  # pylint: disable=import-outside-toplevel

    opts = dict(options) if options is not None else {}
    target2 = write_source_from_arg(target, mode='wb')
    with target2.open('wb') as target_file:
        if fmt == 'parquet':
            writer = pq.ParquetWriter(target_file, arrow_schema, **opts)
        else:
            writer = ipc.new_file(target_file, arrow_schema, **opts)
        try:
            for batch in record_batches:
                if fmt == 'parquet':
                    # ParquetWriter.write_batch() was added in PyArrow 7.0;
                    # use write_table() for PyArrow >=4.0 compatibility.
                    writer.write_table(
                        pa.Table.from_batches([batch], schema=arrow_schema)
                    )
                else:
                    writer.write_batch(batch)
        finally:
            writer.close()


# endregion Implementation


Table.toarrow = toarrow
