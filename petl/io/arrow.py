"""Module for reading and writing `Arrow <https://arrow.apache.org/>`_ files.

Supports Parquet and Feather/IPC single-file formats through ``pyarrow``.

The ``pyarrow`` package is an optional dependency and is imported lazily, so
the rest of ``petl`` keeps working when it is not installed.

.. versionadded:: 1.7.33

"""
from __future__ import absolute_import, division, print_function  # noqa: UP010

import os
from itertools import islice

from petl.errors import ArgumentError
from petl.io.sources import read_source_from_arg, write_source_from_arg
from petl.util.base import Table, header, iterpeek

__all__ = (
    'fromarrow', 'toarrow',
)

# region API


def fromarrow(
    source,
    format='parquet',  # pylint: disable=redefined-builtin
    columns=None,
    **kwargs
):
    """Extract a table from a Parquet or Arrow IPC/Feather file.

    The `source` argument can be a string path, an `os.PathLike`, a
    :class:`petl.io.sources.URLSource`, a :class:`petl.io.sources.MemorySource`
    or any petl source object.

    The `format` argument selects the file format and defaults to
    ``'parquet'``. Supported values are ``'parquet'``, ``'feather'`` and
    ``'ipc'``.

    The `columns` argument (list of strings, optional) selects a subset of
    the columns to extract.

    Additional keyword arguments are forwarded to the underlying reader, e.g.
    `pyarrow.parquet.ParquetFile.iter_batches()`.

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

    .. versionadded:: 1.7.33

    """
    _check_format(format)
    if isinstance(source, os.PathLike):
        source = os.fspath(source)
    return ArrowView(source, format=format, columns=columns, **kwargs)


def toarrow(
    table,
    target,
    format='parquet',  # pylint: disable=redefined-builtin
    schema=None,
    **kwargs
):
    """Write a table to a Parquet or Arrow IPC/Feather file.

    The `target` argument can be a string path, an `os.PathLike`, a
    :class:`petl.io.sources.MemorySource` or any petl source object.

    The `format` argument selects the file format and defaults to
    ``'parquet'``. Supported values are ``'parquet'``, ``'feather'`` and
    ``'ipc'``.

    The `schema` argument (`pyarrow.Schema`, optional) defines the column
    types. When omitted, the schema is inferred from a bounded sample of the
    `table`; columns whose sample values are all ``None`` are typed as string.

    The table is consumed in batches, so memory usage does not grow with the
    number of rows.

    Additional keyword arguments are forwarded to the underlying writer, e.g.
    `pyarrow.parquet.ParquetWriter` or `pyarrow.ipc.new_file`.

    Example usage::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> sink = etl.MemorySource()
        >>> _ = etl.toarrow(table1, sink)  # doctest: +SKIP
        >>> _ = etl.wrap(table1).toarrow(sink)  # doctest: +SKIP

    .. versionadded:: 1.7.33

    """
    _check_format(format)
    if isinstance(target, os.PathLike):
        target = os.fspath(target)
    arrow_schema, record_batches = _prepare_batches(table, schema)
    _write_file(target, format, arrow_schema, record_batches, kwargs)
    return table


# endregion API

# region Implementation


SUPPORTED_FORMATS = ('parquet', 'feather', 'ipc')
SAMPLE_SIZE = 1024
BATCH_SIZE = 1024


class ArrowView(Table):

    def __init__(
        self,
        source,
        format='parquet',  # pylint: disable=redefined-builtin
        columns=None,
        **kwargs
    ):
        self._source = source
        self._format = format
        self._columns = columns
        self._kwargs = kwargs

    def __iter__(self):
        return _iter_file(
            self._source, self._format, columns=self._columns, **self._kwargs
        )


def _check_format(fmt):
    if fmt not in SUPPORTED_FORMATS:
        msg = 'unsupported format {!r}; expected one of {!r}'
        raise ArgumentError(msg.format(fmt, SUPPORTED_FORMATS))


def _iter_file(source, fmt, columns=None, **kwargs):
    source2 = read_source_from_arg(source)
    with source2.open('rb') as source_file:
        reader = _open_reader(source_file, fmt)
        names = list(reader.schema.names)
        if columns is None:
            out_names = tuple(names)
            indices = list(range(len(names)))
        else:
            indices = [names.index(col) for col in columns]
            out_names = tuple(columns)
        yield out_names
        if fmt == 'parquet':
            proj = list(out_names) if columns is not None else None
            for batch in reader.iter_batches(columns=proj, **kwargs):
                for row in _batch_rows(batch, range(batch.num_columns)):
                    yield row
        else:
            for batch in _iter_ipc_batches(reader):
                for row in _batch_rows(batch, indices):
                    yield row


def _open_reader(source_file, fmt):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    import pyarrow.parquet as pq  # pylint: disable=import-outside-toplevel
    from pyarrow import ipc  # pylint: disable=import-outside-toplevel

    if fmt == 'parquet':
        return pq.ParquetFile(source_file)
    if not getattr(source_file, 'seekable', lambda: True)():
        return ipc.open_stream(source_file)
    try:
        return ipc.open_file(source_file)
    except (pa.lib.ArrowException, OSError):
        source_file.seek(0)
        return ipc.open_stream(source_file)


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
    values = [batch.column(index).to_pylist() for index in indices]
    return zip(*values)


def _prepare_batches(table, schema):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    it = iter(table)
    names = header(it)
    padded = (_pad_row(row, len(names)) for row in it)
    if schema is not None:
        arrow_schema = (
            schema if isinstance(schema, pa.Schema) else pa.schema(schema)
        )
        rows = padded
    else:
        sample, rows = iterpeek(padded, SAMPLE_SIZE)
        arrow_schema = pa.schema(_infer_fields(names, sample))
    return arrow_schema, _record_batches(rows, names, arrow_schema)


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
        # A column that is all None in the sample cannot use the null type,
        # because later chunks may have values that would not cast to it.
        field_type = pa.array(values).type if values else pa.string()
        fields.append(pa.field(name, field_type))
    return fields


def _record_batches(rows, names, schema):
    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    while True:
        chunk = list(islice(rows, BATCH_SIZE))
        if not chunk:
            return
        columns = dict(zip(names, zip(*chunk)))
        yield pa.Table.from_pydict(columns, schema=schema).to_batches()[0]


def _write_file(target, fmt, arrow_schema, record_batches, kwargs):
    import pyarrow.parquet as pq  # pylint: disable=import-outside-toplevel
    from pyarrow import ipc  # pylint: disable=import-outside-toplevel

    target2 = write_source_from_arg(target, mode='wb')
    with target2.open('wb') as target_file:
        if fmt == 'parquet':
            writer = pq.ParquetWriter(target_file, arrow_schema, **kwargs)
        else:
            writer = ipc.new_file(target_file, arrow_schema, **kwargs)
        try:
            for batch in record_batches:
                writer.write_batch(batch)
        finally:
            writer.close()


# endregion Implementation


Table.toarrow = toarrow
