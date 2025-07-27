# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

# internal dependencies
from petl.util.base import Table, header, data

# third-party dependencies
import pyarrow as pa               # PyArrow core
import pyarrow.dataset as ds       # Arrow dataset API
import pyarrow.parquet as pq       # Parquet reader/writer

__all__ = (
    'fromarrow', 'toarrow',
)

def fromarrow(source, **kwargs):
    """
    Extract data from an Arrow-compatible dataset into a PETL Table.

    :param source: file path, list of paths, or directory
    :param format: dataset format (e.g., 'parquet', 'orc', 'ipc'); default 'parquet'
    :param columns: list of columns to load; default None (all)
    :param kwargs: other keyword arguments passed to pyarrow.dataset.dataset
    :returns: a streaming PETL Table (first row is header)
    """
    fmt = kwargs.pop('format', 'parquet')
    cols_opt = kwargs.pop('columns', None)
    dataset = ds.dataset(source, format=fmt, **kwargs)
    column_names = [field.name for field in dataset.schema]

    def all_rows():
        # yield header
        yield tuple(column_names)
        # yield data rows
        for batch in dataset.to_batches(columns=cols_opt):
            for rec in batch.to_pylist():
                yield tuple(rec.get(c) for c in column_names)

    return Table(all_rows())


def toarrow(table, target, **kwargs):
    """
    Write a PETL Table to an Arrow dataset or file.

    :param table: PETL Table (first row is header)
    :param target: output file path or directory
    :param format: format name (e.g., 'parquet', 'ipc', 'orc'); default 'parquet'
    :param schema: optional pa.Schema; default None (infer schema)
    :param kwargs: passed to writer (pq.write_table or ds.write_dataset)
    :returns: the original PETL Table
    """
    fmt = kwargs.pop('format', 'parquet')
    schema = kwargs.pop('schema', None)
    hdr = header(table)
    rows = data(table)

    # accumulate data by column
    arrays = {c: [] for c in hdr}
    for row in rows:
        for c, v in zip(hdr, row):
            arrays[c].append(v)

    # build Arrow Table
    arrow_tbl = pa.Table.from_pydict(arrays, schema=schema)

    if fmt == 'parquet':
        # single-file Parquet write
        pq.write_table(arrow_tbl, target, **kwargs)
    else:
        # directory-based dataset write for other formats
        ds.write_dataset(arrow_tbl, target, format=fmt, **kwargs)

    return table

# attach to Table class
Table.fromarrow = staticmethod(fromarrow)
Table.toarrow = staticmethod(toarrow)
