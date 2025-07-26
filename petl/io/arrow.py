# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

# internal dependencies
from petl.util.base import Table, header, data
from petl.io.sources import read_source_from_arg, write_source_from_arg
# third-party dependencies
import pyarrow as pa                 # for Table construction
import pyarrow.dataset as ds         # for streaming reads and dataset writes
import pyarrow.parquet as pq         # for Parquet I/O

__all__ = (
    'fromarrow', 'toarrow',
)

def fromarrow(source=None, *, format='parquet', columns=None, **kwargs):
    """
    Extract data from an Arrow-compatible dataset into a PETL table.

    :param source: path/URL (fsspec-supported) to file(s) or directory
    :param format: dataset format ('parquet', 'orc', etc.)
    :param columns: list of columns to select
    :param kwargs: passed to pyarrow.dataset.dataset
    :returns: a PETL Table (streaming rows)
    """
    src = read_source_from_arg(source)
    dataset = ds.dataset(src.path, format=format, **kwargs)
    cols = [field.name for field in dataset.schema]

    def _rows():
        for batch in dataset.to_batches(columns=columns):
            for rec in batch.to_pylist():
                yield tuple(rec.get(c) for c in cols)

    from petl import fromrows  # import here to avoid circular
    return fromrows(cols, _rows())



def toarrow(table, target=None, *, format='parquet', schema=None, **kwargs):
    """
    Write a PETL table to an Arrow file or dataset.

    :param table: PETL Table (first row = header)
    :param target: path/URL for file or directory
    :param format: format name ('parquet', 'ipc', etc.)
    :param schema: optional pyarrow.Schema
    :param kwargs: passed to writer (write_table or write_dataset)
    :returns: the original PETL Table for chaining
    """
    tgt = write_source_from_arg(target)
    hdr = header(table)
    rows = data(table)

    arrays = {c: [] for c in hdr}
    for row in rows:
        for c, v in zip(hdr, row):
            arrays[c].append(v)

    arrow_tbl = pa.Table.from_pydict(arrays, schema=schema)

    if format == 'parquet':
        with tgt.open('wb') as f:
            pq.write_table(arrow_tbl, f, **kwargs)
    else:
        ds.write_dataset(arrow_tbl, base_dir=tgt.path, format=format, **kwargs)

    return table

# Attach to Table class
Table.fromarrow = staticmethod(fromarrow)
Table.toarrow = staticmethod(toarrow)
