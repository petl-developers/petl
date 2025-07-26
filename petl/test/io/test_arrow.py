# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

# internal dependencies
from petl.util.base import Table, header, data  # core PETL Table helpers

# third-party dependencies
import pyarrow as pa               # for Table construction
import pyarrow.dataset as ds       # for streaming reads and dataset writes
import pyarrow.parquet as pq       # for Parquet I/O

__all__ = (
    'fromarrow', 'toarrow',
)

def fromarrow(source, *, format='parquet', columns=None, **kwargs):
    """
    Extract data from an Arrow-compatible dataset into a PETL table.

    :param source: path or list of paths for file(s) or directory
    :param format: dataset format ('parquet', 'orc', etc.)
    :param columns: list of columns to select
    :param kwargs: passed to pyarrow.dataset.dataset
    :returns: a PETL Table with streaming rows
    """
    # Create a PyArrow Dataset directly from the source path(s)
    dataset = ds.dataset(source, format=format, **kwargs)
    cols = [field.name for field in dataset.schema]

    def all_rows():
        # header row
        yield tuple(cols)
        # data rows
        for batch in dataset.to_batches(columns=columns):
            for rec in batch.to_pylist():
                yield tuple(rec.get(c) for c in cols)

    # Wrap the generator in a PETL Table
    return Table(all_rows())


def toarrow(table, target, *, format='parquet', schema=None, **kwargs):
    """
    Write a PETL table to an Arrow file or dataset.

    :param table: PETL Table (first row = header)
    :param target: output file path or directory
    :param format: format name ('parquet', 'ipc', etc.)
    :param schema: optional PyArrow Schema
    :param kwargs: passed to the writer
    :returns: the original PETL Table for chaining
    """
    # Extract header and data rows
    hdr = header(table)
    rows = data(table)

    # Build column-wise Python lists
    arrays = {c: [] for c in hdr}
    for row in rows:
        for c, v in zip(hdr, row):
            arrays[c].append(v)

    # Create an Arrow Table
    arrow_tbl = pa.Table.from_pydict(arrays, schema=schema)

    if format == 'parquet':
        # Write a single Parquet file
        pq.write_table(arrow_tbl, target, **kwargs)
    else:
        # Write a directory-based dataset
        ds.write_dataset(arrow_tbl, target, format=format, **kwargs)

    return table

# Attach methods to the PETL Table class
Table.fromarrow = staticmethod(fromarrow)
Table.toarrow = staticmethod(toarrow)
