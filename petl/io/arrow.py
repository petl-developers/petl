# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

# internal dependencies
from petl.util.base import Table, header, data

__all__ = (
    'fromarrow', 'toarrow',
)

def fromarrow(source, **kwargs):
    # Lazy import so module can load on Python 2
    import pyarrow as pa
    import pyarrow.dataset as ds
    fmt      = kwargs.pop('format', 'parquet')
    cols_opt = kwargs.pop('columns', None)
    dataset  = ds.dataset(source, format=fmt, **kwargs)
    column_names = [field.name for field in dataset.schema]

    def all_rows():
        # header row
        yield tuple(column_names)
        # data rows
        for batch in dataset.to_batches(columns=cols_opt):
            for rec in batch.to_pylist():
                yield tuple(rec.get(c) for c in column_names)

    return Table(all_rows())


def toarrow(table, target, **kwargs):
    # Lazy imports so module can load on Python 2
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
    fmt    = kwargs.pop('format', 'parquet')
    schema = kwargs.pop('schema', None)

    hdr  = header(table)
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
Table.toarrow   = staticmethod(toarrow)
