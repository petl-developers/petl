# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

# standard library dependencies
from petl.compat import PY2
from petl.io.pandas import fromdataframe, todataframe
# internal dependencies
from petl.util.base import Table
from petl.io.sources import read_source_from_arg, write_source_from_arg

# third-party dependencies
import pandas as pd


def fromparquet(source=None, **kwargs):
    """
    Extract data from a Parquet file and return as a PETL table.

    The input can be a local filesystem path or any URL supported by fsspec
    (e.g., S3, GCS).

    :param source: path or URL to Parquet file
    :param kwargs: passed through to pandas.read_parquet
    :returns: a PETL Table
    """
    src = read_source_from_arg(source)
    with src.open('rb') as f:
        df = pd.read_parquet(f, **kwargs)
    return fromdataframe(df)


def toparquet(table, source=None, **kwargs):
    """
    Write a PETL table or pandas DataFrame out to a Parquet file via pandas.

    :param table_or_df: PETL table or pandas DataFrame
    :param source: filesystem path or fsspec-supported URL for output
    :param kwargs: passed through to pandas.DataFrame.to_parquet
    :returns: the original PETL Table or pandas DataFrame
    """
    src = write_source_from_arg(source)
    with src.open('wb') as f:
        df = todataframe(table)
        df.to_parquet(f, **kwargs)
    return table


Table.fromparquet = fromparquet
Table.toparquet   = toparquet
