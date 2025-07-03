import os
import pandas as pd
import pytest
import petl as etl

def make_sample(tmp_path):
    data = [{'x': 1}, {'x': 2}, {'x': 3}]
    df = pd.DataFrame(data)
    path = tmp_path / 'foo.parquet'
    df.to_parquet(path)
    return path

def test_fromparquet(tmp_path):
    path = make_sample(tmp_path)
    tbl = etl.io.fromparquet(str(path))
    assert tbl.header() == ('x',)
    assert list(tbl.values()) == [(1,), (2,), (3,)]

def test_toparquet(tmp_path):
    tbl = etl.fromdicts([{'y':10},{'y':20}])
    out = tmp_path / 'out.parquet'
    tbl.toparquet(str(out))
    df2 = pd.read_parquet(out)
    assert list(df2['y']) == [10,20]


