# petl/test/test_standardize.py
import math
from petl import standardize

def approx_equal(a, b, tol=1e-9):
    return abs(a - b) <= tol

def test_standardize_overwrite_single_column():
    table = [
        ('id', 'x', 'y'),
        (1, 10.0, 100),
        (2, 20.0, 200),
        (3, 30.0, 300),
    ]

    out = list(standardize(table, 'x'))  # overwrite x
    header, rows = out[0], out[1:]
    assert header == ('id', 'x', 'y')

    # standardized x should have mean ~0 and std ~1 (population by default ddof=0)
    xs = [r[1] for r in rows]
    m = sum(xs) / len(xs)
    var = sum((v - m) ** 2 for v in xs) / len(xs)
    assert approx_equal(m, 0.0, 1e-12)
    assert approx_equal(var, 1.0, 1e-12)

def test_standardize_write_new_column_multiple_fields():
    table = [
        ('id', 'a', 'b'),
        (1, 5,  10.0),
        (2, 6,  20.0),
        (3, 7,  30.0),
        (4, 8,  None),
    ]

    out = list(standardize(table, fields=['a', 'b'], newfields=['a_z', 'b_z'], ddof=1))
    header, rows = out[0], out[1:]
    assert header == ('id', 'a', 'b', 'a_z', 'b_z')

    # Check lengths and None handling
    assert len(rows) == 4
    assert rows[-1][3] is not None  # a_z exists
    assert rows[-1][4] is None      # b was None → stays None in z

def test_standardize_constant_column_returns_zeroes():
    table = [
        ('id', 'c'),
        (1, 5.0),
        (2, 5.0),
        (3, 5.0),
    ]
    out = list(standardize(table, 'c'))
    xs = [r[1] for r in out[1:]]
    assert all(approx_equal(x, 0.0) for x in xs)

def test_standardize_raises_on_missing_field():
    table = [
        ('id', 'x'),
        (1, 1.0),
    ]
    try:
        _ = standardize(table, 'nope')
        assert False, "expected KeyError"
    except KeyError:
        assert True
