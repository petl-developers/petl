from __future__ import absolute_import, print_function, division
import pytest
import petl as etl
from petl.errors import FieldSelectionError


# ---------------------------------------------------------------------------
# Basic behaviour
# ---------------------------------------------------------------------------

def test_filldown_basic():
    """None values should be replaced by the last non-None value above."""
    table = [['site', 'reading'],
             ['A',    1.2],
             [None,   1.5],
             [None,   2.0],
             ['B',    0.9],
             [None,   1.1]]
    result = list(etl.filldown(table, 'site'))
    assert result[1][0] == 'A'
    assert result[2][0] == 'A'  # filled
    assert result[3][0] == 'A'  # filled
    assert result[4][0] == 'B'
    assert result[5][0] == 'B'  # filled


def test_filldown_header_preserved():
    """The header row must be returned unchanged."""
    table = [['a', 'b'], [1, None]]
    result = list(etl.filldown(table, 'b'))
    assert list(result[0]) == ['a', 'b']


def test_filldown_no_nones():
    """A column with no None values should be completely unchanged."""
    table = [['v'], [1], [2], [3]]
    result = list(etl.filldown(table, 'v'))
    assert result[1][0] == 1
    assert result[2][0] == 2
    assert result[3][0] == 3


def test_filldown_leading_none_stays_none():
    """If the very first value is None it should remain None."""
    table = [['v'], [None], [None], [5]]
    result = list(etl.filldown(table, 'v'))
    assert result[1][0] is None
    assert result[2][0] is None
    assert result[3][0] == 5


def test_filldown_non_target_columns_unchanged():
    """Columns not listed in *fields should not be modified."""
    table = [['a', 'b'], [1, 'x'], [None, None]]
    result = list(etl.filldown(table, 'a'))
    assert result[2][1] is None  # 'b' column untouched


# ---------------------------------------------------------------------------
# Multiple fields
# ---------------------------------------------------------------------------

def test_filldown_multiple_fields():
    """Multiple fields can be filled in a single call."""
    table = [['a', 'b'],
             [1,   'x'],
             [None, None],
             [2,   None]]
    result = list(etl.filldown(table, 'a', 'b'))
    assert result[2][0] == 1    # a filled
    assert result[2][1] == 'x'  # b filled
    assert result[3][0] == 2    # a new value
    assert result[3][1] == 'x'  # b still filled


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_filldown_all_none():
    """A column that is entirely None should remain entirely None."""
    table = [['v'], [None], [None], [None]]
    result = list(etl.filldown(table, 'v'))
    assert result[1][0] is None
    assert result[2][0] is None
    assert result[3][0] is None


def test_filldown_single_row():
    """A table with only one data row should work without error."""
    table = [['v'], [42]]
    result = list(etl.filldown(table, 'v'))
    assert result[1][0] == 42


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_filldown_missing_field_raises():
    """Referencing a field not in the header should raise ValueError."""
    table = [['v'], [1]]
    with pytest.raises(FieldSelectionError):
        list(etl.filldown(table, 'nonexistent'))