from __future__ import absolute_import, print_function, division
import pytest
import petl as etl


# ---------------------------------------------------------------------------
# Basic behaviour
# ---------------------------------------------------------------------------

def test_clampvalues_clamps_below_low():
    """Values below the lower bound should be raised to low."""
    table = [['id', 'score'], [1, -10], [2, 50]]
    result = list(etl.clampvalues(table, 'score', 0, 100))
    assert result[1][1] == 0    # -10 clamped to 0
    assert result[2][1] == 50   # 50 unchanged


def test_clampvalues_clamps_above_high():
    """Values above the upper bound should be lowered to high."""
    table = [['id', 'score'], [1, 150], [2, 50]]
    result = list(etl.clampvalues(table, 'score', 0, 100))
    assert result[1][1] == 100  # 150 clamped to 100
    assert result[2][1] == 50   # 50 unchanged


def test_clampvalues_within_range_unchanged():
    """Values already within [low, high] must not be modified."""
    table = [['x'], [0], [50], [100]]
    result = list(etl.clampvalues(table, 'x', 0, 100))
    assert result[1][0] == 0
    assert result[2][0] == 50
    assert result[3][0] == 100


def test_clampvalues_none_passthrough():
    """None values should pass through without error."""
    table = [['val'], [None], [5]]
    result = list(etl.clampvalues(table, 'val', 0, 10))
    assert result[1][0] is None
    assert result[2][0] == 5


def test_clampvalues_header_preserved():
    """The header row must be returned unchanged."""
    table = [['id', 'score'], [1, 50]]
    result = list(etl.clampvalues(table, 'score', 0, 100))
    assert list(result[0]) == ['id', 'score']


# ---------------------------------------------------------------------------
# Boundary / edge cases
# ---------------------------------------------------------------------------

def test_clampvalues_float_values():
    """Should work correctly with floating-point numbers."""
    table = [['v'], [-0.5], [0.5], [1.5]]
    result = list(etl.clampvalues(table, 'v', 0.0, 1.0))
    assert result[1][0] == 0.0
    assert result[2][0] == 0.5
    assert result[3][0] == 1.0


def test_clampvalues_low_equals_high():
    """When low == high every value should be clamped to that single value."""
    table = [['v'], [0], [5], [10]]
    result = list(etl.clampvalues(table, 'v', 5, 5))
    assert result[1][0] == 5
    assert result[2][0] == 5
    assert result[3][0] == 5


def test_clampvalues_negative_range():
    """Should work correctly with a fully negative range."""
    table = [['v'], [-100], [-50], [-1]]
    result = list(etl.clampvalues(table, 'v', -75, -25))
    assert result[1][0] == -75   # -100 clamped to -75
    assert result[2][0] == -50   # -50 unchanged
    assert result[3][0] == -25   # -1 clamped to -25


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_clampvalues_invalid_range_raises():
    """low > high should raise ValueError immediately."""
    table = [['v'], [1]]
    with pytest.raises(ValueError):
        etl.clampvalues(table, 'v', 10, 0)


def test_clampvalues_missing_field_raises():
    """Referencing a field not in the header should raise ValueError."""
    table = [['v'], [1]]
    with pytest.raises(ValueError):
        list(etl.clampvalues(table, 'nonexistent', 0, 10))
