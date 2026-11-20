import random as pyrandom
import time
from functools import partial

from petl.util.random import randomseed, randomtable, RandomTable, dummytable, DummyTable


def test_randomseed():
    """
    Ensure that randomseed provides a non-empty string that changes.
    """
    seed_1 = randomseed()
    time.sleep(1)
    seed_2 = randomseed()

    assert isinstance(seed_1, str)
    assert seed_1 != ""
    assert seed_1 != seed_2


def test_randomtable():
    """
    Ensure that randomtable provides a table with the right number of rows and columns.
    """
    columns, rows = 3, 10
    table = randomtable(columns, rows)

    assert len(table[0]) == columns
    assert len(table) == rows + 1


def test_randomtable_class():
    """
    Ensure that RandomTable provides a table with the right number of rows and columns.
    """
    columns, rows = 4, 60
    table = RandomTable(numflds=columns, numrows=rows)

    assert len(table[0]) == columns
    assert len(table) == rows + 1


def test_randomtable_interleaved_iterations():
    table = randomtable(3, 4, seed=42)
    expected = list(table)
    first = iter(table)
    second = iter(table)

    assert next(first) == next(second) == expected[0]
    for row in expected[1:]:
        assert next(first) == row
        assert next(second) == row
    assert list(first) == list(second) == []


def test_randomtable_unrelated_random_calls():
    table = randomtable(3, 4, seed=42)
    expected = list(table)
    rows = iter(table)

    assert next(rows) == expected[0]
    for row in expected[1:]:
        pyrandom.random()
        assert next(rows) == row


def test_randomtable_preserves_global_random_state():
    state = pyrandom.getstate()
    try:
        pyrandom.seed(123)
        expected = pyrandom.getstate()
        list(randomtable(3, 4, seed=42))
        assert pyrandom.getstate() == expected
    finally:
        pyrandom.setstate(state)


def test_dummytable_custom_fields():
    """
    Ensure that dummytable provides a table with the right number of rows
    and that it accepts and uses custom column names provided.
    """
    columns = (
        ('count', partial(pyrandom.randint, 0, 100)),
        ('pet', partial(pyrandom.choice, ['dog', 'cat', 'cow', ])),
        ('color', partial(pyrandom.choice, ['yellow', 'orange', 'brown'])),
        ('value', pyrandom.random),
    )
    rows = 35

    table = dummytable(numrows=rows, fields=columns)
    assert table[0] == ('count', 'pet', 'color', 'value')
    assert len(table) == rows + 1


def test_dummytable_no_seed():
    """
    Ensure that dummytable provides a table with the right number of rows
    and columns when not provided with a seed.
    """
    rows = 35

    table = dummytable(numrows=rows)
    assert len(table[0]) == 3
    assert len(table) == rows + 1


def test_dummytable_int_seed():
    """
    Ensure that dummytable provides a table with the right number of rows
    and columns when provided with an integer as a seed.
    """
    rows = 35
    seed = 42
    table = dummytable(numrows=rows, seed=seed)
    assert len(table[0]) == 3
    assert len(table) == rows + 1


def test_dummytable_class():
    """
    Ensure that DummyTable provides a table with the right number of rows
    and columns.
    """
    rows = 70
    table = DummyTable(numrows=rows)

    assert len(table) == rows + 1
