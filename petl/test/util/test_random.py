import random
from functools import partial

from petl.util.random import randomtable, RandomTable, dummytable, DummyTable


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


def test_dummytable_custom_fields():
    """
    Ensure that dummytable provides a table with the right number of rows
    and that it accepts and uses custom column names provided.
    """
    columns = (
        ('count', partial(random.randint, 0, 100)),
        ('pet', partial(random.choice, ('dog', 'cat', 'cow'))),
        ('color', partial(random.choice, ('yellow', 'orange', 'brown'))),
        ('value', random.random)
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
