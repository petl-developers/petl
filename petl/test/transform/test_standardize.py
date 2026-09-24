from __future__ import absolute_import, print_function, division


import pytest


from petl.errors import FieldSelectionError
from petl.test.helpers import ieq
from petl.transform.standardize import standardize
from petl.util.base import wrap


def test_standardize_overwrite_single_field():
    table = (('id', 'x', 'y'),
             (1, 10.0, 100),
             (2, 20.0, 200),
             (3, 30.0, 300))

    actual = standardize(table, 'x')
    expected = (('id', 'x', 'y'),
                (1, -1.224744871391589, 100),
                (2, 0.0, 200),
                (3, 1.224744871391589, 300))
    ieq(expected, actual)
    ieq(expected, actual)


def test_standardize_new_fields_and_sample_std():
    table = (('id', 'a', 'b'),
             (1, 5, 10.0),
             (2, 6, 20.0),
             (3, 7, 30.0),
             (4, 8, None))

    actual = standardize(table, ('a', 'b'),
                         newfields=('a_z', 'b_z'), ddof=1)
    rows = list(actual)

    assert rows[0] == ('id', 'a', 'b', 'a_z', 'b_z')
    assert rows[-1][4] is None
    a_values = [row[3] for row in rows[1:]]
    b_values = [row[4] for row in rows[1:-1]]
    assert sum(a_values) == pytest.approx(0)
    assert sum(value ** 2 for value in a_values) / 3 == pytest.approx(1)
    assert sum(b_values) == pytest.approx(0)
    assert sum(value ** 2 for value in b_values) / 2 == pytest.approx(1)


def test_standardize_constant_and_non_numeric_values():
    table = (('id', 'value'),
             (1, 5.0),
             (2, 5.0),
             (3, None),
             (4, True),
             (5, '5'))

    actual = standardize(table, 1)
    expected = (('id', 'value'),
                (1, 0.0),
                (2, 0.0),
                (3, None),
                (4, True),
                (5, '5'))
    ieq(expected, actual)


def test_standardize_table_method():
    table = wrap((('id', 'value'), (1, 10), (2, 20), (3, 30)))
    actual = table.standardize('value', newfields='value_z')
    assert list(actual)[0] == ('id', 'value', 'value_z')


def test_standardize_one_shot_input():
    table = iter((('id', 'value'), (1, 10), (2, 20), (3, 30)))
    actual = standardize(table, 'value')
    assert len(actual.list()) == 4


def test_standardize_empty_and_header_only_tables():
    ieq([], standardize([], 'value'))
    ieq([('value',)], standardize([('value',)], 'value'))


def test_standardize_invalid_arguments():
    table = (('id', 'value'), (1, 10))

    with pytest.raises(FieldSelectionError):
        list(standardize(table, 'missing'))

    with pytest.raises(ValueError):
        list(standardize(table, ('id', 'value'), newfields='z'))

    with pytest.raises(ValueError):
        list(standardize(table, 'value', ddof=-1))

    with pytest.raises(ValueError):
        list(standardize(table, 'value', ddof=1))
