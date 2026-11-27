from __future__ import absolute_import, print_function, division

import copy
import pytest
import petl as etl


@pytest.mark.parametrize('field', [-1, -2])
@pytest.mark.parametrize('surplus', [[], [999], [999, -999]])
def test_clamp_negative_header_position(field, surplus):
    table = [['left', 'right'], [-5, 50] + surplus, [50, -5] + surplus]
    before = copy.deepcopy(table)
    positive = len(table[0]) + field
    actual = etl.clampvalues(table, field, 0, 10)
    expected = list(etl.clampvalues(table, positive, 0, 10))
    assert list(actual) == expected
    assert list(actual) == expected
    assert table == before


def test_clamp_negative_csv_surplus(tmp_path):
    path = tmp_path / 'clamp.csv'
    path.write_text('left,right\n-5,50,999\n50,-5,-999\n')
    table = etl.fromcsv(str(path)).convertall(int)
    assert list(etl.clampvalues(table, -1, 0, 10)) == [
        ('left', 'right'), (-5, 10, '999'), (50, 0, '-999'),
    ]


def test_clamp_negative_noncomparable_surplus():
    table = [['left', 'right'], [1, -5, 'tail'], [2, 50, None]]
    assert list(etl.wrap(table).clampvalues(-1, 0, 10)) == [
        ['left', 'right'], (1, 0, 'tail'), (2, 10, None),
    ]
