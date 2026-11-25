import pytest

import petl as etl
from petl.errors import FieldSelectionError


@pytest.mark.parametrize('selector', [-1, 2, 'amount'])
def test_cut_selects_the_header_column_on_short_and_long_rows(selector):
    table = [('id', 'name', 'amount'), ('a', 'alpha', 12), ('b', 'beta'),
             ('c', 'gamma', 24, 'extra'), ()]
    assert list(etl.cut(table, selector, missing='missing')) == [
        ('amount',), (12,), ('missing',), (24,), ('missing',)]


def test_mixed_cut_selectors_and_lists_keep_header_positions():
    table = [('id', 'name', 'amount'), ('a', 'alpha'), ('b', 'beta', 12, 'extra')]
    assert list(etl.cut(table, -1, 'id', -2)) == [
        ('amount', 'id', 'name'), (None, 'a', 'alpha'), (12, 'b', 'beta')]
    assert list(etl.cut(table, [-2, -1])) == [
        ('name', 'amount'), ('alpha', None), ('beta', 12)]


@pytest.mark.parametrize('selector', [-1, 2, 'amount'])
def test_filldown_targets_the_header_column_when_rows_have_extra_cells(selector):
    table = [('id', 'name', 'amount'), ('a', 'alpha', 12, 'keep'),
             ('b', 'beta', None, 'extra'), ('c', 'gamma'), ('d', 'delta', 24)]
    assert list(etl.filldown(table, selector)) == [
        ('id', 'name', 'amount'), ('a', 'alpha', 12, 'keep'),
        ('b', 'beta', 12, 'extra'), ('c', 'gamma', 12), ('d', 'delta', 24)]


@pytest.mark.parametrize('transform', [etl.cut, etl.filldown])
def test_negative_selector_outside_the_header_is_rejected(transform):
    with pytest.raises(FieldSelectionError):
        list(transform([('id', 'amount'), ('a', 12)], -3))
