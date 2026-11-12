"""Pivot category ordering should match petl's heterogeneous sort contract."""
import pytest

from petl import pivot, sort


@pytest.mark.parametrize('options', [{}, {'presorted': True}, {'buffersize': 2}])
@pytest.mark.parametrize('categories', [(None, 'retail'), (1, 'retail'), (None, 1, 'retail')])
def test_pivot_mixed_categories(options, categories):
    source = [('region', 'category', 'sales')]
    for region, multiplier in [('north', 1), ('south', 10)]:
        for index, category in enumerate(categories):
            source.extend([(region, category, multiplier * (index + 1)),
                           (region, category, multiplier * (index + 1))])
    if options.get('presorted'):
        source = list(sort(source, key=('region', 'category')))
    result = pivot(source, 'region', 'category', 'sales', sum, **options)
    expected = [('region',) + categories,
                ('north',) + tuple(2 * (i + 1) for i in range(len(categories))),
                ('south',) + tuple(20 * (i + 1) for i in range(len(categories)))]
    assert list(result) == expected
    assert list(result) == expected


@pytest.mark.parametrize('options', [{}, {'presorted': True}, {'buffersize': 2}])
def test_pivot_missing_category_preserves_missing_cell_value(options):
    source = [('region', 'category', 'sales'),
              ('north', None, 3), ('north', 'retail', 10),
              ('south', 'retail', 7)]
    expected = [('region', None, 'retail'), ('north', 3, 10), ('south', -1, 7)]
    assert list(pivot(source, 'region', 'category', 'sales', sum, missing=-1, **options)) == expected
