import math
import random
import pytest
import petl
from fractions import Fraction


def fixtures():
    yield 'constant-positive', [1e308, 1e308]
    yield 'constant-negative', [-1e308, -1e308]
    yield 'constant-three', [1e308] * 3
    yield 'large-positive', [8e307, 1e308]
    yield 'large-negative', [-8e307, -1e308]
    rng = random.Random(5380843)
    for i in range(100):
        yield 'seeded-%03d' % i, [rng.uniform(7e307, 1e308) for _ in range(rng.randint(3, 25))]


@pytest.mark.parametrize('name,values', list(fixtures()))
def test_mean_finite(name, values):
    result = petl.stats([['v']] + [[x] for x in values], 'v')
    expected = float(sum(map(Fraction.from_float, values)) / len(values))
    assert math.isfinite(result.mean)
    assert math.isclose(result.mean, expected, rel_tol=2e-15)
    if len(set(values)) == 1:
        assert result.pvariance == 0


@pytest.mark.parametrize('values', [[float('inf')], [float('-inf')], [float('nan')]])
def test_nonfinite_mean_semantics(values):
    result = petl.stats([['v']] + [[x] for x in values], 'v')
    if math.isnan(values[0]):
        assert math.isnan(result.mean)
    else:
        assert result.mean == values[0]
