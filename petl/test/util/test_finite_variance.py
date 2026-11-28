import math
import pytest
import petl
"""Offline variance reference fixtures, not a PETL patch or PETL test result.

Uses exact conversion of binary64 inputs and a high-precision two-pass Decimal
calculation with exact rational intermediates. Reports both population and sample conventions so a future adapter
must explicitly choose the upstream API's documented convention.
"""
from decimal import Decimal, localcontext
from fractions import Fraction
import math
import random
import unittest


def reference(values, ddof=0):
    values = list(values)
    if not values or len(values) <= ddof:
        raise ValueError('not enough values')
    if any(not math.isfinite(x) for x in values):
        raise ValueError('finite fixtures only; nonfinite API semantics are separate')
    xs = [Fraction.from_float(float(x)) for x in values]
    mean = sum(xs) / len(xs)
    result = sum((x - mean) ** 2 for x in xs) / (len(xs) - ddof)
    with localcontext() as ctx:
        ctx.prec = 800
        return Decimal(result.numerator) / Decimal(result.denominator)


def fixtures():
    yield 'symmetric-large', [1e154, -1e154]
    yield 'symmetric-large-reversed', [-1e154, 1e154]
    yield 'constant-large', [1e154] * 4
    yield 'mixed-sign-zero', [1e154, 0.0, -1e154]
    rng = random.Random(5380843)
    for i in range(100):
        scale = 10.0 ** rng.uniform(150, 153.9)
        yield 'seeded-%03d' % i, [rng.uniform(-scale, scale) for _ in range(rng.randint(2, 15))]




@pytest.mark.parametrize('name,values', list(fixtures()))
def test_finite_population_variance(name, values):
    actual = petl.stats([['v']] + [[x] for x in values], 'v')
    expected = float(reference(values))
    assert actual.count == len(values)
    assert actual.errors == 0
    assert math.isfinite(actual.pvariance)
    assert math.isclose(actual.pvariance, expected, rel_tol=4e-14, abs_tol=0.0)

@pytest.mark.parametrize('values', [[float('nan')], [float('inf')], [float('-inf')]])
def test_nonfinite_path_retained(values):
    result = petl.stats([['v']] + [[x] for x in values], 'v')
    assert math.isnan(result.pvariance)
