from __future__ import absolute_import, print_function, division

import pytest

from petl import lookupone, valuecounter
from petl.errors import DuplicateKeyError, FieldSelectionError


@pytest.mark.parametrize('value, expected', [
    ((), '()'),
    (('foo',), "('foo',)"),
    (('foo', 'bar'), "('foo', 'bar')"),
    ('foo', "'foo'"),
    (3, '3'),
    (None, 'None'),
])
@pytest.mark.parametrize('error_class, prefix', [
    (DuplicateKeyError, 'duplicate key: '),
    (FieldSelectionError, 'selection is not a field or valid field index: '),
])
def test_error_message(error_class, prefix, value, expected):
    assert str(error_class(value)) == prefix + expected


def test_valuecounter_invalid_tuple_field_message():
    table = [['foo', 'bar'], ['a', 1]]
    with pytest.raises(FieldSelectionError) as excinfo:
        valuecounter(table, ('foo', 'bar'))
    assert str(excinfo.value) == (
        "selection is not a field or valid field index: ('foo', 'bar')")


def test_lookupone_duplicate_compound_key_message():
    table = [['foo', 'bar', 'baz'], ['a', 1, 2], ['a', 1, 3]]
    with pytest.raises(DuplicateKeyError) as excinfo:
        lookupone(table, ('foo', 'bar'), 'baz', strict=True)
    assert str(excinfo.value) == "duplicate key: ('a', 1)"
