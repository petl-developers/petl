# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import logging

import pytest

import petl as etl
from petl.transform.validation import validate
from petl.test.helpers import ieq
from petl.errors import FieldSelectionError


logger = logging.getLogger(__name__)
debug = logger.debug


def test_constraints():

    constraints = [
        dict(name='C1', field='foo', test=int),
        dict(name='C2', field='bar', test=etl.dateparser('%Y-%m-%d')),
        dict(name='C3', field='baz', assertion=lambda v: v in ['Y', 'N']),
        dict(name='C4', assertion=lambda row: None not in row)
    ]

    table = (('foo', 'bar', 'baz'),
             (1, '2000-01-01', 'Y'),
             ('x', '2010-10-10', 'N'),
             (2, '2000/01/01', 'Y'),
             (3, '2015-12-12', 'x'),
             (4, None, 'N'),
             ('y', '1999-99-99', 'z'))

    expect = (('name', 'row', 'field', 'value', 'error'),
              ('C1', 2, 'foo', 'x', 'ValueError'),
              ('C2', 3, 'bar', '2000/01/01', 'ValueError'),
              ('C3', 4, 'baz', 'x', 'AssertionError'),
              ('C2', 5, 'bar', None, 'AttributeError'),
              ('C4', 5, None, None, 'AssertionError'),
              ('C1', 6, 'foo', 'y', 'ValueError'),
              ('C2', 6, 'bar', '1999-99-99', 'ValueError'),
              ('C3', 6, 'baz', 'z', 'AssertionError'))

    actual = validate(table, constraints)
    debug(actual)

    ieq(expect, actual)
    ieq(expect, actual)


def test_non_optional_constraint_with_missing_field():
    constraints = [
        dict(name='C1', field='foo', test=int),
    ]

    table = (('bar', 'baz'),
             ('1999-99-99', 'z'))

    actual = validate(table, constraints)
    with pytest.raises(FieldSelectionError):
        debug(actual)


def test_optional_constraint_with_missing_field():
    constraints = [
        dict(name='C1', field='foo', test=int, optional=True),
    ]

    table = (('bar', 'baz'),
             ('1999-99-99', 'z'))

    expect = (('name', 'row', 'field', 'value', 'error'),)

    actual = validate(table, constraints)
    debug(actual)

    ieq(expect, actual)


def test_row_length():

    table = (('foo', 'bar', 'baz'),
             (1, '2000-01-01', 'Y'),
             ('x', '2010-10-10'),
             (2, '2000/01/01', 'Y', True))

    expect = (('name', 'row', 'field', 'value', 'error'),
              ('__len__', 2, None, 2, 'AssertionError'),
              ('__len__', 3, None, 4, 'AssertionError'))

    actual = validate(table)
    debug(actual)

    ieq(expect, actual)
    ieq(expect, actual)


def test_header():

    header = ('foo', 'bar', 'baz')

    table = (('foo', 'bar', 'bazzz'),
             (1, '2000-01-01', 'Y'),
             ('x', '2010-10-10', 'N'))

    expect = (('name', 'row', 'field', 'value', 'error'),
              ('__header__', 0, None, None, 'AssertionError'))

    actual = validate(table, header=header)
    debug(actual)

    ieq(expect, actual)
    ieq(expect, actual)

    header = ('foo', 'bar', 'baz', 'quux')

    table = (('foo', 'bar', 'baz'),
             (1, '2000-01-01', 'Y'),
             ('x', '2010-10-10', 'N'))

    expect = (('name', 'row', 'field', 'value', 'error'),
              ('__header__', 0, None, None, 'AssertionError'),
              ('__len__', 1, None, 3, 'AssertionError'),
              ('__len__', 2, None, 3, 'AssertionError'))

    actual = validate(table, header=header)
    debug(actual)

    ieq(expect, actual)
    ieq(expect, actual)


def test_validation_headerless():
    header = ('foo', 'bar', 'baz')
    table = []
    # Expect only a missing header - no exceptions please
    expect = (('name', 'row', 'field', 'value', 'error'),
              ('__header__', 0, None, None, 'AssertionError'))
    actual = validate(table, header=header)
    ieq(expect, actual)
    ieq(expect, actual)
