# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import operator
from petl.compat import text_type


from petl.util.base import Table, asindices, Record


def validate(table, constraints=None, header=None):
    """
    Validate a `table` against a set of `constraints` and/or an expected
    `header`, e.g.::

        >>> import petl as etl
        >>> # define some validation constraints
        ... header = ('foo', 'bar', 'baz')
        >>> constraints = [
        ...     dict(name='foo_int', field='foo', test=int),
        ...     dict(name='bar_date', field='bar', test=etl.dateparser('%Y-%m-%d')),
        ...     dict(name='baz_enum', field='baz', assertion=lambda v: v in ['Y', 'N']),
        ...     dict(name='not_none', assertion=lambda row: None not in row),
        ...     dict(name='qux_int', field='qux', test=int, optional=True),
        ... ]
        >>> # now validate a table
        ... table = (('foo', 'bar', 'bazzz'),
        ...          (1, '2000-01-01', 'Y'),
        ...          ('x', '2010-10-10', 'N'),
        ...          (2, '2000/01/01', 'Y'),
        ...          (3, '2015-12-12', 'x'),
        ...          (4, None, 'N'),
        ...          ('y', '1999-99-99', 'z'),
        ...          (6, '2000-01-01'),
        ...          (7, '2001-02-02', 'N', True))
        >>> problems = etl.validate(table, constraints=constraints, header=header)
        >>> problems.lookall()
        +--------------+-----+-------+--------------+------------------+
        | name         | row | field | value        | error            |
        +==============+=====+=======+==============+==================+
        | '__header__' |   0 | None  | None         | 'AssertionError' |
        +--------------+-----+-------+--------------+------------------+
        | 'foo_int'    |   2 | 'foo' | 'x'          | 'ValueError'     |
        +--------------+-----+-------+--------------+------------------+
        | 'bar_date'   |   3 | 'bar' | '2000/01/01' | 'ValueError'     |
        +--------------+-----+-------+--------------+------------------+
        | 'baz_enum'   |   4 | 'baz' | 'x'          | 'AssertionError' |
        +--------------+-----+-------+--------------+------------------+
        | 'bar_date'   |   5 | 'bar' | None         | 'AttributeError' |
        +--------------+-----+-------+--------------+------------------+
        | 'not_none'   |   5 | None  | None         | 'AssertionError' |
        +--------------+-----+-------+--------------+------------------+
        | 'foo_int'    |   6 | 'foo' | 'y'          | 'ValueError'     |
        +--------------+-----+-------+--------------+------------------+
        | 'bar_date'   |   6 | 'bar' | '1999-99-99' | 'ValueError'     |
        +--------------+-----+-------+--------------+------------------+
        | 'baz_enum'   |   6 | 'baz' | 'z'          | 'AssertionError' |
        +--------------+-----+-------+--------------+------------------+
        | '__len__'    |   7 | None  |            2 | 'AssertionError' |
        +--------------+-----+-------+--------------+------------------+
        | 'baz_enum'   |   7 | 'baz' | None         | 'AssertionError' |
        +--------------+-----+-------+--------------+------------------+
        | '__len__'    |   8 | None  |            4 | 'AssertionError' |
        +--------------+-----+-------+--------------+------------------+

    Returns a table of validation problems.

    """  # noqa

    return ProblemsView(table, constraints=constraints, header=header)


Table.validate = validate


class ProblemsView(Table):

    def __init__(self, table, constraints, header):
        self.table = table
        self.constraints = constraints
        self.header = header

    def __iter__(self):
        return iterproblems(self.table, self.constraints, self.header)


def normalize_constraints(constraints, flds):
    """
    This method renders local constraints such that return value is:
      * a list, not None
      * a list of dicts
      * a list of non-optional constraints or optional with defined field

    .. note:: We use a new variable 'local_constraints' because the constraints
              parameter may be a mutable collection, and we do not wish to
              cause side-effects by modifying it locally
    """
    local_constraints = constraints or []
    local_constraints = [dict(**c) for c in local_constraints]
    local_constraints = [
        c for c in local_constraints
        if c.get('field') in flds or
        not c.get('optional')
    ]
    return local_constraints


def iterproblems(table, constraints, expected_header):

    outhdr = ('name', 'row', 'field', 'value', 'error')
    yield outhdr

    it = iter(table)
    try:
        actual_header = next(it)
    except StopIteration:
        actual_header = []

    if expected_header is None:
        flds = list(map(text_type, actual_header))
    else:
        expected_flds = list(map(text_type, expected_header))
        actual_flds = list(map(text_type, actual_header))
        try:
            assert expected_flds == actual_flds
        except Exception as e:
            yield ('__header__', 0, None, None, type(e).__name__)
        flds = expected_flds

    local_constraints = normalize_constraints(constraints, flds)

    # setup getters
    for constraint in local_constraints:
        if 'getter' not in constraint:
            if 'field' in constraint:
                # should ensure FieldSelectionError if bad field in constraint
                indices = asindices(flds, constraint['field'])
                getter = operator.itemgetter(*indices)
                constraint['getter'] = getter

    # generate problems
    expected_len = len(flds)
    for i, row in enumerate(it):
        row = tuple(row)

        # row length constraint
        l = None
        try:
            l = len(row)
            assert l == expected_len
        except Exception as e:
            yield ('__len__', i+1, None, l, type(e).__name__)

        # user defined constraints
        row = Record(row, flds)
        for constraint in local_constraints:
            name = constraint.get('name', None)
            field = constraint.get('field', None)
            assertion = constraint.get('assertion', None)
            test = constraint.get('test', None)
            getter = constraint.get('getter', lambda x: x)
            try:
                target = getter(row)
            except Exception as e:
                # getting target value failed, report problem
                yield (name, i+1, field, None, type(e).__name__)
            else:
                value = target if field else None
                if test is not None:
                    try:
                        test(target)
                    except Exception as e:
                        # test raised exception, report problem
                        yield (name, i+1, field, value, type(e).__name__)
                if assertion is not None:
                    try:
                        assert assertion(target)
                    except Exception as e:
                        # assertion raised exception, report problem
                        yield (name, i+1, field, value, type(e).__name__)
