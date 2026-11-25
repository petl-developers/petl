# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import pytest

from petl.compat import next
from petl.errors import ArgumentError
from petl.test.helpers import ieq, eq_
from petl.transform.regex import capture, split, search, searchcomplement, splitdown
from petl.transform.basics import TransformError


def test_capture():

    table = (('id', 'variable', 'value'),
             ('1', 'A1', '12'),
             ('2', 'A2', '15'),
             ('3', 'B1', '18'),
             ('4', 'C12', '19'))

    expectation = (('id', 'value', 'treat', 'time'),
                   ('1', '12', 'A', '1'),
                   ('2', '15', 'A', '2'),
                   ('3', '18', 'B', '1'),
                   ('4', '19', 'C', '12'))

    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'))
    ieq(expectation, result)
    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'),
                     include_original=False)
    ieq(expectation, result)

    # integer field reference
    result = capture(table, 1, '(\\w)(\\d+)', ('treat', 'time'))
    ieq(expectation, result)

    # what about including the original field?
    expectation = (('id', 'variable', 'value', 'treat', 'time'),
                   ('1', 'A1', '12', 'A', '1'),
                   ('2', 'A2', '15', 'A', '2'),
                   ('3', 'B1', '18', 'B', '1'),
                   ('4', 'C12', '19', 'C', '12'))
    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'),
                     include_original=True)
    ieq(expectation, result)

    # what about if number of captured groups is different from new fields?
    expectation = (('id', 'value'),
                   ('1', '12', 'A', '1'),
                   ('2', '15', 'A', '2'),
                   ('3', '18', 'B', '1'),
                   ('4', '19', 'C', '12'))
    result = capture(table, 'variable', '(\\w)(\\d+)')
    ieq(expectation, result)


def test_capture_iterator_newfields():
    table = [('id', 'code'), (7, 'A12'), (8, 'B34')]
    names = iter(['letter', 'number'])
    result = capture(table, 'code', r'([A-Z])(\d+)', names)
    expected = [('id', 'letter', 'number'), (7, 'A', '12'), (8, 'B', '34')]
    assert result.header() == expected[0]
    assert list(iter(result)) == expected
    assert list(iter(result)) == expected
    assert list(result) == expected


def test_capture_newfields_validation_and_failure():
    reads = []

    def names():
        reads.append('first')
        yield 'letter'
        raise ValueError('bad name')

    fields = names()
    table = [('code',), ('A',)]
    invalid = iter(capture(table, 'absent', '(.)', fields))
    assert not reads
    with pytest.raises(ArgumentError):
        next(invalid)
    assert not reads
    view = capture(table, 'code', '(.)', fields)
    rows = iter(view)
    assert not reads
    with pytest.raises(ValueError, match='bad name'):
        next(rows)
    # Never silently report a partial header after materialization failed.
    with pytest.raises((ValueError, TransformError)):
        next(iter(view))


def test_capture_validates_field_before_opening_newfields():
    class UnopenableNames(object):
        def __iter__(self):
            raise ValueError('names opened')

    table = [('code',), ('A',)]
    names = UnopenableNames()
    rows = iter(capture(table, 'absent', '(.)', names))
    with pytest.raises(ArgumentError):
        next(rows)
    rows = iter(capture(table, 'code', '(.)', names))
    with pytest.raises(ValueError, match='names opened'):
        next(rows)


def test_capture_rereads_repeatable_newfields():
    class Names(list):
        def __next__(self):
            raise AssertionError('use the list iterator')

    fields = Names(['before'])
    view = capture([('code',), ('A',)], 'code', '(.)', fields)
    assert view.header() == ('before',)
    fields[:] = ['after']
    assert view.header() == ('after',)


def test_capture_iterator_on_first_list():
    class Names(object):
        def __init__(self):
            self.names = iter(['label'])

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.names)

    view = capture([('code',), ('A',)], 'code', '(.)', Names())
    assert list(view) == [('label',), ('A',)]
    assert view.header() == ('label',)


def test_capture_does_not_reopen_collection_cursor():
    events = []

    class Cursor(object):
        def __init__(self):
            self.names = iter(['label'])

        def __iter__(self):
            events.append('cursor')
            return self

        def __next__(self):
            return next(self.names)

    class Collection(object):
        def __iter__(self):
            events.append('collection')
            return Cursor()

    view = capture([('code',)], 'code', '(.)', Collection())
    assert view.header() == view.header() == ('label',)
    assert events == ['collection', 'collection']


def test_capture_opens_name_iterator_once():
    class Names(object):
        def __init__(self):
            self.values = iter(['label'])
            self.opens = 0

        def __iter__(self):
            self.opens += 1
            return self

        def __next__(self):
            return next(self.values)

    names = Names()
    view = capture([('code',)], 'code', '(.)', names)
    assert view.header() == view.header() == ('label',)
    assert names.opens == 1


def test_capture_keeps_names_for_open_traversal():
    view = capture([('code',)], 'code', '(.)', iter(['before']))
    rows = iter(view)
    view.newfields = ['after']
    assert next(rows) == ('before',)
    assert view.header() == ('after',)


def test_capture_replaces_failed_names():
    def names():
        yield 'first'
        raise ValueError('bad name')

    view = capture([('code',)], 'code', '(.)', names())
    with pytest.raises(ValueError, match='bad name'):
        view.header()
    view.newfields = ['valid']
    assert view.header() == ('valid',)


def test_capture_retries_before_first_name_is_yielded():
    class Names(object):
        def __init__(self):
            self.values = iter(['letter', 'number'])
            self.first = True

        def __iter__(self):
            return self

        def __next__(self):
            if self.first:
                self.first = False
                raise ValueError('try again')
            return next(self.values)

    view = capture([('code',), ('A1',)], 'code', r'(.)(.)', Names())
    with pytest.raises(ValueError, match='try again'):
        view.header()
    expected = [('letter', 'number'), ('A', '1')]
    assert list(iter(view)) == expected
    assert list(iter(view)) == expected


def test_capture_skips_disabled_names_on_retry():
    class Names(object):
        enabled = True

        def __bool__(self):
            return self.enabled

        def __iter__(self):
            assert self.enabled
            return self

        def __next__(self):
            self.enabled = False
            raise ValueError('disabled')

    view = capture([('code',)], 'code', '(.)', Names())
    with pytest.raises(ValueError, match='disabled'):
        view.header()
    assert view.header() == ()


def test_capture_refreshes_source_fields_with_cached_names():
    table = [('id', 'code'), (7, 'A')]
    view = capture(table, 'code', '(.)', iter(['label']))
    assert list(iter(view)) == [('id', 'label'), (7, 'A')]
    table[0] = ('key', 'code')
    table[1] = (8, 'B')
    assert list(iter(view)) == [('key', 'label'), (8, 'B')]


def test_capture_retries_after_length_hint_failure():
    class Names(object):
        def __init__(self):
            self.values = iter(['label'])
            self.first = True

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.values)

        def __length_hint__(self):
            if self.first:
                self.first = False
                raise ValueError('length unavailable')
            return 1

    view = capture([('code',)], 'code', '(.)', Names())
    with pytest.raises(ValueError, match='length unavailable'):
        view.header()
    assert view.header() == ('label',)


@pytest.mark.parametrize('wrap', [lambda it: it, lambda it: map(str, it)])
def test_capture_retries_exhausted_names_like_any_empty_iterator(wrap):
    def names():
        raise ValueError('bad name')
        yield 'label'

    view = capture([('code',), ('A',)], 'code', '(.)', wrap(names()))
    with pytest.raises(ValueError, match='bad name'):
        view.header()
    assert view.header() == ()


def test_capture_collection_needs_no_class_attribute_probe():
    class Meta(type):
        @property
        def __next__(cls):
            raise RuntimeError('class lookup')

    class Names(list, metaclass=Meta):
        pass

    names = Names(['before'])
    view = capture([('code',)], 'code', '(.)', names)
    assert view.header() == ('before',)
    names[0] = 'after'
    assert view.header() == ('after',)


def test_capture_accepts_valid_empty_retry():
    class Names(object):
        first = True

        def __iter__(self):
            return self

        def __next__(self):
            if self.first:
                self.first = False
                raise ValueError('try again')
            raise StopIteration

    view = capture([('code',), ('A',)], 'code', 'A', Names())
    with pytest.raises(ValueError, match='try again'):
        view.header()
    assert list(iter(view)) == [(), ()]
    assert list(iter(view)) == [(), ()]


def test_capture_falsey_repeatable_collection():
    class Hidden(list):
        def __iter__(self):
            raise AssertionError('should not open hidden names')

        def __next__(self):
            raise AssertionError('not a self-iterator')

        def __bool__(self):
            return False

    view = capture([('code',)], 'code', '(.)', Hidden(['hidden']))
    assert view.header() == ()


def test_capture_tests_collection_before_opening():
    events = []

    class Names(list):
        @property
        def __next__(self):
            raise AssertionError('not used by collection iteration')

        def __bool__(self):
            events.append('truth')
            return True

        def __iter__(self):
            events.append('open')
            return super().__iter__()

    view = capture([('code',)], 'code', '(.)', Names(['label']))
    assert view.header() == ('label',)
    assert events == ['truth', 'open']


def test_capture_negative_index():

    table = (('id', 'value', 'variable'),
             ('1', '12', 'A1'),
             ('2', '15', 'B12'))

    expectation = (('id', 'value', 'treat', 'time'),
                   ('1', '12', 'A', '1'),
                   ('2', '15', 'B', '12'))
    result = capture(table, -1, '(\\w)(\\d+)', ('treat', 'time'))
    ieq(expectation, result)

    with pytest.raises(ArgumentError):
        capture(table, -4, '(\\w)(\\d+)', ('treat', 'time')).nrows()


def test_capture_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'baz', 'qux'),)
    actual = capture(table, 'bar', r'(\w)(\d)', ('baz', 'qux'))
    ieq(expect, actual)


def test_capture_headerless():
    table = []
    with pytest.raises(ArgumentError):
        for i in capture(table, 'bar', r'(\w)(\d)', ('baz', 'qux')):
            pass


def test_capture_nonmatching():

    table = (('id', 'variable', 'value'),
             ('1', 'A1', '12'),
             ('2', 'A2', '15'),
             ('3', 'B1', '18'),
             ('4', 'C12', '19'))

    expectation = (('id', 'value', 'treat', 'time'),
                   ('1', '12', 'A', '1'),
                   ('2', '15', 'A', '2'),
                   ('3', '18', 'B', '1'))

    # default behaviour, raise exception
    result = capture(table, 'variable', r'([A-B])(\d+)', ('treat', 'time'))
    it = iter(result)
    eq_(expectation[0], next(it))  # header
    eq_(expectation[1], next(it))
    eq_(expectation[2], next(it))
    eq_(expectation[3], next(it))
    try:
        next(it)  # doesn't match
    except TransformError:
        pass  # expected
    else:
        assert False, 'expected exception'

    # explicit fill
    result = capture(table, 'variable', r'([A-B])(\d+)',
                     newfields=('treat', 'time'), fill=['', 0])
    it = iter(result)
    eq_(expectation[0], next(it))  # header
    eq_(expectation[1], next(it))
    eq_(expectation[2], next(it))
    eq_(expectation[3], next(it))
    eq_(('4', '19', '', 0), next(it))


def test_split():

    table = (('id', 'variable', 'value'),
             ('1', 'parad1', '12'),
             ('2', 'parad2', '15'),
             ('3', 'tempd1', '18'),
             ('4', 'tempd2', '19'))

    expectation = (('id', 'value', 'variable', 'day'),
                   ('1', '12', 'para', '1'),
                   ('2', '15', 'para', '2'),
                   ('3', '18', 'temp', '1'),
                   ('4', '19', 'temp', '2'))

    result = split(table, 'variable', 'd', ('variable', 'day'))
    ieq(expectation, result)
    ieq(expectation, result)

    # proper regex
    result = split(table, 'variable', '[Dd]', ('variable', 'day'))
    ieq(expectation, result)

    # integer field reference
    result = split(table, 1, 'd', ('variable', 'day'))
    ieq(expectation, result)

    expectation = (('id', 'variable', 'value', 'variable', 'day'),
                   ('1', 'parad1', '12', 'para', '1'),
                   ('2', 'parad2', '15', 'para', '2'),
                   ('3', 'tempd1', '18', 'temp', '1'),
                   ('4', 'tempd2', '19', 'temp', '2'))

    result = split(table, 'variable', 'd', ('variable', 'day'),
                   include_original=True)
    ieq(expectation, result)

    # what about if no new fields?
    expectation = (('id', 'value'),
                   ('1', '12', 'para', '1'),
                   ('2', '15', 'para', '2'),
                   ('3', '18', 'temp', '1'),
                   ('4', '19', 'temp', '2'))
    result = split(table, 'variable', 'd')
    ieq(expectation, result)


def test_split_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'baz', 'qux'),)
    actual = split(table, 'bar', 'd', ('baz', 'qux'))
    ieq(expect, actual)


def test_split_headerless():
    table = []
    with pytest.raises(ArgumentError):
        for i in split(table, 'bar', 'd', ('baz', 'qux')):
            pass


def test_search():

    table1 = (('foo', 'bar', 'baz'),
              ('orange', 12, 'oranges are nice fruit'),
              ('mango', 42, 'I like them'),
              ('banana', 74, 'lovely too'),
              ('cucumber', 41, 'better than mango'))

    # search any field
    table2 = search(table1, '.g.')
    expect2 = (('foo', 'bar', 'baz'),
               ('orange', 12, 'oranges are nice fruit'),
               ('mango', 42, 'I like them'),
               ('cucumber', 41, 'better than mango'))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # search a specific field
    table3 = search(table1, 'foo', '.g.')
    expect3 = (('foo', 'bar', 'baz'),
               ('orange', 12, 'oranges are nice fruit'),
               ('mango', 42, 'I like them'))
    ieq(expect3, table3)
    ieq(expect3, table3)


def test_search_2():

    # test ported from selectre
    table = (('foo', 'bar', 'baz'),
             ('aa', 4, 9.3),
             ('aaa', 2, 88.2),
             ('b', 1, 23.3),
             ('ccc', 8, 42.0),
             ('bb', 7, 100.9),
             ('c', 2))
    actual = search(table, 'foo', '[ab]{2}')
    expect = (('foo', 'bar', 'baz'),
              ('aa', 4, 9.3),
              ('aaa', 2, 88.2),
              ('bb', 7, 100.9))
    ieq(expect, actual)
    ieq(expect, actual)


def test_search_headerless():
    table = []
    actual = search(table, 'foo', '[ab]{2}')
    expect = []
    ieq(expect, actual)
    ieq(expect, actual)


def test_searchcomplement():

    table1 = (('foo', 'bar', 'baz'),
              ('orange', 12, 'oranges are nice fruit'),
              ('mango', 42, 'I like them'),
              ('banana', 74, 'lovely too'),
              ('cucumber', 41, 'better than mango'))

    # search any field
    table2 = searchcomplement(table1, '.g.')
    expect2 = (('foo', 'bar', 'baz'),
               ('banana', 74, 'lovely too'))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # search a specific field
    table3 = searchcomplement(table1, 'foo', '.g.')
    expect3 = (('foo', 'bar', 'baz'),
               ('banana', 74, 'lovely too'),
               ('cucumber', 41, 'better than mango'))
    ieq(expect3, table3)
    ieq(expect3, table3)

    # search any field, using complement
    table2 = search(table1, '.g.', complement=True)
    expect2 = (('foo', 'bar', 'baz'),
               ('banana', 74, 'lovely too'))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # search a specific field, using complement
    table3 = search(table1, 'foo', '.g.', complement=True)
    expect3 = (('foo', 'bar', 'baz'),
               ('banana', 74, 'lovely too'),
               ('cucumber', 41, 'better than mango'))
    ieq(expect3, table3)
    ieq(expect3, table3)


def test_search_unicode():
    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4))
    actual = search(tbl, u'.Խա.')
    expect = ((u'name', u'id'),
              (u'Արամ Խաչատրյան', 1))
    ieq(expect, actual)
    ieq(expect, actual)


def test_splitdown():

    tbl = ((u'name', u'roles'),
           (u'Jane Doe', u'president,engineer,tailor,lawyer'),
           (u'John Doe', u'rocket scientist,optometrist,chef,knight,sailor'))

    actual = splitdown(tbl, 'roles', ',')
    expect = ((u'name', u'roles'),
              (u'Jane Doe', u'president'),
              (u'Jane Doe', u'engineer'),
              (u'Jane Doe', u'tailor'),
              (u'Jane Doe', u'lawyer'),
              (u'John Doe', u'rocket scientist'),
              (u'John Doe', u'optometrist'),
              (u'John Doe', u'chef'),
              (u'John Doe', u'knight'),
              (u'John Doe', u'sailor'))

    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)


# TODO test sub()



def test_capture_none_value():
    table = (('id', 'variable'),
             ('1', None),
             ('2', 'A1'))
    result = capture(table, 'variable', r'(\w)(\d+)', ('treat', 'time'),
                     fill=['', ''])
    expect = (('id', 'treat', 'time'),
              ('1', '', ''),
              ('2', 'A', '1'))
    ieq(expect, result)


def test_split_negative_index():
    table = [('id', 'tags'), (1, 'red,blue')]
    ieq([('id', 'first', 'second'), (1, 'red', 'blue')],
        split(table, -1, ',', newfields=['first', 'second']))


def test_splitdown_negative_index():
    table = [('id', 'tags'), (1, 'red,blue')]
    ieq([('id', 'tags'), (1, 'red'), (1, 'blue')],
        splitdown(table, -1, ','))


@pytest.mark.parametrize('transform', [split, splitdown])
@pytest.mark.parametrize('field', [-3, 2])
def test_split_invalid_index(transform, field):
    with pytest.raises(ArgumentError):
        list(transform([('id', 'tags'), (1, 'red,blue')], field, ','))


def test_split_duplicate_field_names_by_index():
    table = [('tag', 'id', 'tag'), ('left', 1, 'red,blue')]
    expectation = [('tag', 'id', 'first', 'second'),
                   ('left', 1, 'red', 'blue')]
    ieq(expectation, split(table, 2, ',', newfields=['first', 'second']))


def test_split_missing_values():
    # None is missing data; new fields receive None instead of a TypeError
    table = (('id', 'variable'),
             (1, 'parad1'),
             (2, None))
    expectation = (('id', 'variable', 'day'),
                   (1, 'para', '1'),
                   (2, None, None))
    result = split(table, 'variable', 'd', ('variable', 'day'))
    ieq(expectation, result)
    ieq(expectation, result)  # check twice

    # missing value with include_original keeps the original column
    result = split(table, 'variable', 'd', ('variable', 'day'),
                   include_original=True)
    expectation = (('id', 'variable', 'variable', 'day'),
                   (1, 'parad1', 'para', '1'),
                   (2, None, None, None))
    ieq(expectation, result)


def test_split_iterable_newfields_missing_values():
    # regression test for review finding: newfields given as a non-sequence
    # iterable must not fail on missing (None) values
    table = (('id', 'variable'),
             (1, 'parad1'),
             (2, None))
    expectation = (('id', 'variable', 'day'),
                   (1, 'para', '1'),
                   (2, None, None))
    result = split(table, 'variable', 'd', iter(['variable', 'day']))
    ieq(expectation, result)
    ieq(expectation, result)  # check twice
