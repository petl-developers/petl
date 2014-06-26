__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


from petl.testutils import ieq
from petl.transform.dedup import duplicates, unique, conflicts


def test_duplicates():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3))

    result = duplicates(table, 'foo')
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', u'3', u'7.8', True),
                   ('B', '2', 42),
                   ('D', 'xyz', 9.0),
                   ('D', 4, 12.3))
    ieq(expectation, result)

    # test with compound key
    result = duplicates(table, key=('foo', 'bar'))
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', '2', 42))
    ieq(expectation, result)


def test_duplicates_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = duplicates(table, key='foo')
    ieq(expect, actual)


def test_duplicates_wholerow():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '3.4'),
             ('D', 4, 12.3))

    result = duplicates(table)
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', '2', '3.4'))
    ieq(expectation, result)


def test_unique():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3),
             ('F', 7, 2.3))

    result = unique(table, 'foo')
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('E', None),
                   ('F', 7, 2.3))
    ieq(expectation, result)
    ieq(expectation, result)

    # test with compound key
    result = unique(table, key=('foo', 'bar'))
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('B', u'3', u'7.8', True),
                   ('D', 4, 12.3),
                   ('D', 'xyz', 9.0),
                   ('E', None),
                   ('F', 7, 2.3))
    ieq(expectation, result)
    ieq(expectation, result)


def test_unique_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = unique(table, key='foo')
    ieq(expect, actual)


def test_unique_wholerow():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '3.4'),
             ('D', 4, 12.3))

    result = unique(table)
    expectation = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('D', 4, 12.3))
    ieq(expectation, result)


def test_conflicts():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', None),
             ('D', 'xyz', 9.4),
             ('B', None, u'7.8', True),
             ('E', None),
             ('D', 'xyz', 12.3),
             ('A', 2, None))

    result = conflicts(table, 'foo', missing=None)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None),
                   ('D', 'xyz', 9.4),
                   ('D', 'xyz', 12.3))
    ieq(expectation, result)
    ieq(expectation, result)

    result = conflicts(table, 'foo', missing=None, exclude='baz')
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None))
    ieq(expectation, result)
    ieq(expectation, result)

    result = conflicts(table, 'foo', missing=None, exclude=('bar', 'baz'))
    expectation = (('foo', 'bar', 'baz'),)
    ieq(expectation, result)
    ieq(expectation, result)

    result = conflicts(table, 'foo', missing=None, include='bar')
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None))
    ieq(expectation, result)
    ieq(expectation, result)

    result = conflicts(table, 'foo', missing=None, include=('bar', 'baz'))
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None),
                   ('D', 'xyz', 9.4),
                   ('D', 'xyz', 12.3))
    ieq(expectation, result)
    ieq(expectation, result)


def test_conflicts_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = conflicts(table, key='foo')
    ieq(expect, actual)


# TODO unit tests for distinct()

