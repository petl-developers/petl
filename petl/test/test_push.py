from __future__ import absolute_import, print_function, division
# N.B., do not import unicode_literals in tests


from tempfile import NamedTemporaryFile


from petl.io import fromcsv, fromtsv, frompickle
from petl.test.helpers import ieq
from petl.push import tocsv, totsv, topickle, partition, sort, duplicates, \
    unique, diff


def test_topickle():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', 12),
         ('banana', 'London', 42),
         ('orange', 'Paris', 31),
         ('banana', 'Amsterdam', 74),
         ('kiwi', 'Berlin', 55)]

    fn = NamedTemporaryFile().name
    p = topickle(fn)
    p.push(t)

    ieq(t, frompickle(fn))


def test_topickle_pipe():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', 12),
         ('banana', 'London', 42),
         ('orange', 'Paris', 31),
         ('banana', 'Amsterdam', 74),
         ('kiwi', 'Berlin', 55)]

    fn1 = NamedTemporaryFile().name
    fn2 = NamedTemporaryFile().name
    p = topickle(fn1)
    p.pipe(topickle(fn2))
    p.push(t)

    ieq(t, frompickle(fn1))
    ieq(t, frompickle(fn2))


def test_tocsv():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', '12'),
         ('banana', 'London', '42'),
         ('orange', 'Paris', '31'),
         ('banana', 'Amsterdam', '74'),
         ('kiwi', 'Berlin', '55')]

    fn = NamedTemporaryFile().name
    p = tocsv(fn)
    p.push(t)

    ieq(t, fromcsv(fn))


def test_tocsv_pipe():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', '12'),
         ('banana', 'London', '42'),
         ('orange', 'Paris', '31'),
         ('banana', 'Amsterdam', '74'),
         ('kiwi', 'Berlin', '55')]

    fn1 = NamedTemporaryFile().name
    fn2 = NamedTemporaryFile().name
    p = tocsv(fn1)
    p.pipe(tocsv(fn2))
    p.push(t)

    ieq(t, fromcsv(fn1))
    ieq(t, fromcsv(fn2))


def test_totsv():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', '12'),
         ('banana', 'London', '42'),
         ('orange', 'Paris', '31'),
         ('banana', 'Amsterdam', '74'),
         ('kiwi', 'Berlin', '55')]

    fn = NamedTemporaryFile().name
    p = totsv(fn)
    p.push(t)

    ieq(t, fromtsv(fn))


def test_partition():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', 12),
         ('banana', 'London', 42),
         ('orange', 'Paris', 31),
         ('banana', 'Amsterdam', 74),
         ('kiwi', 'Berlin', 55)]

    p = partition('fruit')
    fn1 = NamedTemporaryFile().name
    fn2 = NamedTemporaryFile().name
    p.pipe('orange', tocsv(fn1))
    p.pipe('banana', tocsv(fn2))
    p.push(t)

    oranges_expected = [('fruit', 'city', 'sales'),
                        ('orange', 'London', '12'),
                        ('orange', 'Paris', '31')]

    bananas_expected = [('fruit', 'city', 'sales'),
                        ('banana', 'London', '42'),
                        ('banana', 'Amsterdam', '74')]

    oranges_actual = fromcsv(fn1)
    bananas_actual = fromcsv(fn2)
    ieq(oranges_expected, oranges_actual)
    ieq(bananas_expected, bananas_actual)

    # alternative syntax

    p = partition('fruit')
    p | ('orange', tocsv(fn1))
    p | ('banana', tocsv(fn2))
    p.push(t)
    ieq(oranges_expected, oranges_actual)
    ieq(bananas_expected, bananas_actual)
    
    # test with callable discriminator

    p = partition(lambda row: row['sales'] > 40)
    p | (True, tocsv(fn1))
    p | (False, tocsv(fn2))
    p.push(t)

    high_expected = [('fruit', 'city', 'sales'),
                     ('banana', 'London', '42'),
                     ('banana', 'Amsterdam', '74'),
                     ('kiwi', 'Berlin', '55')]

    low_expected = [('fruit', 'city', 'sales'),
                    ('orange', 'London', '12'),
                    ('orange', 'Paris', '31')]

    high_actual = fromcsv(fn1)
    low_actual = fromcsv(fn2)
    ieq(high_expected, high_actual)
    ieq(low_expected, low_actual)


def test_sort():
    table = (('foo', 'bar'),
             ('C', '2'),
             ('A', '9'),
             ('A', '6'),
             ('F', '1'),
             ('D', '10'))
    
    fn = NamedTemporaryFile().name
    p = sort('foo')
    p.pipe(topickle(fn))
    p.push(table)

    expectation = (('foo', 'bar'),
                   ('A', '9'),
                   ('A', '6'),
                   ('C', '2'),
                   ('D', '10'),
                   ('F', '1'))
    ieq(expectation, frompickle(fn))


def test_sort_buffered():
    table = (('foo', 'bar'),
             ('C', '2'),
             ('A', '9'),
             ('A', '6'),
             ('F', '1'),
             ('D', '10'))
    
    fn = NamedTemporaryFile().name
    p = sort('foo', buffersize=2)
    p.pipe(topickle(fn))
    p.push(table)

    expectation = (('foo', 'bar'),
                   ('A', '9'),
                   ('A', '6'),
                   ('C', '2'),
                   ('D', '10'),
                   ('F', '1'))
    actual = frompickle(fn)
    ieq(expectation, actual)


def test_duplicates():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3))

    fn1 = NamedTemporaryFile().name
    fn2 = NamedTemporaryFile().name
    p = sort('foo')
    q = p.pipe(duplicates('foo'))
    q.pipe(topickle(fn1))
    q.pipe('remainder', topickle(fn2))
    p.push(table)

    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', u'3', u'7.8', True),
                   ('B', '2', 42),
                   ('D', 'xyz', 9.0),
                   ('D', 4, 12.3))
    ieq(expectation, frompickle(fn1))

    exremainder = (('foo', 'bar', 'baz'),
                   ('A', 1, 2), 
                   ('E', None))
    ieq(exremainder, frompickle(fn2))
    
    # test with compound key
    p = sort(key=('foo', 'bar'))
    q = p.pipe(duplicates(key=('foo', 'bar')))
    q.pipe(topickle(fn1))
    q.pipe('remainder', topickle(fn2))
    p.push(table)
    
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', '2', 42))
    ieq(expectation, frompickle(fn1))

    exremainder = (('foo', 'bar', 'baz'),
                   ('A', 1, 2), 
                   ('B', u'3', u'7.8', True),
                   ('D', 4, 12.3),
                   ('D', 'xyz', 9.0),
                   ('E', None))
    ieq(exremainder, frompickle(fn2))


def test_unique():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3))

    fn1 = NamedTemporaryFile().name
    fn2 = NamedTemporaryFile().name
    p = sort('foo')
    q = p.pipe(unique('foo'))
    q.pipe(topickle(fn1))
    q.pipe('remainder', topickle(fn2))
    p.push(table)

    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2), 
                   ('E', None))
    ieq(expectation, frompickle(fn1))

    exremainder = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', u'3', u'7.8', True),
                   ('B', '2', 42),
                   ('D', 'xyz', 9.0),
                   ('D', 4, 12.3))
    ieq(exremainder, frompickle(fn2))
    

def test_operator_overload():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3))

    fn1 = NamedTemporaryFile().name
    p = sort('foo')
    p | duplicates('foo') | topickle(fn1)
    p.push(table)

    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', u'3', u'7.8', True),
                   ('B', '2', 42),
                   ('D', 'xyz', 9.0),
                   ('D', 4, 12.3))
    ieq(expectation, frompickle(fn1))


def test_diff():

    tablea = (('foo', 'bar', 'baz'),
              ('A', 1, True),
              ('B', 2, False),
              ('C', 7, False),
              ('C', 9, True))
    
    tableb = (('x', 'y', 'z'),
              ('A', 9, False),
              ('B', 2, False),
              ('B', 3, True),
              ('C', 9, True))
    
    aminusb = (('foo', 'bar', 'baz'),
               ('A', 1, True),
               ('C', 7, False))
    
    bminusa = (('foo', 'bar', 'baz'),
               ('A', 9, False),
               ('B', 3, True))

    both = (('foo', 'bar', 'baz'),
            ('B', 2, False),
            ('C', 9, True))

    fn1 = NamedTemporaryFile().name
    fn2 = NamedTemporaryFile().name
    fn3 = NamedTemporaryFile().name
    p = diff()
    p.pipe('+', topickle(fn1))
    p.pipe('-', topickle(fn2))
    p.pipe(topickle(fn3))
    p.push(tablea, tableb)

    added, subtracted, common = (frompickle(fn1), frompickle(fn2),
                                 frompickle(fn3))
    ieq(bminusa, added)
    ieq(aminusb, subtracted)
    ieq(both, common)
