from __future__ import absolute_import, print_function, division
# N.B., do not import unicode_literals in tests


from tempfile import NamedTemporaryFile

from petl.io import fromcsv, fromtsv, frompickle
from petl.testutils import ieq

from petl.push import tocsv, totsv, topickle, partition, sort, duplicates, \
    unique, diff


def test_topickle():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', 12),
         ('banana', 'London', 42),
         ('orange', 'Paris', 31),
         ('banana', 'Amsterdam', 74),
         ('kiwi', 'Berlin', 55)]

    f = NamedTemporaryFile(delete=False)
    p = topickle(f.name)
    p.push(t)

    ieq(t, frompickle(f.name))


def test_topickle_pipe():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', 12),
         ('banana', 'London', 42),
         ('orange', 'Paris', 31),
         ('banana', 'Amsterdam', 74),
         ('kiwi', 'Berlin', 55)]

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)
    p = topickle(f1.name)
    p.pipe(topickle(f2.name))
    p.push(t)

    ieq(t, frompickle(f1.name))
    ieq(t, frompickle(f2.name))


def test_tocsv():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', '12'),
         ('banana', 'London', '42'),
         ('orange', 'Paris', '31'),
         ('banana', 'Amsterdam', '74'),
         ('kiwi', 'Berlin', '55')]

    f = NamedTemporaryFile(delete=False)
    p = tocsv(f.name)
    p.push(t)

    ieq(t, fromcsv(f.name))


def test_tocsv_pipe():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', '12'),
         ('banana', 'London', '42'),
         ('orange', 'Paris', '31'),
         ('banana', 'Amsterdam', '74'),
         ('kiwi', 'Berlin', '55')]

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)
    p = tocsv(f1.name)
    p.pipe(tocsv(f2.name))
    p.push(t)

    ieq(t, fromcsv(f1.name))
    ieq(t, fromcsv(f2.name))


def test_totsv():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', '12'),
         ('banana', 'London', '42'),
         ('orange', 'Paris', '31'),
         ('banana', 'Amsterdam', '74'),
         ('kiwi', 'Berlin', '55')]

    f = NamedTemporaryFile(delete=False)
    p = totsv(f.name)
    p.push(t)

    ieq(t, fromtsv(f.name))


def test_partition():

    t = [('fruit', 'city', 'sales'),
         ('orange', 'London', 12),
         ('banana', 'London', 42),
         ('orange', 'Paris', 31),
         ('banana', 'Amsterdam', 74),
         ('kiwi', 'Berlin', 55)]

    p = partition('fruit')
    p.pipe('orange', tocsv('tmp/oranges.csv'))
    p.pipe('banana', tocsv('tmp/bananas.csv'))
    p.push(t)

    oranges_expected = [('fruit', 'city', 'sales'),
                        ('orange', 'London', '12'),
                        ('orange', 'Paris', '31')]

    bananas_expected = [('fruit', 'city', 'sales'),
                        ('banana', 'London', '42'),
                        ('banana', 'Amsterdam', '74')]

    oranges_actual = fromcsv('tmp/oranges.csv')
    bananas_actual = fromcsv('tmp/bananas.csv')
    ieq(oranges_expected, oranges_actual)
    ieq(bananas_expected, bananas_actual)

    # alternative syntax

    p = partition('fruit')
    p | ('orange', tocsv('tmp/oranges.csv'))
    p | ('banana', tocsv('tmp/bananas.csv'))
    p.push(t)
    ieq(oranges_expected, oranges_actual)
    ieq(bananas_expected, bananas_actual)
    
    # test with callable discriminator

    p = partition(lambda row: row['sales'] > 40)
    p | (True, tocsv('tmp/high.csv'))
    p | (False, tocsv('tmp/low.csv'))
    p.push(t)

    high_expected = [('fruit', 'city', 'sales'),
                     ('banana', 'London', '42'),
                     ('banana', 'Amsterdam', '74'),
                     ('kiwi', 'Berlin', '55')]

    low_expected = [('fruit', 'city', 'sales'),
                    ('orange', 'London', '12'),
                    ('orange', 'Paris', '31')]

    high_actual = fromcsv('tmp/high.csv')
    low_actual = fromcsv('tmp/low.csv')
    ieq(high_expected, high_actual)
    ieq(low_expected, low_actual)


def test_sort():
    table = (('foo', 'bar'),
             ('C', '2'),
             ('A', '9'),
             ('A', '6'),
             ('F', '1'),
             ('D', '10'))
    
    f = NamedTemporaryFile(delete=False)
    p = sort('foo')
    p.pipe(topickle(f.name))
    p.push(table)

    expectation = (('foo', 'bar'),
                   ('A', '9'),
                   ('A', '6'),
                   ('C', '2'),
                   ('D', '10'),
                   ('F', '1'))
    ieq(expectation, frompickle(f.name))


def test_sort_buffered():
    table = (('foo', 'bar'),
             ('C', '2'),
             ('A', '9'),
             ('A', '6'),
             ('F', '1'),
             ('D', '10'))
    
    f = NamedTemporaryFile(delete=False)
    p = sort('foo', buffersize=2)
    p.pipe(topickle(f.name))
    p.push(table)

    expectation = (('foo', 'bar'),
                   ('A', '9'),
                   ('A', '6'),
                   ('C', '2'),
                   ('D', '10'),
                   ('F', '1'))
    actual = frompickle(f.name)
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

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)
    p = sort('foo')
    q = p.pipe(duplicates('foo'))
    q.pipe(topickle(f1.name))
    q.pipe('remainder', topickle(f2.name))
    p.push(table)

    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', u'3', u'7.8', True),
                   ('B', '2', 42),
                   ('D', 'xyz', 9.0),
                   ('D', 4, 12.3))
    ieq(expectation, frompickle(f1.name))

    exremainder = (('foo', 'bar', 'baz'),
                   ('A', 1, 2), 
                   ('E', None))
    ieq(exremainder, frompickle(f2.name))
    
    # test with compound key
    p = sort(key=('foo', 'bar'))
    q = p.pipe(duplicates(key=('foo', 'bar')))
    q.pipe(topickle(f1.name))
    q.pipe('remainder', topickle(f2.name))
    p.push(table)
    
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', '2', 42))
    ieq(expectation, frompickle(f1.name))

    exremainder = (('foo', 'bar', 'baz'),
                   ('A', 1, 2), 
                   ('B', u'3', u'7.8', True),
                   ('D', 4, 12.3),
                   ('D', 'xyz', 9.0),
                   ('E', None))
    ieq(exremainder, frompickle(f2.name))


def test_unique():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)
    p = sort('foo')
    q = p.pipe(unique('foo'))
    q.pipe(topickle(f1.name))
    q.pipe('remainder', topickle(f2.name))
    p.push(table)

    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2), 
                   ('E', None))
    ieq(expectation, frompickle(f1.name))

    exremainder = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', u'3', u'7.8', True),
                   ('B', '2', 42),
                   ('D', 'xyz', 9.0),
                   ('D', 4, 12.3))
    ieq(exremainder, frompickle(f2.name))
    

def test_operator_overload():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3))

    f1 = NamedTemporaryFile(delete=False)
    p = sort('foo')
    p | duplicates('foo') | topickle(f1.name)
    p.push(table)

    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', u'3', u'7.8', True),
                   ('B', '2', 42),
                   ('D', 'xyz', 9.0),
                   ('D', 4, 12.3))
    ieq(expectation, frompickle(f1.name))


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

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)
    f3 = NamedTemporaryFile(delete=False)
    p = diff()
    p.pipe('+', topickle(f1.name))
    p.pipe('-', topickle(f2.name))
    p.pipe(topickle(f3.name))
    p.push(tablea, tableb)

    added, subtracted, common = (frompickle(f1.name), frompickle(f2.name),
                                 frompickle(f3.name))
    ieq(bminusa, added)
    ieq(aminusb, subtracted)
    ieq(both, common)
