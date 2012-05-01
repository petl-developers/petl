"""
Tests for the push module.

"""


from petl.io import fromcsv
from petl.testutils import iassertequal
from petl.push import partition, tocsv


def test_partition():

    t = [['fruit', 'city', 'sales'],
         ['orange', 'London', 12],
         ['banana', 'London', 42],
         ['orange', 'Paris', 31],
         ['banana', 'Amsterdam', 74],
         ['kiwi', 'Berlin', 55]]

    p = partition('fruit')
    p.pipe('orange', tocsv('oranges.csv'))
    p.pipe('banana', tocsv('bananas.csv'))
    p.push(t)

    oranges_expected = [('fruit', 'city', 'sales'),
                        ('orange', 'London', '12'),
                        ('orange', 'Paris', '31')]

    bananas_expected = [('fruit', 'city', 'sales'),
                        ('banana', 'London', '42'),
                        ('banana', 'Amsterdam', '74')]

    oranges_actual = fromcsv('oranges.csv')
    bananas_actual = fromcsv('bananas.csv')
    iassertequal(oranges_expected, oranges_actual)
    iassertequal(bananas_expected, bananas_actual)

    # alternative syntax

    p = partition('fruit')
    p | ('orange', tocsv('oranges.csv'))
    p | ('banana', tocsv('bananas.csv'))
    p.push(t)
    iassertequal(oranges_expected, oranges_actual)
    iassertequal(bananas_expected, bananas_actual)
    
    # test with callable discriminator

    p = partition(lambda row: row['sales'] > 40)
    p | (True, tocsv('high.csv'))
    p | (False, tocsv('low.csv'))
    p.push(t)

    high_expected = [('fruit', 'city', 'sales'),
                     ('banana', 'London', '42'),
                     ('banana', 'Amsterdam', '74'),
                     ('kiwi', 'Berlin', '55')]

    low_expected = [('fruit', 'city', 'sales'),
                    ('orange', 'London', '12'),
                    ('orange', 'Paris', '31')]

    high_actual = fromcsv('high.csv')
    low_actual = fromcsv('low.csv')
    iassertequal(high_expected, high_actual)
    iassertequal(low_expected, low_actual)
