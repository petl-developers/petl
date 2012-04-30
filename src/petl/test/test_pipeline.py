"""
Tests for the pipeline module.

"""


from petl.io import fromcsv
from petl.testutils import iassertequal
from petl.pipeline import pipeline, partition, tocsv


def test_partition():

    t = [['fruit', 'city', 'sales'],
         ['orange', 'London', 12],
         ['banana', 'London', 42],
         ['orange', 'Paris', 31],
         ['banana', 'Amsterdam', 74],
         ['kiwi', 'Berlin', 55]]

    p = pipeline(t)
    q = p.pipe(partition('fruit'))
    q.pipe('orange', tocsv('oranges.csv'))
    q.pipe('banana', tocsv('bananas.csv'))
    p.run()

    oranges_actual = fromcsv('oranges.csv')
    oranges_expected = [('fruit', 'city', 'sales'),
                        ('orange', 'London', '12'),
                        ('orange', 'Paris', '31')]
    iassertequal(oranges_expected, oranges_actual)

    bananas_actual = fromcsv('bananas.csv')
    bananas_expected = [('fruit', 'city', 'sales'),
                        ('banana', 'London', '42'),
                        ('banana', 'Amsterdam', '74')]
    iassertequal(bananas_expected, bananas_actual)

    
    
