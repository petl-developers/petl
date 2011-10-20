"""
TODO doc me

"""

from petl import fields, data, records


def itercompare(it1, it2):
    for x, y in zip(it1, it2):
        assert x == y, (x, y)


def test_fields():
    """Test the fields function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    expectation = ['foo', 'bar']
    assert expectation == fields(table)
    

def test_data():
    """Test the data function."""

    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    expectation = [['a', 1], ['b', 2]]
    itercompare(expectation, data(table))


def test_records():
    """Test the records function."""

    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    expectation = [{'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': 2}]
    itercompare(expectation, records(table))
    
        
def test_records_shortrows():
    """Test the records function on a table with short rows."""

    table = [['foo', 'bar'], ['a', 1], ['b']]
    expectation = [{'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': None}]
    itercompare(expectation, records(table))
    
    