"""
TODO doc me

"""

from petl import fields, data, records, count, look, see, values, valueset,\
                unique


def assertequal(expect, actual):
    assert expect == actual, (expect, actual)


def iassertequal(expect, actual):
    for e, a in zip(expect, actual):
        assert e == a, (e, a)


def test_fields():
    """Test the fields function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    expect = ['foo', 'bar']
    actual = fields(table)
    assertequal(expect, actual)
    

def test_data():
    """Test the data function."""

    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    expect = [['a', 1], ['b', 2]]
    actual = data(table)
    iassertequal(expect, actual)


def test_records():
    """Test the records function."""

    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    expect = [{'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': 2}]
    actual = records(table)
    iassertequal(expect, actual)
    
        
def test_records_shortrows():
    """Test the records function on a table with short rows."""

    table = [['foo', 'bar'], ['a', 1], ['b']]
    expect = [{'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': None}]
    actual = records(table)
    iassertequal(expect, actual)
    
    
def test_count():
    """Test the count function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b']]
    expect = 2
    actual = count(table)
    assertequal(expect, actual)
    
    
def test_look():
    """Test the look function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    expect = u"""+-------+-------+
| 'foo' | 'bar' |
+=======+=======+
| 'a'   | 1     |
+-------+-------+
| 'b'   | 2     |
+-------+-------+
"""
    actual = repr(look(table))
    assertequal(expect, actual)

        
def test_look_irregular_rows():
    """Test the look function with a table where row lengths are irregular."""
    
    table = [['foo', 'bar'], ['a'], ['b', 2, True]]
    expect = u"""+-------+-------+------+
| 'foo' | 'bar' |      |
+=======+=======+======+
| 'a'   |       |      |
+-------+-------+------+
| 'b'   | 2     | True |
+-------+-------+------+
"""
    actual = repr(look(table))
    assertequal(expect, actual)

    
def test_see():
    """Test the see function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    expect = u"""'foo': 'a', 'b'
'bar': 1, 2
"""
    actual = repr(see(table))
    assertequal(expect, actual)

        
def test_values():
    """Test the values function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 7]]
    expect = (('value', 'count'), ('b', 2), ('a', 1))
    actual = values(table, 'foo')
    iassertequal(expect, actual) 


def test_values_shortrows():
    """Test the values function with short rows."""
    
    table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
    expect = (('value', 'count'), (True, 2), (False, 1))
    actual = values(table, 'bar')
    iassertequal(expect, actual) 

    
def test_valueset():
    """Test the valueset function."""

    table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
    expect = {'a', 'b', 'c'}
    actual = valueset(table, 'foo')
    assertequal(expect, actual)
    expect = {True, False}
    actual = valueset(table, 'bar')
    assertequal(expect, actual)


def test_unique():
    """Test the unique function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b'], ['b', 2], ['c', 3, True]]
    assert not unique(table, 'foo')
    assert unique(table, 'bar')
    
    