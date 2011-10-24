"""
TODO doc me

"""

from petl import fields, data, records, count, look, see, values, valueset,\
                unique, index, indexone, recindex, recindexone, DuplicateKeyError


def assertequal(expect, actual):
    assert expect == actual, (expect, actual)


def iassertequal(expect, actual):
    for e, a in zip(expect, actual):
        assert e == a, (e, a)


def test_fields():
    """Test the fields function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    actual = fields(table)
    expect = ['foo', 'bar']
    assertequal(expect, actual)
    

def test_data():
    """Test the data function."""

    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    actual = data(table)
    expect = [['a', 1], ['b', 2]]
    iassertequal(expect, actual)


def test_records():
    """Test the records function."""

    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    actual = records(table)
    expect = [{'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': 2}]
    iassertequal(expect, actual)
    
        
def test_records_shortrows():
    """Test the records function on a table with short rows."""

    table = [['foo', 'bar'], ['a', 1], ['b']]
    actual = records(table)
    expect = [{'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': None}]
    iassertequal(expect, actual)
    
    
def test_count():
    """Test the count function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b']]
    actual = count(table)
    expect = 2
    assertequal(expect, actual)
    
    
def test_look():
    """Test the look function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    actual = repr(look(table))
    expect = """+-------+-------+
| 'foo' | 'bar' |
+=======+=======+
| 'a'   | 1     |
+-------+-------+
| 'b'   | 2     |
+-------+-------+
"""
    assertequal(expect, actual)

        
def test_look_irregular_rows():
    """Test the look function with a table where row lengths are irregular."""
    
    table = [['foo', 'bar'], ['a'], ['b', 2, True]]
    actual = repr(look(table))
    expect = """+-------+-------+------+
| 'foo' | 'bar' |      |
+=======+=======+======+
| 'a'   |       |      |
+-------+-------+------+
| 'b'   | 2     | True |
+-------+-------+------+
"""
    assertequal(expect, actual)

    
def test_see():
    """Test the see function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2]]
    actual = repr(see(table))
    expect = """'foo': 'a', 'b'
'bar': 1, 2
"""
    assertequal(expect, actual)

        
def test_values():
    """Test the values function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 7]]
    actual = values(table, 'foo')
    expect = (('value', 'count'), ('b', 2), ('a', 1))
    iassertequal(expect, actual) 


def test_values_shortrows():
    """Test the values function with short rows."""
    
    table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
    actual = values(table, 'bar')
    expect = (('value', 'count'), (True, 2), (False, 1))
    iassertequal(expect, actual) 

    
def test_valueset():
    """Test the valueset function."""

    table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]

    actual = valueset(table, 'foo')
    expect = {'a', 'b', 'c'}
    assertequal(expect, actual)

    actual = valueset(table, 'bar')
    expect = {True, False}
    assertequal(expect, actual)


def test_unique():
    """Test the unique function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b'], ['b', 2], ['c', 3, True]]
    assert not unique(table, 'foo')
    assert unique(table, 'bar')
    

def test_index():
    """Test the index function."""

    t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    
    # index one column on another
    actual = index(t1, 'foo', 'bar')
    expect = {'a': [1], 'b': [2, 3]}
    assertequal(expect, actual)

    # test default value - tuple of whole row
    actual = index(t1, 'foo') # no value selector
    expect = {'a': [('a', 1)], 'b': [('b', 2), ('b', 3)]}
    assertequal(expect, actual)
    
    t2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
    
    # test value selection
    actual = index(t2, 'foo', ('bar', 'baz'))
    expect = {'a': [(1, True)], 'b': [(2, False), (3, True), (3, False)]}
    assertequal(expect, actual)
    
    # test compound key
    actual = index(t2, ('foo', 'bar'), 'baz')
    expect = {('a', 1): [True], ('b', 2): [False], ('b', 3): [True, False]}
    assertequal(expect, actual)
    
    
def test_indexone():
    """Test the indexone function."""
    
    t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    
    # index one column on another under strict mode
    try:
        indexone(t1, 'foo', 'bar')
    except DuplicateKeyError:
        pass # expected
    else:
        assert False, 'expected error'
        
    # index one column on another under, not strict 
    actual = indexone(t1, 'foo', 'bar', strict=False)
    expect = {'a': 1, 'b': 3} # last value wins
    assertequal(expect, actual)

    # test default value - tuple of whole row
    actual = indexone(t1, 'foo', strict=False) # no value selector
    expect = {'a': ('a', 1), 'b': ('b', 3)} # last wins
    assertequal(expect, actual)
    
    t2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
    
    # test value selection
    actual = indexone(t2, 'foo', ('bar', 'baz'), strict=False)
    expect = {'a': (1, True), 'b': (3, False)}
    assertequal(expect, actual)
    
    # test compound key
    actual = indexone(t2, ('foo', 'bar'), 'baz', strict=False)
    expect = {('a', 1): True, ('b', 2): False, ('b', 3): False} # last wins
    assertequal(expect, actual)
    

def test_recindex():
    """Test the recindex function."""
    
    t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    
    actual = recindex(t1, 'foo') 
    expect = {'a': [{'foo': 'a', 'bar': 1}], 'b': [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]}
    assertequal(expect, actual)
    
    t2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
    
    # test compound key
    actual = recindex(t2, ('foo', 'bar'))
    expect = {('a', 1): [{'foo': 'a', 'bar': 1, 'baz': True}], 
              ('b', 2): [{'foo': 'b', 'bar': 2, 'baz': False}], 
              ('b', 3): [{'foo': 'b', 'bar': 3, 'baz': True}, 
                         {'foo': 'b', 'bar': 3, 'baz': False}]}
    assertequal(expect, actual)
    
    
def test_recindexone():
    """Test the recindexone function."""
    
    t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    
    try:
        recindexone(t1, 'foo')
    except DuplicateKeyError:
        pass # expected
    else:
        assert False, 'expected error'
        
    # relax 
    actual = recindexone(t1, 'foo', strict=False)
    expect = {'a': {'foo': 'a', 'bar': 1}, 'b': {'foo': 'b', 'bar': 3}} # last wins
    assertequal(expect, actual)

    t2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
    
    # test compound key
    actual = recindexone(t2, ('foo', 'bar'), strict=False)
    expect = {('a', 1): {'foo': 'a', 'bar': 1, 'baz': True}, 
              ('b', 2): {'foo': 'b', 'bar': 2, 'baz': False}, 
              ('b', 3): {'foo': 'b', 'bar': 3, 'baz': False}} # last wins
    assertequal(expect, actual)
    

