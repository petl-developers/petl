"""
TODO doc me

"""

from petl import fields, data, records, count, look, see, values, valuecounter, valuecounts, valueset,\
                unique, lookup, lookupone, recordlookup, recordlookupone, \
                DuplicateKeyError


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
    expect = ['a', 'b', 'b']
    iassertequal(expect, actual) 

    actual = values(table, 'bar')
    expect = [1, 2, 7]
    iassertequal(expect, actual) 


def test_valuecounter():
    """Test the valuecounter function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 7]]
    actual = valuecounter(table, 'foo')
    expect = {'b': 2, 'a': 1}
    assertequal(expect, actual) 
    
        
def test_valuecounts():
    """Test the valuecounts function."""
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 7]]
    actual = valuecounts(table, 'foo')
    expect = (('value', 'count'), ('b', 2), ('a', 1))
    iassertequal(expect, actual) 


def test_valuecounts_shortrows():
    """Test the values function with short rows."""
    
    table = [['foo', 'bar'], ['a', True], ['b'], ['b', True], ['c', False]]
    actual = valuecounts(table, 'bar')
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
    

def test_lookup():
    """Test the lookup function."""

    t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    
    # lookup one column on another
    actual = lookup(t1, 'foo', 'bar')
    expect = {'a': [1], 'b': [2, 3]}
    assertequal(expect, actual)

    # test default value - tuple of whole row
    actual = lookup(t1, 'foo') # no value selector
    expect = {'a': [('a', 1)], 'b': [('b', 2), ('b', 3)]}
    assertequal(expect, actual)
    
    t2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
    
    # test value selection
    actual = lookup(t2, 'foo', ('bar', 'baz'))
    expect = {'a': [(1, True)], 'b': [(2, False), (3, True), (3, False)]}
    assertequal(expect, actual)
    
    # test compound key
    actual = lookup(t2, ('foo', 'bar'), 'baz')
    expect = {('a', 1): [True], ('b', 2): [False], ('b', 3): [True, False]}
    assertequal(expect, actual)
    
    
def test_lookupone():
    """Test the lookupone function."""
    
    t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    
    # lookup one column on another under strict mode
    try:
        lookupone(t1, 'foo', 'bar')
    except DuplicateKeyError:
        pass # expected
    else:
        assert False, 'expected error'
        
    # lookup one column on another under, not strict 
    actual = lookupone(t1, 'foo', 'bar', strict=False)
    expect = {'a': 1, 'b': 3} # last value wins
    assertequal(expect, actual)

    # test default value - tuple of whole row
    actual = lookupone(t1, 'foo', strict=False) # no value selector
    expect = {'a': ('a', 1), 'b': ('b', 3)} # last wins
    assertequal(expect, actual)
    
    t2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
    
    # test value selection
    actual = lookupone(t2, 'foo', ('bar', 'baz'), strict=False)
    expect = {'a': (1, True), 'b': (3, False)}
    assertequal(expect, actual)
    
    # test compound key
    actual = lookupone(t2, ('foo', 'bar'), 'baz', strict=False)
    expect = {('a', 1): True, ('b', 2): False, ('b', 3): False} # last wins
    assertequal(expect, actual)
    

def test_recordlookup():
    """Test the recordlookup function."""
    
    t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    
    actual = recordlookup(t1, 'foo') 
    expect = {'a': [{'foo': 'a', 'bar': 1}], 'b': [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]}
    assertequal(expect, actual)
    
    t2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
    
    # test compound key
    actual = recordlookup(t2, ('foo', 'bar'))
    expect = {('a', 1): [{'foo': 'a', 'bar': 1, 'baz': True}], 
              ('b', 2): [{'foo': 'b', 'bar': 2, 'baz': False}], 
              ('b', 3): [{'foo': 'b', 'bar': 3, 'baz': True}, 
                         {'foo': 'b', 'bar': 3, 'baz': False}]}
    assertequal(expect, actual)
    
    
def test_recordlookupone():
    """Test the recordlookupone function."""
    
    t1 = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    
    try:
        recordlookupone(t1, 'foo')
    except DuplicateKeyError:
        pass # expected
    else:
        assert False, 'expected error'
        
    # relax 
    actual = recordlookupone(t1, 'foo', strict=False)
    expect = {'a': {'foo': 'a', 'bar': 1}, 'b': {'foo': 'b', 'bar': 3}} # last wins
    assertequal(expect, actual)

    t2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
    
    # test compound key
    actual = recordlookupone(t2, ('foo', 'bar'), strict=False)
    expect = {('a', 1): {'foo': 'a', 'bar': 1, 'baz': True}, 
              ('b', 2): {'foo': 'b', 'bar': 2, 'baz': False}, 
              ('b', 3): {'foo': 'b', 'bar': 3, 'baz': False}} # last wins
    assertequal(expect, actual)
    

