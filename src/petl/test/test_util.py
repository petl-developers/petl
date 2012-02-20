"""
TODO doc me

"""

from petl import header, fieldnames, data, records, rowcount, look, see, values, valuecounter, valuecounts, valueset,\
                unique, lookup, lookupone, recordlookup, recordlookupone, \
                DuplicateKeyError, rowlengths, stats, typecounts, parsecounts, typeset, \
                valuecount, parsenumber, stringpatterns, diffheaders, diffvalues

from petl.testutils import assertequal, iassertequal
import sys


def test_header():
    """Test the header function."""
    
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = header(table)
    expect = ('foo', 'bar')
    assertequal(expect, actual)
    
    
def test_fieldnames():
    """Test the fieldnames function."""
    
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = fieldnames(table)
    expect = ['foo', 'bar']
    assertequal(expect, actual)
    
    class CustomField(object):
        def __init__(self, id, description):
            self.id = id
            self.description = description
        def __str__(self):
            return self.id
        def __repr__(self):
            return 'CustomField(%r, %r)' % (self.id, self.description)
        
    table = ((CustomField('foo', 'Get some foo.'), CustomField('bar', 'A lot of bar.')), 
             ('a', 1), 
             ('b', 2))
    actual = fieldnames(table)
    expect = ['foo', 'bar']
    assertequal(expect, actual)
    
    
def test_data():
    """Test the data function."""

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = data(table)
    expect = (('a', 1), ('b', 2))
    iassertequal(expect, actual)


def test_records():
    """Test the records function."""

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = records(table)
    expect = ({'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': 2})
    iassertequal(expect, actual)
    
        
def test_records_shortrows():
    """Test the records function on a table with short rows."""

    table = (('foo', 'bar'), ('a', 1), ('b',))
    actual = records(table)
    expect = ({'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': None})
    iassertequal(expect, actual)
    
    
def test_rowcount():
    """Test the rowcount function."""
    
    table = (('foo', 'bar'), ('a', 1), ('b',))
    actual = rowcount(table)
    expect = 2
    assertequal(expect, actual)
    
    
def test_look():
    """Test the look function."""
    
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
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
    
    table = (('foo', 'bar'), ('a',), ('b', 2, True))
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
    
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = repr(see(table))
    expect = """'foo': 'a', 'b'
'bar': 1, 2
"""
    assertequal(expect, actual)


def test_values():
    """Test the values function."""
    
    table = (('foo', 'bar', 'baz'), 
             ('a', 1, True), 
             ('b', 2), 
             ('b', 7, False))

    actual = values(table, 'foo')
    expect = ('a', 'b', 'b')
    iassertequal(expect, actual) 

    actual = values(table, 'bar')
    expect = (1, 2, 7)
    iassertequal(expect, actual) 
    
    actual = values(table, ('foo', 'bar'))
    expect = (('a', 1), ('b', 2), ('b', 7))
    iassertequal(expect, actual)
    
    actual = values(table, 'baz')
    expect = (True, False)
    iassertequal(expect, actual)


def test_valuecount():
    """Test the valuecount function."""
    
    table = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 7))
    n, f = valuecount(table, 'foo', 'b')
    assertequal(2, n)
    assertequal(2./3, f) 
    
        
def test_valuecounter():
    """Test the valuecounter function."""
    
    table = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 7))
    actual = valuecounter(table, 'foo')
    expect = {'b': 2, 'a': 1}
    assertequal(expect, actual) 
    
        
def test_valuecounts():
    """Test the valuecounts function."""
    
    table = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 7))
    actual = valuecounts(table, 'foo')
    expect = (('value', 'count', 'frequency'), ('b', 2, 2./3), ('a', 1, 1./3))
    iassertequal(expect, actual) 


def test_valuecounts_shortrows():
    """Test the values function with short rows."""
    
    table = (('foo', 'bar'), ('a', True), ('b',), ('b', True), ('c', False))
    actual = valuecounts(table, 'bar')
    expect = (('value', 'count', 'frequency'), (True, 2, 2./3), (False, 1, 1./3))
    iassertequal(expect, actual) 

    
def test_valueset():
    """Test the valueset function."""

    table = (('foo', 'bar'), ('a', True), ('b',), ('b', True), ('c', False))

    actual = valueset(table, 'foo')
    expect = {'a', 'b', 'c'}
    assertequal(expect, actual)

    actual = valueset(table, 'bar')
    expect = {True, False}
    assertequal(expect, actual)


def test_unique():
    """Test the unique function."""
    
    table = (('foo', 'bar'), ('a', 1), ('b'), ('b', 2), ('c', 3, True))
    assert not unique(table, 'foo')
    assert unique(table, 'bar')
    

def test_lookup():
    """Test the lookup function."""

    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))
    
    # lookup one column on another
    actual = lookup(t1, 'foo', 'bar')
    expect = {'a': [1], 'b': [2, 3]}
    assertequal(expect, actual)

    # test default value - tuple of whole row
    actual = lookup(t1, 'foo') # no value selector
    expect = {'a': [('a', 1)], 'b': [('b', 2), ('b', 3)]}
    assertequal(expect, actual)
    
    t2 = (('foo', 'bar', 'baz'),
          ('a', 1, True),
          ('b', 2, False),
          ('b', 3, True),
          ('b', 3, False))
    
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
    
    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))
    
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
    
    t2 = (('foo', 'bar', 'baz'),
          ('a', 1, True),
          ('b', 2, False),
          ('b', 3, True),
          ('b', 3, False))
    
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
    
    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))
    
    actual = recordlookup(t1, 'foo') 
    expect = {'a': [{'foo': 'a', 'bar': 1}], 'b': [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]}
    assertequal(expect, actual)
    
    t2 = (('foo', 'bar', 'baz'),
          ('a', 1, True),
          ('b', 2, False),
          ('b', 3, True),
          ('b', 3, False))
    
    # test compound key
    actual = recordlookup(t2, ('foo', 'bar'))
    expect = {('a', 1): [{'foo': 'a', 'bar': 1, 'baz': True}], 
              ('b', 2): [{'foo': 'b', 'bar': 2, 'baz': False}], 
              ('b', 3): [{'foo': 'b', 'bar': 3, 'baz': True}, 
                         {'foo': 'b', 'bar': 3, 'baz': False}]}
    assertequal(expect, actual)
    
    
def test_recordlookupone():
    """Test the recordlookupone function."""
    
    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))
    
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

    t2 = (('foo', 'bar', 'baz'),
          ('a', 1, True),
          ('b', 2, False),
          ('b', 3, True),
          ('b', 3, False))
    
    # test compound key
    actual = recordlookupone(t2, ('foo', 'bar'), strict=False)
    expect = {('a', 1): {'foo': 'a', 'bar': 1, 'baz': True}, 
              ('b', 2): {'foo': 'b', 'bar': 2, 'baz': False}, 
              ('b', 3): {'foo': 'b', 'bar': 3, 'baz': False}} # last wins
    assertequal(expect, actual)
    

def test_rowlengths():
    """Test the rowlengths function."""

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             (u'B', u'3', u'7.8', True),
             ('D', 'xyz', 9.0),
             ('E', None),
             ('F', 9))
    actual = rowlengths(table)
    expect = (('length', 'count'), (3, 3), (2, 2), (4, 1))
    iassertequal(expect, actual) 


def test_stats():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             (u'B', u'3', u'7.8', True),
             ('D', 'xyz', 9.0),
             ('E', None))

    result = stats(table, 'bar')    
    assert result['min'] == 1.0
    assert result['max'] == 3.0
    assert result['sum'] == 6.0
    assert result['count'] == 3
    assert result['errors'] == 2
    assert result['mean'] == 2.0


def test_typecounts():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, '2'),
             ('B', u'2', '3.4'),
             (u'B', u'3', '7.8', True),
             ('D', u'xyz', 9.0),
             ('E', 42))

    actual = typecounts(table, 'foo') 
    expect = (('type', 'count'), ('str', 4), ('unicode', 1))
    iassertequal(expect, actual)

    actual = typecounts(table, 'bar') 
    expect = (('type', 'count'), ('unicode', 3), ('int', 2))
    iassertequal(expect, actual)

    actual = typecounts(table, 'baz') 
    expect = (('type', 'count'), ('str', 3), ('float', 1))
    iassertequal(expect, actual)


def test_typeset():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, '2'),
             ('B', u'2', '3.4'),
             (u'B', u'3', '7.8', True),
             ('D', u'xyz', 9.0),
             ('E', 42))

    actual = typeset(table, 'foo') 
    expect = {str, unicode}
    assertequal(expect, actual)


def test_parsecounts():

    table = (('foo', 'bar', 'baz'),
             ('A', 'aaa', 2),
             ('B', u'2', '3.4'),
             (u'B', u'3', u'7.8', True),
             ('D', '3.7', 9.0),
             ('E', 42))

    actual = parsecounts(table, 'bar') 
    expect = (('type', 'count', 'errors'), ('float', 3, 1), ('int', 2, 2))
    iassertequal(expect, actual)
    
    
def test_parsenumber():
    
    assert parsenumber('1') == 1
    assert parsenumber('1.0') == 1.0
    assert parsenumber(str(sys.maxint + 1)) == sys.maxint + 1
    assert parsenumber('3+4j') == 3 + 4j
    assert parsenumber('aaa') == 'aaa'
    assert parsenumber(None) == None
    
    
def test_stringpatterns():
    
    table = (('foo', 'bar'),
             ('Mr. Foo', '123-1254'),
             ('Mrs. Bar', '234-1123'),
             ('Mr. Spo', '123-1254'),
             (u'Mr. Baz', u'321 1434'),
             (u'Mrs. Baz', u'321 1434'),
             ('Mr. Quux', '123-1254-XX'))
    
    actual = stringpatterns(table, 'foo')
    expect = (('pattern', 'count', 'frequency'), 
              ('Aa. Aaa', 3, 3./6), 
              ('Aaa. Aaa', 2, 2./6), 
              ('Aa. Aaaa', 1, 1./6))
    iassertequal(expect, actual) 
    
    actual = stringpatterns(table, 'bar')
    expect = (('pattern', 'count', 'frequency'), 
              ('999-9999', 3, 3./6), 
              ('999 9999', 2, 2./6),
              ('999-9999-AA', 1, 1./6))
    iassertequal(expect, actual) 
    

def test_diffheaders():
    
    table1 = (('foo', 'bar', 'baz'),
              ('a', 1, .3))

    table2 = (('baz', 'bar', 'quux'),
              ('a', 1, .3))
    
    add, sub = diffheaders(table1, table2)
    assertequal({'quux'}, add)
    assertequal({'foo'}, sub)
    
    
def test_diffvalues():
    
    table1 = (('foo', 'bar'),
              ('a', 1),
              ('b', 3))

    table2 = (('bar', 'foo'),
              (1, 'a'),
              (3, 'c'))
    
    add, sub = diffvalues(table1, table2, 'foo')
    assertequal({'c'}, add)
    assertequal({'b'}, sub)
    
    