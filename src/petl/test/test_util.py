"""
TODO doc me

"""

from petl import header, fieldnames, data, records, rowcount, look, see, itervalues, valuecounter, valuecounts, valueset,\
                isunique, lookup, lookupone, recordlookup, recordlookupone, \
                DuplicateKeyError, rowlengths, stats, typecounts, parsecounts, typeset, \
                valuecount, parsenumber, stringpatterns, diffheaders, diffvalues, \
                datetimeparser, values, columns, facetcolumns, isordered, \
                rowgroupby

from petl.testutils import assertequal, ieq
import sys
from nose.tools import eq_


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
        def __init__(self, key, description):
            self.key = key
            self.description = description
        def __str__(self):
            return self.key
        def __repr__(self):
            return 'CustomField(%r, %r)' % (self.key, self.description)
        
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
    ieq(expect, actual)


def test_records():
    """Test the records function."""

    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = records(table)
    expect = ({'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': 2})
    ieq(expect, actual)
    
        
def test_records_shortrows():
    """Test the records function on a table with short rows."""

    table = (('foo', 'bar'), ('a', 1), ('b',))
    actual = records(table)
    expect = ({'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': None})
    ieq(expect, actual)
    
    
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


def test_itervalues():
    """Test the itervalues function."""
    
    table = (('foo', 'bar', 'baz'), 
             ('a', 1, True), 
             ('b', 2), 
             ('b', 7, False))

    actual = itervalues(table, 'foo')
    expect = ('a', 'b', 'b')
    ieq(expect, actual) 

    actual = itervalues(table, 'bar')
    expect = (1, 2, 7)
    ieq(expect, actual) 
    
    actual = itervalues(table, ('foo', 'bar'))
    expect = (('a', 1), ('b', 2), ('b', 7))
    ieq(expect, actual)
    
    actual = itervalues(table, 'baz')
    expect = (True, None, False)
    ieq(expect, actual)


def test_values():
    """Test the values function."""
    
    table = (('foo', 'bar', 'baz'), 
             ('a', 1, True), 
             ('b', 2), 
             ('b', 7, False))

    actual = values(table, 'foo')
    expect = ('a', 'b', 'b')
    ieq(expect, actual) 
    ieq(expect, actual) 

    actual = values(table, 'bar')
    expect = (1, 2, 7)
    ieq(expect, actual) 
    ieq(expect, actual) 
    
    actual = values(table, ('foo', 'bar'))
    expect = (('a', 1), ('b', 2), ('b', 7))
    ieq(expect, actual) 
    ieq(expect, actual) 
    
    actual = values(table, 'baz')
    expect = (True, None, False)
    ieq(expect, actual)
    ieq(expect, actual) 


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
    ieq(expect, actual) 
    ieq(expect, actual) 


def test_valuecounts_shortrows():
    
    table = (('foo', 'bar'), 
             ('a', True), 
             ('x', True), 
             ('b',), 
             ('b', True), 
             ('c', False), 
             ('z', False))
    actual = valuecounts(table, 'bar')
    expect = (('value', 'count', 'frequency'), 
              (True, 3, 3./6), 
              (False, 2, 2./6), 
              (None, 1, 1./6))
    ieq(expect, actual) 
    ieq(expect, actual) 


def test_valuecounts_allfields():
    
    table = (('foo', 'bar'), 
             ('a', True), 
             ('b', True), 
             ('b',), 
             ('b', True), 
             ('c', False), 
             ('c', False))
    actual = valuecounts(table)
    expect = (('field', 'value', 'count', 'frequency'),
              ('foo', 'b', 3, 3./6),
              ('foo', 'c', 2, 2./6),
              ('foo', 'a', 1, 1./6), 
              ('bar', True, 3, 3./6), 
              ('bar', False, 2, 2./6),
              ('bar', None, 1, 1./6))
    ieq(expect, actual) 
    ieq(expect, actual) 

    
def test_valuecounts_somefields():
    
    table = (('foo', 'bar', 'baz'), 
             ('a', True, .12), 
             ('b', True, .34), 
             ('b',), 
             ('b', True, .56), 
             ('c', False, .86), 
             ('c', False, .92))
    actual = valuecounts(table, 'foo', 'bar')
    expect = (('field', 'value', 'count', 'frequency'),
              ('foo', 'b', 3, 3./6),
              ('foo', 'c', 2, 2./6),
              ('foo', 'a', 1, 1./6), 
              ('bar', True, 3, 3./6), 
              ('bar', False, 2, 2./6),
              ('bar', None, 1, 1./6))
    ieq(expect, actual) 
    ieq(expect, actual) 

    
def test_valueset():
    """Test the valueset function."""

    table = (('foo', 'bar'), 
             ('a', True), 
             ('x', True), 
             ('b',), 
             ('b', True), 
             ('c', False), 
             ('z', False))

    actual = valueset(table, 'foo')
    expect = {'a', 'b', 'c', 'x', 'z'}
    assertequal(expect, actual)

    actual = valueset(table, 'bar')
    expect = {True, False, None}
    assertequal(expect, actual)


def test_isunique():
    """Test the isunique function."""
    
    table = (('foo', 'bar'), ('a', 1), ('b'), ('b', 2), ('c', 3, True))
    assert not isunique(table, 'foo')
    assert isunique(table, 'bar')
    

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
        lookupone(t1, 'foo', 'bar', strict=True)
    except DuplicateKeyError:
        pass # expected
    else:
        assert False, 'expected error'
        
    # lookup one column on another under, not strict 
    actual = lookupone(t1, 'foo', 'bar', strict=False)
    expect = {'a': 1, 'b': 2} # first value wins
    assertequal(expect, actual)

    # test default value - tuple of whole row
    actual = lookupone(t1, 'foo', strict=False) # no value selector
    expect = {'a': ('a', 1), 'b': ('b', 2)} # first wins
    assertequal(expect, actual)
    
    t2 = (('foo', 'bar', 'baz'),
          ('a', 1, True),
          ('b', 2, False),
          ('b', 3, True),
          ('b', 3, False))
    
    # test value selection
    actual = lookupone(t2, 'foo', ('bar', 'baz'), strict=False)
    expect = {'a': (1, True), 'b': (2, False)}
    assertequal(expect, actual)
    
    # test compound key
    actual = lookupone(t2, ('foo', 'bar'), 'baz', strict=False)
    expect = {('a', 1): True, ('b', 2): False, ('b', 3): True} # first wins
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
        recordlookupone(t1, 'foo', strict=True)
    except DuplicateKeyError:
        pass # expected
    else:
        assert False, 'expected error'
        
    # relax 
    actual = recordlookupone(t1, 'foo', strict=False)
    expect = {'a': {'foo': 'a', 'bar': 1}, 'b': {'foo': 'b', 'bar': 2}} # first wins
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
              ('b', 3): {'foo': 'b', 'bar': 3, 'baz': True}} # first wins
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
    ieq(expect, actual) 


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
             ('A', 1, 2.),
             ('B', u'2', 3.4),
             (u'B', u'3', 7.8, True),
             ('D', u'xyz', 9.0),
             ('E', 42))

    actual = typecounts(table, 'foo') 
    expect = (('type', 'count', 'frequency'), ('str', 4, 4./5), ('unicode', 1, 1./5))
    ieq(expect, actual)

    actual = typecounts(table, 'bar') 
    expect = (('type', 'count', 'frequency'), ('unicode', 3, 3./5), ('int', 2, 2./5))
    ieq(expect, actual)

    actual = typecounts(table, 'baz') 
    expect = (('type', 'count', 'frequency'), ('float', 4, 4./5), ('NoneType', 1, 1./5))
    ieq(expect, actual)


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
    ieq(expect, actual)
    
    
def test_parsenumber():
    
    assert parsenumber('1') == 1
    assert parsenumber('1.0') == 1.0
    assert parsenumber(str(sys.maxint + 1)) == sys.maxint + 1
    assert parsenumber('3+4j') == 3 + 4j
    assert parsenumber('aaa') == 'aaa'
    assert parsenumber(None) == None
    
    
def test_parsenumber_strict():
    
    assert parsenumber('1', strict=True) == 1
    assert parsenumber('1.0', strict=True) == 1.0
    assert parsenumber(str(sys.maxint + 1), strict=True) == sys.maxint + 1
    assert parsenumber('3+4j', strict=True) == 3 + 4j
    try:
        parsenumber('aaa', strict=True)
    except:
        pass # expected
    else:
        assert False, 'expected exception'
    try:
        parsenumber(None, strict=True)
    except:
        pass # expected
    else:
        assert False, 'expected exception'
    
    
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
    ieq(expect, actual) 
    
    actual = stringpatterns(table, 'bar')
    expect = (('pattern', 'count', 'frequency'), 
              ('999-9999', 3, 3./6), 
              ('999 9999', 2, 2./6),
              ('999-9999-AA', 1, 1./6))
    ieq(expect, actual) 
    

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
    
    
def test_laxparsers():
    
    p1 = datetimeparser('%Y-%m-%dT%H:%M:%S')
    try:
        v = p1('2002-12-25 00:00:00')
    except:
        pass
    else:
        assert False, 'expected exception'
    
    p2 = datetimeparser('%Y-%m-%dT%H:%M:%S', strict=False)
    try:
        v = p2('2002-12-25 00:00:00')
    except:
        assert False, 'did not expect exception'
    else:
        assertequal('2002-12-25 00:00:00', v)
    
    
def test_columns():
    
    table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    cols = columns(table)
    assertequal(['a', 'b', 'b'], cols['foo'])
    assertequal([1, 2, 3], cols['bar'])


def test_facetcolumns():
    
    table = [['foo', 'bar', 'baz'], 
             ['a', 1, True], 
             ['b', 2, True], 
             ['b', 3]]
    
    fc = facetcolumns(table, 'foo')
    assertequal(['a'], fc['a']['foo'])
    assertequal([1], fc['a']['bar'])
    assertequal([True], fc['a']['baz'])
    assertequal(['b', 'b'], fc['b']['foo'])
    assertequal([2, 3], fc['b']['bar'])
    assertequal([True, None], fc['b']['baz'])
    
    
def test_isordered():
    
    table1 = (('foo', 'bar', 'baz'), 
              ('a', 1, True), 
              ('b', 3, True), 
              ('b', 2))
    assert isordered(table1, key='foo')
    assert not isordered(table1, key='foo', reverse=True)
    assert not isordered(table1, key='foo', strict=True)

    table2 = (('foo', 'bar', 'baz'), 
              ('b', 2, True), 
              ('a', 1, True), 
              ('b', 3))
    assert not isordered(table2, key='foo')

    table3 = (('foo', 'bar', 'baz'), 
              ('a', 1, True), 
              ('b', 2, True), 
              ('b', 3))
    assert isordered(table3, key=('foo', 'bar'))
    assert isordered(table3)

    table4 = (('foo', 'bar', 'baz'), 
              ('a', 1, True), 
              ('b', 3, True), 
              ('b', 2))
    assert not isordered(table4, key=('foo', 'bar'))
    assert not isordered(table4)

    table5 = (('foo', 'bar', 'baz'), 
              ('b', 3, True), 
              ('b', 2),
              ('a', 1, True)) 
    assert not isordered(table5, key='foo')
    assert isordered(table5, key='foo', reverse=True)
    assert not isordered(table5, key='foo', reverse=True, strict=True)


def test_rowgroupby():
    
    table = (('foo', 'bar', 'baz'), 
             ('a', 1, True), 
             ('b', 2, True), 
             ('b', 3))
    
    # simplest form

    g = rowgroupby(table, 'foo')

    key, vals = g.next()
    vals = list(vals)
    eq_('a', key)
    eq_(1, len(vals))
    eq_(('a', 1, True), vals[0])

    key, vals = g.next()
    vals = list(vals)
    eq_('b', key)
    eq_(2, len(vals))
    eq_(('b', 2, True), vals[0])
    eq_(('b', 3), vals[1])

    # specify value
    
    g = rowgroupby(table, 'foo', 'bar')
    
    key, vals = g.next()
    vals = list(vals)
    eq_('a', key)
    eq_(1, len(vals))
    eq_(1, vals[0])

    key, vals = g.next()
    vals = list(vals)
    eq_('b', key)
    eq_(2, len(vals))
    eq_(2, vals[0])
    eq_(3, vals[1])

    # callable key
    
    g = rowgroupby(table, lambda r: r['foo'], lambda r: r['baz'])
    
    key, vals = g.next()
    vals = list(vals)
    eq_('a', key)
    eq_(1, len(vals))
    eq_(True, vals[0])

    key, vals = g.next()
    vals = list(vals)
    eq_('b', key)
    eq_(2, len(vals))
    eq_(True, vals[0])
    eq_(None, vals[1]) # gets padded
    