"""
Tests for the petl.transform module.

"""


from petl.testfun import iassertequal
from petl import rename, fieldnames, cut, cat, convert, translate


def test_rename():
    """Test the rename function."""

    table = [['foo', 'bar'],
             ['M', 12],
             ['F', 34],
             ['-', 56]]
    
    result = rename(table, {'foo': 'foofoo', 'bar': 'barbar'})
    assert list(fieldnames(result)) == ['foofoo', 'barbar']
    
    result = rename(table)
    result['foo'] = 'spong'
    assert list(fieldnames(result)) == ['spong', 'bar']
    
    # TODO test cachetag


def test_cut():
    """Test the cut function."""
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    cut1 = cut(table, 'foo')
    expectation = [['foo'],
                   ['A'],
                   ['B'],
                   [u'B'],
                   ['D'],
                   ['E']]
    iassertequal(expectation, cut1)
    
    cut2 = cut(table, 'foo', 'baz')
    expectation = [['foo', 'baz'],
                   ['A', 2],
                   ['B', '3.4'],
                   [u'B', u'7.8'],
                   ['D', 9.0],
                   ['E', None]]
    iassertequal(expectation, cut2)
    
    cut3 = cut(table, 0, 2)
    expectation = [['foo', 'baz'],
                   ['A', 2],
                   ['B', '3.4'],
                   [u'B', u'7.8'],
                   ['D', 9.0],
                   ['E', None]]
    iassertequal(expectation, cut3)
    
    cut4 = cut(table, 'bar', 0)
    expectation = [['bar', 'foo'],
                   [1, 'A'],
                   ['2', 'B'],
                   [u'3', u'B'],
                   ['xyz', 'D'],
                   [None, 'E']]
    iassertequal(expectation, cut4)
    

def test_cat():
    """Test the cat function."""
    
    table1 = [['foo', 'bar'],
              [1, 'A'],
              [2, 'B']]

    table2 = [['bar', 'baz'],
              ['C', True],
              ['D', False]]
    
    cat1 = cat(table1, table2, missing=None)
    expectation = [['foo', 'bar', 'baz'],
                   [1, 'A', None],
                   [2, 'B', None],
                   [None, 'C', True],
                   [None, 'D', False]]
    iassertequal(expectation, cat1)

    # how does cat cope with uneven rows?
    
    table3 = [['foo', 'bar', 'baz'],
              ['A', 1, 2],
              ['B', '2', '3.4'],
              [u'B', u'3', u'7.8', True],
              ['D', 'xyz', 9.0],
              ['E', None]]

    cat3 = cat(table3, missing=None)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2],
                   ['B', '2', '3.4'],
                   [u'B', u'3', u'7.8'],
                   ['D', 'xyz', 9.0],
                   ['E', None, None]]
    iassertequal(expectation, cat3)
    
    # cat more than two tables?
    cat4 = cat(table1, table2, table3)
    expectation = [['foo', 'bar', 'baz'],
                   [1, 'A', None],
                   [2, 'B', None],
                   [None, 'C', True],
                   [None, 'D', False],
                   ['A', 1, 2],
                   ['B', '2', '3.4'],
                   [u'B', u'3', u'7.8'],
                   ['D', 'xyz', 9.0],
                   ['E', None, None]]
    iassertequal(expectation, cat4)
    
    # TODO test cachetag


def test_convert():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]
    
    # test the style where the converters functions are passed in as a dictionary
    converters = {'foo': str, 'bar': int, 'baz': float}
    conv = convert(table, converters, errorvalue='error')
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2.0],
                   ['B', 2, 3.4],
                   ['B', 3, 7.8, True], # N.B., long rows are preserved
                   ['D', 'error', 9.0],
                   ['E', 'error']] # N.B., short rows are preserved
    iassertequal(expectation, conv) 
    
    # test the style where the converters functions are added one at a time
    conv = convert(table, errorvalue='err')
    conv['foo'] = str
    conv['bar'] = int
    conv['baz'] = float 
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2.0],
                   ['B', 2, 3.4],
                   ['B', 3, 7.8, True],
                   ['D', 'err', 9.0],
                   ['E', 'err']]
    iassertequal(expectation, conv) 
    
    
def test_translate():
    """Test the translate function."""
    
    table = [['foo', 'bar'],
             ['M', 12],
             ['F', 34],
             ['-', 56]]
    
    trans = {'M': 'male', 'F': 'female'}
    result = translate(table, 'foo', trans)
    expectation = [['foo', 'bar'],
                   ['male', 12],
                   ['female', 34],
                   ['-', 56]]
    iassertequal(expectation, result)

