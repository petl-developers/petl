"""
Tests for the petl.transform module.

"""


from collections import OrderedDict

from petl.testfun import iassertequal, assertequal
from petl import rename, fieldnames, cut, cat, convert, fieldconvert, extend, \
                rowslice, head, tail, sort, melt, recast, duplicates, conflicts, \
                mergereduce, select, complement, diff, capture, \
                split, expr, fieldmap, facet, rowreduce, aggregate, recordreduce, \
                rowmap, recordmap, rowmapmany, recordmapmany, setheader, pushheader, \
                skip, extendheader, unpack, join, leftjoin, rightjoin, outerjoin, \
                crossjoin, antijoin, rangeaggregate, rangecounts, rangefacet, \
                rangerowreduce, rangerecordreduce, selectre, rowselect, recordselect, \
                rowlenselect, strjoin, transpose, intersection, pivot, recorddiff, \
                recordcomplement


def test_rename():
    """Test the rename function."""

    table = (('foo', 'bar'),
             ('M', 12),
             ('F', 34),
             ('-', 56))
    
    result = rename(table, {'foo': 'foofoo', 'bar': 'barbar'})
    assert fieldnames(result) == ['foofoo', 'barbar']
    
    result = rename(table)
    result['foo'] = 'spong'
    assert fieldnames(result) == ['spong', 'bar']
    
    # TODO test cachetag


def test_cut():
    """Test the cut function."""
    
    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             (u'B', u'3', u'7.8', True),
             ('D', 'xyz', 9.0),
             ('E', None))

    cut1 = cut(table, 'foo')
    expectation = (('foo',),
                   ('A',),
                   ('B',),
                   (u'B',),
                   ('D',),
                   ('E',))
    iassertequal(expectation, cut1)
    
    cut2 = cut(table, 'foo', 'baz')
    expectation = (('foo', 'baz'),
                   ('A', 2),
                   ('B', '3.4'),
                   (u'B', u'7.8'),
                   ('D', 9.0),
                   ('E', None))
    iassertequal(expectation, cut2)
    
    cut3 = cut(table, 0, 2)
    expectation = (('foo', 'baz'),
                   ('A', 2),
                   ('B', '3.4'),
                   (u'B', u'7.8'),
                   ('D', 9.0),
                   ('E', None))
    iassertequal(expectation, cut3)
    
    cut4 = cut(table, 'bar', 0)
    expectation = (('bar', 'foo'),
                   (1, 'A'),
                   ('2', 'B'),
                   (u'3', u'B'),
                   ('xyz', 'D'),
                   (None, 'E'))
    iassertequal(expectation, cut4)
    

def test_cat():
    """Test the cat function."""
    
    table1 = (('foo', 'bar'),
              (1, 'A'),
              (2, 'B'))

    table2 = (('bar', 'baz'),
              ('C', True),
              ('D', False))
    
    cat1 = cat(table1, table2, missing=None)
    expectation = (('foo', 'bar', 'baz'),
                   (1, 'A', None),
                   (2, 'B', None),
                   (None, 'C', True),
                   (None, 'D', False))
    iassertequal(expectation, cat1)

    # how does cat cope with uneven rows?
    
    table3 = (('foo', 'bar', 'baz'),
              ('A', 1, 2),
              ('B', '2', '3.4'),
              (u'B', u'3', u'7.8', True),
              ('D', 'xyz', 9.0),
              ('E', None))

    cat3 = cat(table3, missing=None)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('B', '2', '3.4'),
                   (u'B', u'3', u'7.8'),
                   ('D', 'xyz', 9.0),
                   ('E', None, None))
    iassertequal(expectation, cat3)
    
    # cat more than two tables?
    cat4 = cat(table1, table2, table3)
    expectation = (('foo', 'bar', 'baz'),
                   (1, 'A', None),
                   (2, 'B', None),
                   (None, 'C', True),
                   (None, 'D', False),
                   ('A', 1, 2),
                   ('B', '2', '3.4'),
                   (u'B', u'3', u'7.8'),
                   ('D', 'xyz', 9.0),
                   ('E', None, None))
    iassertequal(expectation, cat4)
    
    # TODO test cachetag


def test_convert():
    
    table1 = (('foo', 'bar', 'baz'),
              ('A', 1, 2),
              ('B', '2', '3.4'),
              (u'B', u'3', u'7.8', True),
              ('D', 'xyz', 9.0),
              ('E', None))
    
    # test the simplest style - single field, lambda function
    table2 = convert(table1, 'foo', lambda s: s.lower())
    expect2 = (('foo', 'bar', 'baz'),
               ('a', 1, 2),
               ('b', '2', '3.4'),
               (u'b', u'3', u'7.8', True),
               ('d', 'xyz', 9.0),
               ('e', None))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2)
    
    # test single field with method call
    table3 = convert(table1, 'foo', 'lower')
    expect3 = expect2
    iassertequal(expect3, table3)

    # test single field with method call with arguments
    table4 = convert(table1, 'foo', 'replace', 'B', 'BB')
    expect4 = (('foo', 'bar', 'baz'),
               ('A', 1, 2),
               ('BB', '2', '3.4'),
               (u'BB', u'3', u'7.8', True),
               ('D', 'xyz', 9.0),
               ('E', None))
    iassertequal(expect4, table4)
    
    # test multiple fields with the same conversion
    table5 = convert(table1, ('bar', 'baz'), str)
    expect5 = (('foo', 'bar', 'baz'),
               ('A', '1', '2'),
               ('B', '2', '3.4'),
               (u'B', u'3', u'7.8', True),
               ('D', 'xyz', '9.0'),
               ('E', 'None'))
    iassertequal(expect5, table5)
    
    # test convert with dictionary
    table6 = convert(table1, 'foo', {'A': 'Z', 'B': 'Y'})
    expect6 = (('foo', 'bar', 'baz'),
               ('Z', 1, 2),
               ('Y', '2', '3.4'),
               (u'Y', u'3', u'7.8', True),
               ('D', 'xyz', 9.0),
               ('E', None))
    iassertequal(expect6, table6)


def test_fieldconvert():

    table1 = (('foo', 'bar', 'baz'),
              ('A', 1, 2),
              ('B', '2', '3.4'),
              (u'B', u'3', u'7.8', True),
              ('D', 'xyz', 9.0),
              ('E', None))
    
    # test the style where the converters functions are passed in as a dictionary
    converters = {'foo': str, 'bar': int, 'baz': float}
    table5 = fieldconvert(table1, converters, errorvalue='error')
    expect5 = (('foo', 'bar', 'baz'),
               ('A', 1, 2.0),
               ('B', 2, 3.4),
               ('B', 3, 7.8, True), # N.B., long rows are preserved
               ('D', 'error', 9.0),
               ('E', 'error')) # N.B., short rows are preserved
    iassertequal(expect5, table5) 
    
    # test the style where the converters functions are added one at a time
    table6 = fieldconvert(table1, errorvalue='err')
    table6['foo'] = str
    table6['bar'] = int
    table6['baz'] = float 
    expect6 = (('foo', 'bar', 'baz'),
               ('A', 1, 2.0),
               ('B', 2, 3.4),
               ('B', 3, 7.8, True),
               ('D', 'err', 9.0),
               ('E', 'err'))
    iassertequal(expect6, table6) 
    
    # test some different converters
    table7 = fieldconvert(table1)
    table7['foo'] = 'replace', 'B', 'BB'
    expect7 = (('foo', 'bar', 'baz'),
               ('A', 1, 2),
               ('BB', '2', '3.4'),
               (u'BB', u'3', u'7.8', True),
               ('D', 'xyz', 9.0),
               ('E', None))
    iassertequal(expect7, table7) 
    
    
def test_translate():
    """Test the translate function."""
    
    table = (('foo', 'bar'),
             ('M', 12),
             ('F', 34),
             ('-', 56))
    
    trans = {'M': 'male', 'F': 'female'}
    result = convert(table, 'foo', trans)
    expectation = (('foo', 'bar'),
                   ('male', 12),
                   ('female', 34),
                   ('-', 56))
    iassertequal(expectation, result)


def test_extend():
    """Test the extend function."""

    table = (('foo', 'bar'),
             ('M', 12),
             ('F', 34),
             ('-', 56))
    
    result = extend(table, 'baz', 42)
    expectation = (('foo', 'bar', 'baz'),
                   ('M', 12, 42),
                   ('F', 34, 42),
                   ('-', 56, 42))
    iassertequal(expectation, result)

    result = extend(table, 'baz', lambda rec: rec['bar'] * 2)
    expectation = (('foo', 'bar', 'baz'),
                   ('M', 12, 24),
                   ('F', 34, 68),
                   ('-', 56, 112))
    iassertequal(expectation, result)

    result = extend(table, 'baz', expr('{bar} * 2'))
    expectation = (('foo', 'bar', 'baz'),
                   ('M', 12, 24),
                   ('F', 34, 68),
                   ('-', 56, 112))
    iassertequal(expectation, result)


def test_rowslice():
    """Test the rowslice function."""
    
    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             (u'B', u'3', u'7.8', True),
             ('D', 'xyz', 9.0),
             ('E', None))

    result = rowslice(table, 0, 2)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('B', '2', '3.4'))
    iassertequal(expectation, result)

    result = rowslice(table, 1, 2)
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'))
    iassertequal(expectation, result)

    result = rowslice(table, 1, 5, 2)
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('D', 'xyz', 9.0))
    iassertequal(expectation, result)



def test_head():
    """Test the head function."""
    
    table1 = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 5),
              ('d', 7),
              ('f', 42),
              ('f', 3),
              ('h', 90),
              ('k', 12),
              ('l', 77),
              ('q', 2))
    
    table2 = head(table1, 4)
    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 5),
              ('d', 7))
    iassertequal(expect, table2)


def test_tail():
    """Test the tail function."""
    
    table1 = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 5),
              ('d', 7),
              ('f', 42),
              ('f', 3),
              ('h', 90),
              ('k', 12),
              ('l', 77),
              ('q', 2))
    
    table2 = tail(table1, 4)
    expect = (('foo', 'bar'),
              ('h', 90),
              ('k', 12),
              ('l', 77),
              ('q', 2))
    iassertequal(expect, table2)
    
    
def test_sort_1():
    
    table = (('foo', 'bar'),
            ('C', '2'),
            ('A', '9'),
            ('A', '6'),
            ('F', '1'),
            ('D', '10'))
    
    result = sort(table, 'foo')
    expectation = (('foo', 'bar'),
                   ('A', '9'),
                   ('A', '6'),
                   ('C', '2'),
                   ('D', '10'),
                   ('F', '1'))
    iassertequal(expectation, result)
    
    
def test_sort_2():
    
    table = (('foo', 'bar'),
            ('C', '2'),
            ('A', '9'),
            ('A', '6'),
            ('F', '1'),
            ('D', '10'))
    
    result = sort(table, key=('foo', 'bar'))
    expectation = (('foo', 'bar'),
                   ('A', '6'),
                   ('A', '9'),
                   ('C', '2'),
                   ('D', '10'),
                   ('F', '1'))
    iassertequal(expectation, result)
    
    result = sort(table) # default is lexical sort
    expectation = (('foo', 'bar'),
                   ('A', '6'),
                   ('A', '9'),
                   ('C', '2'),
                   ('D', '10'),
                   ('F', '1'))
    iassertequal(expectation, result)
    
    
def test_sort_3():
    
    table = (('foo', 'bar'),
            ('C', '2'),
            ('A', '9'),
            ('A', '6'),
            ('F', '1'),
            ('D', '10'))
    
    result = sort(table, 'bar')
    expectation = (('foo', 'bar'),
                   ('F', '1'),
                   ('D', '10'),
                   ('C', '2'),
                   ('A', '6'),
                   ('A', '9'))
    iassertequal(expectation, result)
    
    
def test_sort_4():
    
    table = (('foo', 'bar'),
            ('C', 2),
            ('A', 9),
            ('A', 6),
            ('F', 1),
            ('D', 10))
    
    result = sort(table, 'bar')
    expectation = (('foo', 'bar'),
                   ('F', 1),
                   ('C', 2),
                   ('A', 6),
                   ('A', 9),
                   ('D', 10))
    iassertequal(expectation, result)
    
    
def test_sort_5():
    
    table = (('foo', 'bar'),
            (2.3, 2),
            (1.2, 9),
            (2.3, 6),
            (3.2, 1),
            (1.2, 10))
    
    expectation = (('foo', 'bar'),
                   (1.2, 9),
                   (1.2, 10),
                   (2.3, 2),
                   (2.3, 6),
                   (3.2, 1))

    # can use either field names or indices (from 1) to specify sort key
    result = sort(table, key=('foo', 'bar'))
    iassertequal(expectation, result)
    result = sort(table, key=(0, 1))
    iassertequal(expectation, result)
    result = sort(table, key=('foo', 1))
    iassertequal(expectation, result)
    result = sort(table, key=(0, 'bar'))
    iassertequal(expectation, result)
    
    
def test_sort_6():
    
    table = (('foo', 'bar'),
            (2.3, 2),
            (1.2, 9),
            (2.3, 6),
            (3.2, 1),
            (1.2, 10))
    
    expectation = (('foo', 'bar'),
                   (3.2, 1),
                   (2.3, 6),
                   (2.3, 2),
                   (1.2, 10),
                   (1.2, 9))

    result = sort(table, key=('foo', 'bar'), reverse=True)
    iassertequal(expectation, result)
    

def test_sort_merge():
    
    table = (('foo', 'bar'),
             ('C', 2),
             ('A', 9),
             ('A', 6),
             ('F', 1),
             ('D', 10))

    # test sort forwards
    expectation = (('foo', 'bar'),
                   ('F', 1),
                   ('C', 2),
                   ('A', 6),
                   ('A', 9),
                   ('D', 10))
    result = sort(table, 'bar')
    iassertequal(expectation, result)
    result = sort(table, 'bar', buffersize=2)
#    print list(result)
    iassertequal(expectation, result)
        
    # sort in reverse
    expectation = (('foo', 'bar'),
                   ('D', 10),
                   ('A', 9),
                   ('A', 6),
                   ('C', 2),
                   ('F', 1))
    
    result = sort(table, 'bar', reverse=True)
    iassertequal(expectation, result)
    result = sort(table, 'bar', reverse=True, buffersize=2)
    iassertequal(expectation, result)

    # no key
    expectation = (('foo', 'bar'),
                   ('F', 1),
                   ('D', 10),
                   ('C', 2),
                   ('A', 9),
                   ('A', 6))
    result = sort(table, reverse=True)
    iassertequal(expectation, result)
    result = sort(table, reverse=True, buffersize=2)
    iassertequal(expectation, result)
    
    
def test_melt_1():
    
    table = (('id', 'gender', 'age'),
             (1, 'F', 12),
             (2, 'M', 17),
             (3, 'M', 16))
    
    expectation = (('id', 'variable', 'value'),
                   (1, 'gender', 'F'),
                   (1, 'age', 12),
                   (2, 'gender', 'M'),
                   (2, 'age', 17),
                   (3, 'gender', 'M'),
                   (3, 'age', 16))
    
    result = melt(table, key='id')
    iassertequal(expectation, result)

    result = melt(table, key='id', variablefield='variable', valuefield='value')
    iassertequal(expectation, result)


def test_melt_2():
    
    table = (('id', 'time', 'height', 'weight'),
             (1, 11, 66.4, 12.2),
             (2, 16, 53.2, 17.3),
             (3, 12, 34.5, 9.4))
    
    expectation = (('id', 'time', 'variable', 'value'),
                   (1, 11, 'height', 66.4),
                   (1, 11, 'weight', 12.2),
                   (2, 16, 'height', 53.2),
                   (2, 16, 'weight', 17.3),
                   (3, 12, 'height', 34.5),
                   (3, 12, 'weight', 9.4))
    result = melt(table, key=('id', 'time'))
    iassertequal(expectation, result)

    expectation = (('id', 'time', 'variable', 'value'),
                   (1, 11, 'height', 66.4),
                   (2, 16, 'height', 53.2),
                   (3, 12, 'height', 34.5))
    result = melt(table, key=('id', 'time'), variables='height')
    iassertequal(expectation, result)
    

def test_recast_1():
    
    table = (('id', 'variable', 'value'),
             (3, 'age', 16),
             (1, 'gender', 'F'),
             (2, 'gender', 'M'),
             (2, 'age', 17),
             (1, 'age', 12),
             (3, 'gender', 'M'))
    
    expectation = (('id', 'age', 'gender'),
                   (1, 12, 'F'),
                   (2, 17, 'M'),
                   (3, 16, 'M'))
    
    result = recast(table) # by default lift 'variable' field, hold everything else
    iassertequal(expectation, result)

    result = recast(table, variablefield='variable')
    iassertequal(expectation, result)

    result = recast(table, key='id', variablefield='variable')
    iassertequal(expectation, result)

    result = recast(table, key='id', variablefield='variable', valuefield='value')
    iassertequal(expectation, result)


def test_recast_2():
    
    table = (('id', 'variable', 'value'),
             (3, 'age', 16),
             (1, 'gender', 'F'),
             (2, 'gender', 'M'),
             (2, 'age', 17),
             (1, 'age', 12),
             (3, 'gender', 'M'))
    
    expectation = (('id', 'gender'),
                   (1, 'F'),
                   (2, 'M'),
                   (3, 'M'))
    
    # can manually pick which variables you want to recast as fields
    # TODO this is awkward
    result = recast(table, key='id', variablefield={'variable':['gender']})
    iassertequal(expectation, result)


def test_recast_3():
    
    table = (('id', 'time', 'variable', 'value'),
             (1, 11, 'weight', 66.4),
             (1, 14, 'weight', 55.2),
             (2, 12, 'weight', 53.2),
             (2, 16, 'weight', 43.3),
             (3, 12, 'weight', 34.5),
             (3, 17, 'weight', 49.4))
    
    expectation = (('id', 'time', 'weight'),
                   (1, 11, 66.4),
                   (1, 14, 55.2),
                   (2, 12, 53.2),
                   (2, 16, 43.3),
                   (3, 12, 34.5),
                   (3, 17, 49.4))
    result = recast(table)
    iassertequal(expectation, result)

    # in the absence of an aggregation function, list all values
    expectation = (('id', 'weight'),
                   (1, [66.4, 55.2]),
                   (2, [53.2, 43.3]),
                   (3, [34.5, 49.4]))
    result = recast(table, key='id')
    iassertequal(expectation, result)

    # max aggregation
    expectation = (('id', 'weight'),
                   (1, 66.4),
                   (2, 53.2),
                   (3, 49.4))
    result = recast(table, key='id', reducers={'weight': max})
    iassertequal(expectation, result)

    # min aggregation
    expectation = (('id', 'weight'),
                   (1, 55.2),
                   (2, 43.3),
                   (3, 34.5))
    result = recast(table, key='id', reducers={'weight': min})
    iassertequal(expectation, result)

    # mean aggregation
    expectation = (('id', 'weight'),
                   (1, 60.80),
                   (2, 48.25),
                   (3, 41.95))
    def mean(values):
        return float(sum(values)) / len(values)
    def meanf(precision):
        def f(values):
            v = mean(values)
            v = round(v, precision)
            return v
        return f
    result = recast(table, key='id', reducers={'weight': meanf(precision=2)})
    iassertequal(expectation, result)

    
def test_recast4():
    
    # deal with missing data
    table = (('id', 'variable', 'value'),
             (1, 'gender', 'F'),
             (2, 'age', 17),
             (1, 'age', 12),
             (3, 'gender', 'M'))
    result = recast(table, key='id')
    expect = (('id', 'age', 'gender'),
              (1, 12, 'F'),
              (2, 17, None),
              (3, None, 'M'))
    iassertequal(expect, result)


def test_duplicates():
    
    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3))

    result = duplicates(table, 'foo')
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', u'3', u'7.8', True),
                   ('B', '2', 42),
                   ('D', 'xyz', 9.0),
                   ('D', 4, 12.3))
    iassertequal(expectation, result)
    
    # test with compound key
    result = duplicates(table, key=('foo', 'bar'))
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', '2', 42))
    iassertequal(expectation, result)
    

def test_conflicts():
    
    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', None),
             ('D', 'xyz', 9.4),
             ('B', None, u'7.8', True),
             ('E', None),
             ('D', 'xyz', 12.3),
             ('A', 2, None))

    result = conflicts(table, 'foo', missing=None)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None),
                   ('D', 'xyz', 9.4),
                   ('D', 'xyz', 12.3))
    iassertequal(expectation, result)
    
    
def test_mergereduce():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', None),
             ('D', 'xyz', 9.4),
             ('B', None, u'7.8', True),
             ('E', None),
             ('D', 'xyz', 12.3),
             ('A', 2, None))

    # value overrides missing; last value wins
    result = mergereduce(table, 'foo', missing=None)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', (1, 2), 2),
                   ('B', '2', u'7.8', True),
                   ('D', 'xyz', (9.4, 12.3)),
                   ('E', None))
    iassertequal(expectation, result)
    
    
def test_complement_1():

    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 7))
    
    table2 = (('foo', 'bar'),
              ('A', 9),
              ('B', 2),
              ('B', 3))
    
    expectation = (('foo', 'bar'),
                   ('A', 1),
                   ('C', 7))
    
    result = complement(table1, table2)
    iassertequal(expectation, result)
    
    
def test_complement_2():

    tablea = (('foo', 'bar', 'baz'),
              ('A', 1, True),
              ('C', 7, False),
              ('B', 2, False),
              ('C', 9, True))
    
    tableb = (('x', 'y', 'z'),
              ('B', 2, False),
              ('A', 9, False),
              ('B', 3, True),
              ('C', 9, True))
    
    aminusb = (('foo', 'bar', 'baz'),
               ('A', 1, True),
               ('C', 7, False))
    
    result = complement(tablea, tableb)
    iassertequal(aminusb, result)
    
    bminusa = (('x', 'y', 'z'),
               ('A', 9, False),
               ('B', 3, True))
    
    result = complement(tableb, tablea)
    iassertequal(bminusa, result)
    

def test_diff():

    tablea = (('foo', 'bar', 'baz'),
              ('A', 1, True),
              ('C', 7, False),
              ('B', 2, False),
              ('C', 9, True))
    
    tableb = (('x', 'y', 'z'),
              ('B', 2, False),
              ('A', 9, False),
              ('B', 3, True),
              ('C', 9, True))
    
    aminusb = (('foo', 'bar', 'baz'),
               ('A', 1, True),
               ('C', 7, False))
    
    bminusa = (('x', 'y', 'z'),
               ('A', 9, False),
               ('B', 3, True))
    
    added, subtracted = diff(tablea, tableb)
    iassertequal(bminusa, added)
    iassertequal(aminusb, subtracted)
    

def test_recordcomplement_1():

    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 7))
    
    table2 = (('bar', 'foo'),
              (9, 'A'),
              (2, 'B'),
              (3, 'B'))
    
    expectation = (('foo', 'bar'),
                   ('A', 1),
                   ('C', 7))
    
    result = recordcomplement(table1, table2)
    iassertequal(expectation, result)
    
    
def test_recordcomplement_2():

    tablea = (('foo', 'bar', 'baz'),
              ('A', 1, True),
              ('C', 7, False),
              ('B', 2, False),
              ('C', 9, True))
    
    tableb = (('bar', 'foo', 'baz'),
              (2, 'B', False),
              (9, 'A', False),
              (3, 'B', True),
              (9, 'C', True))
    
    aminusb = (('foo', 'bar', 'baz'),
               ('A', 1, True),
               ('C', 7, False))
    
    result = recordcomplement(tablea, tableb)
    iassertequal(aminusb, result)
    
    bminusa = (('bar', 'foo', 'baz'),
               (3, 'B', True),
               (9, 'A', False))
    
    result = recordcomplement(tableb, tablea)
    iassertequal(bminusa, result)
    

def test_recorddiff():

    tablea = (('foo', 'bar', 'baz'),
              ('A', 1, True),
              ('C', 7, False),
              ('B', 2, False),
              ('C', 9, True))
    
    tableb = (('bar', 'foo', 'baz'),
              (2, 'B', False),
              (9, 'A', False),
              (3, 'B', True),
              (9, 'C', True))
    
    aminusb = (('foo', 'bar', 'baz'),
               ('A', 1, True),
               ('C', 7, False))
    
    bminusa = (('bar', 'foo', 'baz'),
               (3, 'B', True),
               (9, 'A', False))
    
    added, subtracted = recorddiff(tablea, tableb)
    iassertequal(aminusb, subtracted)
    iassertequal(bminusa, added)
    

def test_capture():
    
    table = (('id', 'variable', 'value'),
            ('1', 'A1', '12'),
            ('2', 'A2', '15'),
            ('3', 'B1', '18'),
            ('4', 'C12', '19'))
    
    expectation = (('id', 'value', 'treat', 'time'),
                   ('1', '12', 'A', '1'),  
                   ('2', '15', 'A', '2'),
                   ('3', '18', 'B', '1'),
                   ('4', '19', 'C', '12'))
    
    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'))
    iassertequal(expectation, result)
    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'),
                           include_original=False)
    iassertequal(expectation, result)

    # what about including the original field?
    expectation = (('id', 'variable', 'value', 'treat', 'time'),
                   ('1', 'A1', '12', 'A', '1'),  
                   ('2', 'A2', '15', 'A', '2'),
                   ('3', 'B1', '18', 'B', '1'),
                   ('4', 'C12', '19', 'C', '12'))
    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'),
                           include_original=True)
    iassertequal(expectation, result)
    
    # what about if number of captured groups is different from new fields?
    expectation = (('id', 'value'),
                   ('1', '12', 'A', '1'),  
                   ('2', '15', 'A', '2'),
                   ('3', '18', 'B', '1'),
                   ('4', '19', 'C', '12'))
    result = capture(table, 'variable', '(\\w)(\\d+)')
    iassertequal(expectation, result)
    
    
    
def test_split():
    
    table = (('id', 'variable', 'value'),
             ('1', 'parad1', '12'),
             ('2', 'parad2', '15'),
             ('3', 'tempd1', '18'),
             ('4', 'tempd2', '19'))
    
    expectation = (('id', 'value', 'variable', 'day'),
                   ('1', '12', 'para', '1'),  
                   ('2', '15', 'para', '2'),
                   ('3', '18', 'temp', '1'),
                   ('4', '19', 'temp', '2'))
    
    result = split(table, 'variable', 'd', ('variable', 'day'))
    iassertequal(expectation, result)
    iassertequal(expectation, result)
    # proper regex
    result = split(table, 'variable', '[Dd]', ('variable', 'day'))
    iassertequal(expectation, result)

    expectation = (('id', 'variable', 'value', 'variable', 'day'),
                   ('1', 'parad1', '12', 'para', '1'),  
                   ('2', 'parad2', '15', 'para', '2'),
                   ('3', 'tempd1', '18', 'temp', '1'),
                   ('4', 'tempd2', '19', 'temp', '2'))
    
    result = split(table, 'variable', 'd', ('variable', 'day'), include_original=True)
    iassertequal(expectation, result)
    
    # what about if no new fields?
    expectation = (('id', 'value'),
                   ('1', '12', 'para', '1'),  
                   ('2', '15', 'para', '2'),
                   ('3', '18', 'temp', '1'),
                   ('4', '19', 'temp', '2'))
    result = split(table, 'variable', 'd')
    iassertequal(expectation, result)

    

def test_melt_and_capture():
    
    table = (('id', 'parad0', 'parad1', 'parad2'),
             ('1', '12', '34', '56'),
             ('2', '23', '45', '67'))
    
    expectation = (('id', 'parasitaemia', 'day'),
                   ('1', '12', '0'),
                   ('1', '34', '1'),
                   ('1', '56', '2'),
                   ('2', '23', '0'),
                   ('2', '45', '1'),
                   ('2', '67', '2'))
    
    step1 = melt(table, key='id', valuefield='parasitaemia')
    step2 = capture(step1, 'variable', 'parad(\\d+)', ('day',))
    iassertequal(expectation, step2)


def test_melt_and_split():
    
    table = (('id', 'parad0', 'parad1', 'parad2', 'tempd0', 'tempd1', 'tempd2'),
            ('1', '12', '34', '56', '37.2', '37.4', '37.9'),
            ('2', '23', '45', '67', '37.1', '37.8', '36.9'))
    
    expectation = (('id', 'value', 'variable', 'day'),
                   ('1', '12', 'para', '0'),
                   ('1', '34', 'para', '1'),
                   ('1', '56', 'para', '2'),
                   ('1', '37.2', 'temp', '0'),
                   ('1', '37.4', 'temp', '1'),
                   ('1', '37.9', 'temp', '2'),
                   ('2', '23', 'para', '0'),
                   ('2', '45', 'para', '1'),
                   ('2', '67', 'para', '2'),
                   ('2', '37.1', 'temp', '0'),
                   ('2', '37.8', 'temp', '1'),
                   ('2', '36.9', 'temp', '2'))
    
    step1 = melt(table, key='id')
    step2 = split(step1, 'variable', 'd', ('variable', 'day'))
    iassertequal(expectation, step2)


def test_select():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = select(table, lambda rec: rec['foo'] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # check can iterate twice
 
    actual = select(table, lambda rec: rec['foo'] == 'a' and rec['bar'] > 3)
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3))
    iassertequal(expect, actual)

    actual = select(table, "{foo} == 'a'")
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    iassertequal(expect, actual)

    actual = select(table, "{foo} == 'a' and {bar} > 3")
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3))
    iassertequal(expect, actual)

    # check error handling on short rows
    actual = select(table, lambda rec: rec['baz'] > 88.1)
    expect = (('foo', 'bar', 'baz'),
              ('a', 2, 88.2),
              ('d', 7, 100.9))
    iassertequal(expect, actual)
    
    # check single field tests
    actual = select(table, 'foo', lambda v: v == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # check can iterate twice


def test_rowselect():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = rowselect(table, lambda row: row[0] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # check can iterate twice


def test_rowlenselect():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = rowlenselect(table, 3)
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2),
              ('b', 1, 23.3),
              ('c', 8, 42.0),
              ('d', 7, 100.9))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # check can iterate twice


def test_recordselect():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = recordselect(table, lambda rec: rec['foo'] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # check can iterate twice


def test_selectre():
    
    table = (('foo', 'bar', 'baz'),
             ('aa', 4, 9.3),
             ('aaa', 2, 88.2),
             ('b', 1, 23.3),
             ('ccc', 8, 42.0),
             ('bb', 7, 100.9),
             ('c', 2))
    actual = selectre(table, 'foo', '[ab]{2}')
    expect = (('foo', 'bar', 'baz'),
             ('aa', 4, 9.3),
             ('aaa', 2, 88.2),
             ('bb', 7, 100.9))
    iassertequal(expect, actual)
    iassertequal(expect, actual)

    
def test_fieldmap():
    
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, 'female', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33, 45.2),
             (5, '-', 25, 1.65, 51.9))
    
    mappings = OrderedDict()
    mappings['subject_id'] = 'id'
    mappings['gender'] = 'sex', {'male': 'M', 'female': 'F'}
    mappings['age_months'] = 'age', lambda v: v * 12
    mappings['bmi'] = lambda rec: rec['weight'] / rec['height']**2 
    actual = fieldmap(table, mappings)  
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16*12, 62.0/1.45**2),
              (2, 'F', 19*12, 55.4/1.34**2),
              (3, 'F', 17*12, 74.4/1.78**2),
              (4, 'M', 21*12, 45.2/1.33**2),
              (5, '-', 25*12, 51.9/1.65**2))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # can iteratate twice?
    
    # do it with suffix
    actual = fieldmap(table)
    actual['subject_id'] = 'id'
    actual['gender'] = 'sex', {'male': 'M', 'female': 'F'}
    actual['age_months'] = 'age', lambda v: v * 12
    actual['bmi'] = '{weight} / {height}**2'
    iassertequal(expect, actual)
    
    # test short rows
    table2 = (('id', 'sex', 'age', 'height', 'weight'),
              (1, 'male', 16, 1.45, 62.0),
              (2, 'female', 19, 1.34, 55.4),
              (3, 'female', 17, 1.78, 74.4),
              (4, 'male', 21, 1.33, 45.2),
              (5, '-', 25, 1.65))
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16*12, 62.0/1.45**2),
              (2, 'F', 19*12, 55.4/1.34**2),
              (3, 'F', 17*12, 74.4/1.78**2),
              (4, 'M', 21*12, 45.2/1.33**2),
              (5, '-', 25*12, None))
    actual = fieldmap(table2, mappings)
    iassertequal(expect, actual)


def test_facet():

    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))
    fct = facet(table, 'foo')
    assert set(fct.keys()) == {'a', 'b', 'c', 'd'}
    expect_fcta = (('foo', 'bar', 'baz'),
                   ('a', 4, 9.3),
                   ('a', 2, 88.2))
    iassertequal(fct['a'], expect_fcta)
    iassertequal(fct['a'], expect_fcta) # check can iterate twice
    expect_fctc = (('foo', 'bar', 'baz'),
                   ('c', 8, 42.0),
                   ('c', 2))
    iassertequal(fct['c'], expect_fctc)
    iassertequal(fct['c'], expect_fctc) # check can iterate twice
    

def test_rangefacet():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4),
              ('d', 3))
    rf = rangefacet(table1, 'bar', 2)
    assertequal([(1, 3), (3, 5), (5, 7), (7, 9)], rf.keys())
    expect_13 = (('foo', 'bar'),
                 ('b', 2),
                 ('b', 1)) # N.B., it get's sorted
    iassertequal(expect_13, rf[(1, 3)])
    iassertequal(expect_13, rf[(1, 3)])
    expect_79 = (('foo', 'bar'),
                 ('a', 7),
                 ('b', 9))
    iassertequal(expect_79, rf[(7, 9)])


def test_rowreduce():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def sumbar(key, rows):
        return [key, sum([row[1] for row in rows])]
        
    table2 = rowreduce(table1, key='foo', reducer=sumbar, fields=['foo', 'barsum'])
    expect2 = (('foo', 'barsum'),
               ('a', 10),
               ('b', 12),
               ('c', 4))
    iassertequal(expect2, table2)
    

def test_recordreduce():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def sumbar(key, records):
        return [key, sum([rec['bar'] for rec in records])]
        
    table2 = recordreduce(table1, key='foo', reducer=sumbar, fields=['foo', 'barsum'])
    expect2 = (('foo', 'barsum'),
               ('a', 10),
               ('b', 12),
               ('c', 4))
    iassertequal(expect2, table2)
    

def test_rangerowreduce():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def redu(minv, maxv, rows):
        return [minv, maxv, ''.join([row[0] for row in rows])]
        
    table2 = rangerowreduce(table1, 'bar', 2, reducer=redu, 
                            fields=['minbar', 'maxbar', 'foos'])
    expect2 = (('minbar', 'maxbar', 'foos'),
               (1, 3, 'bb'),
               (3, 5, 'ac'),
               (5, 7, ''),
               (7, 9, 'a'),
               (9, 11, 'b'))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2)
    

def test_rangerecordreduce():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def redu(minv, maxv, recs):
        return [minv, maxv, ''.join([rec['foo'] for rec in recs])]
        
    table2 = rangerecordreduce(table1, 'bar', 2, reducer=redu, 
                               fields=['minbar', 'maxbar', 'foos'])
    expect2 = (('minbar', 'maxbar', 'foos'),
               (1, 3, 'bb'),
               (3, 5, 'ac'),
               (5, 7, ''),
               (7, 9, 'a'),
               (9, 11, 'b'))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2)
    

def test_aggregate():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4),
              ('d', 3),
              ('d',),
              ('e',))
    
    aggregators = OrderedDict()
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum
    aggregators['listbar'] = 'bar', list
    aggregators['bars'] = 'bar', strjoin(', ')

    table2 = aggregate(table1, 'foo', aggregators)
    expect2 = (('foo', 'minbar', 'maxbar', 'sumbar', 'listbar', 'bars'),
               ('a', 3, 7, 10, [3, 7], '3, 7'),
               ('b', 1, 9, 12, [2, 1, 9], '2, 1, 9'),
               ('c', 4, 4, 4, [4], '4'),
               ('d', 3, 3, 3, [3], '3'),
               ('e', None, None, 0, [], ''))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2) # check can iterate twice
    
    table3 = aggregate(table1, 'foo')
    table3['minbar'] = 'bar', min
    table3['maxbar'] = 'bar', max
    table3['sumbar'] = 'bar', sum
    table3['listbar'] = 'bar' # default aggregation is list
    table3['bars'] = 'bar', strjoin(', ')
    iassertequal(expect2, table3)
    
    
def test_rangeaggregate():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4),
              ('d', 3))

    table2 = rangeaggregate(table1, 'bar', width=2)
    table2['foocount'] = 'foo', len
    table2['foolist'] = 'foo' # default is list
    expect2 = (('bar', 'foocount', 'foolist'),
               ((1, 3), 2, ['b', 'b']),
               ((3, 5), 3, ['a', 'd', 'c']),
               ((5, 7), 0, []),
               ((7, 9), 1, ['a']),
               ((9, 11), 1, ['b']))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2)

    table3 = rangeaggregate(table1, 'bar', width=2, minv=0)
    table3['foocount'] = 'foo', len
    expect3 = (('bar', 'foocount'),
               ((0, 2), 1),
               ((2, 4), 3),
               ((4, 6), 1),
               ((6, 8), 1),
               ((8, 10), 1))
    iassertequal(expect3, table3)

    table4 = rangeaggregate(table1, 'bar', width=2, minv=2, maxv=6)
    table4['foocount'] = 'foo', len
    expect4 = (('bar', 'foocount'),
               ((2, 4), 3),
               ((4, 6), 1))
    iassertequal(expect4, table4)

    # last bin is open if maxv is specified
    table5 = rangeaggregate(table1, 'bar', width=2, maxv=9)
    table5['foocount'] = 'foo', len
    expect5 = (('bar', 'foocount'),
               ((1, 3), 2),
               ((3, 5), 3),
               ((5, 7), 0),
               ((7, 9), 2))
    iassertequal(expect5, table5)


def test_rangecounts():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4),
              ('d', 3))

    table2 = rangecounts(table1, 'bar', width=2)
    expect2 = (('range', 'count'),
               ((1, 3), 2),
               ((3, 5), 3),
               ((5, 7), 0),
               ((7, 9), 1),
               ((9, 11), 1))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2)

    table3 = rangecounts(table1, 'bar', width=2, minv=0)
    expect3 = (('range', 'count'),
               ((0, 2), 1),
               ((2, 4), 3),
               ((4, 6), 1),
               ((6, 8), 1),
               ((8, 10), 1))
    iassertequal(expect3, table3)

    table4 = rangecounts(table1, 'bar', width=2, minv=2, maxv=6)
    expect4 = (('range', 'count'),
               ((2, 4), 3),
               ((4, 6), 1))
    iassertequal(expect4, table4)

    # N.B., last bin is open if maxv is specified
    table5 = rangecounts(table1, 'bar', width=2, maxv=9)
    expect5 = (('range', 'count'),
               ((1, 3), 2),
               ((3, 5), 3),
               ((5, 7), 0),
               ((7, 9), 2))
    iassertequal(expect5, table5)


def test_rowmap():
    
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, 'female', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33, 45.2),
             (5, '-', 25, 1.65, 51.9))
    
    def rowmapper(row):
        transmf = {'male': 'M', 'female': 'F'}
        return [row[0],
                transmf[row[1]] if row[1] in transmf else row[1],
                row[2] * 12,
                row[4] / row[3] ** 2]
    actual = rowmap(table, rowmapper, fields=['subject_id', 'gender', 'age_months', 'bmi'])  
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16*12, 62.0/1.45**2),
              (2, 'F', 19*12, 55.4/1.34**2),
              (3, 'F', 17*12, 74.4/1.78**2),
              (4, 'M', 21*12, 45.2/1.33**2),
              (5, '-', 25*12, 51.9/1.65**2))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # can iteratate twice?
        
    # test short rows
    table2 = (('id', 'sex', 'age', 'height', 'weight'),
              (1, 'male', 16, 1.45, 62.0),
              (2, 'female', 19, 1.34, 55.4),
              (3, 'female', 17, 1.78, 74.4),
              (4, 'male', 21, 1.33, 45.2),
              (5, '-', 25, 1.65))
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16*12, 62.0/1.45**2),
              (2, 'F', 19*12, 55.4/1.34**2),
              (3, 'F', 17*12, 74.4/1.78**2),
              (4, 'M', 21*12, 45.2/1.33**2))
    actual = rowmap(table2, rowmapper, fields=['subject_id', 'gender', 'age_months', 'bmi'])  
    iassertequal(expect, actual)


def test_recordmap():
    
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, 'female', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33, 45.2),
             (5, '-', 25, 1.65, 51.9))
    
    def recmapper(rec):
        transmf = {'male': 'M', 'female': 'F'}
        return [rec['id'],
                transmf[rec['sex']] if rec['sex'] in transmf else rec['sex'],
                rec['age'] * 12,
                rec['weight'] / rec['height'] ** 2]
    actual = recordmap(table, recmapper, fields=['subject_id', 'gender', 'age_months', 'bmi'])  
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16*12, 62.0/1.45**2),
              (2, 'F', 19*12, 55.4/1.34**2),
              (3, 'F', 17*12, 74.4/1.78**2),
              (4, 'M', 21*12, 45.2/1.33**2),
              (5, '-', 25*12, 51.9/1.65**2))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # can iteratate twice?
        
    # test short rows
    table2 = (('id', 'sex', 'age', 'height', 'weight'),
              (1, 'male', 16, 1.45, 62.0),
              (2, 'female', 19, 1.34, 55.4),
              (3, 'female', 17, 1.78, 74.4),
              (4, 'male', 21, 1.33, 45.2),
              (5, '-', 25, 1.65))
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16*12, 62.0/1.45**2),
              (2, 'F', 19*12, 55.4/1.34**2),
              (3, 'F', 17*12, 74.4/1.78**2),
              (4, 'M', 21*12, 45.2/1.33**2))
    actual = recordmap(table2, recmapper, fields=['subject_id', 'gender', 'age_months', 'bmi'])  
    iassertequal(expect, actual)


def test_rowmapmany():
    
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, '-', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33))
    
    def rowgenerator(row):
        transmf = {'male': 'M', 'female': 'F'}
        yield [row[0], 'gender', transmf[row[1]] if row[1] in transmf else row[1]]
        yield [row[0], 'age_months', row[2] * 12]
        yield [row[0], 'bmi', row[4] / row[3] ** 2]

    actual = rowmapmany(table, rowgenerator, fields=['subject_id', 'variable', 'value'])  
    expect = (('subject_id', 'variable', 'value'),
              (1, 'gender', 'M'),
              (1, 'age_months', 16*12),
              (1, 'bmi', 62.0/1.45**2),
              (2, 'gender', 'F'),
              (2, 'age_months', 19*12),
              (2, 'bmi', 55.4/1.34**2),
              (3, 'gender', '-'),
              (3, 'age_months', 17*12),
              (3, 'bmi', 74.4/1.78**2),
              (4, 'gender', 'M'),
              (4, 'age_months', 21*12))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # can iteratate twice?
        

def test_recordmapmany():
    
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, '-', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33))
    
    def rowgenerator(rec):
        transmf = {'male': 'M', 'female': 'F'}
        yield [rec['id'], 'gender', transmf[rec['sex']] if rec['sex'] in transmf else rec['sex']]
        yield [rec['id'], 'age_months', rec['age'] * 12]
        yield [rec['id'], 'bmi', rec['weight'] / rec['height'] ** 2]

    actual = recordmapmany(table, rowgenerator, fields=['subject_id', 'variable', 'value'])  
    expect = (('subject_id', 'variable', 'value'),
              (1, 'gender', 'M'),
              (1, 'age_months', 16*12),
              (1, 'bmi', 62.0/1.45**2),
              (2, 'gender', 'F'),
              (2, 'age_months', 19*12),
              (2, 'bmi', 55.4/1.34**2),
              (3, 'gender', '-'),
              (3, 'age_months', 17*12),
              (3, 'bmi', 74.4/1.78**2),
              (4, 'gender', 'M'),
              (4, 'age_months', 21*12))
    iassertequal(expect, actual)
    iassertequal(expect, actual) # can iteratate twice?
        

def test_setfields():
    
    table1 = (('foo', 'bar'),
              ('a', 1),
              ('b', 2))
    table2 = setheader(table1, ['foofoo', 'barbar'])
    expect2 = (('foofoo', 'barbar'),
               ('a', 1),
               ('b', 2))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2) # can iterate twice?
    
    
def test_extendfields():
    
    table1 = (('foo',),
              ('a', 1, True),
              ('b', 2, False))
    table2 = extendheader(table1, ['bar', 'baz'])
    expect2 = (('foo', 'bar', 'baz'),
               ('a', 1, True),
               ('b', 2, False))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2) # can iterate twice?
    
    
def test_pushfields():
    
    table1 = (('a', 1),
              ('b', 2))
    table2 = pushheader(table1, ['foo', 'bar'])
    expect2 = (('foo', 'bar'),
               ('a', 1),
               ('b', 2))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2) # can iterate twice?
    

def test_skip():
    
    table1 = (('#aaa', 'bbb', 'ccc'),
              ('#mmm'),
              ('foo', 'bar'),
              ('a', 1),
              ('b', 2))
    table2 = skip(table1, 2)
    expect2 = (('foo', 'bar'),
               ('a', 1),
               ('b', 2))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2) # can iterate twice?
    
    
def test_unpack():
    
    table1 = (('foo', 'bar'),
              (1, ['a', 'b']),
              (2, ['c', 'd']),
              (3, ['e', 'f']))
    table2 = unpack(table1, 'bar', ['baz', 'quux'])
    expect2 = (('foo', 'baz', 'quux'),
               (1, 'a', 'b'),
               (2, 'c', 'd'),
               (3, 'e', 'f'))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2) # check twice
    
    # check no new fields
    table3 = unpack(table1, 'bar')
    expect3 = (('foo',),
               (1, 'a', 'b'),
               (2, 'c', 'd'),
               (3, 'e', 'f'))
    iassertequal(expect3, table3)
    
    # check maxv
    table4 = unpack(table1, 'bar', ['baz'], maxunpack=1)
    expect4 = (('foo', 'baz'),
               (1, 'a'),
               (2, 'c'),
               (3, 'e'))
    iassertequal(expect4, table4)
    
    # check include original
    table5 = unpack(table1, 'bar', ['baz'], maxunpack=1, include_original=True)
    expect5 = (('foo', 'bar', 'baz'),
              (1, ['a', 'b'], 'a'),
              (2, ['c', 'd'], 'c'),
              (3, ['e', 'f'], 'e'))
    iassertequal(expect5, table5)
    
    
def test_join():
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    
    # normal inner join
    table3 = join(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice
    
    # natural join
    table4 = join(table1, table2)
    expect4 = expect3
    iassertequal(expect4, table4)
    iassertequal(expect4, table4) # check twice
    
    # multiple rows for each key
    table5 = (('id', 'colour'),
              (1, 'blue'),
              (1, 'red'),
              (2, 'purple'))
    table6 = (('id', 'shape'),
              (1, 'circle'),
              (1, 'square'),
              (2, 'ellipse'))
    table7 = join(table5, table6, key='id')
    expect7 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (1, 'blue', 'square'),
               (1, 'red', 'circle'),
               (1, 'red', 'square'),
               (2, 'purple', 'ellipse'))
    iassertequal(expect7, table7)
    
    
def test_join_compound_keys():
    
    # compound keys
    table8 = (('id', 'time', 'height'),
              (1, 1, 12.3),
              (1, 2, 34.5),
              (2, 1, 56.7))
    table9 = (('id', 'time', 'weight'),
              (1, 2, 4.5),
              (2, 1, 6.7),
              (2, 2, 8.9))
    table10 = join(table8, table9, key=['id', 'time'])
    expect10 = (('id', 'time', 'height', 'weight'),
                (1, 2, 34.5, 4.5),
                (2, 1, 56.7, 6.7))
    iassertequal(expect10, table10)

    # natural join on compound key
    table11 = join(table8, table9)
    expect11 = expect10
    iassertequal(expect11, table11)
    
    
def test_leftjoin():
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = leftjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice
    
    # natural join
    table4 = leftjoin(table1, table2)
    expect4 = expect3
    iassertequal(expect4, table4)
    
    
def test_leftjoin_2():
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = leftjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice
    
    # natural join
    table4 = leftjoin(table1, table2)
    expect4 = expect3
    iassertequal(expect4, table4)
    
    
def test_leftjoin_3():
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'triangle'))
    table3 = leftjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice
    
    # natural join
    table4 = leftjoin(table1, table2)
    expect4 = expect3
    iassertequal(expect4, table4)
    
    
def test_leftjoin_compound_keys():
    
    # compound keys
    table5 = (('id', 'time', 'height'),
              (1, 1, 12.3),
              (1, 2, 34.5),
              (2, 1, 56.7))
    table6 = (('id', 'time', 'weight', 'bp'),
              (1, 2, 4.5, 120),
              (2, 1, 6.7, 110),
              (2, 2, 8.9, 100))
    table7 = leftjoin(table5, table6, key=['id', 'time'])
    expect7 = (('id', 'time', 'height', 'weight', 'bp'),
                (1, 1, 12.3, None, None),
                (1, 2, 34.5, 4.5, 120),
                (2, 1, 56.7, 6.7, 110))
    iassertequal(expect7, table7)


def test_rightjoin():
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice
    
    # natural join
    table4 = rightjoin(table1, table2)
    expect4 = expect3
    iassertequal(expect4, table4)
    
    
def test_rightjoin_2():
    
    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = rightjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice
    
    # natural join
    table4 = rightjoin(table1, table2)
    expect4 = expect3
    iassertequal(expect4, table4)
    
    
def test_rightjoin_3():
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (4, 'orange'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (5, 'ellipse'),
              (7, 'pentagon'))
    table3 = rightjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (5, None, 'ellipse'),
               (7, None, 'pentagon'))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice
    
    # natural join
    table4 = rightjoin(table1, table2)
    expect4 = expect3
    iassertequal(expect4, table4)
    
    
def test_outerjoin():
    
    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, 'black', None),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, 'yellow', None),
               (7, 'white', None))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice

    # natural join
    table4 = outerjoin(table1, table2)
    expect4 = expect3
    iassertequal(expect4, table4)
    
    
def test_outerjoin_2():
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'pentagon'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'triangle'))
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'pentagon'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'triangle'))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice

    # natural join
    table4 = outerjoin(table1, table2)
    expect4 = expect3
    iassertequal(expect4, table4)
    
    
def test_outerjoin_fieldorder():

    table1 = (('colour', 'id'),
              ('blue', 1),
              ('red', 2),
              ('purple', 3))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('colour', 'id', 'shape'),
               ('blue', 1, 'circle'),
               ('red', 2, None),
               ('purple', 3, 'square'),
               (None, 4, 'ellipse'))
    iassertequal(expect3, table3)
    iassertequal(expect3, table3) # check twice
    
    
def test_crossjoin():
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = crossjoin(table1, table2)
    expect3 = (('id', 'colour', 'id', 'shape'),
               (1, 'blue', 1, 'circle'),
               (1, 'blue', 3, 'square'),
               (2, 'red', 1, 'circle'),
               (2, 'red', 3, 'square'))
    iassertequal(expect3, table3)
    
    
def test_antijoin():
    
    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = antijoin(table1, table2, key='id')
    expect3 = (('id', 'colour'),
               (0, 'black'),
               (2, 'red'),
               (4, 'yellow'),
               (5, 'white'))
    iassertequal(expect3, table3)

    table4 = antijoin(table1, table2) 
    expect4 = expect3
    iassertequal(expect4, table4)
    
    
def test_transpose():
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = transpose(table1)
    expect2 = (('id', 1, 2, 3, 5, 7),
               ('colour', 'blue', 'red', 'purple', 'yellow', 'orange'))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2)
    
    
def test_intersection_1():

    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 7))
    
    table2 = (('foo', 'bar'),
              ('A', 9),
              ('B', 2),
              ('B', 3))
    
    expectation = (('foo', 'bar'),
                   ('B', 2))
    
    result = intersection(table1, table2)
    iassertequal(expectation, result)
    
    
def test_intersection_2():

    table1 = (('foo', 'bar', 'baz'),
              ('A', 1, True),
              ('C', 7, False),
              ('B', 2, False),
              ('C', 9, True))
    
    table2 = (('x', 'y', 'z'),
              ('B', 2, False),
              ('A', 9, False),
              ('B', 3, True),
              ('C', 9, True))
    
    expect = (('foo', 'bar', 'baz'),
              ('B', 2, False),
              ('C', 9, True))
    
    table3 = intersection(table1, table2)
    iassertequal(expect, table3)
    

def test_pivot():
    
    table1 = (('region', 'gender', 'style', 'units'),
              ('east', 'boy', 'tee', 12),
              ('east', 'boy', 'golf', 14),
              ('east', 'boy', 'fancy', 7),
              ('east', 'girl', 'tee', 3),
              ('east', 'girl', 'golf', 8),
              ('east', 'girl', 'fancy', 18),
              ('west', 'boy', 'tee', 12),
              ('west', 'boy', 'golf', 15),
              ('west', 'boy', 'fancy', 8),
              ('west', 'girl', 'tee', 6),
              ('west', 'girl', 'golf', 16),
              ('west', 'girl', 'fancy', 1))
    
    table2 = pivot(table1, 'region', 'gender', 'units', sum)
    expect2 = (('region', 'boy', 'girl'),
               ('east', 33, 29),
               ('west', 35, 23))
    iassertequal(expect2, table2)
    iassertequal(expect2, table2)
    
    
    