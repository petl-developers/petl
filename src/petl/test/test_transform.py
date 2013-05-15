"""
Tests for the petl.transform module.

"""

from datetime import datetime
from nose.tools import eq_
import operator


from ..util import OrderedDict
from ..testutils import ieq
from petl import rename, fieldnames, cut, cat, convert, addfield, \
                rowslice, head, tail, sort, melt, recast, duplicates, \
                conflicts, mergeduplicates, select, complement, diff, capture, \
                split, expr, fieldmap, facet, rowreduce, aggregate, \
                rowmap, recordmap, rowmapmany, setheader, pushheader, \
                skip, extendheader, unpack, join, leftjoin, rightjoin, \
                outerjoin, crossjoin, antijoin, rangeaggregate, rangecounts, \
                rangefacet, rangerowreduce, selectre, rowselect, \
                rowlenselect, strjoin, transpose, intersection, pivot, \
                recorddiff, recordcomplement, cutout, skipcomments, \
                convertall, convertnumbers, hashjoin, hashleftjoin, \
                hashrightjoin, hashantijoin, hashcomplement, hashintersection, \
                flatten, unflatten, mergesort, annex, unpackdict, unique, \
                selectin, fold, addrownumbers, selectcontains, search, \
                addcolumn, lookupjoin, hashlookupjoin, filldown, fillright, \
                fillleft, multirangeaggregate, unjoin, coalesce 
from ..transform import Conflict


def test_rename():

    table = (('foo', 'bar'),
             ('M', 12),
             ('F', 34),
             ('-', 56))
    
    result = rename(table, 'foo', 'foofoo')
    assert fieldnames(result) == ['foofoo', 'bar']
    
    result = rename(table, {'foo': 'foofoo', 'bar': 'barbar'})
    assert fieldnames(result) == ['foofoo', 'barbar']
    
    result = rename(table)
    result['foo'] = 'spong'
    assert fieldnames(result) == ['spong', 'bar']
    

def test_rename_empty():
    table = (('foo', 'bar'),)
    expect = (('foofoo', 'bar'),)
    actual = rename(table, 'foo', 'foofoo')
    ieq(expect, actual)
    
    
def test_cut():
    
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
    ieq(expectation, cut1)
    
    cut2 = cut(table, 'foo', 'baz')
    expectation = (('foo', 'baz'),
                   ('A', 2),
                   ('B', '3.4'),
                   (u'B', u'7.8'),
                   ('D', 9.0),
                   ('E', None))
    ieq(expectation, cut2)
    
    cut3 = cut(table, 0, 2)
    expectation = (('foo', 'baz'),
                   ('A', 2),
                   ('B', '3.4'),
                   (u'B', u'7.8'),
                   ('D', 9.0),
                   ('E', None))
    ieq(expectation, cut3)
    
    cut4 = cut(table, 'bar', 0)
    expectation = (('bar', 'foo'),
                   (1, 'A'),
                   ('2', 'B'),
                   (u'3', u'B'),
                   ('xyz', 'D'),
                   (None, 'E'))
    ieq(expectation, cut4)

    cut5 = cut(table, ('foo', 'baz'))
    expectation = (('foo', 'baz'),
                   ('A', 2),
                   ('B', '3.4'),
                   (u'B', u'7.8'),
                   ('D', 9.0),
                   ('E', None))
    ieq(expectation, cut5)


def test_cut_empty():
    table = (('foo', 'bar'),)
    expect = (('bar',),)
    actual = cut(table, 'bar')
    ieq(expect, actual)
        

def test_cutout():
    
    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             (u'B', u'3', u'7.8', True),
             ('D', 'xyz', 9.0),
             ('E', None))

    cut1 = cutout(table, 'bar', 'baz')
    expectation = (('foo',),
                   ('A',),
                   ('B',),
                   (u'B',),
                   ('D',),
                   ('E',))
    ieq(expectation, cut1)
    
    cut2 = cutout(table, 'bar')
    expectation = (('foo', 'baz'),
                   ('A', 2),
                   ('B', '3.4'),
                   (u'B', u'7.8'),
                   ('D', 9.0),
                   ('E', None))
    ieq(expectation, cut2)
    
    cut3 = cutout(table, 1)
    expectation = (('foo', 'baz'),
                   ('A', 2),
                   ('B', '3.4'),
                   (u'B', u'7.8'),
                   ('D', 9.0),
                   ('E', None))
    ieq(expectation, cut3)
    

def test_cat():
    
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
    ieq(expectation, cat1)

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
    ieq(expectation, cat3)
    
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
    ieq(expectation, cat4)
    

def test_cat_with_header():

    table1 = (('bar', 'foo'),
              ('A', 1),
              ('B', 2))

    table2 = (('bar', 'baz'),
              ('C', True),
              ('D', False))

    actual = cat(table1, header=['A', 'foo', 'B', 'bar', 'C'])
    expect = (('A', 'foo', 'B', 'bar', 'C'),
              (None, 1, None, 'A', None),
              (None, 2, None, 'B', None))
    ieq(expect, actual)
    ieq(expect, actual)

    actual = cat(table1, table2, header=['A', 'foo', 'B', 'bar', 'C'])
    expect = (('A', 'foo', 'B', 'bar', 'C'),
              (None, 1, None, 'A', None),
              (None, 2, None, 'B', None),
              (None, None, None, 'C', None),
              (None, None, None, 'D', None))
    ieq(expect, actual)
    ieq(expect, actual)


def test_cat_empty():
    table1 = (('foo', 'bar'),
              (1, 'A'),
              (2, 'B'))
    table2 = (('bar', 'baz'),)
    expect = (('foo', 'bar', 'baz'),
              (1, 'A', None),
              (2, 'B', None))
    actual = cat(table1, table2)
    ieq(expect, actual)


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
    ieq(expect2, table2)
    ieq(expect2, table2)
    
    # test single field with method call
    table3 = convert(table1, 'foo', 'lower')
    expect3 = expect2
    ieq(expect3, table3)

    # test single field with method call with arguments
    table4 = convert(table1, 'foo', 'replace', 'B', 'BB')
    expect4 = (('foo', 'bar', 'baz'),
               ('A', 1, 2),
               ('BB', '2', '3.4'),
               (u'BB', u'3', u'7.8', True),
               ('D', 'xyz', 9.0),
               ('E', None))
    ieq(expect4, table4)
    
    # test multiple fields with the same conversion
    table5 = convert(table1, ('bar', 'baz'), str)
    expect5 = (('foo', 'bar', 'baz'),
               ('A', '1', '2'),
               ('B', '2', '3.4'),
               (u'B', u'3', u'7.8', True),
               ('D', 'xyz', '9.0'),
               ('E', 'None'))
    ieq(expect5, table5)
    
    # test convert with dictionary
    table6 = convert(table1, 'foo', {'A': 'Z', 'B': 'Y'})
    expect6 = (('foo', 'bar', 'baz'),
               ('Z', 1, 2),
               ('Y', '2', '3.4'),
               (u'Y', u'3', u'7.8', True),
               ('D', 'xyz', 9.0),
               ('E', None))
    ieq(expect6, table6)


def test_convert_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = convert(table, 'foo', int)
    ieq(expect, actual)
        

def test_convert_indexes():
    
    table1 = (('foo', 'bar', 'baz'),
              ('A', 1, 2),
              ('B', '2', '3.4'),
              (u'B', u'3', u'7.8', True),
              ('D', 'xyz', 9.0),
              ('E', None))
    
    # test the simplest style - single field, lambda function
    table2 = convert(table1, 0, lambda s: s.lower())
    expect2 = (('foo', 'bar', 'baz'),
               ('a', 1, 2),
               ('b', '2', '3.4'),
               (u'b', u'3', u'7.8', True),
               ('d', 'xyz', 9.0),
               ('e', None))
    ieq(expect2, table2)
    ieq(expect2, table2)
    
    # test single field with method call
    table3 = convert(table1, 0, 'lower')
    expect3 = expect2
    ieq(expect3, table3)

    # test single field with method call with arguments
    table4 = convert(table1, 0, 'replace', 'B', 'BB')
    expect4 = (('foo', 'bar', 'baz'),
               ('A', 1, 2),
               ('BB', '2', '3.4'),
               (u'BB', u'3', u'7.8', True),
               ('D', 'xyz', 9.0),
               ('E', None))
    ieq(expect4, table4)
    
    # test multiple fields with the same conversion
    table5a = convert(table1, (1, 2), str)
    table5b = convert(table1, (1, 'baz'), str)
    table5c = convert(table1, ('bar', 2), str)
    table5d = convert(table1, range(1, 3), str)
    expect5 = (('foo', 'bar', 'baz'),
               ('A', '1', '2'),
               ('B', '2', '3.4'),
               (u'B', u'3', u'7.8', True),
               ('D', 'xyz', '9.0'),
               ('E', 'None'))
    ieq(expect5, table5a)
    ieq(expect5, table5b)
    ieq(expect5, table5c)
    ieq(expect5, table5d)
    
    # test convert with dictionary
    table6 = convert(table1, 0, {'A': 'Z', 'B': 'Y'})
    expect6 = (('foo', 'bar', 'baz'),
               ('Z', 1, 2),
               ('Y', '2', '3.4'),
               (u'Y', u'3', u'7.8', True),
               ('D', 'xyz', 9.0),
               ('E', None))
    ieq(expect6, table6)


def test_fieldconvert():

    table1 = (('foo', 'bar', 'baz'),
              ('A', 1, 2),
              ('B', '2', '3.4'),
              (u'B', u'3', u'7.8', True),
              ('D', 'xyz', 9.0),
              ('E', None))
    
    # test the style where the converters functions are passed in as a dictionary
    converters = {'foo': str, 'bar': int, 'baz': float}
    table5 = convert(table1, converters, errorvalue='error')
    expect5 = (('foo', 'bar', 'baz'),
               ('A', 1, 2.0),
               ('B', 2, 3.4),
               ('B', 3, 7.8, True), # N.B., long rows are preserved
               ('D', 'error', 9.0),
               ('E', 'error')) # N.B., short rows are preserved
    ieq(expect5, table5) 
    
    # test the style where the converters functions are added one at a time
    table6 = convert(table1, errorvalue='err')
    table6['foo'] = str
    table6['bar'] = int
    table6['baz'] = float 
    expect6 = (('foo', 'bar', 'baz'),
               ('A', 1, 2.0),
               ('B', 2, 3.4),
               ('B', 3, 7.8, True),
               ('D', 'err', 9.0),
               ('E', 'err'))
    ieq(expect6, table6) 
    
    # test some different converters
    table7 = convert(table1)
    table7['foo'] = 'replace', 'B', 'BB'
    expect7 = (('foo', 'bar', 'baz'),
               ('A', 1, 2),
               ('BB', '2', '3.4'),
               (u'BB', u'3', u'7.8', True),
               ('D', 'xyz', 9.0),
               ('E', None))
    ieq(expect7, table7)

    # test the style where the converters functions are passed in as a list
    converters = [str, int, float]
    table8 = convert(table1, converters, errorvalue='error')
    expect8 = (('foo', 'bar', 'baz'),
               ('A', 1, 2.0),
               ('B', 2, 3.4),
               ('B', 3, 7.8, True), # N.B., long rows are preserved
               ('D', 'error', 9.0),
               ('E', 'error')) # N.B., short rows are preserved
    ieq(expect8, table8) 
    
    # test the style where the converters functions are passed in as a list
    converters = [str, None, float]
    table9 = convert(table1, converters, errorvalue='error')
    expect9 = (('foo', 'bar', 'baz'),
               ('A', 1, 2.0),
               ('B', '2', 3.4),
               ('B', u'3', 7.8, True), # N.B., long rows are preserved
               ('D', 'xyz', 9.0),
               ('E', None)) # N.B., short rows are preserved
    ieq(expect9, table9) 
    
    
def test_convertall():
    
    table1 = (('foo', 'bar', 'baz'),
              ('1', '3', '9'),
              ('2', '1', '7'))
    table2 = convertall(table1, int)
    expect2 = (('foo', 'bar', 'baz'),
               (1, 3, 9),
               (2, 1, 7))
    ieq(expect2, table2)
    ieq(expect2, table2)
    
    
def test_convertnumbers():
    
    table1 = (('foo', 'bar', 'baz', 'quux'),
              ('1', '3.0', '9+3j', 'aaa'),
              ('2', '1.3', '7+2j', None))
    table2 = convertnumbers(table1)
    expect2 = (('foo', 'bar', 'baz', 'quux'),
               (1, 3.0, 9+3j, 'aaa'),
               (2, 1.3, 7+2j, None))
    ieq(expect2, table2)
    ieq(expect2, table2)
    
    
def test_convert_translate():
    
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
    ieq(expectation, result)


def test_addfield():
    table = (('foo', 'bar'),
             ('M', 12),
             ('F', 34),
             ('-', 56))
    
    result = addfield(table, 'baz', 42)
    expectation = (('foo', 'bar', 'baz'),
                   ('M', 12, 42),
                   ('F', 34, 42),
                   ('-', 56, 42))
    ieq(expectation, result)
    ieq(expectation, result)

    result = addfield(table, 'baz', lambda rec: rec['bar'] * 2)
    expectation = (('foo', 'bar', 'baz'),
                   ('M', 12, 24),
                   ('F', 34, 68),
                   ('-', 56, 112))
    ieq(expectation, result)
    ieq(expectation, result)

    result = addfield(table, 'baz', expr('{bar} * 2'))
    expectation = (('foo', 'bar', 'baz'),
                   ('M', 12, 24),
                   ('F', 34, 68),
                   ('-', 56, 112))
    ieq(expectation, result)
    ieq(expectation, result)

    result = addfield(table, 'baz', 42, index=0)
    expectation = (('baz', 'foo', 'bar'),
                   (42, 'M', 12),
                   (42, 'F', 34),
                   (42, '-', 56))
    ieq(expectation, result)
    ieq(expectation, result)
    

def test_addfield_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar', 'baz'),)
    actual = addfield(table, 'baz', 42)
    ieq(expect, actual)
    ieq(expect, actual)


def test_addfield_coalesce():
    table = (('foo', 'bar', 'baz', 'quux'),
             ('M', 12, 23, 44),
             ('F', None, 23, 11),
             ('-', None, None, 42))
    
    result = addfield(table, 'spong', coalesce('bar', 'baz', 'quux'))
    expect = (('foo', 'bar', 'baz', 'quux', 'spong'),
              ('M', 12, 23, 44, 12),
              ('F', None, 23, 11, 23),
              ('-', None, None, 42, 42))
    ieq(expect, result)
    ieq(expect, result)

    result = addfield(table, 'spong', coalesce(1, 2, 3))
    expect = (('foo', 'bar', 'baz', 'quux', 'spong'),
              ('M', 12, 23, 44, 12),
              ('F', None, 23, 11, 23),
              ('-', None, None, 42, 42))
    ieq(expect, result)
    ieq(expect, result)


def test_rowslice():
    """Test the rowslice function."""
    
    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             (u'B', u'3', u'7.8', True),
             ('D', 'xyz', 9.0),
             ('E', None))

    result = rowslice(table, 2)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('B', '2', '3.4'))
    ieq(expectation, result)

    result = rowslice(table, 1, 2)
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'))
    ieq(expectation, result)

    result = rowslice(table, 1, 5, 2)
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('D', 'xyz', 9.0))
    ieq(expectation, result)


def test_rowslice_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = rowslice(table, 1, 2)
    ieq(expect, actual)
        

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
    ieq(expect, table2)


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
    ieq(expect, table2)
    

def test_tail_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = tail(table)
    ieq(expect, actual)
        
    
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
    ieq(expectation, result)
    
    
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
    ieq(expectation, result)
    
    result = sort(table) # default is lexical sort
    expectation = (('foo', 'bar'),
                   ('A', '6'),
                   ('A', '9'),
                   ('C', '2'),
                   ('D', '10'),
                   ('F', '1'))
    ieq(expectation, result)
    
    
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
    ieq(expectation, result)
    
    
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
    ieq(expectation, result)
    
    
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
    ieq(expectation, result)
    result = sort(table, key=(0, 1))
    ieq(expectation, result)
    result = sort(table, key=('foo', 1))
    ieq(expectation, result)
    result = sort(table, key=(0, 'bar'))
    ieq(expectation, result)
    
    
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
    ieq(expectation, result)
    

def test_sort_buffered():
    
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
    ieq(expectation, result)
    result = sort(table, 'bar', buffersize=2)
#    print list(result)
    ieq(expectation, result)
        
    # sort in reverse
    expectation = (('foo', 'bar'),
                   ('D', 10),
                   ('A', 9),
                   ('A', 6),
                   ('C', 2),
                   ('F', 1))
    
    result = sort(table, 'bar', reverse=True)
    ieq(expectation, result)
    result = sort(table, 'bar', reverse=True, buffersize=2)
    ieq(expectation, result)

    # no key
    expectation = (('foo', 'bar'),
                   ('F', 1),
                   ('D', 10),
                   ('C', 2),
                   ('A', 9),
                   ('A', 6))
    result = sort(table, reverse=True)
    ieq(expectation, result)
    result = sort(table, reverse=True, buffersize=2)
    ieq(expectation, result)
    
    
def test_sort_buffered_tempdir():
    
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
    ieq(expectation, result)
    result = sort(table, 'bar', buffersize=2, tempdir='/tmp')
    ieq(expectation, result)
            
    
def test_sort_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = sort(table)
    ieq(expect, actual)


def test_sort_none():
    
    table = (('foo', 'bar'),
            ('C', 2),
            ('A', 9),
            ('A', None),
            ('F', 1),
            ('D', 10))
    
    result = sort(table, 'bar')
    expectation = (('foo', 'bar'),
                   ('A', None),
                   ('F', 1),
                   ('C', 2),
                   ('A', 9),
                   ('D', 10))
    ieq(expectation, result)
    
    dt = datetime.now().replace

    table = (('foo', 'bar'),
            ('C', dt(hour=5)),
            ('A', dt(hour=1)),
            ('A', None),
            ('F', dt(hour=9)),
            ('D', dt(hour=17)))
    
    result = sort(table, 'bar')
    expectation = (('foo', 'bar'),
                   ('A', None),
                   ('A', dt(hour=1)),
                   ('C', dt(hour=5)),
                   ('F', dt(hour=9)),
                   ('D', dt(hour=17)))
    ieq(expectation, result)
    
    
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
    ieq(expectation, result)

    result = melt(table, key='id', variablefield='variable', valuefield='value')
    ieq(expectation, result)


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
    ieq(expectation, result)

    expectation = (('id', 'time', 'variable', 'value'),
                   (1, 11, 'height', 66.4),
                   (2, 16, 'height', 53.2),
                   (3, 12, 'height', 34.5))
    result = melt(table, key=('id', 'time'), variables='height')
    ieq(expectation, result)
    

def test_melt_empty():
    table = (('foo', 'bar', 'baz'),)
    expect = (('foo', 'variable', 'value'),)
    actual = melt(table, key='foo')
    ieq(expect, actual)


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
    ieq(expectation, result)

    result = recast(table, variablefield='variable')
    ieq(expectation, result)

    result = recast(table, key='id', variablefield='variable')
    ieq(expectation, result)

    result = recast(table, key='id', variablefield='variable', valuefield='value')
    ieq(expectation, result)


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
    ieq(expectation, result)


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
    ieq(expectation, result)

    # in the absence of an aggregation function, list all values
    expectation = (('id', 'weight'),
                   (1, [66.4, 55.2]),
                   (2, [53.2, 43.3]),
                   (3, [34.5, 49.4]))
    result = recast(table, key='id')
    ieq(expectation, result)

    # max aggregation
    expectation = (('id', 'weight'),
                   (1, 66.4),
                   (2, 53.2),
                   (3, 49.4))
    result = recast(table, key='id', reducers={'weight': max})
    ieq(expectation, result)

    # min aggregation
    expectation = (('id', 'weight'),
                   (1, 55.2),
                   (2, 43.3),
                   (3, 34.5))
    result = recast(table, key='id', reducers={'weight': min})
    ieq(expectation, result)

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
    ieq(expectation, result)

    
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
    ieq(expect, result)


def test_recast_empty():
    table = (('foo', 'variable', 'value'),)
    expect = (('foo',),)
    actual = recast(table)
    ieq(expect, actual)


def test_recast_date():
    
    dt = datetime.now().replace
    table = (('id', 'variable', 'value'),
             (dt(hour=3), 'age', 16),
             (dt(hour=1), 'gender', 'F'),
             (dt(hour=2), 'gender', 'M'),
             (dt(hour=2), 'age', 17),
             (dt(hour=1), 'age', 12),
             (dt(hour=3), 'gender', 'M'))
    
    expectation = (('id', 'age', 'gender'),
                   (dt(hour=1), 12, 'F'),
                   (dt(hour=2), 17, 'M'),
                   (dt(hour=3), 16, 'M'))
    
    result = recast(table) # by default lift 'variable' field, hold everything else
    ieq(expectation, result)

    result = recast(table, variablefield='variable')
    ieq(expectation, result)

    result = recast(table, key='id', variablefield='variable')
    ieq(expectation, result)

    result = recast(table, key='id', variablefield='variable', valuefield='value')
    ieq(expectation, result)



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
    ieq(expectation, result)
    
    # test with compound key
    result = duplicates(table, key=('foo', 'bar'))
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', '2', 42))
    ieq(expectation, result)
    

def test_duplicates_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = duplicates(table, key='foo')
    ieq(expect, actual)


def test_duplicates_wholerow():
    
    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '3.4'),
             ('D', 4, 12.3))

    result = duplicates(table)
    expectation = (('foo', 'bar', 'baz'),
             ('B', '2', '3.4'),
             ('B', '2', '3.4'))
    ieq(expectation, result)
    

def test_unique():
    
    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3),
             ('F', 7, 2.3))

    result = unique(table, 'foo')
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('E', None),
                   ('F', 7, 2.3))
    ieq(expectation, result)
    ieq(expectation, result)
    
    # test with compound key
    result = unique(table, key=('foo', 'bar'))
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('B', u'3', u'7.8', True),
                   ('D', 4, 12.3),
                   ('D', 'xyz', 9.0),
                   ('E', None),
                   ('F', 7, 2.3))
    ieq(expectation, result)
    ieq(expectation, result)
    

def test_unique_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = unique(table, key='foo')
    ieq(expect, actual)


def test_unique_wholerow():
    
    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '3.4'),
             ('D', 4, 12.3))

    result = unique(table)
    expectation = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('D', 4, 12.3))
    ieq(expectation, result)
    

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
    ieq(expectation, result)
    ieq(expectation, result)
    
    result = conflicts(table, 'foo', missing=None, exclude='baz')
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None))
    ieq(expectation, result)
    ieq(expectation, result)
    
    result = conflicts(table, 'foo', missing=None, exclude=('bar', 'baz'))
    expectation = (('foo', 'bar', 'baz'),)
    ieq(expectation, result)
    ieq(expectation, result)

    result = conflicts(table, 'foo', missing=None, include='bar')
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None))
    ieq(expectation, result)
    ieq(expectation, result)
    
    result = conflicts(table, 'foo', missing=None, include=('bar', 'baz'))
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None),
                   ('D', 'xyz', 9.4),
                   ('D', 'xyz', 12.3))
    ieq(expectation, result)
    ieq(expectation, result)

    
def test_conflicts_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = conflicts(table, key='foo')
    ieq(expect, actual)


def test_mergeduplicates():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', None),
             ('D', 'xyz', 9.4),
             ('B', None, u'7.8', True),
             ('E', None, 42.),
             ('D', 'xyz', 12.3),
             ('A', 2, None))

    # value overrides missing
    result = mergeduplicates(table, 'foo', missing=None)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', Conflict([1, 2]), 2),
                   ('B', '2', u'7.8'),
                   ('D', 'xyz', Conflict([9.4, 12.3])),
                   ('E', None, 42.))
    ieq(expectation, result)
    
    
def test_mergeduplicates_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = mergeduplicates(table, key='foo')
    ieq(expect, actual)


def test_mergeduplicates_shortrows():
    table = [['foo', 'bar', 'baz'], 
             ['a', 1, True], 
             ['b', 2, True], 
             ['b', 3]]
    actual = mergeduplicates(table, 'foo')
    expect = [('foo', 'bar', 'baz'), ('a', 1, True), ('b', Conflict([2, 3]), True)]
    ieq(expect, actual)
        
    
def test_mergeduplicates_compoundkey():
    table = [['foo', 'bar', 'baz'], 
             ['a', 1, True], 
             ['a', 1, True], 
             ['a', 2, False],
             ['a', 2, None],
             ['c', 3, True],
             ['c', 3, False],
             ]
    actual = mergeduplicates(table, key=('foo', 'bar'))
    expect = [('foo', 'bar', 'baz'), 
              ('a', 1, True), 
              ('a', 2, False), 
              ('c', 3, Conflict([True, False]))]
    ieq(expect, actual)
        
    
def _test_complement_1(f):

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
    
    result = f(table1, table2)
    ieq(expectation, result)
    
    
def _test_complement_2(f):

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
    
    result = f(tablea, tableb)
    ieq(aminusb, result)
    
    bminusa = (('x', 'y', 'z'),
               ('A', 9, False),
               ('B', 3, True))
    
    result = f(tableb, tablea)
    ieq(bminusa, result)
    

def _test_complement_3(f):

    # make sure we deal with empty tables
    
    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2))
    
    table2 = (('foo', 'bar'),)
    
    expectation = (('foo', 'bar'),
                   ('A', 1),
                   ('B', 2))
    result = f(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)
    
    expectation = (('foo', 'bar'),)
    result = f(table2, table1)
    ieq(expectation, result)
    ieq(expectation, result)
    
    
def _test_complement_4(f):

    # test behaviour with duplicate rows
    
    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('B', 2),
              ('C', 7))
    
    table2 = (('foo', 'bar'),
              ('B', 2))
    
    expectation = (('foo', 'bar'),
                   ('A', 1),
                   ('B', 2),
                   ('C', 7))
    
    result = f(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)


def _test_complement_none(f):
    # test behaviour with unsortable types
    now = datetime.now()

    ta = [['a', 'b'], [None, None]]
    tb = [['a', 'b'], [None, now]]

    expectation = (('a', 'b'), (None, None))
    result = f(ta, tb)
    ieq(expectation, result)

    ta = [['a'], [now], [None]]
    tb = [['a'], [None], [None]]

    expectation = (('a',), (now,))
    result = f(ta, tb)
    ieq(expectation, result)


def _test_complement(f):
    _test_complement_1(f)
    _test_complement_2(f)
    _test_complement_3(f)
    _test_complement_4(f)
    _test_complement_none(f)


def test_complement():
    _test_complement(complement)


def test_complement_seqtypes():
    # test complement isn't confused by list vs tuple
    ta = [['a', 'b'], ['A', 1], ['B', 2]]
    tb = [('a', 'b'), ('A', 1), ('B', 2)]
    expectation = (('a', 'b'),)
    actual = complement(ta, tb, presorted=True)
    ieq(expectation, actual)


def test_hashcomplement_seqtypes():
    # test complement isn't confused by list vs tuple
    ta = [['a', 'b'], ['A', 1], ['B', 2]]
    tb = [('a', 'b'), ('A', 1), ('B', 2)]
    expectation = (('a', 'b'),)
    actual = hashcomplement(ta, tb)
    ieq(expectation, actual)


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
    ieq(bminusa, added)
    ieq(aminusb, subtracted)
    

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
    ieq(expectation, result)
    
    
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
    ieq(aminusb, result)
    
    bminusa = (('bar', 'foo', 'baz'),
               (3, 'B', True),
               (9, 'A', False))
    
    result = recordcomplement(tableb, tablea)
    ieq(bminusa, result)
    

def test_recordcomplement_3():

    # make sure we deal with empty tables
    
    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2))
    
    table2 = (('bar', 'foo'),)
    
    expectation = (('foo', 'bar'),
                   ('A', 1),
                   ('B', 2))
    result = recordcomplement(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)
    
    expectation = (('bar', 'foo'),)
    result = recordcomplement(table2, table1)
    ieq(expectation, result)
    ieq(expectation, result)
    
    
def test_recordcomplement_4():

    # test behaviour with duplicate rows
    
    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('B', 2),
              ('C', 7))
    
    table2 = (('bar', 'foo'),
              (2, 'B'))
    
    expectation = (('foo', 'bar'),
                   ('A', 1),
                   ('B', 2),
                   ('C', 7))
    
    result = recordcomplement(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)
    
    
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
    ieq(aminusb, subtracted)
    ieq(bminusa, added)
    

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
    ieq(expectation, result)
    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'),
                           include_original=False)
    ieq(expectation, result)

    # what about including the original field?
    expectation = (('id', 'variable', 'value', 'treat', 'time'),
                   ('1', 'A1', '12', 'A', '1'),  
                   ('2', 'A2', '15', 'A', '2'),
                   ('3', 'B1', '18', 'B', '1'),
                   ('4', 'C12', '19', 'C', '12'))
    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'),
                           include_original=True)
    ieq(expectation, result)
    
    # what about if number of captured groups is different from new fields?
    expectation = (('id', 'value'),
                   ('1', '12', 'A', '1'),  
                   ('2', '15', 'A', '2'),
                   ('3', '18', 'B', '1'),
                   ('4', '19', 'C', '12'))
    result = capture(table, 'variable', '(\\w)(\\d+)')
    ieq(expectation, result)
    
    
def test_capture_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'baz', 'qux'),)
    actual = capture(table, 'bar', r'(\w)(\d)', ('baz', 'qux'))
    ieq(expect, actual)


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
    ieq(expectation, result)
    ieq(expectation, result)
    # proper regex
    result = split(table, 'variable', '[Dd]', ('variable', 'day'))
    ieq(expectation, result)

    expectation = (('id', 'variable', 'value', 'variable', 'day'),
                   ('1', 'parad1', '12', 'para', '1'),  
                   ('2', 'parad2', '15', 'para', '2'),
                   ('3', 'tempd1', '18', 'temp', '1'),
                   ('4', 'tempd2', '19', 'temp', '2'))
    
    result = split(table, 'variable', 'd', ('variable', 'day'), include_original=True)
    ieq(expectation, result)
    
    # what about if no new fields?
    expectation = (('id', 'value'),
                   ('1', '12', 'para', '1'),  
                   ('2', '15', 'para', '2'),
                   ('3', '18', 'temp', '1'),
                   ('4', '19', 'temp', '2'))
    result = split(table, 'variable', 'd')
    ieq(expectation, result)


def test_split_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'baz', 'qux'),)
    actual = split(table, 'bar', 'd', ('baz', 'qux'))
    ieq(expect, actual)


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
    ieq(expectation, step2)


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
    ieq(expectation, step2)


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
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice
 
    # check select complement
    actual = select(table, lambda rec: rec['foo'] == 'a', complement=True)
    expect = (('foo', 'bar', 'baz'),
              ('b', 1, 23.3),
              ('c', 8, 42.0),
              ('d', 7, 100.9),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice

    actual = select(table, lambda rec: rec['foo'] == 'a' and rec['bar'] > 3)
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3))
    ieq(expect, actual)

    actual = select(table, "{foo} == 'a'")
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)

    actual = select(table, "{foo} == 'a' and {bar} > 3")
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3))
    ieq(expect, actual)

    # check error handling on short rows
    actual = select(table, lambda rec: rec['baz'] > 88.1)
    expect = (('foo', 'bar', 'baz'),
              ('a', 2, 88.2),
              ('d', 7, 100.9))
    ieq(expect, actual)
    
    # check single field tests
    actual = select(table, 'foo', lambda v: v == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice
    
    # check select complement
    actual = select(table, 'foo', lambda v: v == 'a', complement=True)
    expect = (('foo', 'bar', 'baz'),
              ('b', 1, 23.3),
              ('c', 8, 42.0),
              ('d', 7, 100.9),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice


def test_select_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = select(table, lambda r: r['foo'] == r['bar'])
    ieq(expect, actual)


def test_selectin():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = selectin(table, 'foo', ['a', 'x', 'y'])
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice


def test_selectcontains():
    
    table = (('foo', 'bar', 'baz'),
             ('aaa', 4, 9.3),
             ('aa', 2, 88.2),
             ('bab', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = selectcontains(table, 'foo', 'a')
    expect = (('foo', 'bar', 'baz'),
              ('aaa', 4, 9.3),
              ('aa', 2, 88.2),
              ('bab', 1, 23.3))
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice


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
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice


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
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice


def test_recordselect():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = rowselect(table, lambda rec: rec['foo'] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice


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
    ieq(expect, actual)
    ieq(expect, actual)

    
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
    ieq(expect, actual)
    ieq(expect, actual) # can iteratate twice?
    
    # do it with suffix
    actual = fieldmap(table)
    actual['subject_id'] = 'id'
    actual['gender'] = 'sex', {'male': 'M', 'female': 'F'}
    actual['age_months'] = 'age', lambda v: v * 12
    actual['bmi'] = '{weight} / {height}**2'
    ieq(expect, actual)
    
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
    ieq(expect, actual)


def test_fieldmap_empty():
    
    table = (('foo', 'bar'),)
    expect = (('foo', 'baz'),)
    mappings = OrderedDict()
    mappings['foo'] = 'foo'
    mappings['baz'] = 'bar', lambda v: v*2 
    actual = fieldmap(table, mappings)
    ieq(expect, actual)


def test_facet():

    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))
    fct = facet(table, 'foo')
    assert set(fct.keys()) == set(['a', 'b', 'c', 'd'])
    expect_fcta = (('foo', 'bar', 'baz'),
                   ('a', 4, 9.3),
                   ('a', 2, 88.2))
    ieq(fct['a'], expect_fcta)
    ieq(fct['a'], expect_fcta) # check can iterate twice
    expect_fctc = (('foo', 'bar', 'baz'),
                   ('c', 8, 42.0),
                   ('c', 2))
    ieq(fct['c'], expect_fctc)
    ieq(fct['c'], expect_fctc) # check can iterate twice
    

def test_facet_2():

    table = (('foo', 'bar', 'baz'),
             ('aa', 4, 9.3),
             ('aa', 2, 88.2),
             ('bb', 1, 23.3),
             ('cc', 8, 42.0),
             ('dd', 7, 100.9),
             ('cc', 2))
    fct = facet(table, 'foo')
    assert set(fct.keys()) == set(['aa', 'bb', 'cc', 'dd'])
    expect_fcta = (('foo', 'bar', 'baz'),
                   ('aa', 4, 9.3),
                   ('aa', 2, 88.2))
    ieq(fct['aa'], expect_fcta)
    ieq(fct['aa'], expect_fcta) # check can iterate twice
    expect_fctc = (('foo', 'bar', 'baz'),
                   ('cc', 8, 42.0),
                   ('cc', 2))
    ieq(fct['cc'], expect_fctc)
    ieq(fct['cc'], expect_fctc) # check can iterate twice
    

def test_facet_empty():
    table = (('foo', 'bar'),)
    actual = facet(table, 'foo')
    eq_(list(), actual.keys())


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
    eq_([(1, 3), (3, 5), (5, 7), (7, 9)], rf.keys())
    expect_13 = (('foo', 'bar'),
                 ('b', 2),
                 ('b', 1)) # N.B., it get's sorted
    ieq(expect_13, rf[(1, 3)])
    ieq(expect_13, rf[(1, 3)])
    expect_79 = (('foo', 'bar'),
                 ('a', 7),
                 ('b', 9))
    ieq(expect_79, rf[(7, 9)])


def test_rowreduce():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def sumbar(key, rows):
        return [key, sum(row[1] for row in rows)]
        
    table2 = rowreduce(table1, key='foo', reducer=sumbar, fields=['foo', 'barsum'])
    expect2 = (('foo', 'barsum'),
               ('a', 10),
               ('b', 12),
               ('c', 4))
    ieq(expect2, table2)
    

def test_rowreduce_fieldnameaccess():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def sumbar(key, records):
        return [key, sum([rec['bar'] for rec in records])]
        
    table2 = rowreduce(table1, key='foo', reducer=sumbar, fields=['foo', 'barsum'])
    expect2 = (('foo', 'barsum'),
               ('a', 10),
               ('b', 12),
               ('c', 4))
    ieq(expect2, table2)
    

def test_rowreduce_more():
    
    table1 = (('foo', 'bar'),
              ('aa', 3),
              ('aa', 7),
              ('bb', 2),
              ('bb', 1),
              ('bb', 9),
              ('cc', 4))
    
    def sumbar(key, records):
        return [key, sum(rec['bar'] for rec in records)]
        
    table2 = rowreduce(table1, key='foo', reducer=sumbar, fields=['foo', 'barsum'])
    expect2 = (('foo', 'barsum'),
               ('aa', 10),
               ('bb', 12),
               ('cc', 4))
    ieq(expect2, table2)
    

def test_rowreduce_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    reducer = lambda key, rows: (key, [r[0] for r in rows])
    actual = rowreduce(table, key='foo', reducer=reducer, fields=('foo', 'bar'))
    ieq(expect, actual)


def test_rangerowreduce():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def redu(key, rows):
        return [key[0], key[1], ''.join([row[0] for row in rows])]
        
    table2 = rangerowreduce(table1, 'bar', 2, reducer=redu, 
                            fields=['minbar', 'maxbar', 'foos'])
    expect2 = (('minbar', 'maxbar', 'foos'),
               (1, 3, 'bb'),
               (3, 5, 'ac'),
               (5, 7, ''),
               (7, 9, 'a'),
               (9, 11, 'b'))
    ieq(expect2, table2)
    ieq(expect2, table2)
    

def test_rangerowreduce_fieldnameaccess():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    def redu(key, recs):
        return [key[0], key[1], ''.join([rec['foo'] for rec in recs])]
        
    table2 = rangerowreduce(table1, 'bar', 2, reducer=redu, 
                            fields=['minbar', 'maxbar', 'foos'])
    expect2 = (('minbar', 'maxbar', 'foos'),
               (1, 3, 'bb'),
               (3, 5, 'ac'),
               (5, 7, ''),
               (7, 9, 'a'),
               (9, 11, 'b'))
    ieq(expect2, table2)
    ieq(expect2, table2)
    
    
def test_aggregate_simple():
    
    table1 = (('foo', 'bar', 'baz'),
              ('a', 3, True),
              ('a', 7, False),
              ('b', 2, True),
              ('b', 2, False),
              ('b', 9, False),
              ('c', 4, True))

    # simplest signature - aggregate whole rows
    table2 = aggregate(table1, 'foo', len)
    expect2 = (('key', 'value'),
               ('a', 2),
               ('b', 3),
               ('c', 1))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # next simplest signature - aggregate single field
    table3 = aggregate(table1, 'foo', sum, 'bar')
    expect3 = (('key', 'value'),
               ('a', 10),
               ('b', 13),
               ('c', 4))
    ieq(expect3, table3)
    ieq(expect3, table3)
    
    # alternative signature for simple aggregation
    table4 = aggregate(table1, key=('foo', 'bar'), aggregation=list, value=('bar', 'baz'))
    expect4 = (('key', 'value'),
               (('a', 3), [(3, True)]),
               (('a', 7), [(7, False)]),
               (('b', 2), [(2, True), (2, False)]),
               (('b', 9), [(9, False)]),
               (('c', 4), [(4, True)]))
    ieq(expect4, table4)
    ieq(expect4, table4)
    
    
def test_aggregate_multifield():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4))
    
    # dict arg
    
    aggregators = OrderedDict()
    aggregators['count'] = len
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum
    aggregators['listbar'] = 'bar', list
    aggregators['bars'] = 'bar', strjoin(', ')

    table2 = aggregate(table1, 'foo', aggregators)
    expect2 = (('key', 'count', 'minbar', 'maxbar', 'sumbar', 'listbar', 'bars'),
               ('a', 2, 3, 7, 10, [3, 7], '3, 7'),
               ('b', 3, 1, 9, 12, [2, 1, 9], '2, 1, 9'),
               ('c', 1, 4, 4, 4, [4], '4'))
    ieq(expect2, table2)
    ieq(expect2, table2) # check can iterate twice
    
    # use suffix notation
    
    table3 = aggregate(table1, 'foo')
    table3['count'] = len
    table3['minbar'] = 'bar', min
    table3['maxbar'] = 'bar', max
    table3['sumbar'] = 'bar', sum
    table3['listbar'] = 'bar' # default aggregation is list
    table3['bars'] = 'bar', strjoin(', ')
    ieq(expect2, table3)
    
    # list arg

    aggregators = [('count', len),
                   ('minbar', 'bar', min),
                   ('maxbar', 'bar', max),
                   ('sumbar', 'bar', sum),
                   ('listbar', 'bar', list),
                   ('bars', 'bar', strjoin(', '))]

    table4 = aggregate(table1, 'foo', aggregators)
    ieq(expect2, table4)
    ieq(expect2, table4) # check can iterate twice
    
    
def test_aggregate_more():
    
    table1 = (('foo', 'bar'),
              ('aa', 3),
              ('aa', 7),
              ('bb', 2),
              ('bb', 1),
              ('bb', 9),
              ('cc', 4),
              ('dd', 3))
    
    aggregators = OrderedDict()
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum
    aggregators['listbar'] = 'bar' # default aggregation is list
    aggregators['bars'] = 'bar', strjoin(', ')

    table2 = aggregate(table1, 'foo', aggregators)
    expect2 = (('key', 'minbar', 'maxbar', 'sumbar', 'listbar', 'bars'),
               ('aa', 3, 7, 10, [3, 7], '3, 7'),
               ('bb', 1, 9, 12, [2, 1, 9], '2, 1, 9'),
               ('cc', 4, 4, 4, [4], '4'),
               ('dd', 3, 3, 3, [3], '3'))
    ieq(expect2, table2)
    ieq(expect2, table2) # check can iterate twice
    
    table3 = aggregate(table1, 'foo')
    table3['minbar'] = 'bar', min
    table3['maxbar'] = 'bar', max
    table3['sumbar'] = 'bar', sum
    table3['listbar'] = 'bar' # default aggregation is list
    table3['bars'] = 'bar', strjoin(', ')
    ieq(expect2, table3)
    
    
def test_aggregate_empty():
    
    table = (('foo', 'bar'),)
    
    aggregators = OrderedDict()
    aggregators['minbar'] = 'bar', min
    aggregators['maxbar'] = 'bar', max
    aggregators['sumbar'] = 'bar', sum

    actual = aggregate(table, 'foo', aggregators)
    expect = (('key', 'minbar', 'maxbar', 'sumbar'),)
    ieq(expect, actual)


def test_rangeaggregate_simple():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4),
              ('d', 3))

    # simplest signature - aggregate whole rows
    table2 = rangeaggregate(table1, 'bar', 2, len)
    expect2 = (('key', 'value'),
               ((1, 3), 2),
               ((3, 5), 3),
               ((5, 7), 0),
               ((7, 9), 1),
               ((9, 11), 1))
    ieq(expect2, table2)
    ieq(expect2, table2) # verify can iterate twice

    # next simplest signature - aggregate single field
    table3 = rangeaggregate(table1, 'bar', 2, list, 'foo')
    expect3 = (('key', 'value'),
               ((1, 3), ['b', 'b']),
               ((3, 5), ['a', 'd', 'c']),
               ((5, 7), []),
               ((7, 9), ['a']),
               ((9, 11), ['b']))
    ieq(expect3, table3)

    # alternative signature for simple aggregation
    table4 = rangeaggregate(table1, key='bar', width=2, aggregation=list, value='foo')
    ieq(expect3, table4)
    
    
def test_rangeaggregate_minmax():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4),
              ('d', 3))

    # check specifying minimum value for first bin
    table2 = rangeaggregate(table1, 'bar', 2, len, minv=0)
    expect2 = (('key', 'value'),
               ((0, 2), 1),
               ((2, 4), 3),
               ((4, 6), 1),
               ((6, 8), 1),
               ((8, 10), 1))
    ieq(expect2, table2)

    # check specifying min and max values
    table3 = rangeaggregate(table1, 'bar', 2, len, minv=2, maxv=6)
    expect3 = (('key', 'value'),
               ((2, 4), 3),
               ((4, 6), 1))
    ieq(expect3, table3)

    # check last bin is open if maxv is specified
    table4 = rangeaggregate(table1, 'bar', 2, len, maxv=9)
    expect4 = (('key', 'value'),
               ((1, 3), 2),
               ((3, 5), 3),
               ((5, 7), 0),
               ((7, 9), 2))
    ieq(expect4, table4)
    
    # check we get empty bins if maxv is large
    table5 = rangeaggregate(table1, 'bar', 2, len, minv=10, maxv=14)
    expect5 = (('key', 'value'),
               ((10, 12), 0),
               ((12, 14), 0))
    ieq(expect5, table5)


def test_rangeaggregate_empty():
    
    table1 = (('foo', 'bar'),)
    table2 = rangeaggregate(table1, 'bar', 2, len)
    expect2 = (('key', 'value'),)
    ieq(expect2, table2)

    table3 = rangeaggregate(table1, 'bar', 2, len, minv=0)
    ieq(expect2, table3)

    table4 = rangeaggregate(table1, 'bar', 2, len, minv=0, maxv=4)
    expect4 = (('key', 'value'),
               ((0, 2), 0),
               ((2, 4), 0))
    ieq(expect4, table4)


def test_rangeaggregate_multifield():
    
    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4),
              ('d', 3))

    # dict arg

    aggregators = OrderedDict()
    aggregators['foocount'] = len 
    aggregators['foojoin'] = 'foo', strjoin('')
    aggregators['foolist'] = 'foo' # default is list
    
    table2 = rangeaggregate(table1, 'bar', 2, aggregators)
    expect2 = (('key', 'foocount', 'foojoin', 'foolist'),
               ((1, 3), 2, 'bb', ['b', 'b']),
               ((3, 5), 3, 'adc', ['a', 'd', 'c']),
               ((5, 7), 0, '', []),
               ((7, 9), 1, 'a', ['a']),
               ((9, 11), 1, 'b', ['b']))
    ieq(expect2, table2)

    # suffix notation
    
    table3 = rangeaggregate(table1, 'bar', 2)
    table3['foocount'] = len 
    table3['foojoin'] = 'foo', strjoin('')
    table3['foolist'] = 'foo' # default is list
    ieq(expect2, table3)

    # list arg
    
    aggregators = [('foocount', len),
                   ('foojoin', 'foo', strjoin('')),
                   ('foolist', 'foo', list)]
    table4 = rangeaggregate(table1, 'bar', 2, aggregators)
    ieq(expect2, table4)


def test_rangeaggregate_multifield_2():
    
    table1 = (('foo', 'bar'),
              ('aa', 3),
              ('aa', 7),
              ('bb', 2),
              ('bb', 1),
              ('bb', 9),
              ('cc', 4),
              ('dd', 3))

    table2 = rangeaggregate(table1, 'bar', 2)
    table2['foocount'] = len
    table2['foolist'] = 'foo' # default is list
    expect2 = (('key', 'foocount', 'foolist'),
               ((1, 3), 2, ['bb', 'bb']),
               ((3, 5), 3, ['aa', 'dd', 'cc']),
               ((5, 7), 0, []),
               ((7, 9), 1, ['aa']),
               ((9, 11), 1, ['bb']))
    ieq(expect2, table2)


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
    expect2 = (('key', 'value'),
               ((1, 3), 2),
               ((3, 5), 3),
               ((5, 7), 0),
               ((7, 9), 1),
               ((9, 11), 1))
    ieq(expect2, table2)
    ieq(expect2, table2)

    table3 = rangecounts(table1, 'bar', width=2, minv=0)
    expect3 = (('key', 'value'),
               ((0, 2), 1),
               ((2, 4), 3),
               ((4, 6), 1),
               ((6, 8), 1),
               ((8, 10), 1))
    ieq(expect3, table3)

    table4 = rangecounts(table1, 'bar', width=2, minv=2, maxv=6)
    expect4 = (('key', 'value'),
               ((2, 4), 3),
               ((4, 6), 1))
    ieq(expect4, table4)

    # N.B., last bin is open if maxv is specified
    table5 = rangecounts(table1, 'bar', width=2, maxv=9)
    expect5 = (('key', 'value'),
               ((1, 3), 2),
               ((3, 5), 3),
               ((5, 7), 0),
               ((7, 9), 2))
    ieq(expect5, table5)


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
    ieq(expect, actual)
    ieq(expect, actual) # can iteratate twice?
        
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
    ieq(expect, actual)


def test_rowmap_empty():
    
    table = (('id', 'sex', 'age', 'height', 'weight'),)
    def rowmapper(row):
        transmf = {'male': 'M', 'female': 'F'}
        return [row[0],
                transmf[row[1]] if row[1] in transmf else row[1],
                row[2] * 12,
                row[4] / row[3] ** 2]
    actual = rowmap(table, rowmapper, fields=['subject_id', 'gender', 'age_months', 'bmi'])  
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),)
    ieq(expect, actual)


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
    actual = rowmap(table, recmapper, fields=['subject_id', 'gender', 'age_months', 'bmi'])  
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16*12, 62.0/1.45**2),
              (2, 'F', 19*12, 55.4/1.34**2),
              (3, 'F', 17*12, 74.4/1.78**2),
              (4, 'M', 21*12, 45.2/1.33**2),
              (5, '-', 25*12, 51.9/1.65**2))
    ieq(expect, actual)
    ieq(expect, actual) # can iteratate twice?
        
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
    ieq(expect, actual)


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
    ieq(expect, actual)
    ieq(expect, actual) # can iteratate twice?
        

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
    ieq(expect, actual)
    ieq(expect, actual) # can iteratate twice?
        

def test_setheader():
    
    table1 = (('foo', 'bar'),
              ('a', 1),
              ('b', 2))
    table2 = setheader(table1, ['foofoo', 'barbar'])
    expect2 = (('foofoo', 'barbar'),
               ('a', 1),
               ('b', 2))
    ieq(expect2, table2)
    ieq(expect2, table2) # can iterate twice?
    
    
def test_setheader_empty():
    
    table1 = (('foo', 'bar'),)
    table2 = setheader(table1, ['foofoo', 'barbar'])
    expect2 = (('foofoo', 'barbar'),)
    ieq(expect2, table2)
    
    
def test_extendheader():
    
    table1 = (('foo',),
              ('a', 1, True),
              ('b', 2, False))
    table2 = extendheader(table1, ['bar', 'baz'])
    expect2 = (('foo', 'bar', 'baz'),
               ('a', 1, True),
               ('b', 2, False))
    ieq(expect2, table2)
    ieq(expect2, table2) # can iterate twice?
    
    
def test_extendheader_empty():
    
    table1 = (('foo',),)
    table2 = extendheader(table1, ['bar', 'baz'])
    expect2 = (('foo', 'bar', 'baz'),)
    ieq(expect2, table2)
    
    
def test_pushheader():
    
    table1 = (('a', 1),
              ('b', 2))
    table2 = pushheader(table1, ['foo', 'bar'])
    expect2 = (('foo', 'bar'),
               ('a', 1),
               ('b', 2))
    ieq(expect2, table2)
    ieq(expect2, table2) # can iterate twice?
    

def test_pushheader_empty():
    
    table1 = (('a', 1),)
    table2 = pushheader(table1, ['foo', 'bar'])
    expect2 = (('foo', 'bar'),
               ('a', 1))
    ieq(expect2, table2)
    
    table1 = tuple()
    table2 = pushheader(table1, ['foo', 'bar'])
    expect2 = (('foo', 'bar'),)
    ieq(expect2, table2)
    

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
    ieq(expect2, table2)
    ieq(expect2, table2) # can iterate twice?
    
    
def test_skip_empty():
    
    table1 = (('#aaa', 'bbb', 'ccc'),
              ('#mmm'),
              ('foo', 'bar'))
    table2 = skip(table1, 2)
    expect2 = (('foo', 'bar'),)
    ieq(expect2, table2)
    
    
def test_skipcomments():

    table1 = (('##aaa', 'bbb', 'ccc'),
              ('##mmm',),
              ('#foo', 'bar'),
              ('##nnn', 1),
              ('a', 1),
              ('b', 2))
    table2 = skipcomments(table1, '##')
    expect2 = (('#foo', 'bar'),
               ('a', 1),
               ('b', 2))
    ieq(expect2, table2)
    ieq(expect2, table2) # can iterate twice?
    
    
def test_skipcomments_empty():

    table1 = (('##aaa', 'bbb', 'ccc'),
              ('##mmm',),
              ('#foo', 'bar'),
              ('##nnn', 1))
    table2 = skipcomments(table1, '##')
    expect2 = (('#foo', 'bar'),)
    ieq(expect2, table2)
    
    
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
    ieq(expect2, table2)
    ieq(expect2, table2) # check twice
    
    # check no new fields
    table3 = unpack(table1, 'bar')
    expect3 = (('foo',),
               (1, 'a', 'b'),
               (2, 'c', 'd'),
               (3, 'e', 'f'))
    ieq(expect3, table3)
    
    # check maxv
    table4 = unpack(table1, 'bar', ['baz'], maxunpack=1)
    expect4 = (('foo', 'baz'),
               (1, 'a'),
               (2, 'c'),
               (3, 'e'))
    ieq(expect4, table4)
    
    # check include original
    table5 = unpack(table1, 'bar', ['baz'], maxunpack=1, include_original=True)
    expect5 = (('foo', 'bar', 'baz'),
              (1, ['a', 'b'], 'a'),
              (2, ['c', 'd'], 'c'),
              (3, ['e', 'f'], 'e'))
    ieq(expect5, table5)
    
    
def test_unpack_empty():
    
    table1 = (('foo', 'bar'),)
    table2 = unpack(table1, 'bar', ['baz', 'quux'])
    expect2 = (('foo', 'baz', 'quux'),)
    ieq(expect2, table2)
    

def _test_join_basic(join):
    
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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice
    
    # natural join
    table4 = join(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    ieq(expect4, table4) # check twice
    
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
    ieq(expect7, table7)
    
    
def _test_join_compound_keys(join):
    
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
    ieq(expect10, table10)

    # natural join on compound key
    table11 = join(table8, table9)
    expect11 = expect10
    ieq(expect11, table11)
    
    
def _test_join_string_key(join):
    
    table1 = (('id', 'colour'),
              ('aa', 'blue'),
              ('bb', 'red'),
              ('cc', 'purple'))
    table2 = (('id', 'shape'),
              ('aa', 'circle'),
              ('cc', 'square'),
              ('dd', 'ellipse'))
    
    # normal inner join
    table3 = join(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               ('aa', 'blue', 'circle'),
               ('cc', 'purple', 'square'))
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice


def _test_join_empty(join):
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),)
    table3 = join(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),)
    ieq(expect3, table3)
    
    table1 = (('id', 'colour'),)
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = join(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),)
    ieq(expect3, table3)
    

def _test_join(join):
    _test_join_basic(join)
    _test_join_compound_keys(join)
    _test_join_string_key(join)
    _test_join_empty(join)


def test_join():
    _test_join(join)
    
    
def _test_leftjoin_1(leftjoin):
    
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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice
    
    # natural join
    table4 = leftjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    
    
def _test_leftjoin_2(leftjoin):
    
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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice
    
    # natural join
    table4 = leftjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    
    
def _test_leftjoin_3(leftjoin):
    
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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice
    
    # natural join
    table4 = leftjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    
    
def _test_leftjoin_compound_keys(leftjoin):
    
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
    ieq(expect7, table7)


def _test_leftjoin_empty(leftjoin):
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),)
    table3 = leftjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', None),
               (2, 'red', None),
               (3, 'purple', None),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)
   
   
def _test_leftjoin_multiple(leftjoin):

    table1 = (('id', 'color', 'cost'), 
              (1, 'blue', 12), 
              (2, 'red', 8), 
              (3, 'purple', 4))
    
    table2 = (('id', 'shape', 'size'), 
              (1, 'circle', 'big'), 
              (1, 'circle', 'small'), 
              (2, 'square', 'tiny'), 
              (2, 'square', 'big'), 
              (3, 'ellipse', 'small'), 
              (3, 'ellipse', 'tiny'))

    actual = leftjoin(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'), 
              (1, 'blue', 12, 'circle', 'small'), 
              (2, 'red', 8, 'square', 'tiny'), 
              (2, 'red', 8, 'square', 'big'), 
              (3, 'purple', 4, 'ellipse', 'small'),
              (3, 'purple', 4, 'ellipse', 'tiny'))
    ieq(expect, actual)


def _test_leftjoin(leftjoin):
    _test_leftjoin_1(leftjoin)
    _test_leftjoin_2(leftjoin)
    _test_leftjoin_3(leftjoin)
    _test_leftjoin_compound_keys(leftjoin)
    _test_leftjoin_empty(leftjoin)
    _test_leftjoin_multiple(leftjoin)


def test_leftjoin():
    _test_leftjoin(leftjoin)
    
    
def _test_rightjoin_1(rightjoin):
    
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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice
    
    # natural join
    table4 = rightjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    
    
def _test_rightjoin_2(rightjoin):
    
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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice
    
    # natural join
    table4 = rightjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    
    
def _test_rightjoin_3(rightjoin):
    
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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice
    
    # natural join
    table4 = rightjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    
    
def _test_rightjoin_empty(rightjoin):
    
    table1 = (('id', 'colour'),)
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, None, 'circle'),
               (3, None, 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    ieq(expect3, table3)
    

def _test_rightjoin(rightjoin):
    _test_rightjoin_1(rightjoin)
    _test_rightjoin_2(rightjoin)
    _test_rightjoin_3(rightjoin)
    _test_rightjoin_empty(rightjoin)


def test_rightjoin():
    _test_rightjoin(rightjoin)


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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice

    # natural join
    table4 = outerjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    
    
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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice

    # natural join
    table4 = outerjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    
    
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
    ieq(expect3, table3)
    ieq(expect3, table3) # check twice
    
    
def test_outerjoin_empty():
    
    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),)
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, 'black', None),
               (1, 'blue', None),
               (2, 'red', None),
               (3, 'purple', None),
               (5, 'yellow', None),
               (7, 'white', None))
    ieq(expect3, table3)

    
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
    ieq(expect3, table3)
    
    
def test_crossjoin_empty():
    
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),)
    table3 = crossjoin(table1, table2)
    expect3 = (('id', 'colour', 'id', 'shape'),)
    ieq(expect3, table3)
    
    
def _test_antijoin(antijoin):
    
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
    ieq(expect3, table3)

    table4 = antijoin(table1, table2) 
    expect4 = expect3
    ieq(expect4, table4)


def _test_antijoin_empty(antijoin):
    
    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('id', 'shape'),)
    actual = antijoin(table1, table2, key='id')
    expect = table1
    ieq(expect, actual)


def test_antijoin():
    _test_antijoin(antijoin)    
    _test_antijoin_empty(antijoin)    
    
    
def _test_lookupjoin_1(lookupjoin):

    table1 = (('id', 'color', 'cost'), 
              (1, 'blue', 12), 
              (2, 'red', 8), 
              (3, 'purple', 4))
    
    table2 = (('id', 'shape', 'size'), 
              (1, 'circle', 'big'), 
              (2, 'square', 'tiny'), 
              (3, 'ellipse', 'small'))

    actual = lookupjoin(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'), 
              (2, 'red', 8, 'square', 'tiny'), 
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)
    ieq(expect, actual)

    # natural join
    actual = lookupjoin(table1, table2)
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'), 
              (2, 'red', 8, 'square', 'tiny'), 
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)
    ieq(expect, actual)


def _test_lookupjoin_2(lookupjoin):

    table1 = (('id', 'color', 'cost'), 
              (1, 'blue', 12), 
              (2, 'red', 8), 
              (3, 'purple', 4))
    
    table2 = (('id', 'shape', 'size'), 
              (1, 'circle', 'big'), 
              (1, 'circle', 'small'), 
              (2, 'square', 'tiny'), 
              (2, 'square', 'big'), 
              (3, 'ellipse', 'small'), 
              (3, 'ellipse', 'tiny'))

    actual = lookupjoin(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'), 
              (2, 'red', 8, 'square', 'tiny'), 
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)
    ieq(expect, actual)


def _test_lookupjoin(lookupjoin):
    _test_lookupjoin_1(lookupjoin)
    _test_lookupjoin_2(lookupjoin)


def test_lookupjoin():
    _test_lookupjoin(lookupjoin)


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
    ieq(expect2, table2)
    ieq(expect2, table2)
    
    
def test_transpose_empty():
    table1 = (('id', 'colour'),)
    table2 = transpose(table1)
    expect2 = (('id',),
               ('colour',))
    ieq(expect2, table2)
    
    
def _test_intersection_1(intersection):

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
    ieq(expectation, result)
    
    
def _test_intersection_2(intersection):

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
    ieq(expect, table3)
    
    
def _test_intersection_3(intersection):

    # empty table
    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 7))
    
    table2 = (('foo', 'bar'),)
    
    expectation = (('foo', 'bar'),)
    result = intersection(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)
    
    
def _test_intersection_4(intersection):

    # duplicate rows
    
    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('B', 2),
              ('B', 2),
              ('C', 7))
    
    table2 = (('foo', 'bar'),
              ('A', 9),
              ('B', 2),
              ('B', 2),
              ('B', 3))
    
    expectation = (('foo', 'bar'),
                   ('B', 2),
                   ('B', 2))
    
    result = intersection(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)
    
    
def _test_intersection_empty(intersection):

    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 7))
    table2 = (('foo', 'bar'),)
    expectation = (('foo', 'bar'),)
    result = intersection(table1, table2)
    ieq(expectation, result)
    
    
def _test_intersection(intersection):
    _test_intersection_1(intersection)
    _test_intersection_2(intersection)
    _test_intersection_3(intersection)
    _test_intersection_4(intersection)
    _test_intersection_empty(intersection)


def test_intersection():
    _test_intersection(intersection)
    
    
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
    ieq(expect2, table2)
    ieq(expect2, table2)
    
    
def test_pivot_empty():
    
    table1 = (('region', 'gender', 'style', 'units'),)
    table2 = pivot(table1, 'region', 'gender', 'units', sum)
    expect2 = (('region',),)
    ieq(expect2, table2)
    
    
def test_hashjoin():
    _test_join(hashjoin)


def test_hashleftjoin():
    _test_leftjoin(hashleftjoin)


def test_hashrightjoin():
    _test_rightjoin(hashrightjoin)
    

def test_hashantijoin():
    _test_antijoin(hashantijoin)    
    _test_antijoin_empty(hashantijoin)    

    
def test_hashcomplement():
    _test_complement(hashcomplement)


def test_hashintersection():
    _test_intersection(hashintersection)
    

def test_hashlookupjoin():
    _test_lookupjoin(hashlookupjoin)


def test_flatten():

    table1 = (('foo', 'bar', 'baz'),
              ('A', 1, True),
              ('C', 7, False),
              ('B', 2, False),
              ('C', 9, True))

    expect1 = ('A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9, True)

    actual1 = flatten(table1)
    ieq(expect1, actual1)
    ieq(expect1, actual1)
    
    
def test_flatten_empty():

    table1 = (('foo', 'bar', 'baz'),)
    expect1 = []
    actual1 = flatten(table1)
    ieq(expect1, actual1)
    
    
def test_unflatten():
    
    table1 = (('lines',),
              ('A',), 
              (1,), 
              (True,), 
              ('C',), 
              (7,), 
              (False,),
              ('B',), 
              (2,), 
              (False,),
              ('C'), 
              (9,))
    
    expect1 = (('f0', 'f1', 'f2'),
               ('A', 1, True),
               ('C', 7, False),
               ('B', 2, False),
               ('C', 9, None))
    
    actual1 = unflatten(table1, 'lines', 3)

    ieq(expect1, actual1)
    ieq(expect1, actual1)
    
    
def test_unflatten_2():
    
    inpt = ('A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9)
    
    expect1 = (('f0', 'f1', 'f2'),
               ('A', 1, True),
               ('C', 7, False),
               ('B', 2, False),
               ('C', 9, None))
    
    actual1 = unflatten(inpt, 3)

    ieq(expect1, actual1)
    ieq(expect1, actual1)
    
    
def test_unflatten_empty():
    
    table1 = (('lines',),)
    expect1 = (('f0', 'f1', 'f2'),)
    actual1 = unflatten(table1, 'lines', 3)
    print list(actual1)
    ieq(expect1, actual1)


def test_mergesort_1():
    
    table1 = (('foo', 'bar'),
              ('A', 6),
              ('C', 2),
              ('D', 10),
              ('A', 9),
              ('F', 1))
    
    table2 = (('foo', 'bar'),
              ('B', 3),
              ('D', 10),
              ('A', 10),
              ('F', 4))
    
    # should be same as concatenate then sort (but more efficient, esp. when 
    # presorted)
    expect = sort(cat(table1, table2)) 
    
    actual = mergesort(table1, table2)
    ieq(expect, actual)
    ieq(expect, actual)
    
    actual = mergesort(sort(table1), sort(table2), presorted=True)
    ieq(expect, actual)
    ieq(expect, actual)
    
    
def test_mergesort_2():
    
    table1 = (('foo', 'bar'),
              ('A', 9),
              ('C', 2),
              ('D', 10),
              ('A', 6),
              ('F', 1))
    
    table2 = (('foo', 'baz'),
              ('B', 3),
              ('D', 10),
              ('A', 10),
              ('F', 4))
    
    # should be same as concatenate then sort (but more efficient, esp. when 
    # presorted)
    expect = sort(cat(table1, table2), key='foo') 
    
    actual = mergesort(table1, table2, key='foo')
    ieq(expect, actual)
    ieq(expect, actual)
    
    actual = mergesort(sort(table1, key='foo'), sort(table2, key='foo'), key='foo', presorted=True)
    ieq(expect, actual)
    ieq(expect, actual)
    
    
def test_mergesort_3():
    
    table1 = (('foo', 'bar'),
              ('A', 9),
              ('C', 2),
              ('D', 10),
              ('A', 6),
              ('F', 1))
    
    table2 = (('foo', 'baz'),
              ('B', 3),
              ('D', 10),
              ('A', 10),
              ('F', 4))
    
    # should be same as concatenate then sort (but more efficient, esp. when 
    # presorted)
    expect = sort(cat(table1, table2), key='foo', reverse=True) 
    
    actual = mergesort(table1, table2, key='foo', reverse=True)
    ieq(expect, actual)
    ieq(expect, actual)
    
    actual = mergesort(sort(table1, key='foo', reverse=True), 
                       sort(table2, key='foo', reverse=True), 
                       key='foo', reverse=True, presorted=True)
    ieq(expect, actual)
    ieq(expect, actual)


def test_mergesort_4():
    
    table1 = (('foo', 'bar', 'baz'),
              (1, 'A', True),
              (2, 'B', None),
              (4, 'C', True))
    table2 = (('bar', 'baz', 'quux'),
              ('A', True, 42.0),
              ('B', False, 79.3),
              ('C', False, 12.4))

    expect = sort(cat(table1, table2), key='bar') 
    
    actual = mergesort(table1, table2, key='bar')
    ieq(expect, actual)
    ieq(expect, actual)


def test_mergesort_empty():
    
    table1 = (('foo', 'bar'),
              ('A', 9),
              ('C', 2),
              ('D', 10),
              ('F', 1))
    
    table2 = (('foo', 'bar'),)
    
    expect = table1
    actual = mergesort(table1, table2, key='foo')
    ieq(expect, actual)
    ieq(expect, actual)
    
    
def test_annex():

    table1 = (('foo', 'bar'),
              ('A', 9),
              ('C', 2),
              ('F', 1))
    table2 = (('foo', 'baz'),
              ('B', 3),
              ('D', 10))
    expect = (('foo', 'bar', 'foo', 'baz'),
              ('A', 9, 'B', 3),
              ('C', 2, 'D', 10),
              ('F', 1, None, None))
    actual = annex(table1, table2)
    ieq(expect, actual)
    ieq(expect, actual)

    expect21 = (('foo', 'baz', 'foo', 'bar'),
                ('B', 3, 'A', 9),
                ('D', 10, 'C', 2),
                (None, None, 'F', 1))
    actual21 = annex(table2, table1)
    ieq(expect21, actual21)
    ieq(expect21, actual21)


def test_annex_uneven_rows():

    table1 = (('foo', 'bar'),
              ('A', 9, True),
              ('C', 2),
              ('F',))
    table2 = (('foo', 'baz'),
              ('B', 3),
              ('D', 10))
    expect = (('foo', 'bar', 'foo', 'baz'),
              ('A', 9, 'B', 3),
              ('C', 2, 'D', 10),
              ('F', None, None, None))
    actual = annex(table1, table2)
    ieq(expect, actual)
    ieq(expect, actual)


def test_unpackdict():
    
    table1 = (('foo', 'bar'),
              (1, {'baz': 'a', 'quux': 'b'}),
              (2, {'baz': 'c', 'quux': 'd'}),
              (3, {'baz': 'e', 'quux': 'f'}))
    table2 = unpackdict(table1, 'bar')
    expect2 = (('foo', 'baz', 'quux'),
               (1, 'a', 'b'),
               (2, 'c', 'd'),
               (3, 'e', 'f'))
    ieq(expect2, table2)
    ieq(expect2, table2) # check twice
    
    # test include original
    table1 = (('foo', 'bar'),
              (1, {'baz': 'a', 'quux': 'b'}),
              (2, {'baz': 'c', 'quux': 'd'}),
              (3, {'baz': 'e', 'quux': 'f'}))
    table2 = unpackdict(table1, 'bar', includeoriginal=True)
    expect2 = (('foo', 'bar', 'baz', 'quux'),
               (1, {'baz': 'a', 'quux': 'b'}, 'a', 'b'),
               (2, {'baz': 'c', 'quux': 'd'}, 'c', 'd'),
               (3, {'baz': 'e', 'quux': 'f'}, 'e', 'f'))
    ieq(expect2, table2)
    ieq(expect2, table2) # check twice

    # test specify keys    
    table1 = (('foo', 'bar'),
              (1, {'baz': 'a', 'quux': 'b'}),
              (2, {'baz': 'c', 'quux': 'd'}),
              (3, {'baz': 'e', 'quux': 'f'}))
    table2 = unpackdict(table1, 'bar', keys=['quux'])
    expect2 = (('foo', 'quux'),
               (1, 'b'),
               (2, 'd'),
               (3, 'f'))
    ieq(expect2, table2)
    ieq(expect2, table2) # check twice
    
    # test dodgy data    
    table1 = (('foo', 'bar'),
              (1, {'baz': 'a', 'quux': 'b'}),
              (2, 'foobar'),
              (3, {'baz': 'e', 'quux': 'f'}))
    table2 = unpackdict(table1, 'bar')
    expect2 = (('foo', 'baz', 'quux'),
               (1, 'a', 'b'),
               (2, None, None),
               (3, 'e', 'f'))
    ieq(expect2, table2)
    ieq(expect2, table2) # check twice
    

def test_fold():
    
    t1 = (('id', 'count'), (1, 3), (1, 5), (2, 4), (2, 8))        
    t2 = fold(t1, 'id', operator.add, 'count', presorted=True)
    expect = (('key', 'value'), (1, 8), (2, 12))
    ieq(expect, t2)
    ieq(expect, t2)


def test_addrownumbers():

    table1 = (('foo', 'bar'),
              ('A', 9),
              ('C', 2),
              ('F', 1))

    expect = (('row', 'foo', 'bar'),
              (1, 'A', 9),
              (2, 'C', 2),
              (3, 'F', 1))
    actual = addrownumbers(table1)
    ieq(expect, actual)
    ieq(expect, actual)
    
    
def test_search():
    
    table1 = (('foo', 'bar', 'baz'),
              ('orange', 12, 'oranges are nice fruit'),
              ('mango', 42, 'I like them'),
              ('banana', 74, 'lovely too'),
              ('cucumber', 41, 'better than mango'))
    
    # search any field
    table2 = search(table1, '.g.')
    expect2 = (('foo', 'bar', 'baz'),
               ('orange', 12, 'oranges are nice fruit'),
               ('mango', 42, 'I like them'),
               ('cucumber', 41, 'better than mango'))
    ieq(expect2, table2)
    ieq(expect2, table2)
    
    # search a specific field
    table3 = search(table1, 'foo', '.g.')
    expect3 = (('foo', 'bar', 'baz'),
               ('orange', 12, 'oranges are nice fruit'),
               ('mango', 42, 'I like them'))
    ieq(expect3, table3)
    ieq(expect3, table3)
    
    
def test_addcolumn():
    
    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2))
    
    col = [True, False]
    
    expect2 = (('foo', 'bar', 'baz'),
               ('A', 1, True),
               ('B', 2, False))
    table2 = addcolumn(table1, 'baz', col)
    ieq(expect2, table2)
    ieq(expect2, table2)

    # test short column
    table3 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 2))
    expect4 = (('foo', 'bar', 'baz'),
               ('A', 1, True),
               ('B', 2, False),
               ('C', 2, None))
    table4 = addcolumn(table3, 'baz', col)
    ieq(expect4, table4)
    
    # test short table
    col = [True, False, False]
    expect5 = (('foo', 'bar', 'baz'),
               ('A', 1, True),
               ('B', 2, False),
               (None, None, False))
    table5 = addcolumn(table1, 'baz', col)
    ieq(expect5, table5)


def test_filldown():
    
    table = (('foo', 'bar', 'baz'),
             (1, 'a', None),
             (1, None, .23),
             (1, 'b', None),
             (2, None, None),
             (2, None, .56),
             (2, 'c', None),
             (None, 'c', .72))
    
    actual = filldown(table)
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', None),
              (1, 'a', .23),
              (1, 'b', .23),
              (2, 'b', .23),
              (2, 'b', .56),
              (2, 'c', .56),
              (2, 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual)

    actual = filldown(table, 'bar')
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', None),
              (1, 'a', .23),
              (1, 'b', None),
              (2, 'b', None),
              (2, 'b', .56),
              (2, 'c', None),
              (None, 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual)

    actual = filldown(table, 'foo', 'bar')
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', None),
              (1, 'a', .23),
              (1, 'b', None),
              (2, 'b', None),
              (2, 'b', .56),
              (2, 'c', None),
              (2, 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual)


def test_fillright():
    
    table = (('foo', 'bar', 'baz'),
             (1, 'a', None),
             (1, None, .23),
             (1, 'b', None),
             (2, None, None),
             (2, None, .56),
             (2, 'c', None),
             (None, 'c', .72))
    
    actual = fillright(table)
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', 'a'),
              (1, 1, .23),
              (1, 'b', 'b'),
              (2, 2, 2),
              (2, 2, .56),
              (2, 'c', 'c'),
              (None, 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual)


def test_fillleft():
    
    table = (('foo', 'bar', 'baz'),
             (1, 'a', None),
             (1, None, .23),
             (1, 'b', None),
             (2, None, None),
             (None, None, .56),
             (2, 'c', None),
             (None, 'c', .72))
    
    actual = fillleft(table)
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', None),
              (1, .23, .23),
              (1, 'b', None),
              (2, None, None),
              (.56, .56, .56),
              (2, 'c', None),
              ('c', 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual) 
    

def test_multirangeaggregate():

    t1 = (('x', 'y', 'z'),
          (1, 3, 9),
          (2, 3, 12),
          (4, 2, 17),
          (2, 7, 3),
          (1, 6, 1))

    # I'm dubious about whether this would ever be useful, where no minimum
    # or maximum is given, because the second level minimums could then be 
    # different under different first level bins, and usually what you want is
    # a consistent grid.
        
    t2 = multirangeaggregate(t1, keys=('x', 'y'), widths=(2, 2), aggregation=len)
    e2 = (('key', 'value'),
          (((1, 3), (3, 5)), 2),
          (((1, 3), (5, 7)), 1),
          (((1, 3), (7, 9)), 1),
          (((3, 5), (2, 4)), 1))
    ieq(e2, t2)
    ieq(e2, t2)

    # Explicit mins - at least here the grid minimums would be consistent, 
    # however the grid might be sparse because bins are only created as long as
    # there is data, and again usually what you want is a consistent grid, not
    # missing cells.

    t3 = multirangeaggregate(t1, keys=('x', 'y'), widths=(2, 2), aggregation=len, 
                             mins=(0, 0))
    e3 = (('key', 'value'),
          (((0, 2), (0, 2)), 0),
          (((0, 2), (2, 4)), 1),
          (((0, 2), (4, 6)), 0),
          (((0, 2), (6, 8)), 1),
          (((2, 4), (0, 2)), 0),
          (((2, 4), (2, 4)), 1),
          (((2, 4), (4, 6)), 0),
          (((2, 4), (6, 8)), 1),
          (((4, 6), (0, 2)), 0),
          (((4, 6), (2, 4)), 1))
    ieq(e3, t3)

    # Explicit mins and maxs - this is probably the only sensible version of the
    # function, and the most straightforward to implement.
    
    t4 = multirangeaggregate(t1, keys=('x', 'y'), widths=(2, 2), aggregation=len, 
                             mins=(0, 0), maxs=(4, 6))
    e4 = (('key', 'value'),
          (((0, 2), (0, 2)), 0),
          (((0, 2), (2, 4)), 1),
          (((0, 2), (4, 6)), 1),
          (((2, 4), (0, 2)), 0),
          (((2, 4), (2, 4)), 2),
          (((2, 4), (4, 6)), 0))
    ieq(e4, t4)
    
    # Test a different aggregation function.
    
    t5 = multirangeaggregate(t1, keys=('x', 'y'), widths=(2, 2), aggregation=sum, 
                             value='z')
    e5 = (('key', 'value'),
          (((1, 3), (3, 5)), 21),
          (((1, 3), (5, 7)), 1),
          (((1, 3), (7, 9)), 3),
          (((3, 5), (2, 4)), 17))
    ieq(e5, t5)

    # Check different explicit mins and maxs.
    
    t6 = multirangeaggregate(t1, keys=('x', 'y'), widths=(2, 2), aggregation=len, 
                             mins=(-2, 0), maxs=(4, 6))
    e6 = (('key', 'value'),
          (((-2, 0), (0, 2)), 0),
          (((-2, 0), (2, 4)), 0),
          (((-2, 0), (4, 6)), 0),
          (((0, 2), (0, 2)), 0),
          (((0, 2), (2, 4)), 1),
          (((0, 2), (4, 6)), 1),
          (((2, 4), (0, 2)), 0),
          (((2, 4), (2, 4)), 2),
          (((2, 4), (4, 6)), 0))
    ieq(e6, t6)
    
    # check explicit mins and maxs with aggregation function over value
    t7 = multirangeaggregate(t1, keys=('x', 'y'), widths=(2, 2), aggregation=sum, 
                             mins=(-2, 0), maxs=(4, 6), value='z')
    e7 = (('key', 'value'),
          (((-2, 0), (0, 2)), 0),
          (((-2, 0), (2, 4)), 0),
          (((-2, 0), (4, 6)), 0),
          (((0, 2), (0, 2)), 0),
          (((0, 2), (2, 4)), 9),
          (((0, 2), (4, 6)), 1),
          (((2, 4), (0, 2)), 0),
          (((2, 4), (2, 4)), 29),
          (((2, 4), (4, 6)), 0))
    ieq(e7, t7)

def test_multirangeaggregate_empty():
    
    # Check sanity with empty input.
    
    t1 = (('x', 'y', 'z'),)

    # If no mins or maxs are given, output will be empty also.
    
    t2 = multirangeaggregate(t1, keys=('x', 'y'), widths=(2, 2), aggregation=len)
    e2 = (('key', 'value'),)
    ieq(e2, t2)
    
    # It only mins are given, output will be empty also.

    t3 = multirangeaggregate(t1, keys=('x', 'y'), widths=(2, 2), aggregation=len, 
                             mins=(0, 0))
    ieq(e2, t3)

    # If mins and maxs are given, then aggregation function will be applied for
    # each bin to an empty list of rows. This is probably the most useful form
    # of the function.
        
    t4 = multirangeaggregate(t1, keys=('x', 'y'), widths=(2, 2), aggregation=len, 
                             mins=(0, 0), maxs=(4, 4))
    e4 = (('key', 'value'),
          (((0, 2), (0, 2)), 0),
          (((0, 2), (2, 4)), 0),
          (((2, 4), (0, 2)), 0),
          (((2, 4), (2, 4)), 0))
    ieq(e4, t4)
    
    
def test_unjoin_implicit_key():

    # test the case where the join key needs to be reconstructed
        
    table1 = (('foo', 'bar'),
              (1, 'apple'),
              (2, 'apple'),
              (3, 'orange'))
    
    expect_left = (('foo', 'bar_id'),
                   (1, 1),
                   (2, 1),
                   (3, 2))
    expect_right = (('id', 'bar'),
                    (1, 'apple'),
                    (2, 'orange'))
    
    left, right = unjoin(table1, 'bar')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)

    
def test_unjoin_explicit_key():

    # test the case where the join key is still present
    
    table2 = (('Customer ID', 'First Name', 'Surname', 'Telephone Number'),
              (123, 'Robert', 'Ingram', '555-861-2025'),
              (456, 'Jane', 'Wright', '555-403-1659'),
              (456, 'Jane', 'Wright', '555-776-4100'),
              (789, 'Maria', 'Fernandez', '555-808-9633'))
    
    expect_left = (('Customer ID', 'First Name', 'Surname'),
                   (123, 'Robert', 'Ingram'),
                   (456, 'Jane', 'Wright'),
                   (789, 'Maria', 'Fernandez'))
    expect_right = (('Customer ID', 'Telephone Number'),
                    (123, '555-861-2025'),
                    (456, '555-403-1659'),
                    (456, '555-776-4100'),
                    (789, '555-808-9633'))
    left, right = unjoin(table2, 'Telephone Number', key='Customer ID')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_2():
    
    table3 = (('Employee', 'Skill', 'Current Work Location'),
              ('Jones', 'Typing', '114 Main Street'),
              ('Jones', 'Shorthand', '114 Main Street'),
              ('Jones', 'Whittling', '114 Main Street'),
              ('Bravo', 'Light Cleaning', '73 Industrial Way'),
              ('Ellis', 'Alchemy', '73 Industrial Way'),
              ('Ellis', 'Flying', '73 Industrial Way'),
              ('Harrison', 'Light Cleaning', '73 Industrial Way'))
    # N.B., we do expect rows will get sorted
    expect_left = (('Employee', 'Current Work Location'),
                   ('Bravo', '73 Industrial Way'),
                   ('Ellis', '73 Industrial Way'),
                   ('Harrison', '73 Industrial Way'),
                   ('Jones', '114 Main Street'))
    expect_right = (('Employee', 'Skill'),
                    ('Bravo', 'Light Cleaning'),
                    ('Ellis', 'Alchemy'),
                    ('Ellis', 'Flying'),
                    ('Harrison', 'Light Cleaning'),
                    ('Jones', 'Shorthand'),
                    ('Jones', 'Typing'),
                    ('Jones', 'Whittling'))
    left, right = unjoin(table3, 'Skill', key='Employee')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_3():
    
    table4 = (('Tournament', 'Year', 'Winner', 'Date of Birth'),
              ('Indiana Invitational', 1998, 'Al Fredrickson', '21 July 1975'),
              ('Cleveland Open', 1999, 'Bob Albertson', '28 September 1968'),
              ('Des Moines Masters', 1999, 'Al Fredrickson', '21 July 1975'),
              ('Indiana Invitational', 1999, 'Chip Masterson', '14 March 1977'))
    
    # N.B., we do expect rows will get sorted
    expect_left = (('Tournament', 'Year', 'Winner'),
                   ('Cleveland Open', 1999, 'Bob Albertson'),
                   ('Des Moines Masters', 1999, 'Al Fredrickson'),
                   ('Indiana Invitational', 1998, 'Al Fredrickson'),
                   ('Indiana Invitational', 1999, 'Chip Masterson'))    
    expect_right = (('Winner', 'Date of Birth'),
                    ('Al Fredrickson', '21 July 1975'),
                    ('Bob Albertson', '28 September 1968'),
                    ('Chip Masterson', '14 March 1977'))
    left, right = unjoin(table4, 'Date of Birth', key='Winner')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_4():
    
    table5 = (('Restaurant', 'Pizza Variety', 'Delivery Area'),
            ('A1 Pizza', 'Thick Crust', 'Springfield'),
            ('A1 Pizza', 'Thick Crust', 'Shelbyville'),
            ('A1 Pizza', 'Thick Crust', 'Capital City'),
            ('A1 Pizza', 'Stuffed Crust', 'Springfield'),
            ('A1 Pizza', 'Stuffed Crust', 'Shelbyville'),
            ('A1 Pizza', 'Stuffed Crust', 'Capital City'),
            ('Elite Pizza', 'Thin Crust', 'Capital City'),
            ('Elite Pizza', 'Stuffed Crust', 'Capital City'),
            ("Vincenzo's Pizza", "Thick Crust", "Springfield"),
            ("Vincenzo's Pizza", "Thick Crust", "Shelbyville"),
            ("Vincenzo's Pizza", "Thin Crust", "Springfield"),
            ("Vincenzo's Pizza", "Thin Crust", "Shelbyville"))
    
    # N.B., we do expect rows will get sorted
    expect_left = (('Restaurant', 'Pizza Variety'),
            ('A1 Pizza', 'Stuffed Crust'),
            ('A1 Pizza', 'Thick Crust'),
            ('Elite Pizza', 'Stuffed Crust'),
            ('Elite Pizza', 'Thin Crust'),
            ("Vincenzo's Pizza", "Thick Crust"),
            ("Vincenzo's Pizza", "Thin Crust"))  
    expect_right = (('Restaurant', 'Delivery Area'),
            ('A1 Pizza', 'Capital City'),
            ('A1 Pizza', 'Shelbyville'),
            ('A1 Pizza', 'Springfield'),
            ('Elite Pizza', 'Capital City'),
            ("Vincenzo's Pizza", "Shelbyville"),
            ("Vincenzo's Pizza", "Springfield"))
    left, right = unjoin(table5, 'Delivery Area', key='Restaurant')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_5():
    
    table6 = (('ColA', 'ColB', 'ColC'),
            ('A', 1, 'apple'),
            ('B', 1, 'apple'),
            ('C', 2, 'orange'),
            ('D', 3, 'lemon'),
            ('E', 3, 'lemon'))
    
    # N.B., we do expect rows will get sorted
    expect_left = (('ColA', 'ColB'),
            ('A', 1),
            ('B', 1),
            ('C', 2),
            ('D', 3),
            ('E', 3))  
    expect_right = (('ColB', 'ColC'),
            (1, 'apple'),
            (2, 'orange'),
            (3, 'lemon'))
    left, right = unjoin(table6, 'ColC', key='ColB')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)

