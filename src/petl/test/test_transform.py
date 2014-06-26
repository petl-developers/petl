"""
Tests for the petl.transform module.

"""

from datetime import datetime
from nose.tools import eq_
import operator


from petl.testutils import ieq
from petl import rename, fieldnames, cut, cat, addfield, \
    rowslice, head, tail, sort, \
    select, complement, diff, \
    expr, facet, \
    unpack, \
    rangefacet, selectre, rowselect, \
    rowlenselect, intersection, \
    recorddiff, recordcomplement, cutout, skipcomments, \
    hashcomplement, hashintersection, \
    mergesort, annex, unpackdict, \
    selectin, fold, addrownumbers, selectcontains, search, \
    addcolumn, \
    multirangeaggregate, unjoin, coalesce, nrows, \
    empty, selectusingcontext, addfieldusingcontext, \
    prefixheader, suffixheader, movefield


def test_rename():

    table = (('foo', 'bar'),
             ('M', 12),
             ('F', 34),
             ('-', 56))
    
    result = rename(table, 'foo', 'foofoo')
    assert fieldnames(result) == ['foofoo', 'bar']
    
    result = rename(table, 0, 'foofoo')
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
            
    
def test_sort_buffered_independent():
    
    table = (('foo', 'bar'),
             ('C', 2),
             ('A', 9),
             ('A', 6),
             ('F', 1),
             ('D', 10))
    expectation = (('foo', 'bar'),
                   ('F', 1),
                   ('C', 2),
                   ('A', 6),
                   ('A', 9),
                   ('D', 10))

    result = sort(table, 'bar', buffersize=4)
    nrows(result) # cause data to be cached
    # check that two row iterators are independent, i.e., consuming rows
    # from one does not affect the other
    it1 = iter(result)
    it2 = iter(result)
    eq_(expectation[0], it1.next())
    eq_(expectation[1], it1.next())
    eq_(expectation[0], it2.next())
    eq_(expectation[1], it2.next())
    eq_(expectation[2], it2.next())
    eq_(expectation[2], it1.next())
            
    
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
    
    
def _test_complement_1(complement_impl):

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
    
    result = complement_impl(table1, table2)
    ieq(expectation, result)
    
    
def _test_complement_2(complement_impl):

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
    
    result = complement_impl(tablea, tableb)
    ieq(aminusb, result)
    
    bminusa = (('x', 'y', 'z'),
               ('A', 9, False),
               ('B', 3, True))
    
    result = complement_impl(tableb, tablea)
    ieq(bminusa, result)
    

def _test_complement_3(complement_impl):

    # make sure we deal with empty tables
    
    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2))
    
    table2 = (('foo', 'bar'),)
    
    expectation = (('foo', 'bar'),
                   ('A', 1),
                   ('B', 2))
    result = complement_impl(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)
    
    expectation = (('foo', 'bar'),)
    result = complement_impl(table2, table1)
    ieq(expectation, result)
    ieq(expectation, result)
    
    
def _test_complement_4(complement_impl):

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
    
    result = complement_impl(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)


def _test_complement_none(complement_impl):
    # test behaviour with unsortable types
    now = datetime.now()

    ta = [['a', 'b'], [None, None]]
    tb = [['a', 'b'], [None, now]]

    expectation = (('a', 'b'), (None, None))
    result = complement_impl(ta, tb)
    ieq(expectation, result)

    ta = [['a'], [now], [None]]
    tb = [['a'], [None], [None]]

    expectation = (('a',), (now,))
    result = complement_impl(ta, tb)
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
    

def test_select():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = select(table, lambda rec: rec[0] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual) # check can iterate twice
 
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
 
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = select(table, lambda rec: rec.foo == 'a')
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
               (1,),
               (2,),
               (3,))
    ieq(expect3, table3)
    
    # check more values than new fields
    table4 = unpack(table1, 'bar', ['baz'])
    expect4 = (('foo', 'baz'),
               (1, 'a'),
               (2, 'c'),
               (3, 'e'))
    ieq(expect4, table4)
    
    # check include original
    table5 = unpack(table1, 'bar', ['baz'], include_original=True)
    expect5 = (('foo', 'bar', 'baz'),
              (1, ['a', 'b'], 'a'),
              (2, ['c', 'd'], 'c'),
              (3, ['e', 'f'], 'e'))
    ieq(expect5, table5)

    # check specify number to unpack
    table6 = unpack(table1, 'bar', 3)
    expect6 = (('foo', 'bar1', 'bar2', 'bar3'),
               (1, 'a', 'b', None),
               (2, 'c', 'd', None),
               (3, 'e', 'f', None))
    ieq(expect6, table6)

    # check specify number to unpack, non-default missing value
    table7 = unpack(table1, 'bar', 3, missing='NA')
    expect7 = (('foo', 'bar1', 'bar2', 'bar3'),
               (1, 'a', 'b', 'NA'),
               (2, 'c', 'd', 'NA'),
               (3, 'e', 'f', 'NA'))
    ieq(expect7, table7)

    # check can use field index
    table8 = unpack(table1, 1, 3)
    expect8 = (('foo', 'bar1', 'bar2', 'bar3'),
               (1, 'a', 'b', None),
               (2, 'c', 'd', None),
               (3, 'e', 'f', None))
    ieq(expect8, table8)


def test_unpack_empty():
    
    table1 = (('foo', 'bar'),)
    table2 = unpack(table1, 'bar', ['baz', 'quux'])
    expect2 = (('foo', 'baz', 'quux'),)
    ieq(expect2, table2)
    

def _test_intersection_1(intersection_impl):

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
    
    result = intersection_impl(table1, table2)
    ieq(expectation, result)
    
    
def _test_intersection_2(intersection_impl):

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
    
    table3 = intersection_impl(table1, table2)
    ieq(expect, table3)
    
    
def _test_intersection_3(intersection_impl):

    # empty table
    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 7))
    
    table2 = (('foo', 'bar'),)
    
    expectation = (('foo', 'bar'),)
    result = intersection_impl(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)
    
    
def _test_intersection_4(intersection_impl):

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
    
    result = intersection_impl(table1, table2)
    ieq(expectation, result)
    ieq(expectation, result)
    
    
def _test_intersection_empty(intersection_impl):

    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 7))
    table2 = (('foo', 'bar'),)
    expectation = (('foo', 'bar'),)
    result = intersection_impl(table1, table2)
    ieq(expectation, result)
    
    
def _test_intersection(intersection_impl):
    _test_intersection_1(intersection_impl)
    _test_intersection_2(intersection_impl)
    _test_intersection_3(intersection_impl)
    _test_intersection_4(intersection_impl)
    _test_intersection_empty(intersection_impl)


def test_intersection():
    _test_intersection(intersection)
    
    
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


def test_empty_addcolumn():

    table1 = empty()
    table2 = addcolumn(table1, 'foo', ['A', 'B'])
    table3 = addcolumn(table2, 'bar', [1, 2])
    expect = (('foo', 'bar'),
              ('A', 1),
              ('B', 2))
    ieq(expect, table3)
    ieq(expect, table3)


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


def test_selectusingcontext():

    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 4),
              ('C', 5),
              ('D', 9))

    expect = (('foo', 'bar'),
              ('B', 4),
              ('C', 5))

    def query(prv, cur, nxt):
        return ((prv is not None and (cur.bar - prv.bar) < 2)
                or (nxt is not None and (nxt.bar - cur.bar) < 2))

    actual = selectusingcontext(table1, query)
    ieq(expect, actual)
    ieq(expect, actual)


def test_addfieldusingcontext():

    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 4),
              ('C', 5),
              ('D', 9))

    expect = (('foo', 'bar', 'baz', 'quux'),
              ('A', 1, None, 3),
              ('B', 4, 3, 1),
              ('C', 5, 1, 4),
              ('D', 9, 4, None))

    def upstream(prv, cur, nxt):
        if prv is None:
            return None
        else:
            return cur.bar - prv.bar

    def downstream(prv, cur, nxt):
        if nxt is None:
            return None
        else:
            return nxt.bar - cur.bar

    table2 = addfieldusingcontext(table1, 'baz', upstream)
    table3 = addfieldusingcontext(table2, 'quux', downstream)
    ieq(expect, table3)
    ieq(expect, table3)


def test_prefixheader():

    table1 = (('foo', 'bar'),
              (1, 'A'),
              (2, 'B'))

    expect = (('pre_foo', 'pre_bar'),
              (1, 'A'),
              (2, 'B'))

    actual = prefixheader(table1, 'pre_')
    ieq(expect, actual)
    ieq(expect, actual)


def test_suffixheader():

    table1 = (('foo', 'bar'),
              (1, 'A'),
              (2, 'B'))

    expect = (('foo_suf', 'bar_suf'),
              (1, 'A'),
              (2, 'B'))

    actual = suffixheader(table1, '_suf')
    ieq(expect, actual)
    ieq(expect, actual)


def test_movefield():

    table1 = (('foo', 'bar', 'baz'),
              (1, 'A', True),
              (2, 'B', False))

    expect = (('bar', 'foo', 'baz'),
              ('A', 1, True),
              ('B', 2, False))

    actual = movefield(table1, 'bar', 0)
    ieq(expect, actual)
    ieq(expect, actual)

    actual = movefield(table1, 'foo', 1)
    ieq(expect, actual)
    ieq(expect, actual)


def test_hashcomplement():
    _test_complement(hashcomplement)


def test_hashintersection():
    _test_intersection(hashintersection)


