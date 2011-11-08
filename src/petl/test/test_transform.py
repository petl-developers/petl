"""
Tests for the petl.transform module.

"""


from collections import OrderedDict

from petl.testfun import iassertequal
from petl import rename, fieldnames, project, cat, convert, translate, extend, \
                rowslice, head, tail, sort, melt, recast, duplicates, conflicts, \
                merge, select, complement, diff, capture, \
                split, expr, fieldmap, facet


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
    """Test the project function."""
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    cut1 = project(table, 'foo')
    expectation = [['foo'],
                   ['A'],
                   ['B'],
                   [u'B'],
                   ['D'],
                   ['E']]
    iassertequal(expectation, cut1)
    
    cut2 = project(table, 'foo', 'baz')
    expectation = [['foo', 'baz'],
                   ['A', 2],
                   ['B', '3.4'],
                   [u'B', u'7.8'],
                   ['D', 9.0],
                   ['E', None]]
    iassertequal(expectation, cut2)
    
    cut3 = project(table, 0, 2)
    expectation = [['foo', 'baz'],
                   ['A', 2],
                   ['B', '3.4'],
                   [u'B', u'7.8'],
                   ['D', 9.0],
                   ['E', None]]
    iassertequal(expectation, cut3)
    
    cut4 = project(table, 'bar', 0)
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


def test_extend():
    """Test the extend function."""

    table = [['foo', 'bar'],
             ['M', 12],
             ['F', 34],
             ['-', 56]]
    
    result = extend(table, 'baz', 42)
    expectation = [['foo', 'bar', 'baz'],
                   ['M', 12, 42],
                   ['F', 34, 42],
                   ['-', 56, 42]]
    iassertequal(expectation, result)

    result = extend(table, 'baz', lambda rec: rec['bar'] * 2)
    expectation = [['foo', 'bar', 'baz'],
                   ['M', 12, 24],
                   ['F', 34, 68],
                   ['-', 56, 112]]
    iassertequal(expectation, result)

    result = extend(table, 'baz', expr('{bar} * 2'))
    expectation = [['foo', 'bar', 'baz'],
                   ['M', 12, 24],
                   ['F', 34, 68],
                   ['-', 56, 112]]
    iassertequal(expectation, result)


def test_rowslice():
    """Test the rowslice function."""
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             [u'B', u'3', u'7.8', True],
             ['D', 'xyz', 9.0],
             ['E', None]]

    result = rowslice(table, 0, 2)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2],
                   ['B', '2', '3.4']]
    iassertequal(expectation, result)

    result = rowslice(table, 1, 2)
    expectation = [['foo', 'bar', 'baz'],
                   ['B', '2', '3.4']]
    iassertequal(expectation, result)

    result = rowslice(table, 1, 5, 2)
    expectation = [['foo', 'bar', 'baz'],
                   ['B', '2', '3.4'],
                   ['D', 'xyz', 9.0]]
    iassertequal(expectation, result)



def test_head():
    """Test the head function."""
    
    table1 = [['foo', 'bar'],
              ['a', 1],
              ['b', 2],
              ['c', 5],
              ['d', 7],
              ['f', 42],
              ['f', 3],
              ['h', 90],
              ['k', 12],
              ['l', 77],
              ['q', 2]]
    
    table2 = head(table1, 4)
    expect = [['foo', 'bar'],
              ['a', 1],
              ['b', 2],
              ['c', 5],
              ['d', 7]]
    iassertequal(expect, table2)


def test_tail():
    """Test the tail function."""
    
    table1 = [['foo', 'bar'],
              ['a', 1],
              ['b', 2],
              ['c', 5],
              ['d', 7],
              ['f', 42],
              ['f', 3],
              ['h', 90],
              ['k', 12],
              ['l', 77],
              ['q', 2]]
    
    table2 = tail(table1, 4)
    expect = [['foo', 'bar'],
              ['h', 90],
              ['k', 12],
              ['l', 77],
              ['q', 2]]
    iassertequal(expect, table2)
    
    
def test_sort_1():
    
    table = [['foo', 'bar'],
            ['C', '2'],
            ['A', '9'],
            ['A', '6'],
            ['F', '1'],
            ['D', '10']]
    
    result = sort(table, 'foo')
    expectation = [['foo', 'bar'],
                   ['A', '9'],
                   ['A', '6'],
                   ['C', '2'],
                   ['D', '10'],
                   ['F', '1']]
    iassertequal(expectation, result)
    
    
def test_sort_2():
    
    table = [['foo', 'bar'],
            ['C', '2'],
            ['A', '9'],
            ['A', '6'],
            ['F', '1'],
            ['D', '10']]
    
    result = sort(table, key=('foo', 'bar'))
    expectation = [['foo', 'bar'],
                   ['A', '6'],
                   ['A', '9'],
                   ['C', '2'],
                   ['D', '10'],
                   ['F', '1']]
    iassertequal(expectation, result)
    
    result = sort(table) # default is lexical sort
    expectation = [['foo', 'bar'],
                   ['A', '6'],
                   ['A', '9'],
                   ['C', '2'],
                   ['D', '10'],
                   ['F', '1']]
    iassertequal(expectation, result)
    
    
def test_sort_3():
    
    table = [['foo', 'bar'],
            ['C', '2'],
            ['A', '9'],
            ['A', '6'],
            ['F', '1'],
            ['D', '10']]
    
    result = sort(table, 'bar')
    expectation = [['foo', 'bar'],
                   ['F', '1'],
                   ['D', '10'],
                   ['C', '2'],
                   ['A', '6'],
                   ['A', '9']]
    iassertequal(expectation, result)
    
    
def test_sort_4():
    
    table = [['foo', 'bar'],
            ['C', 2],
            ['A', 9],
            ['A', 6],
            ['F', 1],
            ['D', 10]]
    
    result = sort(table, 'bar')
    expectation = [['foo', 'bar'],
                   ['F', 1],
                   ['C', 2],
                   ['A', 6],
                   ['A', 9],
                   ['D', 10]]
    iassertequal(expectation, result)
    
    
def test_sort_5():
    
    table = [['foo', 'bar'],
            [2.3, 2],
            [1.2, 9],
            [2.3, 6],
            [3.2, 1],
            [1.2, 10]]
    
    expectation = [['foo', 'bar'],
                   [1.2, 9],
                   [1.2, 10],
                   [2.3, 2],
                   [2.3, 6],
                   [3.2, 1]]

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
    
    table = [['foo', 'bar'],
            [2.3, 2],
            [1.2, 9],
            [2.3, 6],
            [3.2, 1],
            [1.2, 10]]
    
    expectation = [['foo', 'bar'],
                   [3.2, 1],
                   [2.3, 6],
                   [2.3, 2],
                   [1.2, 10],
                   [1.2, 9]]

    result = sort(table, key=('foo', 'bar'), reverse=True)
    iassertequal(expectation, result)
    
    
def test_melt_1():
    
    table = [['id', 'gender', 'age'],
             [1, 'F', 12],
             [2, 'M', 17],
             [3, 'M', 16]]
    
    expectation = [['id', 'variable', 'value'],
                   [1, 'gender', 'F'],
                   [1, 'age', 12],
                   [2, 'gender', 'M'],
                   [2, 'age', 17],
                   [3, 'gender', 'M'],
                   [3, 'age', 16]]
    
    result = melt(table, key='id')
    iassertequal(expectation, result)

    result = melt(table, key='id', variable_field='variable', value_field='value')
    iassertequal(expectation, result)


def test_melt_2():
    
    table = [['id', 'time', 'height', 'weight'],
             [1, 11, 66.4, 12.2],
             [2, 16, 53.2, 17.3],
             [3, 12, 34.5, 9.4]]
    
    expectation = [['id', 'time', 'variable', 'value'],
                   [1, 11, 'height', 66.4],
                   [1, 11, 'weight', 12.2],
                   [2, 16, 'height', 53.2],
                   [2, 16, 'weight', 17.3],
                   [3, 12, 'height', 34.5],
                   [3, 12, 'weight', 9.4]]
    result = melt(table, key=('id', 'time'))
    iassertequal(expectation, result)

    expectation = [['id', 'time', 'variable', 'value'],
                   [1, 11, 'height', 66.4],
                   [2, 16, 'height', 53.2],
                   [3, 12, 'height', 34.5]]
    result = melt(table, key=('id', 'time'), variables='height')
    iassertequal(expectation, result)
    

def test_recast_1():
    
    table = [['id', 'variable', 'value'],
             [3, 'age', 16],
             [1, 'gender', 'F'],
             [2, 'gender', 'M'],
             [2, 'age', 17],
             [1, 'age', 12],
             [3, 'gender', 'M']]
    
    expectation = [['id', 'age', 'gender'],
                   [1, 12, 'F'],
                   [2, 17, 'M'],
                   [3, 16, 'M']]
    
    result = recast(table) # by default lift 'variable' field, hold everything else
    iassertequal(expectation, result)

    result = recast(table, variable_field='variable')
    iassertequal(expectation, result)

    result = recast(table, key='id', variable_field='variable')
    iassertequal(expectation, result)

    result = recast(table, key='id', variable_field='variable', value_field='value')
    iassertequal(expectation, result)


def test_recast_2():
    
    table = [['id', 'variable', 'value'],
             [3, 'age', 16],
             [1, 'gender', 'F'],
             [2, 'gender', 'M'],
             [2, 'age', 17],
             [1, 'age', 12],
             [3, 'gender', 'M']]
    
    expectation = [['id', 'gender'],
                   [1, 'F'],
                   [2, 'M'],
                   [3, 'M']]
    
    # can manually pick which variables you want to recast as fields
    # TODO this is awkward
    result = recast(table, key='id', variable_field={'variable':['gender']})
    iassertequal(expectation, result)


def test_recast_3():
    
    table = [['id', 'time', 'variable', 'value'],
             [1, 11, 'weight', 66.4],
             [1, 14, 'weight', 55.2],
             [2, 12, 'weight', 53.2],
             [2, 16, 'weight', 43.3],
             [3, 12, 'weight', 34.5],
             [3, 17, 'weight', 49.4]]
    
    expectation = [['id', 'time', 'weight'],
                   [1, 11, 66.4],
                   [1, 14, 55.2],
                   [2, 12, 53.2],
                   [2, 16, 43.3],
                   [3, 12, 34.5],
                   [3, 17, 49.4]]
    result = recast(table)
    iassertequal(expectation, result)

    # in the absence of an aggregation function, list all values
    expectation = [['id', 'weight'],
                   [1, [66.4, 55.2]],
                   [2, [53.2, 43.3]],
                   [3, [34.5, 49.4]]]
    result = recast(table, key='id')
    iassertequal(expectation, result)

    # max aggregation
    expectation = [['id', 'weight'],
                   [1, 66.4],
                   [2, 53.2],
                   [3, 49.4]]
    result = recast(table, key='id', reducers={'weight': max})
    iassertequal(expectation, result)

    # min aggregation
    expectation = [['id', 'weight'],
                   [1, 55.2],
                   [2, 43.3],
                   [3, 34.5]]
    result = recast(table, key='id', reducers={'weight': min})
    iassertequal(expectation, result)

    # mean aggregation
    expectation = [['id', 'weight'],
                   [1, 60.80],
                   [2, 48.25],
                   [3, 41.95]]
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
    table = [['id', 'variable', 'value'],
             [1, 'gender', 'F'],
             [2, 'age', 17],
             [1, 'age', 12],
             [3, 'gender', 'M']]
    result = recast(table, key='id')
    expect = [['id', 'age', 'gender'],
              [1, 12, 'F'],
              [2, 17, None],
              [3, None, 'M']]
    iassertequal(expect, result)


def test_duplicates():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', '3.4'],
             ['D', 'xyz', 9.0],
             ['B', u'3', u'7.8', True],
             ['B', '2', 42],
             ['E', None],
             ['D', 4, 12.3]]

    result = duplicates(table, 'foo')
    expectation = [['foo', 'bar', 'baz'],
                   ['B', '2', '3.4'],
                   ['B', u'3', u'7.8', True],
                   ['B', '2', 42],
                   ['D', 'xyz', 9.0],
                   ['D', 4, 12.3]]
    iassertequal(expectation, result)
    
    # test with compound key
    result = duplicates(table, key=('foo', 'bar'))
    expectation = [['foo', 'bar', 'baz'],
                   ['B', '2', '3.4'],
                   ['B', '2', 42]]
    iassertequal(expectation, result)
    

def test_conflicts():
    
    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', None],
             ['D', 'xyz', 9.4],
             ['B', None, u'7.8', True],
             ['E', None],
             ['D', 'xyz', 12.3],
             ['A', 2, None]]

    result = conflicts(table, 'foo', missing=None)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 1, 2],
                   ['A', 2, None],
                   ['D', 'xyz', 9.4],
                   ['D', 'xyz', 12.3]]
    iassertequal(expectation, result)
    
    
def test_mergeduplicates():

    table = [['foo', 'bar', 'baz'],
             ['A', 1, 2],
             ['B', '2', None],
             ['D', 'xyz', 9.4],
             ['B', None, u'7.8', True],
             ['E', None],
             ['D', 'xyz', 12.3],
             ['A', 2, None]]

    # value overrides missing; last value wins
    result = merge(table, 'foo', missing=None)
    expectation = [['foo', 'bar', 'baz'],
                   ['A', 2, 2],
                   ['B', '2', u'7.8', True],
                   ['D', 'xyz', 12.3],
                   ['E', None]]
    iassertequal(expectation, result)
    
    
def test_complement_1():

    table1 = [['foo', 'bar'],
              ['A', 1],
              ['B', 2],
              ['C', 7]]
    
    table2 = [['foo', 'bar'],
              ['A', 9],
              ['B', 2],
              ['B', 3]]
    
    expectation = [['foo', 'bar'],
                   ['A', 1],
                   ['C', 7]]
    
    result = complement(table1, table2)
    iassertequal(expectation, result)
    
    
def test_complement_2():

    tablea = [['foo', 'bar', 'baz'],
              ['A', 1, True],
              ['C', 7, False],
              ['B', 2, False],
              ['C', 9, True]]
    
    tableb = [['x', 'y', 'z'],
              ['B', 2, False],
              ['A', 9, False],
              ['B', 3, True],
              ['C', 9, True]]
    
    aminusb = [['foo', 'bar', 'baz'],
               ['A', 1, True],
               ['C', 7, False]]
    
    result = complement(tablea, tableb)
    iassertequal(aminusb, result)
    
    bminusa = [['x', 'y', 'z'],
               ['A', 9, False],
               ['B', 3, True]]
    
    result = complement(tableb, tablea)
    iassertequal(bminusa, result)
    

def test_diff():

    tablea = [['foo', 'bar', 'baz'],
              ['A', 1, True],
              ['C', 7, False],
              ['B', 2, False],
              ['C', 9, True]]
    
    tableb = [['x', 'y', 'z'],
              ['B', 2, False],
              ['A', 9, False],
              ['B', 3, True],
              ['C', 9, True]]
    
    aminusb = [['foo', 'bar', 'baz'],
               ['A', 1, True],
               ['C', 7, False]]
    
    bminusa = [['x', 'y', 'z'],
               ['A', 9, False],
               ['B', 3, True]]
    
    added, subtracted = diff(tablea, tableb)
    iassertequal(bminusa, added)
    iassertequal(aminusb, subtracted)
    

def test_capture():
    
    table = [['id', 'variable', 'value'],
            ['1', 'A1', '12'],
            ['2', 'A2', '15'],
            ['3', 'B1', '18'],
            ['4', 'C12', '19']]
    
    expectation = [['id', 'value', 'treat', 'time'],
                   ['1', '12', 'A', '1'],  
                   ['2', '15', 'A', '2'],
                   ['3', '18', 'B', '1'],
                   ['4', '19', 'C', '1']]
    
    result = capture(table, 'variable', '(\\w)(\\d)', ('treat', 'time'))
    iassertequal(expectation, result)

    result = capture(table, 'variable', '(\\w)(\\d)', ('treat', 'time'),
                           include_original=False)
    iassertequal(expectation, result)

    expectation = [['id', 'variable', 'value', 'treat', 'time'],
                   ['1', 'A1', '12', 'A', '1'],  
                   ['2', 'A2', '15', 'A', '2'],
                   ['3', 'B1', '18', 'B', '1'],
                   ['4', 'C12', '19', 'C', '1']]
    
    result = capture(table, 'variable', '(\\w)(\\d)', ('treat', 'time'),
                           include_original=True)
    iassertequal(expectation, result)
    
    
def test_split():
    
    table = [['id', 'variable', 'value'],
             ['1', 'parad1', '12'],
             ['2', 'parad2', '15'],
             ['3', 'tempd1', '18'],
             ['4', 'tempd2', '19']]
    
    expectation = [['id', 'value', 'variable', 'day'],
                   ['1', '12', 'para', '1'],  
                   ['2', '15', 'para', '2'],
                   ['3', '18', 'temp', '1'],
                   ['4', '19', 'temp', '2']]
    
    result = split(table, 'variable', 'd', ('variable', 'day'))
    iassertequal(expectation, result)

    result = split(table, 'variable', 'd', ('variable', 'day'))
    iassertequal(expectation, result)

    expectation = [['id', 'variable', 'value', 'variable', 'day'],
                   ['1', 'parad1', '12', 'para', '1'],  
                   ['2', 'parad2', '15', 'para', '2'],
                   ['3', 'tempd1', '18', 'temp', '1'],
                   ['4', 'tempd2', '19', 'temp', '2']]
    
    result = split(table, 'variable', 'd', ('variable', 'day'), include_original=True)
    iassertequal(expectation, result)
    

def test_melt_and_capture():
    
    table = [['id', 'parad0', 'parad1', 'parad2'],
             ['1', '12', '34', '56'],
             ['2', '23', '45', '67']]
    
    expectation = [['id', 'parasitaemia', 'day'],
                   ['1', '12', '0'],
                   ['1', '34', '1'],
                   ['1', '56', '2'],
                   ['2', '23', '0'],
                   ['2', '45', '1'],
                   ['2', '67', '2']]
    
    step1 = melt(table, key='id', value_field='parasitaemia')
    step2 = capture(step1, 'variable', 'parad(\\d+)', ('day',))
    iassertequal(expectation, step2)


def test_melt_and_split():
    
    table = [['id', 'parad0', 'parad1', 'parad2', 'tempd0', 'tempd1', 'tempd2'],
            ['1', '12', '34', '56', '37.2', '37.4', '37.9'],
            ['2', '23', '45', '67', '37.1', '37.8', '36.9']]
    
    expectation = [['id', 'value', 'variable', 'day'],
                   ['1', '12', 'para', '0'],
                   ['1', '34', 'para', '1'],
                   ['1', '56', 'para', '2'],
                   ['1', '37.2', 'temp', '0'],
                   ['1', '37.4', 'temp', '1'],
                   ['1', '37.9', 'temp', '2'],
                   ['2', '23', 'para', '0'],
                   ['2', '45', 'para', '1'],
                   ['2', '67', 'para', '2'],
                   ['2', '37.1', 'temp', '0'],
                   ['2', '37.8', 'temp', '1'],
                   ['2', '36.9', 'temp', '2']]
    
    step1 = melt(table, key='id')
    step2 = split(step1, 'variable', 'd', ('variable', 'day'))
    iassertequal(expectation, step2)


def test_select():
    
    table = [['foo', 'bar', 'baz'],
             ['a', 4, 9.3],
             ['a', 2, 88.2],
             ['b', 1, 23.3],
             ['c', 8, 42.0],
             ['d', 7, 100.9],
             ['c', 2]]

    actual = select(table, lambda rec: rec['foo'] == 'a')
    expect = [['foo', 'bar', 'baz'],
              ['a', 4, 9.3],
              ['a', 2, 88.2]]
    iassertequal(expect, actual)
    iassertequal(expect, actual) # check can iterate twice
 
    actual = select(table, lambda rec: rec['foo'] == 'a' and rec['bar'] > 3)
    expect = [['foo', 'bar', 'baz'],
              ['a', 4, 9.3]]
    iassertequal(expect, actual)

    actual = select(table, expr("{foo} == 'a'"))
    expect = [['foo', 'bar', 'baz'],
              ['a', 4, 9.3],
              ['a', 2, 88.2]]
    iassertequal(expect, actual)

    actual = select(table, expr( "{foo} == 'a' and {bar} > 3"))
    expect = [['foo', 'bar', 'baz'],
              ['a', 4, 9.3]]
    iassertequal(expect, actual)

    # check error handling on short rows
    actual = select(table, lambda rec: rec['baz'] > 88.1)
    expect = [['foo', 'bar', 'baz'],
              ['a', 2, 88.2],
              ['d', 7, 100.9]]
    iassertequal(expect, actual)
    
    
def test_fieldmap():
    
    table = [['id', 'sex', 'age', 'height', 'weight'],
             [1, 'male', 16, 1.45, 62.0],
             [2, 'female', 19, 1.34, 55.4],
             [3, 'female', 17, 1.78, 74.4],
             [4, 'male', 21, 1.33, 45.2],
             [5, '-', 25, 1.65, 51.9]]
    
    mappings = OrderedDict()
    mappings['subject_id'] = 'id'
    mappings['gender'] = 'sex', {'male': 'M', 'female': 'F'}
    mappings['age_months'] = 'age', lambda v: v * 12
    mappings['bmi'] = lambda rec: rec['weight'] / rec['height']**2 
    actual = fieldmap(table, mappings)  
    expect = [['subject_id', 'gender', 'age_months', 'bmi'],
              [1, 'M', 16*12, 62.0/1.45**2],
              [2, 'F', 19*12, 55.4/1.34**2],
              [3, 'F', 17*12, 74.4/1.78**2],
              [4, 'M', 21*12, 45.2/1.33**2],
              [5, '-', 25*12, 51.9/1.65**2]]
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
    table2 = [['id', 'sex', 'age', 'height', 'weight'],
              [1, 'male', 16, 1.45, 62.0],
              [2, 'female', 19, 1.34, 55.4],
              [3, 'female', 17, 1.78, 74.4],
              [4, 'male', 21, 1.33, 45.2],
              [5, '-', 25, 1.65]]
    expect = [['subject_id', 'gender', 'age_months', 'bmi'],
              [1, 'M', 16*12, 62.0/1.45**2],
              [2, 'F', 19*12, 55.4/1.34**2],
              [3, 'F', 17*12, 74.4/1.78**2],
              [4, 'M', 21*12, 45.2/1.33**2],
              [5, '-', 25*12, None]]
    actual = fieldmap(table2, mappings)
    iassertequal(expect, actual)


def test_facet():

    table = [['foo', 'bar', 'baz'],
             ['a', 4, 9.3],
             ['a', 2, 88.2],
             ['b', 1, 23.3],
             ['c', 8, 42.0],
             ['d', 7, 100.9],
             ['c', 2]]
    fct = facet(table, 'foo')
    assert set(fct.keys()) == {'a', 'b', 'c', 'd'}
    expect_fcta = [['foo', 'bar', 'baz'],
                   ['a', 4, 9.3],
                   ['a', 2, 88.2]]
    iassertequal(fct['a'], expect_fcta)
    iassertequal(fct['a'], expect_fcta) # check can iterate twice
    expect_fctc = [['foo', 'bar', 'baz'],
                   ['c', 8, 42.0],
                   ['c', 2]]
    iassertequal(fct['c'], expect_fctc)
    iassertequal(fct['c'], expect_fctc) # check can iterate twice
    

