"""
TODO doc me

"""

from tempfile import NamedTemporaryFile
import csv


from petl import fromcsv
import os.path


def assertequal(expect, actual):
    assert expect == actual, (expect, actual)


def iassertequal(expect, actual):
    for e, a in zip(expect, actual):
        assert e == a, (e, a)


def test_fromcsv():
    """Test the fromcsv function."""
    
    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f)
    table = [['foo', 'bar'],
             ['a', 1],
             ['b', 2],
             ['c', 2]]
    for row in table:
        writer.writerow(row)
    f.close()
    
    actual = fromcsv(f.name)
    expect = [['foo', 'bar'],
              ['a', '1'],
              ['b', '2'],
              ['c', '2']]
    iassertequal(expect, actual)
    iassertequal(expect, actual) # verify can iterate twice
    
    
def test_fromcsv_cachetag():
    """Test the cachetag method on tables returned by fromcsv."""
    
    # initial data
    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f)
    table = [['foo', 'bar'],
             ['a', 1],
             ['b', 2],
             ['c', 2]]
    for row in table:
        writer.writerow(row)
    f.close()

    # cachetag with initial data
    tbl = fromcsv(f.name)
    tag1 = tbl.cachetag()
    
    # make a change
    with open(f.name, 'wb') as f:
        writer = csv.writer(f)
        rows = [['foo', 'bar'],
                ['d', 3],
                ['e', 5],
                ['f', 4]]
        for row in rows:
            writer.writerow(row)

    # check cachetag has changed
    tag2 = tbl.cachetag()
    assert tag2 != tag1, (tag2, tag1)
    

