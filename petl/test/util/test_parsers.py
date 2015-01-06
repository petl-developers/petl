from __future__ import absolute_import, print_function, division


from petl.compat import maxint
from petl.test.helpers import eq_
from petl.util.parsers import numparser, datetimeparser


def test_numparser():

    parsenumber = numparser()
    assert parsenumber('1') == 1
    assert parsenumber('1.0') == 1.0
    assert parsenumber(str(maxint + 1)) == maxint + 1
    assert parsenumber('3+4j') == 3 + 4j
    assert parsenumber('aaa') == 'aaa'
    assert parsenumber(None) is None


def test_numparser_strict():

    parsenumber = numparser(strict=True)
    assert parsenumber('1') == 1
    assert parsenumber('1.0') == 1.0
    assert parsenumber(str(maxint + 1)) == maxint + 1
    assert parsenumber('3+4j') == 3 + 4j
    try:
        parsenumber('aaa')
    except ValueError:
        pass  # expected
    else:
        assert False, 'expected exception'
    try:
        parsenumber(None)
    except TypeError:
        pass  # expected
    else:
        assert False, 'expected exception'


def test_laxparsers():

    p1 = datetimeparser('%Y-%m-%dT%H:%M:%S')
    try:
        p1('2002-12-25 00:00:00')
    except ValueError:
        pass
    else:
        assert False, 'expected exception'

    p2 = datetimeparser('%Y-%m-%dT%H:%M:%S', strict=False)
    try:
        v = p2('2002-12-25 00:00:00')
    except ValueError:
        assert False, 'did not expect exception'
    else:
        eq_('2002-12-25 00:00:00', v)
