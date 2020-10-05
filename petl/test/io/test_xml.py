# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import sys


from petl.test.helpers import ieq
from petl.util import nrows, look
from petl.io.xml import fromxml
from petl.compat import urlopen, izip_longest
from nose.tools import eq_


def test_fromxml():

    # initial data
    f = NamedTemporaryFile(delete=False, mode='wt')
    data = """<table>
        <tr>
            <td>foo</td><td>bar</td>
        </tr>
        <tr>
            <td>a</td><td>1</td>
        </tr>
        <tr>
            <td>b</td><td>2</td>
        </tr>
        <tr>
            <td>c</td><td>2</td>
        </tr>
      </table>"""
    f.write(data)
    f.close()

    actual = fromxml(f.name, 'tr', 'td')
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromxml_2():

    # initial data
    f = NamedTemporaryFile(delete=False, mode='wt')
    data = """<table>
        <tr>
            <td v='foo'/><td v='bar'/>
        </tr>
        <tr>
            <td v='a'/><td v='1'/>
        </tr>
        <tr>
            <td v='b'/><td v='2'/>
        </tr>
        <tr>
            <td v='c'/><td v='2'/>
        </tr>
      </table>"""
    f.write(data)
    f.close()

    actual = fromxml(f.name, 'tr', 'td', 'v')
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromxml_3():

    # initial data
    f = NamedTemporaryFile(delete=False, mode='wt')
    data = """<table>
        <row>
            <foo>a</foo><baz><bar v='1'/></baz>
        </row>
        <row>
            <foo>b</foo><baz><bar v='2'/></baz>
        </row>
        <row>
            <foo>c</foo><baz><bar v='2'/></baz>
        </row>
      </table>"""
    f.write(data)
    f.close()

    actual = fromxml(f.name, 'row', {'foo': 'foo', 'bar': ('baz/bar', 'v')})
    # N.B., requested fields come out in name sorted order
    expect = (('bar', 'foo'),
              ('1', 'a'),
              ('2', 'b'),
              ('2', 'c'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromxml_4():

    # initial data
    f = NamedTemporaryFile(delete=False, mode='wt')
    data = """<table>
        <row>
            <foo>a</foo><baz><bar>1</bar><bar>3</bar></baz>
        </row>
        <row>
            <foo>b</foo><baz><bar>2</bar></baz>
        </row>
        <row>
            <foo>c</foo><baz><bar>2</bar></baz>
        </row>
      </table>"""
    f.write(data)
    f.close()

    actual = fromxml(f.name, 'row', {'foo': 'foo', 'bar': './/bar'})
    # N.B., requested fields come out in name sorted order
    expect = (('bar', 'foo'),
              (('1', '3'), 'a'),
              ('2', 'b'),
              ('2', 'c'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromxml_5():

    # initial data
    f = NamedTemporaryFile(delete=False, mode='wt')
    data = """<table>
        <row>
            <foo>a</foo><baz><bar v='1'/><bar v='3'/></baz>
        </row>
        <row>
            <foo>b</foo><baz><bar v='2'/></baz>
        </row>
        <row>
            <foo>c</foo><baz><bar v='2'/></baz>
        </row>
      </table>"""
    f.write(data)
    f.close()

    actual = fromxml(f.name, 'row', {'foo': 'foo', 'bar': ('baz/bar', 'v')})
    # N.B., requested fields come out in name sorted order
    expect = (('bar', 'foo'),
              (('1', '3'), 'a'),
              ('2', 'b'),
              ('2', 'c'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromxml_6():

    data = """<table class='petl'>
        <thead>
        <tr>
        <th>foo</th>
        <th>bar</th>
        </tr>
        </thead>
        <tbody>
        <tr>
        <td>a</td>
        <td style='text-align: right'>2</td>
        </tr>
        <tr>
        <td>b</td>
        <td style='text-align: right'>1</td>
        </tr>
        <tr>
        <td>c</td>
        <td style='text-align: right'>3</td>
        </tr>
        </tbody>
      </table>"""
    f = NamedTemporaryFile(delete=False, mode='wt')
    f.write(data)
    f.close()

    actual = fromxml(f.name, './/tr', ('th', 'td'))
    print(look(actual))
    expect = (('foo', 'bar'),
              ('a', '2'),
              ('b', '1'),
              ('c', '3'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromxml_url():
    # check internet connection
    try:
        url = 'http://raw.githubusercontent.com/petl-developers/petl/master/.pydevproject'
        urlopen(url)
    except Exception as e:
        print('SKIP test_fromxml_url: %s' % e, file=sys.stderr)
    else:
        actual = fromxml(url, 'pydev_property', {'name': ( '.', 'name'), 'prop': '.'})
        assert nrows(actual) > 0
        expect = fromxml('.pydevproject', 'pydev_property', {'name': ( '.', 'name'), 'prop': '.'})
        ieq(expect, actual)


def _write_temp_file(data):
    with NamedTemporaryFile(delete=False, mode='wt') as f:
        f.write(data)
        res = f.name
        f.close()
    # txt = open(res, 'r').read()
    # print('TEST %s:\n%s' % (res, txt), file=sys.stderr)
    return res


def _write_test_file(data, pre='', pos=''):
    content = pre + '<table>' + data + pos + '</table>'
    return _write_temp_file(content)


def _compare(expected, actual):
    try:
        _eq_rows(expected, actual)
    except Exception as ex:
        print('Expected:\n', look(expected), file=sys.stderr)
        print('  Actual:\n', look(actual), file=sys.stderr)
        raise ex


def _eq_rows(expect, actual, cast=None):
    '''test when values are equals for eacfh row and column'''
    ie = iter(expect)
    ia = iter(actual)
    for re, ra in izip_longest(ie, ia, fillvalue=None):
        if cast:
            ra = cast(ra)
        for ve, va in izip_longest(re, ra, fillvalue=None):
            if isinstance(ve, list):
                for je, ja in izip_longest(ve, va, fillvalue=None):
                    _eq2(je, ja, re, ra)
            elif not isinstance(ve, dict):
                _eq2(ve, va, re, ra)


def _eq2(ve, va, re, ra):
    try:
        eq_(ve, va)
    except AssertionError as ea:
        print('\nrow: ', re, ' != ', ra)
        print('val: ', ve, ' != ', va)
        raise ea


def test_fromxml_entity():
    _DATA1 = """
        <tr><td>foo</td><td>bar</td></tr>
        <tr><td>a</td><td>1</td></tr>
        <tr><td>b</td><td>2</td></tr>
        <tr><td>c</td><td>3</td></tr>
        """

    _DATA2 = '<td>X</td><td>9</td>'

    _DOCTYPE = """<?xml version="1.0" encoding="ISO-8859-1"?>
    <!DOCTYPE foo [  
        <!ELEMENT table ANY >
        <!ENTITY inserted SYSTEM "file://%s" >]>
        """

    _INSERTED = '<tr>&inserted;</tr>'

    _TABLE1 = (('foo', 'bar'),
               ('a', '1'),
               ('b', '2'),
               ('c', '3'))

    temp_file1 = _write_test_file(_DATA1)

    actual11 = fromxml(temp_file1, 'tr', 'td')
    _compare(_TABLE1, actual11)

    try:
        from lxml import etree
    except:
        return

    data_file_tmp = _write_temp_file(_DATA2)
    doc_type_temp = _DOCTYPE % data_file_tmp
    doc_type_miss = _DOCTYPE % '/tmp/doesnotexist'

    _EXPECT_IT = (('X', '9'),)
    _EXPECT_NO = ((None, None),)

    temp_file2 = _write_test_file(_DATA1, pre=doc_type_temp, pos=_INSERTED)
    temp_file3 = _write_test_file(_DATA1, pre=doc_type_miss, pos=_INSERTED)

    parser_off = etree.XMLParser(resolve_entities=False)
    parser_onn = etree.XMLParser(resolve_entities=True)

    actual12 = fromxml(temp_file1, 'tr', 'td', parser=parser_off)
    _compare(_TABLE1, actual12)

    actual21 = fromxml(temp_file2, 'tr', 'td')
    _compare(_TABLE1 + _EXPECT_NO, actual21)

    actual22 = fromxml(temp_file2, 'tr', 'td', parser=parser_off)
    _compare(_TABLE1 + _EXPECT_NO, actual22)

    actual23 = fromxml(temp_file2, 'tr', 'td', parser=parser_onn)
    _compare(_TABLE1 + _EXPECT_IT, actual23)

    actual31 = fromxml(temp_file3, 'tr', 'td')
    _compare(_TABLE1 + _EXPECT_NO, actual31)

    actual32 = fromxml(temp_file3, 'tr', 'td', parser=parser_off)
    _compare(_TABLE1 + _EXPECT_NO, actual32)

    try:
        actual33 = fromxml(temp_file3, 'tr', 'td', parser=parser_onn)
        for _ in actual33:
            pass
    except etree.XMLSyntaxError:
        # print('XMLSyntaxError', ex, file=sys.stderr)
        pass
    else:
        assert True, 'Error testing XML'
