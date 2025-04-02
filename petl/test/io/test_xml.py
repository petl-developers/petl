# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
from collections import OrderedDict
from tempfile import NamedTemporaryFile

import pytest

from petl.test.helpers import ieq
from petl.util import nrows, look
from petl.io.xml import fromxml, toxml
from petl.compat import urlopen


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
        url = 'http://raw.githubusercontent.com/petl-developers/petl/master/petl/test/resources/test.xml'
        urlopen(url)
        import pkg_resources
        filename = pkg_resources.resource_filename('petl', 'test/resources/test.xml')
    except Exception as e:
        pytest.skip('SKIP test_fromxml_url: %s' % e)
    else:
        actual = fromxml(url, 'pydev_property', {'name': ( '.', 'name'), 'prop': '.'})
        assert nrows(actual) > 0
        expect = fromxml(filename, 'pydev_property', {'name': ( '.', 'name'), 'prop': '.'})
        ieq(expect, actual)


def _write_temp_file(content, out=None):
    with NamedTemporaryFile(delete=False, mode='wt') as f:
        f.write(content)
        res = f.name
        f.close()
    if out is not None:
        outf = sys.stderr if out else sys.stdout
        print('TEST %s:\n%s' % (res, content), file=outf)
    return res


def _dump_file(filename, out=None):
    if out is not None:
        outf = sys.stderr if out else sys.stdout
        print('FILE:\n%s' % open(filename).read(), file=outf)


def _dump_both(expected, actual, out=None):
    if out is not None:
        outf = sys.stderr if out else sys.stdout
        print('EXPECTED:\n', look(expected), file=outf)
        print('ACTUAL:\n', look(actual), file=outf)


def _compare(expected, actual, out=None):
    try:
        _dump_both(expected, actual, out)
        ieq(expected, actual)
    except Exception as ex:
        _dump_both(expected, actual, False)
        raise ex


def _write_test_file(data, pre='', pos=''):
    content = pre + '<table>' + data + pos + '</table>'
    return _write_temp_file(content)


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


def _check_toxml(table, expected, check=(), dump=None, **kwargs):
    with NamedTemporaryFile(delete=True, suffix='.xml') as f:
        filename = f.name

    toxml(table, filename, **kwargs)
    _dump_file(filename, dump)
    if check:
        try:
            actual = fromxml(filename, *check)
            _compare(expected, actual, dump)
        except Exception as ex:
            _dump_file(filename, False)
            raise ex


_HEAD1 = (('ABCD', 'N123'),)
_BODY1 = (('a', '1'),
          ('b', '2'),
          ('c', '3'))
_TABLE1 = _HEAD1 + _BODY1


def test_toxml00():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//tr', ('th', 'td'))
    )


def test_toxml01():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//tr', ('th', 'td')),
        root='table',
        head='thead/tr/th',
        rows='tbody/tr/td'
    )


def test_toxml02():
    _check_toxml(
        _TABLE1, _BODY1,
        check=('.//row', 'col'),
        root='matrix',
        rows='row/col'
    )


def test_toxml03():
    _check_toxml(
        _TABLE1, _BODY1,
        check=('line', 'cell'),
        rows='plan/line/cell'
    )


def test_toxml04():
    _check_toxml(
        _TABLE1, _BODY1,
        check=('.//line', 'cell'),
        rows='dir/file/book/plan/line/cell'
    )


def test_toxml05():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//x', 'y'),
        root='a',
        head='h/k/x/y',
        rows='r/v/x/y'
    )


def test_toxml06():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//row', 'col'),
        root='table',
        head='head/row/col',
        rows='row/col'
    )


def test_toxml07():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//field-list', 'field-name'),
        root='root-tag',
        head='head-tag/field-list/field-name',
        rows='body-row/field-list/field-name'
    )


def test_toxml08():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//field.list', 'field.name'),
        root='root.tag',
        head='head.tag/field.list/field.name',
        rows='body.row/field.list/field.name'
    )


def test_toxml09():
    _check_toxml(
        _TABLE1, _BODY1,
        check=('.//tr/td', '*'),
        style='name',
        root='table',
        rows='tbody/tr/td'
    )


def test_toxml10():
    _check_toxml(
        _TABLE1, _BODY1,
        check=('.//tr/td', '*'),
        style='name',
        root='table',
        head='thead/tr/th',
        rows='tbody/tr/td'
    )


_ATTRIB_COLS = {'ABCD': ('.', 'ABCD'), 'N123': ('.', 'N123')}


def test_toxml11():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//tr/td', _ATTRIB_COLS),
        style='attribute',
        root='table',
        rows='tbody/tr/td'
    )


def test_toxml12():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//tr/td', _ATTRIB_COLS),
        style='attribute',
        root='table',
        head='thead/tr/th',
        rows='tbody/tr/td'
    )


def test_toxml13():
    _check_toxml(
        _TABLE1, _BODY1,
        check=('.//tr', ('td', 'th')),
        style=' <tr><td>{ABCD}</td><td>{N123}</td></tr>\n',
        root='table',
        rows='tbody'
    )


def test_toxml131():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//tr', ('th', 'td')),
        style=' <tr><td>{ABCD}</td><td>{N123}</td></tr>\n',
        root='table',
        head='thead/tr/td',
        rows='tbody'
    )


def test_toxml14():
    table1 = [['foo', 'bar'],
              ['a', 1],
              ['b', 2]]

    _check_toxml(
        table1, table1,
        style='attribute',
        rows='row/col'
    )
    _check_toxml(
        table1, table1,
        style='name',
        rows='row/col'
    )


_ROW_A0 = (('A', '0'),)
_ROW_Z9 = (('Z', '9'),)
_TAB_ABZ = _ROW_A0 + _BODY1 + _ROW_Z9
_TAB_HAZ = _HEAD1 + _ROW_A0 + _BODY1 + _ROW_Z9
_TAG_A0 = ' <row><col>A</col><col>0</col></row>'
_TAG_Z9 = ' <row><col>Z</col><col>9</col></row>'
_TAG_TOP = '<table>\n'
_TAG_END = '\n</table>'


def test_toxml15():
    _check_toxml(
        _TABLE1, _TAB_ABZ,
        check=('row', 'col'),
        root='table',
        rows='row/col',
        prologue=_TAG_A0, 
        epilogue=_TAG_Z9
    )


def test_toxml16():
    _check_toxml(
        _TABLE1, _TAB_HAZ,
        check=('.//row', 'col'),
        root='table',
        head='thead/row/col',
        rows='tbody/row/col',
        prologue=_TAG_A0, 
        epilogue=_TAG_Z9
    )


def test_toxml17():
    _check_toxml(
        _TABLE1, _TAB_ABZ,
        check=('row', 'col'),
        rows='row/col',
        prologue=_TAG_TOP + _TAG_A0, 
        epilogue=_TAG_Z9 + _TAG_END
    )


def test_toxml18():
    _TAB_AHZ = _ROW_A0 + _HEAD1 + _BODY1 + _ROW_Z9
    _check_toxml(
        _TABLE1, _TAB_AHZ,
        check=('.//row', 'col'),
        head='thead/row/col',
        rows='row/col',
        prologue=_TAG_TOP + _TAG_A0, 
        epilogue=_TAG_Z9 + _TAG_END
    )


def test_toxml19():
    _check_toxml(
        _TABLE1, _BODY1,
        check=('.//line', 'cell'),
        rows='tbody/line/cell',
        prologue=_TAG_TOP + _TAG_A0, 
        epilogue=_TAG_Z9 + _TAG_END
    )


def test_toxml20():
    _check_toxml(
        _TABLE1, _TABLE1,
        check=('.//line', 'cell'),
        root='book',
        head='thead/line/cell',
        rows='tbody/line/cell',
        prologue=_TAG_TOP + _TAG_A0,
        epilogue=_TAG_Z9 + _TAG_END
    )


def test_toxml21():
    _check_toxml(
        _TABLE1, _TAB_HAZ,
        check=('.//row', 'col'),
        root='book',
        head='thead/row/col',
        rows='tbody/row/col',
        prologue=_TAG_TOP + _TAG_A0,
        epilogue=_TAG_Z9 + _TAG_END
    )


def test_toxml22():
    _check_toxml(
        _TABLE1, _TAB_HAZ,
        check=('.//tr/td', _ATTRIB_COLS),
        style='attribute',
        root='table',
        rows='tbody/tr/td',
        # dump=True,
        prologue='<td ABCD="A" N123="0" />', 
        epilogue='<td ABCD="Z" N123="9" />'
    )
