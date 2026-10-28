# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
from tempfile import NamedTemporaryFile

import pytest

from petl.test.helpers import ieq
from petl.util import nrows, look
from petl.io.xml import fromxml, toxml
from petl.compat import urlopen

try:
    from importlib.resources import files
except ImportError:  # Python < 3.9
    from importlib_resources import files


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
        filename = str(files('petl') / 'test/resources/test.xml')
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


@pytest.mark.parametrize('style', ['tag', 'name', 'attribute'])
@pytest.mark.parametrize('value', [
    u'R&D <review> "quoted" &amp;', u'caf\u00e9',
    'first\r\nsecond\tthird', '', 42, False, None,
])
def test_toxml_escapes_values(tmpdir, style, value):
    from xml.etree import ElementTree
    from petl.compat import text_type

    filename = str(tmpdir.join('values.xml'))
    table = (row for row in [('label',), (value,)])
    toxml(table, filename, root='table', rows='row/col', style=style, escape=True)
    root = ElementTree.parse(filename).getroot()
    if style == 'tag':
        actual = root.find('.//col').text
    elif style == 'name':
        actual = root.find('.//label').text
    else:
        actual = root.find('.//col').get('label')
    # An empty XML text node is represented as None by ElementTree.
    assert (actual or '') == text_type(value)


@pytest.mark.parametrize('header', [u'R&D <review> "quoted"', 'line\rbreak'])
def test_toxml_escapes_header(tmpdir, header):
    from xml.etree import ElementTree

    filename = str(tmpdir.join('header.xml'))
    toxml([(header,), ('value',)], filename, escape=True)
    root = ElementTree.parse(filename).getroot()
    assert root.find('.//th').text == header
    assert root.find('.//td').text == 'value'


def test_toxml_keeps_custom_markup(tmpdir):
    from xml.etree import ElementTree

    filename = str(tmpdir.join('custom.xml'))
    toxml([['label'], ['<b>value</b>']], filename,
          root='table', rows='body', escape=True,
          style=' <row><col>{label}</col></row>\n',
          prologue='<before/>', epilogue='<after/>')
    root = ElementTree.parse(filename).getroot()
    assert root.find('.//col/b').text == 'value'
    assert root.find('.//before') is not None
    assert root.find('.//after') is not None


def test_toxml_ignores_extra_values(tmpdir):
    from xml.etree import ElementTree

    class Unformattable(object):
        def __str__(self):
            raise AssertionError('unused value must not be formatted')

    filename = str(tmpdir.join('extra.xml'))
    toxml([['label'], ['R&D', Unformattable()]], filename, escape=True)
    assert ElementTree.parse(filename).find('.//td').text == 'R&D'


def test_toxml_preserves_value_formatting(tmpdir):
    from xml.etree import ElementTree

    class Formatted(object):
        def __str__(self):
            return 'different representation'

        def __format__(self, spec):
            assert spec == ''
            return 'R&D'

    filename = str(tmpdir.join('formatted.xml'))
    toxml([['label'], [Formatted()]], filename, escape=True)
    assert ElementTree.parse(filename).find('.//td').text == 'R&D'


@pytest.mark.parametrize('style', ['tag', 'name', 'attribute'])
@pytest.mark.parametrize('escape', [False, True])
def test_toxml_indexed_mapping_rows(style, escape):
    from xml.etree import ElementTree
    from petl.io.sources import MemorySource

    sink = MemorySource()
    toxml([('first', 'second'), {0: 'left', 1: 'right'}], sink,
          root='table', rows='row/col', style=style, escape=escape)
    root = ElementTree.fromstring(sink.getvalue())
    if style == 'tag':
        assert [col.text for col in root.findall('.//col')] == ['left', 'right']
    elif style == 'name':
        assert root.find('.//first').text == 'left'
        assert root.find('.//second').text == 'right'
    else:
        assert root.find('.//col').attrib == {'first': 'left', 'second': 'right'}


@pytest.mark.parametrize('style', ['tag', 'name', 'attribute'])
@pytest.mark.parametrize('escape', [False, True])
def test_toxml_does_not_format_shadowed_fields(style, escape):
    from petl.io.sources import MemorySource

    class Unformattable(object):
        def __format__(self, spec):
            raise AssertionError('shadowed field must not be formatted')

    sink = MemorySource()
    toxml([('value', 'value'), (Unformattable(), 'last')], sink,
          root='table', rows='row/col', style=style, escape=escape)
    assert sink.getvalue().count(b'last') == 2


def test_toxml_formats_duplicate_fields_per_occurrence():
    from xml.etree import ElementTree
    from petl.io.sources import MemorySource

    class Formatted(object):
        calls = 0

        def __format__(self, spec):
            self.calls += 1
            return 'R&D%s' % self.calls

    value = Formatted()
    sink = MemorySource()
    toxml([('value', 'value'), ('unused', value)], sink, escape=True)
    root = ElementTree.fromstring(sink.getvalue())
    assert [col.text for col in root.findall('.//td')] == ['R&D1', 'R&D2']
    assert value.calls == 2


@pytest.mark.parametrize('escape', [False, True])
def test_toxml_index_only_rows(escape):
    from petl.io.sources import MemorySource

    class Indexed(object):
        def __iter__(self):
            raise AssertionError('row must be read by index')

        def __getitem__(self, index):
            return ('first', 'second')[index]

    sink = MemorySource()
    toxml([('a', 'b'), Indexed()], sink, escape=escape)
    assert b'<td>first</td><td>second</td>' in sink.getvalue()


@pytest.mark.parametrize('style', ['tag', 'name', 'attribute'])
@pytest.mark.parametrize('value', ['<b>markup</b>', '&amp;',
                                    'a\r\nb\tc', u'caf\u00e9'])
def test_toxml_keeps_raw_default(style, value):
    from petl.io.sources import MemorySource

    table = [('label',), (value,)]
    if sys.version_info[0] < 3 and value == u'caf\u00e9':
        # Python 2's byte templates cannot format non-ASCII Unicode.
        for kwargs in ({}, {'escape': False}):
            with pytest.raises(UnicodeEncodeError):
                toxml(table, MemorySource(), style=style, **kwargs)
        return
    default = MemorySource()
    explicit = MemorySource()
    toxml(table, default, style=style)
    toxml(table, explicit, style=style, escape=False)
    assert default.getvalue() == explicit.getvalue()
    assert value in default.getvalue().decode('utf-8')


def test_toxml_keeps_raw_header_default():
    from petl.io.sources import MemorySource

    sink = MemorySource()
    toxml([('R&D',), ('&amp;',)], sink)
    assert b'<th>R&D</th>' in sink.getvalue()
    assert b'<td>&amp;</td>' in sink.getvalue()


@pytest.mark.parametrize('escape', [False, True])
def test_toxml_propagates_formatter_error(escape):
    from petl.io.sources import MemorySource

    error = ValueError('invalid value')

    class Unformattable(object):
        def __format__(self, spec):
            raise error

    with pytest.raises(ValueError) as excinfo:
        toxml([('label',), (Unformattable(),)], MemorySource(), escape=escape)
    assert excinfo.value is error


def test_toxml_escapes_after_format_conversions():
    from xml.etree import ElementTree
    from petl.io.sources import MemorySource

    sink = MemorySource()
    toxml([('x', 'x!r', 'x!s', 'x:>5'), ('R&D', 'unused', 'unused', 'unused')],
          sink, escape=True)
    root = ElementTree.fromstring(sink.getvalue())
    assert [col.text for col in root.findall('.//td')] == [
        'R&D', "'R&D'", 'R&D', '  R&D']


def test_toxml_keeps_nested_format_spec_unescaped():
    from xml.etree import ElementTree
    from petl.io.sources import MemorySource

    sink = MemorySource()
    table = [('x', 'fill', 'width', 'x:{fill}>{width}'), ('a', '&', 6, 'unused')]
    toxml(table, sink, escape=True)
    root = ElementTree.fromstring(sink.getvalue())
    assert [col.text for col in root.findall('.//td')] == ['a', '&', '6', '&&&&&a']


@pytest.mark.parametrize('escape', [False, True])
def test_toxml_keeps_positional_field_error(escape):
    from petl.io.sources import MemorySource

    with pytest.raises(IndexError):
        toxml([('',), ('value',)], MemorySource(), escape=escape)


@pytest.mark.parametrize('field', ['self', 'args', 'kwargs', 'format',
                                   'field', 'spec', 'conversion'])
@pytest.mark.parametrize('escape', [False, True])
def test_toxml_accepts_formatter_parameter_field_names(field, escape):
    from xml.etree import ElementTree
    from petl.io.sources import MemorySource

    sink = MemorySource()
    toxml([(field,), ('value',)], sink, escape=escape)
    assert ElementTree.fromstring(sink.getvalue()).find('.//td').text == 'value'


@pytest.mark.parametrize('codepoint', list(range(9)) + [11, 12] +
                         list(range(14, 32)) + [0xfffe, 0xffff])
@pytest.mark.parametrize('style', ['tag', 'name', 'attribute'])
def test_toxml_rejects_forbidden_characters_in_values(codepoint, style):
    from petl.io.sources import MemorySource

    with pytest.raises(ValueError) as excinfo:
        toxml([('label',), (u'before' + (u'%c' % codepoint) + u'after',)],
              MemorySource(), style=style, escape=True)
    assert str(excinfo.value) == (
        'XML 1.0 cannot represent character U+%04X' % codepoint)


@pytest.mark.parametrize('codepoint', [0, 11, 12, 31, 0xfffe, 0xffff])
def test_toxml_rejects_forbidden_characters_in_header(codepoint):
    from petl.io.sources import MemorySource

    with pytest.raises(ValueError) as excinfo:
        toxml([(u'before' + (u'%c' % codepoint) + u'after',)],
              MemorySource(), escape=True)
    assert 'U+%04X' % codepoint in str(excinfo.value)


@pytest.mark.parametrize('style', ['tag', 'name', 'attribute'])
@pytest.mark.parametrize('value', [u'\t\n\r', u'\x7f\x85',
                                   u'\ud7ff\ue000\ufffd', u'\U0001f600'])
def test_toxml_keeps_representable_characters(style, value):
    from xml.etree import ElementTree
    from petl.io.sources import MemorySource

    sink = MemorySource()
    toxml([('label',), (value,)], sink, root='table', rows='row/col',
          style=style, escape=True)
    root = ElementTree.fromstring(sink.getvalue())
    if style == 'tag':
        actual = root.find('.//col').text
    elif style == 'name':
        actual = root.find('.//label').text
    else:
        actual = root.find('.//col').get('label')
    assert actual == value


@pytest.mark.parametrize('escape', [False, True])
def test_toxml_leaves_custom_template_values_unvalidated(escape):
    from petl.io.sources import MemorySource

    sink = MemorySource()
    toxml([('label',), ('raw\x0bvalue',)], sink, escape=escape,
          style='<row>{label}</row>')
    assert b'raw\x0bvalue' in sink.getvalue()


def test_toxml_keeps_forbidden_characters_with_raw_default():
    from petl.io.sources import MemorySource

    table = [('head\x0btext',), ('value\x00text',)]
    for kwargs in ({}, {'escape': False}):
        sink = MemorySource()
        toxml(table, sink, **kwargs)
        assert b'head\x0btext' in sink.getvalue()
        assert b'value\x00text' in sink.getvalue()


def test_toxml_does_not_validate_omitted_header_text():
    from xml.etree import ElementTree
    from petl.io.sources import MemorySource

    sink = MemorySource()
    toxml([('head\x0btext',), ('value',)], sink,
          root='table', rows='row/col', escape=True)
    assert ElementTree.fromstring(sink.getvalue()).find('.//col').text == 'value'


def test_toxml_validates_formatted_output_only():
    from xml.etree import ElementTree
    from petl.io.sources import MemorySource

    class Formatted(object):
        def __str__(self):
            return 'unused\x0btext'

        def __format__(self, spec):
            return 'R&D'

    sink = MemorySource()
    toxml([('label',), (Formatted(),)], sink, escape=True)
    assert ElementTree.fromstring(sink.getvalue()).find('.//td').text == 'R&D'


def test_toxml_rejects_forbidden_formatted_output():
    from petl.io.sources import MemorySource

    class Formatted(object):
        def __format__(self, spec):
            return 'bad\x0btext'

    with pytest.raises(ValueError):
        toxml([('label',), (Formatted(),)], MemorySource(), escape=True)
