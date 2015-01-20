# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import sys


from petl.test.helpers import ieq
from petl.util import nrows, look
from petl.io.xml import fromxml
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


url = 'http://feeds.bbci.co.uk/news/rss.xml'
# check internet connection
try:
    urlopen(url)
except Exception as e:
    print('SKIP test_fromxml_url: %s' % e, file=sys.stderr)
else:

    def test_fromxml_url():

            tbl = fromxml(url, './/item', 'title')
            assert nrows(tbl) > 0
