from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
from petl.test.helpers import eq_


from petl.io.html import tohtml


def test_tohtml():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', (1, 2)),
             ('c', False))

    f = NamedTemporaryFile(delete=False)
    tohtml(table, f.name, encoding='ascii', lineterminator='\n')

    # check what it did
    with open(f.name, 'rb') as o:
        actual = o.read()
        expect = b"""<table class='petl'>
<thead>
<tr>
<th>foo</th>
<th>bar</th>
</tr>
</thead>
<tbody>
<tr>
<td>a</td>
<td style='text-align: right'>1</td>
</tr>
<tr>
<td>b</td>
<td>(1, 2)</td>
</tr>
<tr>
<td>c</td>
<td>False</td>
</tr>
</tbody>
</table>
"""
        eq_(expect, actual)


def test_tohtml_caption():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', (1, 2)))
    f = NamedTemporaryFile(delete=False)
    tohtml(table, f.name, encoding='ascii', caption='my table',
           lineterminator='\n')

    # check what it did
    with open(f.name, 'rb') as o:
        actual = o.read()
        expect = b"""<table class='petl'>
<caption>my table</caption>
<thead>
<tr>
<th>foo</th>
<th>bar</th>
</tr>
</thead>
<tbody>
<tr>
<td>a</td>
<td style='text-align: right'>1</td>
</tr>
<tr>
<td>b</td>
<td>(1, 2)</td>
</tr>
</tbody>
</table>
"""
        eq_(expect, actual)
