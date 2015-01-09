from __future__ import absolute_import, print_function, division


import petl as etl
from petl.test.helpers import eq_


def test_repr():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    expect = str(etl.look(table))
    actual = repr(etl.wrap(table))
    eq_(expect, actual)


def test_str():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    expect = str(etl.look(table, vrepr=str))
    actual = str(etl.wrap(table))
    eq_(expect, actual)


def test_repr_html():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    expect = """<table class='petl'>
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
<td style='text-align: right'>2</td>
</tr>
<tr>
<td>c</td>
<td style='text-align: right'>2</td>
</tr>
</tbody>
</table>
"""
    actual = etl.wrap(table)._repr_html_()
    for l1, l2 in zip(expect.split('\n'), actual.split('\n')):
        eq_(l1, l2)


def test_repr_html_limit():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))

    # lower limit
    etl.config.display_limit = 2

    expect = """<table class='petl'>
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
<td style='text-align: right'>2</td>
</tr>
</tbody>
</table>
<p><strong>...</strong></p>
"""
    actual = etl.wrap(table)._repr_html_()
    print(actual)
    for l1, l2 in zip(expect.split('\n'), actual.split('\n')):
        eq_(l1, l2)
