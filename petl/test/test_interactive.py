from __future__ import absolute_import, print_function, division
# N.B., do not import unicode_literals in tests


import petl as etl
from petl.test.helpers import eq_


def test_repr():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    expect = str(etl.look(table,
                          index_header=etl.config.table_repr_index_header))
    actual = repr(etl.wrap(table))
    eq_(expect, actual)


def test_str():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    expect = str(etl.look(table, vrepr=str,
                          index_header=etl.config.table_str_index_header))
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
<th>0|foo</th>
<th>1|bar</th>
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
    for l1, l2 in zip(expect.split('\n'), actual.split('\r\n')):
        eq_(l1, l2)


def test_repr_html_limit():
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))

    # lower limit
    etl.config.table_repr_html_limit = 2

    expect = """<table class='petl'>
<thead>
<tr>
<th>0|foo</th>
<th>1|bar</th>
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
    for l1, l2 in zip(expect.split('\n'), actual.split('\r\n')):
        eq_(l1, l2)
