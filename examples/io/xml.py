from __future__ import division, print_function, absolute_import


import petl as etl
# setup a file to demonstrate with
d = '''<table>
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
</table>'''
with open('example1.xml', 'w') as f:
    f.write(d)

table1 = etl.fromxml('example1.xml', 'tr', 'td')
table1
# if the data values are stored in an attribute, provide the attribute name
# as an extra positional argument
d = '''<table>
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
</table>'''
with open('example2.xml', 'w') as f:
    f.write(d)

table2 = etl.fromxml('example2.xml', 'tr', 'td', 'v')
table2
# data values can also be extracted by providing a mapping of field
# names to element paths
d = '''<table>
    <row>
        <foo>a</foo><baz><bar v='1'/><bar v='3'/></baz>
    </row>
    <row>
        <foo>b</foo><baz><bar v='2'/></baz>
    </row>
    <row>
        <foo>c</foo><baz><bar v='2'/></baz>
    </row>
</table>'''
with open('example3.xml', 'w') as f:
    f.write(d)

table3 = etl.fromxml('example3.xml', 'row',
                     {'foo': 'foo', 'bar': ('baz/bar', 'v')})
table3
