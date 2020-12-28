# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
try:
    # prefer lxml as it supports XPath
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

from operator import attrgetter
import itertools
from petl.compat import string_types, text_type


# internal dependencies
from petl.util.base import Table, fieldnames, iterpeek
from petl.io.sources import read_source_from_arg
from petl.io.text import totext


def fromxml(source, *args, **kwargs):
    """
    Extract data from an XML file. E.g.::

        >>> import petl as etl
        >>> # setup a file to demonstrate with
        ... d = '''<table>
        ...     <tr>
        ...         <td>foo</td><td>bar</td>
        ...     </tr>
        ...     <tr>
        ...         <td>a</td><td>1</td>
        ...     </tr>
        ...     <tr>
        ...         <td>b</td><td>2</td>
        ...     </tr>
        ...     <tr>
        ...         <td>c</td><td>2</td>
        ...     </tr>
        ... </table>'''
        >>> with open('example.file1.xml', 'w') as f:
        ...     f.write(d)
        ...
        212
        >>> table1 = etl.fromxml('example.file1.xml', 'tr', 'td')
        >>> table1
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' | '1' |
        +-----+-----+
        | 'b' | '2' |
        +-----+-----+
        | 'c' | '2' |
        +-----+-----+


    If the data values are stored in an attribute, provide the attribute
    name as an extra positional argument::

        >>> d = '''<table>
        ...     <tr>
        ...         <td v='foo'/><td v='bar'/>
        ...     </tr>
        ...     <tr>
        ...         <td v='a'/><td v='1'/>
        ...     </tr>
        ...     <tr>
        ...         <td v='b'/><td v='2'/>
        ...     </tr>
        ...     <tr>
        ...         <td v='c'/><td v='2'/>
        ...     </tr>
        ... </table>'''
        >>> with open('example.file2.xml', 'w') as f:
        ...     f.write(d)
        ...
        220
        >>> table2 = etl.fromxml('example.file2.xml', 'tr', 'td', 'v')
        >>> table2
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' | '1' |
        +-----+-----+
        | 'b' | '2' |
        +-----+-----+
        | 'c' | '2' |
        +-----+-----+

    Data values can also be extracted by providing a mapping of field
    names to element paths::

        >>> d = '''<table>
        ...     <row>
        ...         <foo>a</foo><baz><bar v='1'/><bar v='3'/></baz>
        ...     </row>
        ...     <row>
        ...         <foo>b</foo><baz><bar v='2'/></baz>
        ...     </row>
        ...     <row>
        ...         <foo>c</foo><baz><bar v='2'/></baz>
        ...     </row>
        ... </table>'''
        >>> with open('example.file3.xml', 'w') as f:
        ...     f.write(d)
        ...
        223
        >>> table3 = etl.fromxml('example.file3.xml', 'row',
        ...                      {'foo': 'foo', 'bar': ('baz/bar', 'v')})
        >>> table3
        +------------+-----+
        | bar        | foo |
        +============+=====+
        | ('1', '3') | 'a' |
        +------------+-----+
        | '2'        | 'b' |
        +------------+-----+
        | '2'        | 'c' |
        +------------+-----+

    If `lxml <http://lxml.de/>`_ is installed, full XPath expressions can be
    used.

    Note that the implementation is currently **not** streaming, i.e.,
    the whole document is loaded into memory.

    If multiple elements match a given field, all values are reported as a
    tuple.

    If there is more than one element name used for row values, a tuple
    or list of paths can be provided, e.g.,
    ``fromxml('example.file.html', './/tr', ('th', 'td'))``.

    Optionally a custom parser can be provided, e.g.::

        >>> from lxml import etree # doctest: +SKIP
        ... my_parser = etree.XMLParser(resolve_entities=False) # doctest: +SKIP
        ... table4 = etl.fromxml('example.file1.xml', 'tr', 'td', parser=my_parser) # doctest: +SKIP

    """

    source = read_source_from_arg(source)
    return XmlView(source, *args, **kwargs)


class XmlView(Table):

    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.args = args
        if len(args) == 2 and isinstance(args[1], (string_types, tuple, list)):
            self.rmatch = args[0]
            self.vmatch = args[1]
            self.vdict = None
            self.attr = None
        elif len(args) == 2 and isinstance(args[1], dict):
            self.rmatch = args[0]
            self.vmatch = None
            self.vdict = args[1]
            self.attr = None
        elif len(args) == 3:
            self.rmatch = args[0]
            self.vmatch = args[1]
            self.vdict = None
            self.attr = args[2]
        else:
            assert False, 'bad parameters'
        self.missing = kwargs.get('missing', None)
        self.user_parser = kwargs.get('parser', None)

    def __iter__(self):
        vmatch = self.vmatch
        vdict = self.vdict

        with self.source.open('rb') as xmlf:
            parser2 = _create_xml_parser(self.user_parser)
            tree = etree.parse(xmlf, parser=parser2)
            if not hasattr(tree, 'iterfind'):
                # Python 2.6 compatibility
                tree.iterfind = tree.findall

            if vmatch is not None:
                # simple case, all value paths are the same
                for rowelm in tree.iterfind(self.rmatch):
                    if self.attr is None:
                        getv = attrgetter('text')
                    else:
                        getv = lambda e: e.get(self.attr)
                    if isinstance(vmatch, string_types):
                        # match only one path
                        velms = rowelm.findall(vmatch)
                    else:
                        # match multiple paths
                        velms = itertools.chain(*[rowelm.findall(enm)
                                                  for enm in vmatch])
                    yield tuple(getv(velm)
                                for velm in velms)

            else:
                # difficult case, deal with different paths for each field

                # determine output header
                flds = tuple(sorted(map(text_type, vdict.keys())))
                yield flds

                # setup value getters
                vmatches = dict()
                vgetters = dict()
                for f in flds:
                    vmatch = self.vdict[f]
                    if isinstance(vmatch, string_types):
                        # match element path
                        vmatches[f] = vmatch
                        vgetters[f] = element_text_getter(self.missing)
                    else:
                        # match element path and attribute name
                        vmatches[f] = vmatch[0]
                        attr = vmatch[1]
                        vgetters[f] = attribute_text_getter(attr, self.missing)

                # determine data rows
                for rowelm in tree.iterfind(self.rmatch):
                    yield tuple(vgetters[f](rowelm.findall(vmatches[f]))
                                for f in flds)


def _create_xml_parser(user_parser):
    if user_parser is not None:
        return user_parser
    try:
        # Default lxml parser.
        # This will throw an error if parser is not set and lxml could not be imported
        # because Python's built XML parser doesn't like the `resolve_entities` kwarg.
        # return etree.XMLParser(resolve_entities=False)
        return etree.XMLParser(resolve_entities=False)
    except TypeError:
        # lxml not available
        return None


def element_text_getter(missing):
    def _get(v):
        if len(v) > 1:
            return tuple(e.text for e in v)
        elif len(v) == 1:
            return v[0].text
        else:
            return missing
    return _get


def attribute_text_getter(attr, missing):
    def _get(v):
        if len(v) > 1:
            return tuple(e.get(attr) for e in v)
        elif len(v) == 1:
            return v[0].get(attr)
        else:
            return missing
    return _get


def toxml(table, target=None,
          root=None, head=None, rows=None, prologue=None, epilogue=None,
          style='tag', encoding='utf-8'):
    """
    Write the table into a new xml file according to elements defined in the
    function arguments.

    The `root`, `head` and `rows` (string, optional) arguments define the tags
    and the nesting of the xml file. Each one defines xml elements with tags
    separated by slashes (`/`) like in `root/level/tag`. They can have a
    arbitrary number of tags that will reflect in more nesting levels for the
    header or record/row written in the xml file.

    For details on tag naming and nesting rules check xml `specification`_ or
    xml `references`_.

    The `rows` argument define the elements for each row of data to be written
    in the xml file. When specified, it must have at least 2 tags for defining
    the tags for `row/column`. Additional tags will add nesting enclosing all
    records/rows/lines.

    The `head` argument is similar to the rows, but aplies only to one line/row
    of header with fieldnames. When specified, it must have at least 2 tags for
    `fields/name` and the remaining will increase nesting.

    The `root` argument defines the elements enclosing `head` and `rows` and is
    required when using `head` for specifying valid xml documents.

    When none of this arguments are specified, they will default to tags that
    generate output similar to a html table:
    `root='table', head='there/tr/td', rows='tbody/tr/td'`.

    The `prologue` argument (string, optional) could be a snippet of valid xml
    that will be inserted before other elements in the xml. It can optionally
    specify the `XML Prolog` of the file.

    The `epilogue` argument (string, optional) could be a snippet of valid xml
    that will be inserted after all other xml elements except the root closing
    tag. It must specify a closing tag if the `root` argument is not specified. 

    The `style` argument select the format of the elements in the xml file. It
    can be `tag` (default), `name`, `attribute` or a custom string to format
    each row via
    `str.format <http://docs.python.org/library/stdtypes.html#str.format>`_.

    Example usage for writing files::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2]]
        >>> etl.toxml(table1, 'example.file4.xml')
        >>> # see what we did is similar a html table:
        >>> print(open('example.file4.xml').read())
        <?xml version="1.0" encoding="UTF-8"?>
        <table><thead>
         <tr><th>foo</th><th>bar</th></tr>
        </thead><tbody>
         <tr><td>a</td><td>1</td></tr>
         <tr><td>b</td><td>2</td></tr>
        </tbody></table>
        >>> # define the nesting in xml file:
        >>> etl.toxml(table1, 'example.file5.xml', rows='plan/line/cell')
        >>> print(open('example.file5.xml').read())
        <?xml version="1.0" encoding="UTF-8"?>
        <plan>
         <line><cell>a</cell><cell>1</cell></line>
         <line><cell>b</cell><cell>2</cell></line>
        </plan>
        >>> # choose other style:
        >>> etl.toxml(table1, 'example.file6.xml', rows='row/col', style='attribute')
        >>> print(open('example.file6.xml').read())
        <?xml version="1.0" encoding="UTF-8"?>
        <row>
         <col foo="a" bar="1" />
         <col foo="b" bar="2" />
        </row>
        >>> etl.toxml(table1, 'example.file6.xml', rows='row/col', style='name')
        >>> print(open('example.file6.xml').read())
        <?xml version="1.0" encoding="UTF-8"?>
        <row>
         <col><foo>a</foo><bar>1</bar></col>
         <col><foo>b</foo><bar>2</bar></col>
        </row>

    The `toxml()` function is just a wrapper over :func:`petl.io.text.totext`.
    For advanced cases use a template with `totext()` for generating xml files.

    .. versionadded:: 1.7.0

    .. _specification: https://www.w3.org/TR/xml/
    .. _references: https://www.w3schools.com/xml/xml_syntax.asp

    """
    if not root and not head and not rows:
        root = 'table'
        head = 'thead/tr/th'
        rows = 'tbody/tr/td'

    sample, table2 = iterpeek(table, 2)
    props = fieldnames(sample)

    top = _build_xml_header(style, props, root, head, rows, prologue, encoding)
    template = _build_cols(style, props, rows, True)
    bottom = _build_xml_footer(style, epilogue, rows, root)

    totext(table2, source=target, encoding=encoding, errors='strict',
           template=template, prologue=top, epilogue=bottom)


def _build_xml_header(style, props, root, head, rows, prologue, encoding):
    tab = _build_nesting(root, False, None) if root else ''
    nested = -1 if style in ('attribute', 'name') else -2
    if head:
        th1 = _build_nesting(head, False, nested)
        col = _build_cols(style, props, head, False)
        th2 = _build_nesting(head, True, nested)
        thd = '{0}\n{1}{2}'.format(th1, col, th2)
    else:
        thd = ''
    tbd = _build_nesting(rows, False, nested)
    if prologue and prologue.startswith('<?xml'):
        thb = '{0}{1}{2}\n'.format(tab, thd, tbd)
        return prologue + thb
    enc = encoding.upper() if encoding else 'UTF-8'
    xml = '<?xml version="1.0" encoding="%s"?>' % enc
    pre = prologue + '\n' if prologue and not root else ''
    pos = '\n' + prologue if prologue and root else ''
    res = '{0}\n{1}{2}{3}{4}{5}\n'.format(xml, pre, tab, thd, tbd, pos)
    return res


def _build_xml_footer(style, epilogue, rows, root):
    nested = -1 if style in ('attribute', 'name') else -2
    tbd = _build_nesting(rows, True, nested)
    tab = _build_nesting(root, True, 0)
    pre = epilogue + '\n' if epilogue and root else ''
    pos = '\n' + epilogue if epilogue and not root else ''
    return pre + tbd + tab + pos


def _build_nesting(path, closing, index):
    if not path:
        return ''
    fmt = '</%s>' if closing else '<%s>'
    if '/' not in path:
        return fmt % path
    parts = path.split('/')
    elements = parts[0:index] if index else parts
    if closing:
        elements.reverse()
    tags = [fmt % e for e in elements]
    return ''.join(tags)


def _build_cols(style, props, path, is_value):
    is_header = not is_value
    if style == 'tag' or is_header:
        return _build_cols_inline(props, path, is_value, True)
    if style == 'name':
        return _build_cols_inline(props, path, is_value, False)
    if style == 'attribute':
        return _build_cols_attribs(props, path)
    return style  # custom


def _build_cols_inline(props, path, is_value, use_tag):
    parts = path.split('/')
    if use_tag:
        if len(parts) < 2:
            raise ValueError("Tag not in format 'row/col': %s" % path)            
        col = parts[-1]
        row = parts[-2:-1][0]
    else:
        col = '{0}'
        row = parts[-1]
    fld = '{{{0}}}' if is_value else '{0}'
    fmt = '<{0}>{1}</{0}>'.format(col, fld)
    cols = [fmt.format(e) for e in props]
    tags = ''.join(cols)
    res = ' <{0}>{1}</{0}>\n'.format(row, tags)
    return res


def _build_cols_attribs(props, path):
    parts = path.split('/')
    row = parts[-1]
    fmt = '{0}="{{{0}}}"'
    cols = [fmt.format(e) for e in props]
    atts = ' '.join(cols)
    res = ' <{0} {1} />\n'.format(row, atts)
    return res
