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
from petl.util.base import Table
from petl.io.sources import read_source_from_arg


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
        >>> with open('example1.xml', 'w') as f:
        ...     f.write(d)
        ...
        212
        >>> table1 = etl.fromxml('example1.xml', 'tr', 'td')
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
        >>> with open('example2.xml', 'w') as f:
        ...     f.write(d)
        ...
        220
        >>> table2 = etl.fromxml('example2.xml', 'tr', 'td', 'v')
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
        >>> with open('example3.xml', 'w') as f:
        ...     f.write(d)
        ...
        223
        >>> table3 = etl.fromxml('example3.xml', 'row',
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
    ``fromxml('example.html', './/tr', ('th', 'td'))``.

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

    def __iter__(self):
        vmatch = self.vmatch
        vdict = self.vdict

        with self.source.open('rb') as xmlf:

            tree = etree.parse(xmlf)
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
