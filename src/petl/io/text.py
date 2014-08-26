from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


# standard library dependencies
import codecs


# internal dependencies
from petl.util import RowContainer, asdict
from petl.io.sources import read_source_from_arg, write_source_from_arg


def fromtext(source=None, header=('lines',), strip=None):
    """
    Construct a table from lines in the given text file. E.g.::

        >>> # example data
        ... with open('test.txt', 'w') as f:
        ...     f.write('a\\t1\\n')
        ...     f.write('b\\t2\\n')
        ...     f.write('c\\t3\\n')
        ...
        >>> from petl import fromtext, look
        >>> table1 = fromtext('test.txt')
        >>> look(table1)
        +--------------+
        | 'lines'      |
        +==============+
        | 'a\\t1'      |
        +--------------+
        | 'b\\t2'      |
        +--------------+
        | 'c\\t3'      |
        +--------------+

    The :func:`fromtext` function provides a starting point for custom handling
    of text files. E.g., using :func:`capture`::

        >>> from petl import capture
        >>> table2 = capture(table1, 'lines', '(.*)\\\\t(.*)$', ['foo', 'bar'])
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '3'   |
        +-------+-------+

    Supports transparent reading from URLs, ``.gz`` and ``.bz2`` files.

    .. versionchanged:: 0.4

    The strip() function is called on each line, which by default will remove
    leading and trailing whitespace, including the end-of-line character - use
    the `strip` keyword argument to specify alternative characters to strip.

    """

    source = read_source_from_arg(source)
    return TextView(source, header, strip=strip)


class TextView(RowContainer):

    def __init__(self, source, header=('lines',), strip=None):
        self.source = source
        self.header = header
        self.strip = strip

    def __iter__(self):
        with self.source.open_('r') as f:
            if self.header is not None:
                yield tuple(self.header)
            s = self.strip
            for line in f:
                yield (line.strip(s),)


def fromutext(source=None, header=(u'lines',), encoding='utf-8', strip=None):
    """
    Construct a table from lines in the given text file via the given encoding.
    Like :func:`fromtext` but accepts an additional ``encoding`` argument
    which should be one of the Python supported encodings. See also
    :mod:`codecs`.

    .. versionadded:: 0.19

    """
    source = read_source_from_arg(source)
    return UnicodeTextView(source, header, encoding=encoding, strip=strip)


class UnicodeTextView(RowContainer):

    def __init__(self, source, header=(u'lines',), encoding='utf-8',
                 strip=None):
        self.source = source
        self.header = header
        self.encoding = encoding
        self.strip = strip

    def __iter__(self):
        with self.source.open_('r') as f:
            f = codecs.getreader(self.encoding)(f)
            if self.header is not None:
                yield tuple(self.header)
            s = self.strip
            for line in f:
                yield (line.strip(s),)


def totext(table, source=None, template=None, prologue=None, epilogue=None):
    """
    Write the table to a text file. E.g.::

        >>> from petl import totext, look
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+

        >>> prologue = \"\"\"{| class="wikitable"
        ... |-
        ... ! foo
        ... ! bar
        ... \"\"\"
        >>> template = \"\"\"|-
        ... | {foo}
        ... | {bar}
        ... \"\"\"
        >>> epilogue = "|}"
        >>> totext(table, 'test.txt', template, prologue, epilogue)
        >>>
        >>> # see what we did
        ... with open('test.txt') as f:
        ...     print f.read()
        ...
        {| class="wikitable"
        |-
        ! foo
        ! bar
        |-
        | a
        | 1
        |-
        | b
        | 2
        |-
        | c
        | 2
        |}

    The `template` will be used to format each row via
    `str.format <http://docs.python.org/library/stdtypes.html#str.format>`_.

    Supports transparent writing to ``.gz`` and ``.bz2`` files.

    """

    assert template is not None, 'template is required'
    source = write_source_from_arg(source)
    with source.open_('w') as f:
        _writetext(table, f, prologue, template, epilogue)


def appendtext(table, source=None, template=None, prologue=None, epilogue=None):
    """
    Append the table to a text file.

    .. versionadded:: 0.19
    """

    assert template is not None, 'template is required'
    source = write_source_from_arg(source)
    with source.open_('a') as f:
        _writetext(table, f, prologue, template, epilogue)


def toutext(table, source=None, encoding='utf-8', template=None, prologue=None,
            epilogue=None):
    """
    Write the table to a text file via the given encoding. Like :func:`totext`
    but accepts an additional ``encoding`` argument which should be one of
    the Python supported encodings. See also :mod:`codecs`.

    .. versionadded:: 0.19
    """

    assert template is not None, 'template is required'
    if prologue is not None:
        prologue = unicode(prologue)
    template = unicode(template)
    if epilogue is not None:
        epilogue = unicode(epilogue)
    source = write_source_from_arg(source)
    with source.open_('w') as f:
        f = codecs.getwriter(encoding)(f)
        _writetext(table, f, prologue, template, epilogue)


def appendutext(table, source=None, encoding='utf-8', template=None,
                prologue=None, epilogue=None):
    """
    Append the table to a text file via the given encoding. Like
    :func:`appendtext` but accepts an additional ``encoding`` argument which
    should be one of the Python supported encodings. See also :mod:`codecs`.

    .. versionadded:: 0.19
    """

    assert template is not None, 'template is required'
    if prologue is not None:
        prologue = unicode(prologue)
    template = unicode(template)
    if epilogue is not None:
        epilogue = unicode(epilogue)
    source = write_source_from_arg(source)
    with source.open_('a') as f:
        f = codecs.getwriter(encoding)(f)
        _writetext(table, f, prologue, template, epilogue)


def _writetext(table, f, prologue, template, epilogue):
    if prologue is not None:
        f.write(prologue)
    it = iter(table)
    flds = it.next()
    for row in it:
        rec = asdict(flds, row)
        s = template.format(**rec)
        f.write(s)
    if epilogue is not None:
        f.write(epilogue)


def _teetext(table, f, prologue, template, epilogue):
    if prologue is not None:
        f.write(prologue)
    it = iter(table)
    flds = it.next()
    yield flds
    for row in it:
        rec = asdict(flds, row)
        s = template.format(**rec)
        f.write(s)
        yield row
    if epilogue is not None:
        f.write(epilogue)


def teetext(table, source=None, template=None, prologue=None, epilogue=None):
    """
    Return a table that writes rows to a text file as they are iterated over.

    .. versionadded:: 0.25

    """

    assert template is not None, 'template is required'
    return TeeTextContainer(table, source=source, template=template,
                            prologue=prologue, epilogue=epilogue)


class TeeTextContainer(RowContainer):

    def __init__(self, table, source=None, template=None, prologue=None,
                 epilogue=None):
        self.table = table
        self.source = source
        self.template = template
        self.prologue = prologue
        self.epilogue = epilogue

    def __iter__(self):
        source = write_source_from_arg(self.source)
        with source.open_('w') as f:
            for row in _teetext(self.table, f, self.prologue, self.template,
                                self.epilogue):
                yield row


def teeutext(table, source=None, encoding='utf-8', template=None,
             prologue=None, epilogue=None):
    """
    Return a table that writes rows to a Unicode text file as they are
    iterated over.

    .. versionadded:: 0.25

    """

    assert template is not None, 'template is required'
    return TeeUTextContainer(table, source=source,
                             encoding=encoding, template=template,
                             prologue=prologue, epilogue=epilogue)


class TeeUTextContainer(RowContainer):

    def __init__(self, table, source=None, encoding='utf-8', template=None,
                 prologue=None, epilogue=None):
        self.table = table
        self.source = source
        self.encoding = encoding
        self.template = template
        self.prologue = prologue
        self.epilogue = epilogue

    def __iter__(self):
        source = write_source_from_arg(self.source)
        prologue = self.prologue
        if prologue is not None:
            prologue = unicode(prologue)
        template = unicode(self.template)
        epilogue = self.epilogue
        if epilogue is not None:
            epilogue = unicode(epilogue)
        with source.open_('w') as f:
            f = codecs.getwriter(self.encoding)(f)
            for row in _teetext(self.table, f, prologue, template,
                                epilogue):
                yield row
