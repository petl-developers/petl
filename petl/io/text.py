from __future__ import absolute_import, print_function, division, \
    unicode_literals


# standard library dependencies
import codecs
import io
from petl.compat import next, PY2


# internal dependencies
from petl.util import RowContainer, asdict
from petl.io.sources import read_source_from_arg, write_source_from_arg


def fromtext(source=None, header=('lines',), strip=None):
    """Extract a table from lines in the given text file. E.g.::

        >>> # example data
        ... text = 'a,1\\nb,2\\nc,2\\n'
        >>> with open('example.txt', 'w') as f:
        ...     f.write(text)
        ...
        12
        >>> from petl import fromtext, look
        >>> table1 = fromtext('example.txt')
        >>> look(table1)
        +---------+
        | 'lines' |
        +=========+
        | 'a,1'   |
        +---------+
        | 'b,2'   |
        +---------+
        | 'c,2'   |
        +---------+

        >>> # now post-process,e.g., with capture()
        ... from petl import capture
        >>> table2 = capture(table1, 'lines', '(.*),(.*)$', ['foo', 'bar'])
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+

    Note that the strip() function is called on each line, which by default
    will remove leading and trailing whitespace, including the end-of-line
    character - use the `strip` keyword argument to specify alternative
    characters to strip.

    """

    source = read_source_from_arg(source)
    return TextView(source, header=header, strip=strip)


def fromutext(source=None, header=(u'lines',), encoding='utf-8', strip=None):
    """Extract a table from lines in the given text file using the given
    encoding. Like :func:`fromtext` but accepts an additional ``encoding``
    argument which should be one of the Python supported encodings.

    """

    source = read_source_from_arg(source)
    return UnicodeTextView(source, header=header, encoding=encoding,
                           strip=strip)


if PY2:

    class TextView(RowContainer):

        def __init__(self, source, header=('lines',), strip=None):
            self.source = source
            self.header = header
            self.strip = strip

        def __iter__(self):
            with self.source.open_('rU') as f:
                if self.header is not None:
                    yield tuple(self.header)
                s = self.strip
                for line in f:
                    yield (line.strip(s),)

    class UnicodeTextView(RowContainer):

        def __init__(self, source, header=(u'lines',), encoding='utf-8',
                     strip=None):
            self.source = source
            self.header = header
            self.encoding = encoding
            self.strip = strip

        def __iter__(self):
            with self.source.open_('rb') as f:
                f = codecs.getreader(self.encoding)(f)
                if self.header is not None:
                    yield tuple(self.header)
                s = self.strip
                for line in f:
                    yield (line.strip(s),)


else:

    class TextView(RowContainer):

        def __init__(self, source, encoding='ascii', header=('lines',),
                     strip=None):
            self.source = source
            self.encoding = encoding
            self.header = header
            self.strip = strip

        def __iter__(self):
            with self.source.open_('rb') as buffer:
                f = io.TextIOWrapper(buffer, encoding=self.encoding, newline='')
                try:
                    if self.header is not None:
                        yield tuple(self.header)
                    s = self.strip
                    for line in f:
                        yield (line.strip(s),)
                finally:
                    f.detach()

    UnicodeTextView = TextView


def totext(table, source=None, template=None, prologue=None, epilogue=None):
    """Write the table to a text file. E.g.::

        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> from petl import totext, look
        >>> prologue = '''{| class="wikitable"
        ... |-
        ... ! foo
        ... ! bar
        ... '''
        >>> template = '''|-
        ... | {foo}
        ... | {bar}
        ... '''
        >>> epilogue = '|}'
        >>> totext(table1, 'example.txt', template, prologue, epilogue)
        >>> # see what we did
        ... print(open('example.txt').read())
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

    """

    assert template is not None, 'template is required'
    source = write_source_from_arg(source)
    with source.open_('wb') as f:
        if PY2:
            # write direct to buffer
            _writetext(table, f, prologue, template, epilogue)
        else:
            # wrap buffer for text encoding
            f = io.TextIOWrapper(f, encoding='ascii', newline='',
                                 write_through=True)
            try:
                _writetext(table, f, prologue, template, epilogue)
            finally:
                f.detach()


def appendtext(table, source=None, template=None, prologue=None, epilogue=None):
    """Append the table to a text file.

    """

    assert template is not None, 'template is required'
    source = write_source_from_arg(source)
    with source.open_('ab') as f:
        if PY2:
            # write direct to buffer
            _writetext(table, f, prologue, template, epilogue)
        else:
            # wrap buffer for text encoding
            f = io.TextIOWrapper(f, encoding='ascii', newline='',
                                 write_through=True)
            try:
                _writetext(table, f, prologue, template, epilogue)
            finally:
                f.detach()


def toutext(table, source=None, encoding='utf-8', template=None, prologue=None,
            epilogue=None):
    """Write the table to a text file via the given encoding. Like
    :func:`totext` but accepts an additional ``encoding`` argument which
    should be one of the Python supported encodings.

    """

    assert template is not None, 'template is required'
    source = write_source_from_arg(source)
    with source.open_('wb') as f:
        if PY2:
            f = codecs.getwriter(encoding)(f)
            _writetext(table, f, prologue, template, epilogue)
        else:
            f = io.TextIOWrapper(f, encoding=encoding, newline='',
                                 write_through=True)
            try:
                _writetext(table, f, prologue, template, epilogue)
            finally:
                f.detach()


def appendutext(table, source=None, encoding='utf-8', template=None,
                prologue=None, epilogue=None):
    """Append the table to a text file via the given encoding. Like
    :func:`appendtext` but accepts an additional ``encoding`` argument which
    should be one of the Python supported encodings.

    """

    assert template is not None, 'template is required'
    source = write_source_from_arg(source)
    with source.open_('ab') as f:
        if PY2:
            f = codecs.getwriter(encoding)(f)
            _writetext(table, f, prologue, template, epilogue)
        else:
            f = io.TextIOWrapper(f, encoding=encoding, newline='',
                                 write_through=True)
            try:
                _writetext(table, f, prologue, template, epilogue)
            finally:
                f.detach()


def _writetext(table, f, prologue, template, epilogue):
    if prologue is not None:
        f.write(prologue)
    it = iter(table)
    flds = next(it)
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
    flds = next(it)
    yield flds
    for row in it:
        rec = asdict(flds, row)
        s = template.format(**rec)
        f.write(s)
        yield row
    if epilogue is not None:
        f.write(epilogue)


def teetext(table, source=None, template=None, prologue=None, epilogue=None):
    """Return a table that writes rows to a text file as they are iterated over.

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
        with source.open_('wb') as f:
            if PY2:
                # write direct to buffer
                for row in _teetext(self.table, f, self.prologue, self.template,
                                    self.epilogue):
                    yield row
            else:
                # wrap buffer for text encoding
                f = io.TextIOWrapper(f, encoding='ascii', newline='',
                                     write_through=True)
                try:
                    for row in _teetext(self.table, f, self.prologue,
                                        self.template, self.epilogue):
                        yield row
                finally:
                    f.detach()


def teeutext(table, source=None, encoding='utf-8', template=None,
             prologue=None, epilogue=None):
    """Return a table that writes rows to a Unicode text file as they are
    iterated over.

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
        with source.open_('wb') as f:
            if PY2:
                f = codecs.getwriter(self.encoding)(f)
                for row in _teetext(self.table, f, self.prologue, self.template,
                                    self.epilogue):
                    yield row
            else:
                f = io.TextIOWrapper(f, encoding=self.encoding, newline='',
                                     write_through=True)
                try:
                    for row in _teetext(self.table, f, self.prologue,
                                        self.template, self.epilogue):
                        yield row
                finally:
                    f.detach()
