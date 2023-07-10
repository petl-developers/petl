# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
import io
from petl.compat import next, PY2, text_type


# internal dependencies
from petl.util.base import Table, asdict
from petl.io.base import getcodec
from petl.io.sources import read_source_from_arg, write_source_from_arg


def fromtext(source=None, encoding=None, errors='strict', strip=None,
             header=('lines',)):
    """
    Extract a table from lines in the given text file. E.g.::

        >>> import petl as etl
        >>> # setup example file
        ... text = 'a,1\\nb,2\\nc,2\\n'
        >>> with open('example.txt', 'w') as f:
        ...     f.write(text)
        ...
        12
        >>> table1 = etl.fromtext('example.txt')
        >>> table1
        +-------+
        | lines |
        +=======+
        | 'a,1' |
        +-------+
        | 'b,2' |
        +-------+
        | 'c,2' |
        +-------+

        >>> # post-process, e.g., with capture()
        ... table2 = table1.capture('lines', '(.*),(.*)$', ['foo', 'bar'])
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

    Note that the strip() function is called on each line, which by default
    will remove leading and trailing whitespace, including the end-of-line
    character - use the `strip` keyword argument to specify alternative
    characters to strip. Set the `strip` argument to `False` to disable this
    behaviour and leave line endings in place.

    """

    source = read_source_from_arg(source)
    return TextView(source, header=header, encoding=encoding,
                    errors=errors, strip=strip)


class TextView(Table):

    def __init__(self, source, header=('lines',), encoding=None,
                 errors='strict', strip=None):
        self.source = source
        self.header = header
        self.encoding = encoding
        self.errors = errors
        self.strip = strip

    def __iter__(self):
        with self.source.open('rb') as buf:

            # deal with text encoding
            if PY2:
                codec = getcodec(self.encoding)
                f = codec.streamreader(buf, errors=self.errors)
            else:
                f = io.TextIOWrapper(buf,
                                     encoding=self.encoding,
                                     errors=self.errors,
                                     newline='')

            # generate the table
            try:
                if self.header is not None:
                    yield tuple(self.header)
                if self.strip is False:
                    for line in f:
                        yield (line,)
                else:
                    for line in f:
                        yield (line.strip(self.strip),)
            finally:
                if not PY2:
                    f.detach()


def totext(table, source=None, encoding=None, errors='strict', template=None,
           prologue=None, epilogue=None):
    """
    Write the table to a text file. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
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
        >>> etl.totext(table1, 'example.txt', template=template,
        ... prologue=prologue, epilogue=epilogue)
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

    _writetext(table, source=source, mode='wb', encoding=encoding,
               errors=errors, template=template, prologue=prologue,
               epilogue=epilogue)


Table.totext = totext


def appendtext(table, source=None, encoding=None, errors='strict', template=None,
               prologue=None, epilogue=None):
    """
    Append the table to a text file.

    """

    _writetext(table, source=source, mode='ab', encoding=encoding,
               errors=errors, template=template, prologue=prologue,
               epilogue=epilogue)


Table.appendtext = appendtext


def _writetext(table, source, mode, encoding, errors, template, prologue,
               epilogue):

    # guard conditions
    assert template is not None, 'template is required'

    # prepare source
    source = write_source_from_arg(source, mode)

    with source.open(mode) as buf:

        # deal with text encoding
        if PY2:
            codec = getcodec(encoding)
            f = codec.streamwriter(buf, errors=errors)
        else:
            f = io.TextIOWrapper(buf,
                                 encoding=encoding,
                                 errors=errors,
                                 newline='')

        # write the table
        try:
            if prologue is not None:
                f.write(prologue)
            it = iter(table)
            try:
                hdr = next(it)
            except StopIteration:
                hdr = []
            flds = list(map(text_type, hdr))
            for row in it:
                rec = asdict(flds, row)
                s = template.format(**rec)
                f.write(s)
            if epilogue is not None:
                f.write(epilogue)
            f.flush()

        finally:
            if not PY2:
                f.detach()


def teetext(table, source=None, encoding=None, errors='strict', template=None,
            prologue=None, epilogue=None):
    """
    Return a table that writes rows to a text file as they are iterated over.

    """

    assert template is not None, 'template is required'
    return TeeTextView(table, source=source, encoding=encoding, errors=errors,
                       template=template, prologue=prologue, epilogue=epilogue)


Table.teetext = teetext


class TeeTextView(Table):

    def __init__(self, table, source=None, encoding=None, errors='strict',
                 template=None, prologue=None, epilogue=None):
        self.table = table
        self.source = source
        self.encoding = encoding
        self.errors = errors
        self.template = template
        self.prologue = prologue
        self.epilogue = epilogue

    def __iter__(self):
        return _iterteetext(self.table, self.source, self.encoding,
                            self.errors, self.template, self.prologue,
                            self.epilogue)


def _iterteetext(table, source, encoding, errors, template, prologue, epilogue):

    # guard conditions
    assert template is not None, 'template is required'

    # prepare source
    source = write_source_from_arg(source)

    with source.open('wb') as buf:

        # deal with text encoding
        if PY2:
            codec = getcodec(encoding)
            f = codec.streamwriter(buf, errors=errors)
        else:
            f = io.TextIOWrapper(buf,
                                 encoding=encoding,
                                 errors=errors)

        # write the data
        try:
            if prologue is not None:
                f.write(prologue)
            it = iter(table)
            try:
                hdr = next(it)
            except StopIteration:
                return
            yield tuple(hdr)
            flds = list(map(text_type, hdr))
            for row in it:
                rec = asdict(flds, row)
                s = template.format(**rec)
                f.write(s)
                yield row
            if epilogue is not None:
                f.write(epilogue)
            f.flush()

        finally:
            if not PY2:
                f.detach()
