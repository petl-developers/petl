# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

# standard library dependencies
import io
import json
import inspect
from json.encoder import JSONEncoder
from os import unlink
from tempfile import NamedTemporaryFile

from petl.compat import PY2
from petl.compat import pickle
from petl.io.sources import read_source_from_arg, write_source_from_arg
# internal dependencies
from petl.util.base import data, Table, dicts as _dicts, iterpeek


def fromjson(source, *args, **kwargs):
    """
    Extract data from a JSON file. The file must contain a JSON array as
    the top level object, and each member of the array will be treated as a
    row of data. E.g.::

        >>> import petl as etl
        >>> data = '''
        ... [{"foo": "a", "bar": 1},
        ... {"foo": "b", "bar": 2},
        ... {"foo": "c", "bar": 2}]
        ... '''
        >>> with open('example.file1.json', 'w') as f:
        ...     f.write(data)
        ...
        74
        >>> table1 = etl.fromjson('example.file1.json', header=['foo', 'bar'])
        >>> table1
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' |   2 |
        +-----+-----+

    Setting argument `lines` to `True` will enable to
    infer the document as a JSON lines document. For more details about JSON lines
    please visit https://jsonlines.org/.

        >>> import petl as etl
        >>> data_with_jlines = '''{"name": "Gilbert", "wins": [["straight", "7S"], ["one pair", "10H"]]}
        ... {"name": "Alexa", "wins": [["two pair", "4S"], ["two pair", "9S"]]}
        ... {"name": "May", "wins": []}
        ... {"name": "Deloise", "wins": [["three of a kind", "5S"]]}'''
        ...
        >>> with open('example.file2.json', 'w') as f:
        ...     f.write(data_with_jlines)
        ...
        223
        >>> table2 = etl.fromjson('example.file2.json', lines=True)
        >>> table2
        +-----------+-------------------------------------------+
        | name      | wins                                      |
        +===========+===========================================+
        | 'Gilbert' | [['straight', '7S'], ['one pair', '10H']] |
        +-----------+-------------------------------------------+
        | 'Alexa'   | [['two pair', '4S'], ['two pair', '9S']]  |
        +-----------+-------------------------------------------+
        | 'May'     | []                                        |
        +-----------+-------------------------------------------+
        | 'Deloise' | [['three of a kind', '5S']]               |
        +-----------+-------------------------------------------+

    If your JSON file does not fit this structure, you will need to parse it
    via :func:`json.load` and select the array to treat as the data, see also
    :func:`petl.io.json.fromdicts`.

    .. versionchanged:: 1.1.0

    If no `header` is specified, fields will be discovered by sampling keys
    from the first `sample` objects in `source`. The header will be
    constructed from keys in the order discovered. Note that this
    ordering may not be stable, and therefore it may be advisable to specify
    an explicit `header` or to use another function like
    :func:`petl.transform.headers.sortheader` on the resulting table to
    guarantee stability.

    """

    source = read_source_from_arg(source)
    return JsonView(source, *args, **kwargs)


class JsonView(Table):
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.missing = kwargs.pop('missing', None)
        self.header = kwargs.pop('header', None)
        self.sample = kwargs.pop('sample', 1000)
        self.lines = kwargs.pop('lines', False)
        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        with self.source.open('rb') as f:
            if not PY2:
                # wrap buffer for text IO
                f = io.TextIOWrapper(f, encoding='utf-8', newline='',
                                     write_through=True)
            try:
                if self.lines:
                    for row in iterjlines(f, self.header, self.missing):
                        yield row
                else:
                    dicts = json.load(f, *self.args, **self.kwargs)
                    for row in iterdicts(dicts, self.header, self.sample,
                                         self.missing):
                        yield row
            finally:
                if not PY2:
                    f.detach()


def fromdicts(dicts, header=None, sample=1000, missing=None):
    """
    View a sequence of Python :class:`dict` as a table. E.g.::

        >>> import petl as etl
        >>> dicts = [{"foo": "a", "bar": 1},
        ...          {"foo": "b", "bar": 2},
        ...          {"foo": "c", "bar": 2}]
        >>> table1 = etl.fromdicts(dicts, header=['foo', 'bar'])
        >>> table1
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' |   2 |
        +-----+-----+

    Argument `dicts` can also be a generator, the output of generator
    is iterated and cached using a temporary file to support further
    transforms and multiple passes of the table:

        >>> import petl as etl
        >>> dicts = ({"foo": chr(ord("a")+i), "bar":i+1} for i in range(3))
        >>> table1 = etl.fromdicts(dicts, header=['foo', 'bar'])
        >>> table1
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' |   3 |
        +-----+-----+

    If `header` is not specified, `sample` items from `dicts` will be
    inspected to discovery dictionary keys. Note that the order in which
    dictionary keys are discovered may not be stable,

    See also :func:`petl.io.json.fromjson`.

    .. versionchanged:: 1.1.0

    If no `header` is specified, fields will be discovered by sampling keys
    from the first `sample` dictionaries in `dicts`. The header will be
    constructed from keys in the order discovered. Note that this
    ordering may not be stable, and therefore it may be advisable to specify
    an explicit `header` or to use another function like
    :func:`petl.transform.headers.sortheader` on the resulting table to
    guarantee stability.

    .. versionchanged:: 1.7.5

    Full support of generators passed as `dicts` has been added, leveraging
    `itertools.tee`.

    .. versionchanged:: 1.7.11

    Generator support has been modified to use temporary file cache
    instead of `itertools.tee` due to high memory usage.

    """
    view = DictsGeneratorView if inspect.isgenerator(dicts) else DictsView
    return view(dicts, header=header, sample=sample, missing=missing)


class DictsView(Table):

    def __init__(self, dicts, header=None, sample=1000, missing=None):
        self.dicts = dicts
        self._header = header
        self.sample = sample
        self.missing = missing

    def __iter__(self):
        return iterdicts(self.dicts, self._header, self.sample, self.missing)


class DictsGeneratorView(DictsView):

    def __init__(self, dicts, header=None, sample=1000, missing=None):
        super(DictsGeneratorView, self).__init__(dicts, header, sample, missing)
        self._filecache = None
        self._cached = 0

    def __iter__(self):
        if not self._header:
            self._determine_header()
        yield self._header

        if not self._filecache:
            if PY2:
                self._filecache = NamedTemporaryFile(delete=False, mode='wb+', bufsize=0)
            else:
                self._filecache = NamedTemporaryFile(delete=False, mode='wb+', buffering=0)

        position = 0
        it = iter(self.dicts)
        while True:
            if position < self._cached:
                self._filecache.seek(position)
                row = pickle.load(self._filecache)
                position = self._filecache.tell()
                yield row
                continue
            try:
                o = next(it)
            except StopIteration:
                break
            row = tuple(o.get(f, self.missing) for f in self._header)
            self._filecache.seek(self._cached)
            pickle.dump(row, self._filecache, protocol=-1)
            self._cached = position = self._filecache.tell()
            yield row

    def _determine_header(self):
        it = iter(self.dicts)
        header = list()
        peek, it = iterpeek(it, self.sample)
        self.dicts = it
        if isinstance(peek, dict):
            peek = [peek]
        for o in peek:
            if hasattr(o, 'keys'):
                header += [k for k in o.keys() if k not in header]
        self._header = tuple(header)
        return it

    def __del__(self):
        if self._filecache:
            self._filecache.close()
            unlink(self._filecache.name)


def iterjlines(f, header, missing):
    it = iter(f)

    if header is None:
        header = list()
        peek, it = iterpeek(it, 1)
        json_obj = json.loads(peek)
        if hasattr(json_obj, 'keys'):
            header += [k for k in json_obj.keys() if k not in header]
    yield tuple(header)

    for o in it:
        json_obj = json.loads(o)
        yield tuple(json_obj[f] if f in json_obj else missing for f in header)


def iterdicts(dicts, header, sample, missing):
    it = iter(dicts)

    # determine header row
    if header is None:
        # discover fields
        header = list()
        peek, it = iterpeek(it, sample)
        for o in peek:
            if hasattr(o, 'keys'):
                header += [k for k in o.keys() if k not in header]
    yield tuple(header)

    # generate data rows
    for o in it:
        yield tuple(o.get(f, missing) for f in header)


def tojson(table, source=None, prefix=None, suffix=None, *args, **kwargs):
    """
    Write a table in JSON format, with rows output as JSON objects. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> etl.tojson(table1, 'example.file3.json', sort_keys=True)
        >>> # check what it did
        ... print(open('example.file3.json').read())
        [{"bar": 1, "foo": "a"}, {"bar": 2, "foo": "b"}, {"bar": 2, "foo": "c"}]



    Setting argument `lines` to `True` will enable to
    infer the writing format as a JSON lines . For more details about JSON lines
    please visit https://jsonlines.org/.

        >>> import petl as etl
        >>> table1 = [['name', 'wins'],
        ...           ['Gilbert', [['straight', '7S'], ['one pair', '10H']]],
        ...           ['Alexa', [['two pair', '4S'], ['two pair', '9S']]],
        ...           ['May', []],
        ...           ['Deloise',[['three of a kind', '5S']]]]
        >>> etl.tojson(table1, 'example.file3.jsonl', lines = True, sort_keys=True)
        >>> # check what it did
        ... print(open('example.file3.jsonl').read())
        {"name": "Gilbert", "wins": [["straight", "7S"], ["one pair", "10H"]]}
        {"name": "Alexa", "wins": [["two pair", "4S"], ["two pair", "9S"]]}
        {"name": "May", "wins": []}
        {"name": "Deloise", "wins": [["three of a kind", "5S"]]}

    Note that this is currently not streaming, all data is loaded into memory
    before being written to the file.

    """

    obj = list(_dicts(table))
    _writejson(source, obj, prefix, suffix, *args, **kwargs)


Table.tojson = tojson


def tojsonarrays(table, source=None, prefix=None, suffix=None,
                 output_header=False, *args, **kwargs):
    """
    Write a table in JSON format, with rows output as JSON arrays. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> etl.tojsonarrays(table1, 'example.file4.json')
        >>> # check what it did
        ... print(open('example.file4.json').read())
        [["a", 1], ["b", 2], ["c", 2]]

    Note that this is currently not streaming, all data is loaded into memory
    before being written to the file.

    """

    if output_header:
        obj = list(table)
    else:
        obj = list(data(table))
    _writejson(source, obj, prefix, suffix, *args, **kwargs)


Table.tojsonarrays = tojsonarrays


def _writejson(source, obj, prefix, suffix, *args, **kwargs):
    lines = kwargs.pop('lines', False)
    encoder = JSONEncoder(*args, **kwargs)
    source = write_source_from_arg(source)
    with source.open('wb') as f:
        if PY2:
            # write directly to buffer
            _writeobj(encoder, obj, f, prefix, suffix, lines=lines)
        else:
            # wrap buffer for text IO
            f = io.TextIOWrapper(f, encoding='utf-8', newline='',
                                 write_through=True)
            try:
                _writeobj(encoder, obj, f, prefix, suffix, lines=lines)
            finally:
                f.detach()


def _writeobj(encoder, obj, f, prefix, suffix, lines=False):
    if prefix is not None:
        f.write(prefix)
    if lines:
        for rec in obj:
            for chunk in encoder.iterencode(rec):
                f.write(chunk)
            f.write('\n')
    else:
        for chunk in encoder.iterencode(obj):
            f.write(chunk)
    if suffix is not None:
        f.write(suffix)
