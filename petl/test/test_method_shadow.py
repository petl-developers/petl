"""
Regression tests for Table subclasses that store a constructor argument
under a name the fluent API already binds on ``Table``. The instance
attribute wins attribute lookup, so the method becomes unreachable and
calling it raises ``TypeError: '...' object is not callable``.

Collisions found so far: ``header`` (issue #555, PR #704), ``dicts``
(issue #643, PR #697), and ``cache``, ``complement``, ``index``, ``skip``.
``test_no_shadowed_api_names`` scans the source for new ones.
"""

from __future__ import absolute_import, print_function, division


import ast
import json as _json
import os
from collections import OrderedDict
from tempfile import NamedTemporaryFile


import pytest


import petl as etl
from petl.test.helpers import eq_
from petl.util.base import Table


def _csvfile(text):
    f = NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='')
    f.write(text)
    f.close()
    return f.name


def _textfile(text):
    f = NamedTemporaryFile(mode='w', suffix='.txt', delete=False)
    f.write(text)
    f.close()
    return f.name


def _jsonfile(obj):
    f = NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    f.write(_json.dumps(obj))
    f.close()
    return f.name


def test_fromcsv_header():
    t = etl.fromcsv(_csvfile('foo,bar\r\n1,2\r\n'))
    eq_(('foo', 'bar'), t.header())


def test_fromtext_header():
    t = etl.fromtext(_textfile('line one\nline two\n'))
    eq_(('lines',), t.header())


def test_fromjson_header():
    t = etl.fromjson(_jsonfile([{'foo': 1, 'bar': 2}]), header=['foo', 'bar'])
    eq_(('foo', 'bar'), t.header())


def test_fromcolumns_header():
    t = etl.fromcolumns([[1, 2], [3, 4]], ['a', 'b'])
    eq_(('a', 'b'), t.header())


def test_cat_header():
    a = etl.wrap([['foo', 'bar'], [1, 2]])
    b = etl.wrap([['foo', 'baz'], [3, 4]])
    t = etl.cat(a, b, header=['foo', 'bar', 'baz'])
    eq_(('foo', 'bar', 'baz'), t.header())


def test_pushheader_header():
    t = etl.pushheader(etl.wrap([[1, 2]]), ['foo', 'bar'])
    eq_(('foo', 'bar'), t.header())


def test_setheader_header():
    t = etl.setheader(etl.wrap([['foo', 'bar'], [1, 2]]), ['a', 'b'])
    eq_(('a', 'b'), t.header())


def test_rowmap_header():
    src = etl.wrap([['foo', 'bar'], [1, 2]])
    t = etl.rowmap(src, lambda row: [row[0]], header=['foo'])
    eq_(('foo',), t.header())


def test_rowmapmany_header():
    src = etl.wrap([['foo', 'bar'], [1, 2]])
    t = etl.rowmapmany(src, lambda row: [[row[0]]], header=['foo'])
    eq_(('foo',), t.header())


def test_rowgroupmap_header():
    src = etl.wrap([['foo', 'bar'], [1, 2]])
    t = etl.rowgroupmap(src, 'foo', lambda k, rows: [[k]], header=['foo'])
    eq_(('foo',), t.header())


def test_rowreduce_header():
    src = etl.wrap([['foo', 'bar'], [1, 2]])
    t = etl.rowreduce(src, 'foo', lambda k, rows: [k], header=['foo'])
    eq_(('foo',), t.header())


def test_mergesort_header():
    a = etl.wrap([['foo', 'bar'], [1, 2]])
    b = etl.wrap([['foo', 'bar'], [3, 4]])
    t = etl.mergesort(a, b, key='foo')
    eq_(('foo', 'bar'), t.header())


def test_validate_header():
    src = etl.wrap([['foo', 'bar'], [1, 2]])
    t = etl.validate(src, header=['foo', 'bar'])
    eq_(('name', 'row', 'field', 'value', 'error'), t.header())


def test_method_matches_function_form():
    # the fluent method must agree with the etl.header() function
    t = etl.fromcolumns([[1, 2]], ['a'])
    eq_(etl.header(t), t.header())


TABLE = [['foo', 'bar'],
         ['a', 1],
         ['b', 2]]

RIGHT = [['foo', 'baz'],
         ['a', True],
         ['b', False]]

SUBSET = [['foo', 'bar'],
          ['b', 2]]


def test_hashjoin_cache():
    t = etl.hashjoin(TABLE, RIGHT, key='foo')
    eq_([('foo', 'bar', 'baz'), ('a', 1, True), ('b', 2, False)],
        list(t.cache()))


def test_hashleftjoin_cache():
    t = etl.hashleftjoin(TABLE, [['foo', 'baz'], ['a', True]], key='foo')
    eq_([('foo', 'bar', 'baz'), ('a', 1, True), ('b', 2, None)],
        list(t.cache()))


def test_hashrightjoin_cache():
    t = etl.hashrightjoin([['foo', 'baz'], ['a', True]], TABLE, key='foo')
    eq_([('foo', 'baz', 'bar'), ('a', True, 1), ('b', None, 2)],
        list(t.cache()))


def test_sort_cache():
    t = etl.sort(TABLE, 'foo', reverse=True)
    eq_([('foo', 'bar'), ('b', 2), ('a', 1)], list(t.cache()))


def test_cache_cache():
    t = etl.wrap(TABLE).cache()
    eq_([('foo', 'bar'), ('a', 1), ('b', 2)],
        [tuple(row) for row in t.cache()])


def test_search_complement():
    t = etl.search(TABLE, 'a|b')
    eq_([('foo', 'bar'), ('a', 1)], list(t.complement(SUBSET)))


def test_rowselect_complement():
    t = etl.select(TABLE, lambda row: True)
    eq_([('foo', 'bar'), ('a', 1)], list(t.complement(SUBSET)))


def test_fieldselect_complement():
    t = etl.select(TABLE, 'foo', lambda v: True)
    eq_([('foo', 'bar'), ('a', 1)], list(t.complement(SUBSET)))


def test_addfield_index():
    t = etl.addfield(TABLE, 'baz', True)
    eq_([('foo', 'bar', 'baz'), ('a', 1, True), ('b', 2, True)], list(t))
    eq_(1, t.index(('a', 1, True)))


def test_movefield_index():
    t = etl.movefield(TABLE, 'bar', 0)
    eq_([('bar', 'foo'), (1, 'a'), (2, 'b')], list(t))
    eq_(1, t.index((1, 'a')))


# fromavro/frombcolz need optional dependencies for a full round trip, so
# feed the readers directly instead of installing fastavro and bcolz here.

class _FakeCtable(object):

    names = ('foo', 'bar')

    def iter(self, outcols=None, skip=0, limit=None):
        return iter([('a', 1), ('b', 2), ('c', 3)][skip:])


def test_fromavro_skip():
    view = etl.fromavro('nonexistent.avro', skips=2)
    assert isinstance(view.skip(1), Table)
    # ordered so the row order does not rely on dict iteration order
    records = [OrderedDict([('foo', v), ('bar', i)])
               for i, v in enumerate(['a', 'b', 'c'], 1)]
    eq_([('c', 3)],
        list(view._read_rows_from(records, ('foo', 'bar'))))


def test_frombcolz_skip():
    view = etl.frombcolz(_FakeCtable(), skip=1)
    eq_([('foo', 'bar'), ('b', 2), ('c', 3)], list(view))
    # Table.skip() drops leading rows, so the next row becomes the header
    eq_([('b', 2), ('c', 3)], list(view.skip(1)))


# The renamed attributes are still read internally, so check the options they
# carry keep working.

def test_hashjoin_cache_option_still_honoured():
    right = etl.wrap([['foo', 'baz'], ['a', True], ['b', False]])
    cached = etl.hashjoin(TABLE, right, key='foo')
    list(cached)
    assert cached.rlookup is not None
    uncached = etl.hashjoin(TABLE, right, key='foo', cache=False)
    list(uncached)
    first = uncached.rlookup
    list(uncached)
    assert uncached.rlookup is not first


def test_sort_cache_option_still_honoured():
    cached = etl.sort(TABLE, 'foo')
    list(cached)
    assert cached._memcache is not None
    uncached = etl.sort(TABLE, 'foo', cache=False)
    list(uncached)
    assert uncached._memcache is None


def test_cacheview_clearcache_still_works():
    t = etl.wrap(TABLE).cache()
    eq_(3, len(list(t)))
    assert t.cachecomplete
    t.clearcache()
    assert not t.cachecomplete
    eq_(3, len(list(t)))


def test_search_complement_option_still_honoured():
    eq_([('foo', 'bar'), ('a', 1)], list(etl.search(TABLE, 'a')))
    eq_([('foo', 'bar'), ('b', 2)],
        list(etl.search(TABLE, 'a', complement=True)))


def test_select_complement_option_still_honoured():
    eq_([('foo', 'bar'), ('b', 2)],
        list(etl.select(TABLE, lambda row: row.foo == 'a', complement=True)))
    eq_([('foo', 'bar'), ('b', 2)],
        list(etl.select(TABLE, 'foo', lambda v: v == 'a', complement=True)))


def test_addfield_index_option_still_honoured():
    eq_([('baz', 'foo', 'bar'), (True, 'a', 1), (True, 'b', 2)],
        list(etl.addfield(TABLE, 'baz', True, index=0)))


def test_movefield_index_option_still_honoured():
    eq_([('bar', 'foo'), (1, 'a'), (2, 'b')],
        list(etl.movefield(TABLE, 'bar', 0)))


def _class_level_names(node):
    """Names bound in a class body, which legitimately override the API."""
    names = set()
    for stmt in node.body:
        if isinstance(stmt, ast.FunctionDef):
            names.add(stmt.name)
        elif isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
    return names


def _self_assignments(node):
    for child in ast.walk(node):
        if not isinstance(child, ast.Assign):
            continue
        for target in child.targets:
            if (isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == 'self'):
                yield target.attr, child.lineno


def test_no_shadowed_api_names():
    """No Table subclass may store an instance attribute named after a
    method of the fluent API, because the attribute hides the method."""
    api = set(name for name in dir(Table)
              if not name.startswith('__') and callable(getattr(Table, name)))
    assert 'header' in api and 'cache' in api and len(api) > 100

    root = os.path.dirname(os.path.abspath(etl.__file__))
    classes = {}
    for dirpath, dirnames, filenames in os.walk(root):
        if 'test' in dirnames:
            dirnames.remove('test')  # do not scan the suite itself
        for filename in sorted(filenames):
            if not filename.endswith('.py'):
                continue
            path = os.path.join(dirpath, filename)
            # read bytes so the parser applies each module's own coding
            # declaration rather than the platform default encoding
            with open(path, 'rb') as f:
                source = f.read()
            try:
                tree = ast.parse(source)
            except SyntaxError:
                continue  # the py2-only and py3-only modules
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    classes.setdefault(node.name, []).append((path, node))
    assert 'CacheView' in classes and 'SortView' in classes

    def bases(name, seen):
        if name in seen:
            return
        seen.add(name)
        for _, node in classes.get(name, []):
            for base in node.bases:
                if isinstance(base, ast.Name):
                    yield base.id
                    for parent in bases(base.id, seen):
                        yield parent
                elif isinstance(base, ast.Attribute):
                    yield base.attr

    offenders = []
    for name, defs in sorted(classes.items()):
        ancestors = set(bases(name, set()))
        if not ancestors & set(['Table', 'IterContainer']):
            continue
        # a name redefined in the view itself (or in an intermediate view it
        # inherits from) is a deliberate override; the base classes are where
        # the shadowed methods come from, so they do not count
        overridden = set()
        for _, node in defs:
            overridden |= _class_level_names(node)
        for ancestor in ancestors - set(['Table', 'IterContainer']):
            for _, node in classes.get(ancestor, []):
                overridden |= _class_level_names(node)
        for path, node in defs:
            for attr, lineno in _self_assignments(node):
                if attr in api and attr not in overridden:
                    offenders.append('%s:%s: %s.%s shadows Table.%s()'
                                     % (os.path.relpath(path, root), lineno,
                                        name, attr, attr))
    assert not offenders, '\n'.join(offenders)
