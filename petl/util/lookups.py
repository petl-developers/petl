from __future__ import absolute_import, print_function, division, \
    unicode_literals


import operator


from petl.errors import DuplicateKeyError
from petl.util.base import asindices, asdict, Record


def lookup(table, keyspec, valuespec=None, dictionary=None):
    """
    Load a dictionary with data from the given table. E.g.::

        >>> from petl import lookup
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookup(table, 'foo', 'bar')
        >>> lkp['a']
        [1]
        >>> lkp['b']
        [2, 3]

    If no `valuespec` argument is given, defaults to the whole
    row (as a tuple), e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookup(table, 'foo')
        >>> lkp['a']
        [('a', 1)]
        >>> lkp['b']
        [('b', 2), ('b', 3)]

    Compound keys are supported, e.g.::

        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True],
        ...       ['b', 3, False]]
        >>> lkp = lookup(t2, ('foo', 'bar'), 'baz')
        >>> lkp[('a', 1)]
        [True]
        >>> lkp[('b', 2)]
        [False]
        >>> lkp[('b', 3)]
        [True, False]

    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::

        >>> import shelve
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = shelve.open('mylookup.dat')
        >>> lkp = lookup(table, 'foo', 'bar', lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24)
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mylookup.dat')
        >>> lkp['a']
        [1]
        >>> lkp['b']
        [2, 3]

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = next(it)
    if valuespec is None:
        valuespec = flds  # default valuespec is complete row
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    valueindices = asindices(flds, valuespec)
    assert len(valueindices) > 0, 'no valuespec selected'
    getkey = operator.itemgetter(*keyindices)
    getvalue = operator.itemgetter(*valueindices)
    for row in it:
        k = getkey(row)
        v = getvalue(row)
        if k in dictionary:
            # work properly with shelve
            l = dictionary[k]
            l.append(v)
            dictionary[k] = l
        else:
            dictionary[k] = [v]
    return dictionary


def lookupone(table, keyspec, valuespec=None, dictionary=None, strict=False):
    """
    Load a dictionary with data from the given table, assuming there is
    at most one value for each key. E.g.::

        >>> from petl import lookupone
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = lookupone(table, 'foo', 'bar')
        >>> lkp['a']
        1
        >>> lkp['b']
        2
        >>> lkp['c']
        2

    If the specified key is not unique and strict=False (default),
    the first value wins, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookupone(table, 'foo', 'bar', strict=False)
        >>> lkp['a']
        1
        >>> lkp['b']
        2

    If the specified key is not unique and strict=True, will raise
    DuplicateKeyError, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = lookupone(table, 'foo', strict=True)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 451, in lookupone
        petl.util.DuplicateKeyError

    Compound keys are supported, e.g.::

        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True]]
        >>> lkp = lookupone(t2, ('foo', 'bar'), 'baz')
        >>> lkp[('a', 1)]
        True
        >>> lkp[('b', 2)]
        False
        >>> lkp[('b', 3)]
        True

    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::

        >>> from petl import lookupone
        >>> import shelve
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = shelve.open('mylookupone.dat')
        >>> lkp = lookupone(table, 'foo', 'bar', dictionary=lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24)
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mylookupone.dat')
        >>> lkp['a']
        1
        >>> lkp['b']
        2
        >>> lkp['c']
        2

    .. versionchanged:: 0.11

    Changed so that strict=False is default and first value wins.

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = next(it)
    if valuespec is None:
        valuespec = flds
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    valueindices = asindices(flds, valuespec)
    assert len(valueindices) > 0, 'no valuespec selected'
    getkey = operator.itemgetter(*keyindices)
    getvalue = operator.itemgetter(*valueindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError
        elif k not in dictionary:
            v = getvalue(row)
            dictionary[k] = v
    return dictionary


def dictlookup(table, keyspec, dictionary=None):
    """
    Load a dictionary with data from the given table, mapping to dicts. E.g.::

        >>> from petl import dictlookup
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = dictlookup(table, 'foo')
        >>> lkp['a']
        [{'foo': 'a', 'bar': 1}]
        >>> lkp['b']
        [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]

    Compound keys are supported, e.g.::

        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True],
        ...       ['b', 3, False]]
        >>> lkp = dictlookup(t2, ('foo', 'bar'))
        >>> lkp[('a', 1)]
        [{'baz': True, 'foo': 'a', 'bar': 1}]
        >>> lkp[('b', 2)]
        [{'baz': False, 'foo': 'b', 'bar': 2}]
        >>> lkp[('b', 3)]
        [{'baz': True, 'foo': 'b', 'bar': 3}, {'baz': False, 'foo': 'b', 'bar': 3}]

    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::

        >>> import shelve
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = shelve.open('mydictlookup.dat')
        >>> lkp = dictlookup(table, 'foo', dictionary=lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24)
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mydictlookup.dat')
        >>> lkp['a']
        [{'foo': 'a', 'bar': 1}]
        >>> lkp['b']
        [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]

    .. versionchanged:: 0.15

    Renamed from `recordlookup`.

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = next(it)
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = operator.itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        rec = asdict(flds, row)
        if k in dictionary:
            # work properly with shelve
            l = dictionary[k]
            l.append(rec)
            dictionary[k] = l
        else:
            dictionary[k] = [rec]
    return dictionary


def recordlookup(table, keyspec, dictionary=None):
    """
    Load a dictionary with data from the given table, mapping to record objects.

    .. versionadded:: 0.17

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = next(it)
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = operator.itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        rec = Record(row, flds)
        if k in dictionary:
            # work properly with shelve
            l = dictionary[k]
            l.append(rec)
            dictionary[k] = l
        else:
            dictionary[k] = [rec]
    return dictionary


def dictlookupone(table, keyspec, dictionary=None, strict=False):
    """
    Load a dictionary with data from the given table, mapping to dicts,
    assuming there is at most one row for each key. E.g.::

        >>> from petl import dictlookupone
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = dictlookupone(table, 'foo')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}
        >>> lkp['c']
        {'foo': 'c', 'bar': 2}

    If the specified key is not unique and strict=False (default),
    the first dict wins, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = dictlookupone(table, 'foo')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}

    If the specified key is not unique and strict=True, will raise
    DuplicateKeyError, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
        >>> lkp = dictlookupone(table, 'foo', strict=True)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 451, in lookupone
        petl.util.DuplicateKeyError

    Compound keys are supported, e.g.::

        >>> t2 = [['foo', 'bar', 'baz'],
        ...       ['a', 1, True],
        ...       ['b', 2, False],
        ...       ['b', 3, True]]
        >>> lkp = dictlookupone(t2, ('foo', 'bar'), strict=False)
        >>> lkp[('a', 1)]
        {'baz': True, 'foo': 'a', 'bar': 1}
        >>> lkp[('b', 2)]
        {'baz': False, 'foo': 'b', 'bar': 2}
        >>> lkp[('b', 3)]
        {'baz': True, 'foo': 'b', 'bar': 3}

    Data can be loaded into an existing dictionary-like object, including
    persistent dictionaries created via the :mod:`shelve` module, e.g.::

        >>> import shelve
        >>> lkp = shelve.open('mydictlookupone.dat')
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
        >>> lkp = dictlookupone(table, 'foo', dictionary=lkp)
        >>> lkp.close()
        >>> exit()
        $ python
        Python 2.7.1+ (r271:86832, Apr 11 2011, 18:05:24)
        [GCC 4.5.2] on linux2
        Type "help", "copyright", "credits" or "license" for more information.
        >>> import shelve
        >>> lkp = shelve.open('mydictlookupone.dat')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}
        >>> lkp['c']
        {'foo': 'c', 'bar': 2}

    .. versionchanged:: 0.11

    Changed so that strict=False is default and first value wins.

    .. versionchanged:: 0.15

    Renamed from `recordlookupone`.

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = next(it)
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = operator.itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError
        elif k not in dictionary:
            d = asdict(flds, row)
            dictionary[k] = d
    return dictionary


def recordlookupone(table, keyspec, dictionary=None, strict=False):
    """
    Load a dictionary with data from the given table, mapping to record objects,
    assuming there is at most one row for each key.

    .. versionchanged:: 0.17

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    flds = next(it)
    keyindices = asindices(flds, keyspec)
    assert len(keyindices) > 0, 'no keyspec selected'
    getkey = operator.itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError
        elif k not in dictionary:
            d = Record(row, flds)
            dictionary[k] = d
    return dictionary


