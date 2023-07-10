from __future__ import absolute_import, print_function, division


import operator
from petl.compat import text_type


from petl.errors import DuplicateKeyError
from petl.util.base import Table, asindices, asdict, Record, rowgetter


def _setup_lookup(table, key, value):

    # obtain iterator and header row
    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []

    # prepare key getter
    keyindices = asindices(hdr, key)
    assert len(keyindices) > 0, 'no key selected'
    getkey = operator.itemgetter(*keyindices)

    # prepare value getter
    if value is None:
        # default value is complete row
        getvalue = rowgetter(*range(len(hdr)))
    else:
        valueindices = asindices(hdr, value)
        assert len(valueindices) > 0, 'no value selected'
        getvalue = operator.itemgetter(*valueindices)

    return it, getkey, getvalue


def lookup(table, key, value=None, dictionary=None):
    """
    Load a dictionary with data from the given table. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['b', 3]]
        >>> lkp = etl.lookup(table1, 'foo', 'bar')
        >>> lkp['a']
        [1]
        >>> lkp['b']
        [2, 3]
        >>> # if no value argument is given, defaults to the whole
        ... # row (as a tuple)
        ... lkp = etl.lookup(table1, 'foo')
        >>> lkp['a']
        [('a', 1)]
        >>> lkp['b']
        [('b', 2), ('b', 3)]
        >>> # compound keys are supported
        ... table2 = [['foo', 'bar', 'baz'],
        ...           ['a', 1, True],
        ...           ['b', 2, False],
        ...           ['b', 3, True],
        ...           ['b', 3, False]]
        >>> lkp = etl.lookup(table2, ('foo', 'bar'), 'baz')
        >>> lkp[('a', 1)]
        [True]
        >>> lkp[('b', 2)]
        [False]
        >>> lkp[('b', 3)]
        [True, False]
        >>> # data can be loaded into an existing dictionary-like
        ... # object, including persistent dictionaries created via the
        ... # shelve module
        ... import shelve
        >>> lkp = shelve.open('example.dat', flag='n')
        >>> lkp = etl.lookup(table1, 'foo', 'bar', lkp)
        >>> lkp.close()
        >>> lkp = shelve.open('example.dat', flag='r')
        >>> lkp['a']
        [1]
        >>> lkp['b']
        [2, 3]

    """

    if dictionary is None:
        dictionary = dict()

    # setup
    it, getkey, getvalue = _setup_lookup(table, key, value)

    # build lookup dictionary
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


Table.lookup = lookup


def lookupone(table, key, value=None, dictionary=None, strict=False):
    """
    Load a dictionary with data from the given table, assuming there is
    at most one value for each key. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['b', 3]]
        >>> # if the specified key is not unique and strict=False (default),
        ... # the first value wins
        ... lkp = etl.lookupone(table1, 'foo', 'bar')
        >>> lkp['a']
        1
        >>> lkp['b']
        2
        >>> # if the specified key is not unique and strict=True, will raise
        ... # DuplicateKeyError
        ... try:
        ...     lkp = etl.lookupone(table1, 'foo', strict=True)
        ... except etl.errors.DuplicateKeyError as e:
        ...     print(e)
        ...
        duplicate key: 'b'
        >>> # compound keys are supported
        ... table2 = [['foo', 'bar', 'baz'],
        ...           ['a', 1, True],
        ...           ['b', 2, False],
        ...           ['b', 3, True],
        ...           ['b', 3, False]]
        >>> lkp = etl.lookupone(table2, ('foo', 'bar'), 'baz')
        >>> lkp[('a', 1)]
        True
        >>> lkp[('b', 2)]
        False
        >>> lkp[('b', 3)]
        True
        >>> # data can be loaded into an existing dictionary-like
        ... # object, including persistent dictionaries created via the
        ... # shelve module
        ... import shelve
        >>> lkp = shelve.open('example.dat', flag='n')
        >>> lkp = etl.lookupone(table1, 'foo', 'bar', lkp)
        >>> lkp.close()
        >>> lkp = shelve.open('example.dat', flag='r')
        >>> lkp['a']
        1
        >>> lkp['b']
        2

    """

    if dictionary is None:
        dictionary = dict()

    # setup
    it, getkey, getvalue = _setup_lookup(table, key, value)

    # build lookup dictionary
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError(k)
        elif k not in dictionary:
            v = getvalue(row)
            dictionary[k] = v

    return dictionary


Table.lookupone = lookupone


def dictlookup(table, key, dictionary=None):
    """
    Load a dictionary with data from the given table, mapping to dicts. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['b', 3]]
        >>> lkp = etl.dictlookup(table1, 'foo')
        >>> lkp['a']
        [{'foo': 'a', 'bar': 1}]
        >>> lkp['b']
        [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]
        >>> # compound keys are supported
        ... table2 = [['foo', 'bar', 'baz'],
        ...           ['a', 1, True],
        ...           ['b', 2, False],
        ...           ['b', 3, True],
        ...           ['b', 3, False]]
        >>> lkp = etl.dictlookup(table2, ('foo', 'bar'))
        >>> lkp[('a', 1)]
        [{'foo': 'a', 'bar': 1, 'baz': True}]
        >>> lkp[('b', 2)]
        [{'foo': 'b', 'bar': 2, 'baz': False}]
        >>> lkp[('b', 3)]
        [{'foo': 'b', 'bar': 3, 'baz': True}, {'foo': 'b', 'bar': 3, 'baz': False}]
        >>> # data can be loaded into an existing dictionary-like
        ... # object, including persistent dictionaries created via the
        ... # shelve module
        ... import shelve
        >>> lkp = shelve.open('example.dat', flag='n')
        >>> lkp = etl.dictlookup(table1, 'foo', lkp)
        >>> lkp.close()
        >>> lkp = shelve.open('example.dat', flag='r')
        >>> lkp['a']
        [{'foo': 'a', 'bar': 1}]
        >>> lkp['b']
        [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    keyindices = asindices(hdr, key)
    assert len(keyindices) > 0, 'no key selected'
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


Table.dictlookup = dictlookup


def dictlookupone(table, key, dictionary=None, strict=False):
    """
    Load a dictionary with data from the given table, mapping to dicts,
    assuming there is at most one row for each key. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['b', 3]]
        >>> # if the specified key is not unique and strict=False (default),
        ... # the first value wins
        ... lkp = etl.dictlookupone(table1, 'foo')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}
        >>> # if the specified key is not unique and strict=True, will raise
        ... # DuplicateKeyError
        ... try:
        ...     lkp = etl.dictlookupone(table1, 'foo', strict=True)
        ... except etl.errors.DuplicateKeyError as e:
        ...     print(e)
        ...
        duplicate key: 'b'
        >>> # compound keys are supported
        ... table2 = [['foo', 'bar', 'baz'],
        ...           ['a', 1, True],
        ...           ['b', 2, False],
        ...           ['b', 3, True],
        ...           ['b', 3, False]]
        >>> lkp = etl.dictlookupone(table2, ('foo', 'bar'))
        >>> lkp[('a', 1)]
        {'foo': 'a', 'bar': 1, 'baz': True}
        >>> lkp[('b', 2)]
        {'foo': 'b', 'bar': 2, 'baz': False}
        >>> lkp[('b', 3)]
        {'foo': 'b', 'bar': 3, 'baz': True}
        >>> # data can be loaded into an existing dictionary-like
        ... # object, including persistent dictionaries created via the
        ... # shelve module
        ... import shelve
        >>> lkp = shelve.open('example.dat', flag='n')
        >>> lkp = etl.dictlookupone(table1, 'foo', lkp)
        >>> lkp.close()
        >>> lkp = shelve.open('example.dat', flag='r')
        >>> lkp['a']
        {'foo': 'a', 'bar': 1}
        >>> lkp['b']
        {'foo': 'b', 'bar': 2}

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    keyindices = asindices(hdr, key)
    assert len(keyindices) > 0, 'no key selected'
    getkey = operator.itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError(k)
        elif k not in dictionary:
            d = asdict(flds, row)
            dictionary[k] = d
    return dictionary


Table.dictlookupone = dictlookupone


def recordlookup(table, key, dictionary=None):
    """
    Load a dictionary with data from the given table, mapping to record objects.

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    keyindices = asindices(hdr, key)
    assert len(keyindices) > 0, 'no key selected'
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


Table.recordlookup = recordlookup


def recordlookupone(table, key, dictionary=None, strict=False):
    """
    Load a dictionary with data from the given table, mapping to record objects,
    assuming there is at most one row for each key.

    """

    if dictionary is None:
        dictionary = dict()

    it = iter(table)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    keyindices = asindices(hdr, key)
    assert len(keyindices) > 0, 'no key selected'
    getkey = operator.itemgetter(*keyindices)
    for row in it:
        k = getkey(row)
        if strict and k in dictionary:
            raise DuplicateKeyError(k)
        elif k not in dictionary:
            d = Record(row, flds)
            dictionary[k] = d
    return dictionary


Table.recordlookupone = recordlookupone
