from __future__ import absolute_import, print_function, division


import operator
from functools import partial

from petl.compat import text_type, binary_type, numeric_types


class Comparable(object):
    """Wrapper class to allow for flexible comparison of objects of different
    types, preserving the relaxed sorting behaviour of Python 2 with
    additional flexibility to allow for comparison of arbitrary objects with
    the `None` value (for example, the date and time objects from the standard
    library cannot be directly compared with `None` in Python 2).

    """

    __slots__ = ['obj', 'inner']

    def __init__(self, obj):
        # store wrapped object unchanged
        self.inner = obj
        # handle lists and tuples
        if isinstance(obj, (list, tuple)):
            obj = tuple(Comparable(o) for o in obj)
        self.obj = obj

    def __lt__(self, other):

        # convenience
        obj = self.obj
        if isinstance(other, Comparable):
            other = other.obj

        # None < everything else
        if other is None:
            return False
        if obj is None:
            return True

        # numbers < everything else (except None)
        if isinstance(obj, numeric_types) \
                and not isinstance(other, numeric_types):
            return True
        if not isinstance(obj, numeric_types) \
                and isinstance(other, numeric_types):
            return False

        # binary < unicode
        if isinstance(obj, text_type) and isinstance(other, binary_type):
            return False
        if isinstance(obj, binary_type) and isinstance(other, text_type):
            return True

        try:
            # attempt native comparison
            return obj < other

        except TypeError:
            # fall back to comparing type names
            return _typestr(obj) < _typestr(other)

    def __eq__(self, other):
        if isinstance(other, Comparable):
            return self.obj == other.obj
        return self.obj == other

    def __le__(self, other):
        return self < other or self == other

    def __gt__(self, other):
        return not (self < other or self == other)

    def __ge__(self, other):
        return not (self < other)

    def __str__(self):
        return str(self.obj)

    def __unicode__(self):
        return text_type(self.obj)

    def __repr__(self):
        return 'Comparable(' + repr(self.obj) + ')'

    def __iter__(self, *args, **kwargs):
        return iter(self.obj, *args, **kwargs)

    def __len__(self):
        return len(self.obj)

    def __getitem__(self, item):
        return self.obj.__getitem__(item)


def _typestr(x):
    # attempt to preserve Python 2 name orderings
    if isinstance(x, binary_type):
        return 'str'
    if isinstance(x, text_type):
        return 'unicode'
    return type(x).__name__


def comparable_itemgetter(*args):
    getter = operator.itemgetter(*args)
    getter_with_default = _itemgetter_with_default(*args)

    def _getter_with_fallback(obj):
        try:
            return getter(obj)
        except (IndexError, KeyError):
            return getter_with_default(obj)
    g = lambda x: Comparable(_getter_with_fallback(x))
    return g


def _itemgetter_with_default(*args):
    """ itemgetter compatible with `operator.itemgetter` behavior, filling missing
    values with default instead of raising IndexError or KeyError """
    def _get_default(obj, item, default):
        try:
            return obj[item]
        except (IndexError, KeyError):
            return default
    if len(args) == 1:
        return partial(_get_default, item=args[0], default=None)
    return lambda obj: tuple(_get_default(obj, item=item, default=None) for item in args)
