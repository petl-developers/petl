from __future__ import absolute_import, print_function, division


import operator


from petl import compat


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
        if isinstance(obj, compat.numeric_types) \
                and not isinstance(other, compat.numeric_types):
            return True
        if not isinstance(obj, compat.numeric_types) \
                and isinstance(other, compat.numeric_types):
            return False

        # binary < unicode
        if isinstance(obj, compat.text_type) and isinstance(other, compat.binary_type):
            return False
        if isinstance(obj, compat.binary_type) and isinstance(other, compat.text_type):
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
        return compat.text_type(self.obj)

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
    if isinstance(x, compat.binary_type):
        return 'str'
    if isinstance(x, compat.text_type):
        return 'unicode'
    return type(x).__name__


def comparable_itemgetter(*args):
    f = operator.itemgetter(*args)
    g = lambda x: Comparable(f(x))
    return g
