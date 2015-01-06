from __future__ import absolute_import, print_function, division, \
    unicode_literals


class DuplicateKeyError(Exception):
    pass


class FieldSelectionError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return 'selection is not a field or valid field index: %r' % self.value
