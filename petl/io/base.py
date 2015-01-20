# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


import locale
import codecs


def getcodec(encoding):
    if encoding is None:
        encoding = locale.getpreferredencoding()
    codec = codecs.lookup(encoding)
    return codec
