from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


from petl.io.sources import FileSource, GzipSource, BZ2Source, ZipSource, \
    StdinSource, StdoutSource, URLSource, StringSource, PopenSource

from petl.io.csv import fromcsv, tocsv, appendcsv, fromtsv, totsv, appendtsv,\
    fromucsv, toucsv, appenducsv, fromutsv, toutsv, appendutsv, teecsv, \
    teetsv, teeucsv, teeutsv

from petl.io.pickle import frompickle, topickle, appendpickle, teepickle

from petl.io.text import fromtext, totext, appendtext, fromutext, toutext, \
    appendutext, teetext, teeutext

from petl.io.xml import fromxml

from petl.io.html import tohtml, touhtml, teehtml, teeuhtml

from petl.io.json import fromjson, tojson, tojsonarrays, fromdicts

from petl.io.sqlite3 import fromsqlite3, tosqlite3, appendsqlite3

from petl.io.db import fromdb, todb, appenddb