# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import json
from tempfile import NamedTemporaryFile


from petl.test.helpers import ieq
from petl.io.json import tojson, fromjson


def test_json_unicode():

    tbl = ((u'id', u'name'),
           (1, u'Արամ Խաչատրյան'),
           (2, u'Johann Strauß'),
           (3, u'Вагиф Сәмәдоғлу'),
           (4, u'章子怡'),
           )
    fn = NamedTemporaryFile().name
    tojson(tbl, fn)

    result = json.load(open(fn))
    assert len(result) == 4
    for a, b in zip(tbl[1:], result):
        assert a[0] == b['id']
        assert a[1] == b['name']

    actual = fromjson(fn, header=['id', 'name'])
    ieq(tbl, actual)
