# -*- coding: utf-8 -*-


from __future__ import absolute_import, print_function, division, \
    unicode_literals


import json
from petl.testutils import ieq


from petl.io.json import tojson, fromjson


def test_json_unicode():

    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4),
           )
    tojson(tbl, 'tmp/test_tojson_utf8.json')

    result = json.load(open('tmp/test_tojson_utf8.json'))
    assert len(result) == 4
    for a, b in zip(tbl[1:], result):
        assert a[0] == b['name']
        assert a[1] == b['id']

    actual = fromjson('tmp/test_tojson_utf8.json')
    ieq(tbl, actual)
