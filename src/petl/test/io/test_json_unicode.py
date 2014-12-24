# -*- coding: utf-8 -*-


from __future__ import absolute_import, print_function, division, \
    unicode_literals


import json
from petl.testutils import ieq


from petl.io.json import tojson, fromjson


def test_json_unicode():

    tbl = (('name', 'id'),
           ('Արամ Խաչատրյան', 1),
           ('Johann Strauß', 2),
           ('Вагиф Сәмәдоғлу', 3),
           ('章子怡', 4),
           )
    tojson(tbl, 'tmp/test_tojson_utf8.json')

    result = json.load(open('tmp/test_tojson_utf8.json'))
    assert len(result) == 4
    for a, b in zip(tbl[1:], result):
        assert a[0] == b['name']
        assert a[1] == b['id']

    actual = fromjson('tmp/test_tojson_utf8.json')
    ieq(tbl, actual)
