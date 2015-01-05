# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division
# N.B., do not import unicode_literals in tests


import json
from petl.testutils import ieq


from petl.io.json import tojson, fromjson


def test_json_unicode():

    tbl = ((u'id', u'name'),
           (1, u'Արամ Խաչատրյան'),
           (2, u'Johann Strauß'),
           (3, u'Вагиф Сәмәдоғлу'),
           (4, u'章子怡'),
           )
    tojson(tbl, 'tmp/test_tojson_utf8.json')

    result = json.load(open('tmp/test_tojson_utf8.json'))
    assert len(result) == 4
    for a, b in zip(tbl[1:], result):
        assert a[0] == b['id']
        assert a[1] == b['name']

    actual = fromjson('tmp/test_tojson_utf8.json')
    ieq(tbl, actual)
