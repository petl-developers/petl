import json
from tempfile import NamedTemporaryFile


from petl.test.helpers import ieq
from petl.io.json import tojson, fromjson


def test_json_unicode():

    tbl = (('id', 'name'),
           (1, 'Արամ Խաչատրյան'),
           (2, 'Johann Strauß'),
           (3, 'Вагиф Сәмәдоғлу'),
           (4, '章子怡'),
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
