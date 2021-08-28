import io
from tempfile import NamedTemporaryFile


from petl.test.helpers import ieq, eq_
from petl.io.csv import fromcsv, tocsv, appendcsv


def test_fromcsv():

    data = (
        "name,id\n"
        "Արամ Խաչատրյան,1\n"
        "Johann Strauß,2\n"
        "Вагиф Сәмәдоғлу,3\n"
        "章子怡,4\n"
    )
    fn = NamedTemporaryFile().name
    uf = open(fn, encoding='utf-8', mode='wt')
    uf.write(data)
    uf.close()

    actual = fromcsv(fn, encoding='utf-8')
    expect = (('name', 'id'),
              ('Արամ Խաչատրյան', '1'),
              ('Johann Strauß', '2'),
              ('Вагиф Сәмәдоғлу', '3'),
              ('章子怡', '4'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromcsv_lineterminators():
    data = ('name,id',
            'Արամ Խաչատրյան,1',
            'Johann Strauß,2',
            'Вагиф Сәмәдоғлу,3',
            '章子怡,4')
    expect = (('name', 'id'),
              ('Արամ Խաչատրյան', '1'),
              ('Johann Strauß', '2'),
              ('Вагиф Сәмәдоғлу', '3'),
              ('章子怡', '4'))

    for lt in '\r', '\n', '\r\n':
        fn = NamedTemporaryFile().name
        uf = open(fn, encoding='utf-8', mode='wt', newline='')
        uf.write(lt.join(data))
        uf.close()
        actual = fromcsv(fn, encoding='utf-8')
        ieq(expect, actual)


def test_tocsv():

    tbl = (('name', 'id'),
           ('Արամ Խաչատրյան', 1),
           ('Johann Strauß', 2),
           ('Вагиф Сәмәдоғлу', 3),
           ('章子怡', 4))
    fn = NamedTemporaryFile().name
    tocsv(tbl, fn, encoding='utf-8', lineterminator='\n')

    expect = (
        "name,id\n"
        "Արամ Խաչատրյան,1\n"
        "Johann Strauß,2\n"
        "Вагиф Сәмәдоғлу,3\n"
        "章子怡,4\n"
    )
    uf = open(fn, encoding='utf-8', mode='rt', newline='')
    actual = uf.read()
    eq_(expect, actual)

    # Test with write_header=False
    tbl = (('name', 'id'),
           ('Արամ Խաչատրյան', 1),
           ('Johann Strauß', 2),
           ('Вагиф Сәмәдоғлу', 3),
           ('章子怡', 4))
    tocsv(tbl, fn, encoding='utf-8', lineterminator='\n', write_header=False)

    expect = (
        "Արամ Խաչատրյան,1\n"
        "Johann Strauß,2\n"
        "Вагиф Сәмәдоғлу,3\n"
        "章子怡,4\n"
    )
    uf = open(fn, encoding='utf-8', mode='rt', newline='')
    actual = uf.read()
    eq_(expect, actual)


def test_appendcsv():

    data = (
        "name,id\n"
        "Արամ Խաչատրյան,1\n"
        "Johann Strauß,2\n"
        "Вагиф Сәмәдоғлу,3\n"
        "章子怡,4\n"
    )
    fn = NamedTemporaryFile().name
    uf = open(fn, encoding='utf-8', mode='wt')
    uf.write(data)
    uf.close()

    tbl = (('name', 'id'),
           ('ኃይሌ ገብረሥላሴ', 5),
           ('ედუარდ შევარდნაძე', 6))
    appendcsv(tbl, fn, encoding='utf-8', lineterminator='\n')

    expect = (
        "name,id\n"
        "Արամ Խաչատրյան,1\n"
        "Johann Strauß,2\n"
        "Вагиф Сәмәдоғлу,3\n"
        "章子怡,4\n"
        "ኃይሌ ገብረሥላሴ,5\n"
        "ედუარდ შევარდნაძე,6\n"
    )
    uf = open(fn, encoding='utf-8', mode='rt')
    actual = uf.read()
    eq_(expect, actual)


def test_tocsv_none():

    tbl = (('col1', 'colNone'),
           ('a', 1),
           ('b', None),
           ('c', None),
           ('d', 4))
    fn = NamedTemporaryFile().name
    tocsv(tbl, fn, encoding='utf-8', lineterminator='\n')

    expect = (
        'col1,colNone\n'
        'a,1\n'
        'b,\n'
        'c,\n'
        'd,4\n'
    )

    uf = open(fn, encoding='utf-8', mode='rt', newline='')
    actual = uf.read()
    eq_(expect, actual)
