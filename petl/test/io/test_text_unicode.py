import io
from tempfile import NamedTemporaryFile
from petl.test.helpers import ieq, eq_


from petl.io.text import fromtext, totext


def test_fromtext():
    data = (
        "name,id\n"
        "Արամ Խաչատրյան,1\n"
        "Johann Strauß,2\n"
        "Вагиф Сәмәдоғлу,3\n"
        "章子怡,4\n"
    )
    fn = NamedTemporaryFile().name
    f = open(fn, encoding='utf-8', mode='wt')
    f.write(data)
    f.close()

    actual = fromtext(fn, encoding='utf-8')
    expect = (('lines',),
              ('name,id',),
              ('Արամ Խաչատրյան,1',),
              ('Johann Strauß,2',),
              ('Вагиф Сәмәдоғлу,3',),
              ('章子怡,4',),
              )
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_totext():

    # exercise function
    tbl = (('name', 'id'),
           ('Արամ Խաչատրյան', 1),
           ('Johann Strauß', 2),
           ('Вагиф Сәмәдоғлу', 3),
           ('章子怡', 4),
           )
    prologue = (
        "{| class='wikitable'\n"
        "|-\n"
        "! name\n"
        "! id\n"
    )
    template = (
        "|-\n"
        "| {name}\n"
        "| {id}\n"
    )
    epilogue = "|}\n"
    fn = NamedTemporaryFile().name
    totext(tbl, fn, template=template, prologue=prologue,
           epilogue=epilogue, encoding='utf-8')

    # check what it did
    f = open(fn, encoding='utf-8', mode='rt')
    actual = f.read()
    expect = (
        "{| class='wikitable'\n"
        "|-\n"
        "! name\n"
        "! id\n"
        "|-\n"
        "| Արամ Խաչատրյան\n"
        "| 1\n"
        "|-\n"
        "| Johann Strauß\n"
        "| 2\n"
        "|-\n"
        "| Вагиф Сәмәдоғлу\n"
        "| 3\n"
        "|-\n"
        "| 章子怡\n"
        "| 4\n"
        "|}\n"
    )
    eq_(expect, actual)
