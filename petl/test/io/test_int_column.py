from petl.io.db_create import make_sqlalchemy_column
from sqlalchemy import Integer

def test_int_inference():
    col = make_sqlalchemy_column([1, 2, 3], 'n')
    assert col.name == 'n'
    assert isinstance(col.type, Integer)
