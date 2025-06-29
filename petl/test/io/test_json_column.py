from petl.io.db_create import make_sqlalchemy_column
from sqlalchemy import JSON

def test_json_inference():
    data = [{'a': 1}, {'b': 2}, None]
    col = make_sqlalchemy_column(data, 'payload')
    assert col.name == 'payload'
    assert isinstance(col.type, JSON)
