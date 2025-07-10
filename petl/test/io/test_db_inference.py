import pytest

from petl.io.db_create import make_sqlalchemy_column

try:
    import sqlalchemy  # noqa: F401
except ImportError as e:
    pytest.skip('SKIP generic DB inference tests: %s' % e, allow_module_level=True)
else:
    from sqlalchemy import JSON, Integer

def test_json_inference():
    data = [{'a': 1}, {'b': 2}, None]
    col = make_sqlalchemy_column(data, 'payload')
    assert col.name == 'payload'
    assert isinstance(col.type, JSON)

def test_int_inference():
    col = make_sqlalchemy_column([1, 2, 3], 'n')
    assert col.name == 'n'
    assert isinstance(col.type, Integer)
