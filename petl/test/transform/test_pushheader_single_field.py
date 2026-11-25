import pytest

import petl as etl


@pytest.mark.parametrize('name', ['value', '', 0])
def test_pushheader_accepts_one_positional_name(name):
    result = etl.pushheader([(1,), (2,)], name)
    expected = [(name,), (1,), (2,)]

    assert list(result) == expected
    assert list(result) == expected


def test_pushheader_retains_sequence_and_multiple_name_forms():
    table = [(1, 2)]
    expected = [('one', 'two'), (1, 2)]

    assert list(etl.pushheader(table, 'one', 'two')) == expected
    assert list(etl.pushheader(table, ['one', 'two'])) == expected
    assert list(etl.pushheader(table, ('one', 'two'))) == expected
