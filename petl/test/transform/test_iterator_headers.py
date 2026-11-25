import pytest

import petl as etl


def make_view(kind, header):
    if kind == 'extendheader':
        return etl.extendheader([('id',), (1, 'a'), (2, 'b')], header)
    tables = [[('id', 'value'), (1, 'a')], [('id', 'value'), (2, 'b')]]
    if kind == 'cat':
        return etl.cat(*tables, header=header)
    return etl.mergesort(*tables, key='id', header=header)


@pytest.mark.parametrize('kind', ['extendheader', 'cat', 'mergesort'])
def test_iterator_header_survives_interleaved_and_repeated_reads(kind):
    names = ['value'] if kind == 'extendheader' else ['value', 'id']
    seen = []

    def header():
        for name in names:
            seen.append(name)
            yield name

    view = make_view(kind, header())
    first = iter(view)
    assert seen == []
    expected = ([('id', 'value'), (1, 'a'), (2, 'b')]
                if kind == 'extendheader' else
                [('value', 'id'), ('a', 1), ('b', 2)])

    assert next(first) == expected[0]
    second = iter(view)
    assert next(second) == expected[0]
    assert list(first) == expected[1:]
    assert list(second) == expected[1:]
    assert list(view) == expected
    assert seen == names


@pytest.mark.parametrize('kind', ['extendheader', 'cat', 'mergesort'])
def test_repeatable_header_definitions_remain_live(kind):
    names = ['value'] if kind == 'extendheader' else ['value', 'id']
    view = make_view(kind, names)
    initial = etl.header(view)
    names.append('extra')

    assert etl.header(view) == initial + ('extra',)


@pytest.mark.parametrize('kind', ['extendheader', 'cat', 'mergesort'])
def test_stateful_repeatable_headers_do_not_skip_a_definition(kind):
    definitions = ([['value'], ['later']] if kind == 'extendheader'
                   else [['value', 'id'], ['id', 'value']])

    class ChangingFields:
        def __init__(self):
            self.iterations = 0

        def __iter__(self):
            names = definitions[self.iterations % len(definitions)]
            self.iterations += 1
            return iter(names)

    fields = ChangingFields()
    view = make_view(kind, fields)
    assert fields.iterations == 0
    prefix = ('id',) if kind == 'extendheader' else ()

    assert etl.header(view) == prefix + tuple(definitions[0])
    assert etl.header(view) == prefix + tuple(definitions[1])
