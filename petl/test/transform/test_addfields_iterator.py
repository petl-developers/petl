import petl as etl


def test_header_inspection_does_not_consume_added_fields():
    table = [('amount',), (12,), (24,)]
    definitions = iter([('source', 'local'), ('double', lambda row: row.amount * 2)])
    result = etl.addfields(table, definitions)
    assert etl.header(result) == ('amount', 'source', 'double')
    expected = [('amount', 'source', 'double'), (12, 'local', 24), (24, 'local', 48)]
    assert list(result) == expected
    assert list(result) == expected


def test_iterator_definitions_keep_insert_positions_and_missing_values():
    table = [('amount', 'note'), (12,), (24, 'ok')]
    definitions = (definition for definition in [('source', 'local', 0), ('double', lambda row: row.amount * 2)])
    result = etl.addfields(table, definitions, missing='')
    expected = [('source', 'amount', 'note', 'double'), ('local', 12, '', 24), ('local', 24, 'ok', 48)]
    assert list(result) == expected
    assert list(result) == expected


def test_list_definitions_can_still_change_between_traversals():
    definitions = [('source', 'local')]
    result = etl.addfields([('amount',), (12,)], definitions)
    assert list(result) == [('amount', 'source'), (12, 'local')]
    definitions.append(('unit', 'kg'))
    assert list(result) == [('amount', 'source', 'unit'), (12, 'local', 'kg')]


def test_iterator_definitions_are_not_consumed_at_construction():
    consumed = []

    def definitions():
        consumed.append(True)
        yield 'source', 'local'

    result = etl.addfields([('amount',), (12,)], definitions())
    assert consumed == []
    assert list(result) == [('amount', 'source'), (12, 'local')]
    assert list(result) == [('amount', 'source'), (12, 'local')]
    assert consumed == [True]
