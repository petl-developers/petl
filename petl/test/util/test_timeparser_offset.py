import datetime

import pytest

from petl import timeparser


@pytest.mark.parametrize('offset, minutes', [('+0800', 480), ('-0330', -210), ('+0000', 0)])
def test_timeparser_preserves_parsed_offset(offset, minutes):
    parser = timeparser('%H:%M:%S.%f%z')

    result = parser(' 01:02:03.456789' + offset + ' ')

    assert result.utcoffset() == datetime.timedelta(minutes=minutes)
    assert (result.hour, result.minute, result.second, result.microsecond) == (1, 2, 3, 456789)


def test_timeparser_without_offset_stays_naive():
    assert timeparser('%H:%M:%S')('01:02:03') == datetime.time(1, 2, 3)


def test_timeparser_retains_strict_and_non_strict_errors():
    with pytest.raises(ValueError):
        timeparser('%H:%M%z')('bad time')
    assert timeparser('%H:%M%z', strict=False)(' bad time ') == ' bad time '
