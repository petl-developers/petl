from petl.test.helpers import ieq, eq_
import petl.config as config

from nose.tools import nottest


@nottest
def test_failonerror(input_fn, expected_output):
    """In the input rows, the first row should process through the
    transformation cleanly.  The second row should generate an
    exception.  There are no requirements for any other rows."""
    #=========================================================
    # Test function parameters with default config settings
    #=========================================================
    # test the default config setting: failonerror == False
    eq_(config.failonerror, False)

    # By default, a bad conversion does not raise an exception, and
    # values for the failed conversion are returned as None
    table2 = input_fn()
    ieq(expected_output, table2)
    ieq(expected_output, table2)

    # When called with failonerror is False or None, a bad conversion
    # does not raise an exception, and values for the failed conversion
    # are returned as None
    table3 = input_fn(failonerror=False)
    ieq(expected_output, table3)
    ieq(expected_output, table3)
    table3 = input_fn(failonerror=None)
    ieq(expected_output, table3)
    ieq(expected_output, table3)

    # When called with failonerror=True, a bad conversion raises an
    # exception
    try:
        table4 = input_fn(failonerror=True)
        table4.nrows()
    except Exception:
        pass
    else:
        raise Exception('expected exception not raised')

    # When called with failonerror='inline', a bad conversion
    # does not raise an exception, and an Exception for the failed
    # conversion is returned in the result.
    expect5 = expected_output[0], expected_output[1]
    table5 = input_fn(failonerror='inline')
    ieq(expect5, table5.head(1))
    ieq(expect5, table5.head(1))
    excp = table5[2][0]
    assert isinstance(excp, Exception)

    #=========================================================
    # Test config settings
    #=========================================================
    # Save config setting
    saved_config_failonerror = config.failonerror

    # When config.failonerror == True, a bad conversion raises an
    # exception
    config.failonerror = True
    try:
        table6 = input_fn()
        table6.nrows()
    except Exception:
        pass
    else:
        raise Exception('expected exception not raised')

    # When config.failonerror == 'inline', a bad conversion
    # does not raise an exception, and an Exception for the failed
    # conversion is returned in the result.
    expect7 = expected_output[0], expected_output[1]
    config.failonerror = 'inline'
    table7 = input_fn()
    ieq(expect7, table7.head(1))
    ieq(expect7, table7.head(1))
    excp = table7[2][0]
    assert isinstance(excp, Exception)

    # When config.failonerror is an invalid value, but still truthy, it
    # behaves the same as if == True
    config.failonerror = 'invalid'
    try:
        table8 = input_fn()
        table8.nrows()
    except Exception:
        pass
    else:
        raise Exception('expected exception not raised')

    # When config.failonerror is None, it behaves the same as if
    # config.failonerror is False
    config.failonerror = None
    table9 = input_fn()
    ieq(expected_output, table9)
    ieq(expected_output, table9)

    # A False keyword parameter overrides config.failonerror == True
    config.failonerror = True
    table10 = input_fn(failonerror=False)
    ieq(expected_output, table10)
    ieq(expected_output, table10)

    # A None keyword parameter uses config.failonerror == True
    config.failonerror = True
    try:
        table11 = input_fn(failonerror=None)
        table11.nrows()
    except Exception:
        pass
    else:
        raise Exception('expected exception not raised')

    # restore config setting
    config.failonerror = saved_config_failonerror

