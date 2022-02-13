# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import pytest

from petl.test.helpers import eq_, ieq, get_env_vars_named

GET_ENV_PREFIX = "PETL_TEST_HELPER_ENVVAR_"


def _testcase_get_env_vars_named(num_vals, prefix=""):
    res = {}
    for i in range(1, num_vals, 1):
        reskey = prefix + str(i)
        res[reskey] = str(i)
    return res


@pytest.fixture()
def setup_helpers_get_env_vars_named(monkeypatch):
    varlist = _testcase_get_env_vars_named(3, prefix=GET_ENV_PREFIX)
    for k, v in varlist.items():
        monkeypatch.setenv(k, v)


def test_helper_get_env_vars_named_prefixed(setup_helpers_get_env_vars_named):
    expected = _testcase_get_env_vars_named(3, GET_ENV_PREFIX)
    found = get_env_vars_named(GET_ENV_PREFIX, remove_prefix=False)
    ieq(found, expected)


def test_helper_get_env_vars_named_unprefixed(setup_helpers_get_env_vars_named):
    expected = _testcase_get_env_vars_named(3)
    found = get_env_vars_named(GET_ENV_PREFIX, remove_prefix=True)
    ieq(found, expected)


def test_helper_get_env_vars_named_not_found(setup_helpers_get_env_vars_named):
    expected = None
    found = get_env_vars_named("PETL_TEST_HELPER_ENVVAR_NOT_FOUND_")
    eq_(found, expected)
