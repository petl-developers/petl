# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
import os
from importlib import import_module

import pytest

from petl.compat import PY3
from petl.test.helpers import ieq, eq_
from petl.io.avro import fromavro, toavro
from petl.io.csv import fromcsv, tocsv
from petl.io.json import fromjson, tojson
from petl.io.xlsx import fromxlsx, toxlsx
from petl.io.xls import fromxls, toxls
from petl.util.vis import look

# region Codec test cases


def test_helper_local():
    if PY3:
        _ensure_dir("./tmp")
        _write_read_into_url("./tmp/example.")


def test_helper_fsspec():
    try:
        # pylint: disable=unused-import
        import fsspec  # noqa: F401
    except ImportError as e:
        pytest.skip("SKIP FSSPEC helper tests: %s" % e)
    else:
        _write_read_from_env_matching("PETL_TEST_")


def test_helper_smb():
    try:
        # pylint: disable=unused-import
        import smbclient  # noqa: F401
    except ImportError as e:
        pytest.skip("SKIP SMB helper tests: %s" % e)
    else:
        _write_read_from_env_url("PETL_SMB_URL")


def test_helper_smb_url_parse():
    from petl.io.remotes import _parse_smb_url

    url = r"smb://workgroup;user:password@server:444/share/folder/file.csv"
    domain, host, port, user, passwd, server_path = _parse_smb_url(url)
    # print("Parsed:", domain, host, port, user, passwd, server_path)
    eq_(domain, r"workgroup")
    eq_(host, r"server")
    eq_(port, 444)
    eq_(user, r"user")
    eq_(passwd, r"password")
    eq_(server_path, "\\\\server\\share\\folder\\file.csv")


# endregion

# region Execution


def _ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def _write_read_from_env_matching(prefix):
    q = 0
    for variable, base_url in os.environ.items():
        if variable.upper().startswith(prefix.upper()):
            fmsg = "\n  {}: {} -> ".format(variable, base_url)
            print(fmsg, file=sys.stderr, end="")
            _write_read_into_url(base_url)
            print("DONE ", file=sys.stderr, end="")
            q += 1
    if q < 1:
        msg = """SKIPPED
    For testing remote source define a environment variable:
    $ export PETL_TEST_<protocol>='<protocol>://myuser:mypassword@host:port/path/to/folder'"""
        print(msg, file=sys.stderr)


def _write_read_from_env_url(env_var_name):
    base_url = os.getenv(env_var_name, "skip")
    if base_url == "skip":
        print("SKIPPED ", file=sys.stderr, end="")
    else:
        _write_read_into_url(base_url)
        print("DONE ", file=sys.stderr, end="")


def _write_read_into_url(base_url):
    _write_read_file_into_url(base_url, "filename10.csv")
    _write_read_file_into_url(base_url, "filename11.csv", "gz")
    _write_read_file_into_url(base_url, "filename12.csv", "xz")
    _write_read_file_into_url(base_url, "filename13.csv", "zst")
    _write_read_file_into_url(base_url, "filename14.csv", "lz4")
    _write_read_file_into_url(base_url, "filename15.csv", "snappy")
    _write_read_file_into_url(base_url, "filename20.json")
    _write_read_file_into_url(base_url, "filename21.json", "gz")
    _write_read_file_into_url(base_url, "filename30.avro", pkg='fastavro')
    _write_read_file_into_url(base_url, "filename40.xlsx", pkg='openpyxl')
    _write_read_file_into_url(base_url, "filename50.xls", pkg='xlwt')


def _build_source_url_from(base_url, filename, compression=None):
    is_local = base_url.startswith("./")
    if compression is not None:
        if is_local:
            return None
        filename = filename + "." + compression
        import fsspec
        codec = fsspec.utils.infer_compression(filename)
        if codec is None:
            print("\n    - %s SKIPPED " % filename, file=sys.stderr, end="")
            return None
    print("\n    - %s " % filename, file=sys.stderr, end="")
    if is_local:
        source_url = base_url + filename
    else:
        source_url = os.path.join(base_url, filename)
    return source_url


def _write_read_file_into_url(base_url, filename, compression=None, pkg=None):
    if not _is_installed(pkg, filename):
        return
    source_url = _build_source_url_from(base_url, filename, compression)
    if source_url is None:
        return
    actual = None
    if ".avro" in filename:
        toavro(_table, source_url)
        actual = fromavro(source_url)
    elif ".xlsx" in filename:
        toxlsx(_table, source_url, 'test1', mode='overwrite')
        toxlsx(_table2, source_url, 'test2', mode='add')
        actual = fromxlsx(source_url, 'test1')
    elif ".xls" in filename:
        toxls(_table, source_url, 'test')
        actual = fromxls(source_url, 'test')
    elif ".json" in filename:
        tojson(_table, source_url)
        actual = fromjson(source_url)
    elif ".csv" in filename:
        tocsv(_table, source_url, encoding="ascii", lineterminator="\n")
        actual = fromcsv(source_url, encoding="ascii")

    if actual is not None:
        _show__rows_from("Expected:", _table)
        _show__rows_from("Actual:", actual)
        ieq(_table, actual)
        ieq(_table, actual)  # verify can iterate twice
    else:
        print("\n    - %s SKIPPED " % filename, file=sys.stderr, end="")


def _show__rows_from(label, test_rows, limit=0):
    print(label)
    print(look(test_rows, limit=limit))


def _is_installed(package_name, message=None):
    if package_name is None:
        return True  # Not required
    try:
        mod = import_module(package_name)
        found = mod is not None
        if not found:
            msg = message or package_name
            print("\n    - %s SKIPPED " % msg, file=sys.stderr, end="")
        return found
    except Exception as exm:
        print(exm, file=sys.stderr)
        return False


# endregion

# region Mockup data

_table = (
    (u"name", u"friends", u"age"),
    (u"Bob", "42", "33"),
    (u"Jim", "13", "69"),
    (u"Joe", "86", "17"),
    (u"Ted", "23", "51"),
)

_table2 = (
    (u"name", u"friends", u"age"),
    (u"Giannis", "31", "12"),
    (u"James", "38", "8"),
    (u"Stephen", "28", "4"),
    (u"Jason", "23", "12"),
)

# endregion
