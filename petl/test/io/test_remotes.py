# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
import os

from petl.compat import PY3
from petl.test.helpers import ieq, eq_
from petl.io.avro import fromavro, toavro
from petl.io.csv import fromcsv, tocsv
from petl.util.vis import look

# region Codec test cases


def test_helper_local():
    if PY3:
        _write_read_into_url("./example.")


try:
    import fsspec
except ImportError as e:
    print("SKIP FSSPEC helper tests: %s" % e, file=sys.stderr)
else:

    def test_helper_fsspec():
        # _write_read_from_env_url('PETL_TEST_S3')
        _write_read_from_env_matching("PETL_TEST_")


try:
    import smbclient
except ImportError as e:
    print("SKIP SMB helper tests: %s" % e, file=sys.stderr)
else:

    def test_helper_smb():
        _write_read_from_env_url("PETL_SMB_URL")

    def test_helper_smb_url_parse():
        from petl.io.remotes import _parse_smb_url

        url = r"smb://workgroup;user:password@server:444/share/folder/file.csv"
        domain, host, port, user, passwd, server_path = _parse_smb_url(url)
        print("Parsed:", domain, host, port, user, passwd, server_path)
        eq_(domain, r"workgroup")
        eq_(host, r"server")
        eq_(port, 444)
        eq_(user, r"user")
        eq_(passwd, r"password")
        eq_(server_path, "\\\\server\\share\\folder\\file.csv")


# endregion

# region Execution


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
    $ export PETL_TEST_<protocol>='<protocol>://myuser:mypassword@host:port/path/to/file.ext'"""
        print(msg, file=sys.stderr)


def _write_read_from_env_url(env_var_name):
    base_url = os.getenv(env_var_name, "skip")
    if base_url == "skip":
        print("SKIPPED ", file=sys.stderr, end="")
    else:
        _write_read_into_url(base_url)
        print("DONE ", file=sys.stderr, end="")


def _write_read_into_url(base_url):
    _write_read_file_into_url(base_url, "filename1.csv")
    _write_read_file_into_url(base_url, "filename2.avro")
    _write_read_file_into_url(base_url, "filename3.csv", "gz")
    _write_read_file_into_url(base_url, "filename4.avro", "gz")
    _write_read_file_into_url(base_url, "filename5.csv", "xz")
    _write_read_file_into_url(base_url, "filename6.csv", "zst")
    _write_read_file_into_url(base_url, "filename7.csv", "lz4")
    _write_read_file_into_url(base_url, "filename8.csv", "snappy")


def _write_read_file_into_url(base_url, filename, compression=None):
    if ".avro" in filename and not _has_avro:
        return
    is_local = base_url.startswith("./")
    if compression is not None:
        if is_local:
            return
        filename = filename + "." + compression
        codec = fsspec.utils.infer_compression(filename)
        if codec is None:
            print("\n    - %s SKIPPED " % filename, file=sys.stderr, end="")
            return
    print("\n    - %s " % filename, file=sys.stderr, end="")

    if is_local:
        source_url = base_url + filename
    else:
        source_url = os.path.join(base_url, filename)

    _show__rows_from("Expected:", _table)

    if ".avro" in filename:
        toavro(_table, source_url)
        actual = fromavro(source_url)
    else:
        tocsv(_table, source_url, encoding="ascii", lineterminator="\n")
        actual = fromcsv(source_url, encoding="ascii")

    _show__rows_from("Actual:", actual)
    ieq(_table, actual)
    ieq(_table, actual)  # verify can iterate twice


def _show__rows_from(label, test_rows, limit=0):
    print(label)
    print(look(test_rows, limit=limit))


def _test_avro_too():
    try:
        import fastavro

        return True
    except:
        return False


# endregion

# region Mockup data

_has_avro = _test_avro_too()

_table = (
    (u"name", u"friends", u"age"),
    (u"Bob", "42", "33"),
    (u"Jim", "13", "69"),
    (u"Joe", "86", "17"),
    (u"Ted", "23", "51"),
)

# endregion
