from __future__ import print_function, division, absolute_import

import subprocess

def test_executable():
    result = subprocess.run("""
        (echo foo,bar ; echo a,b; echo c,d) |
        petl 'fromcsv().cut("foo").head(1).tocsv()'
    """, shell=True, check=True, capture_output=True)
    assert result.stdout == b'foo\r\na\r\n'
