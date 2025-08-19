from __future__ import print_function, division, absolute_import

import subprocess

def test_executable():
    result = subprocess.check_output("""
        (echo foo,bar ; echo a,b; echo c,d) |
        petl 'fromcsv().cut("foo").head(1).tocsv()'
    """, shell=True)
    assert result == b'foo\r\na\r\n'

def test_json_stdin():
    result = subprocess.check_output("""
        echo '[{"foo": "a", "bar": "b"}]' |
        petl 'fromjson().tocsv()'
    """, shell=True)
    assert result == b'foo,bar\r\na,b\r\n'
    result = subprocess.check_output("""
        ( echo '{"foo": "a", "bar": "b"}' ; echo '{"foo": "c", "bar": "d"}' ) |
        petl 'fromjson(lines=True).tocsv()'
    """, shell=True)
    assert result == b'foo,bar\r\na,b\r\nc,d\r\n'
