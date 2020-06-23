# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
from contextlib import contextmanager

from petl.compat import PY3
from petl.io.sources import register_reader, register_writer


class SMBSource(object):
    '''Downloads or uploads to Windows and Samba network drives. E.g.::

        >>> def example_smb():
        ...     import petl as etl
        ...     url = 'smb://user:password@server/share/folder/file.csv'
        ...     data = b'foo,bar\\na,1\\nb,2\\nc,2\\n'
        ...     etl.tocsv(data, url)
        ...     tbl = etl.fromcsv(url)
        ... 
        >>> example_smb() # doctest: +SKIP
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' | '1' |
        +-----+-----+
        | 'b' | '2' |
        +-----+-----+
        | 'c' | '2' |
        +-----+-----+

    The argument `url` (str) must have a URI with format:
    `smb://workgroup;user:password@server:port/share/folder/file.csv`.

    Note that you need to pass in a valid hostname or IP address for the host 
    component of the URL. Do not use the Windows/NetBIOS machine name for the
    host component.

    The first component of the path in the URL points to the name of the shared
    folder. Subsequent path components will point to the directory/folder/file.

    .. note::

        For working this source require `smbprotocol`_ to be installed, e.g.::

            $ pip install smbprotocol[kerberos]

    .. versionadded:: 1.5.0

    .. _smbprotocol: https://github.com/jborean93/smbprotocol#requirements
    '''

    def __init__(self, url, **kwargs):
        self.url = url
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode='rb'):
        mode2 = mode[:1] + r'b' # python2
        source = _open_file_smbprotocol(self.url, mode=mode2, **self.kwargs)
        try:
            yield source
        finally:
            source.close()

# region SMBHandler


def _open_file_smbprotocol(url, mode='rb', **kwargs):

    domain, host, port, user, passwd, server_path = _parse_smb_url(url)
    import smbclient
    try:
        # register the server with explicit credentials
        if user:
            session = smbclient.register_session(
                host, username=user, password=passwd, port=port)
        # Read an existing file as bytes
        mode2 = mode[:1] + r'b'
        filehandle = smbclient.open_file(server_path, mode=mode2, **kwargs)
        return filehandle

    except Exception as ex:
        raise ConnectionError('SMB error: %s' %
                              ex).with_traceback(sys.exc_info()[2])


def _parse_smb_url(url):
    e = 'SMB url must be smb://workgroup;user:password@server:port/share/folder/file.txt: '

    if not url:
        raise ValueError('SMB error: no host given')
    elif not url.startswith('smb://'):
        raise ValueError(e + url)

    if PY3:
        from urllib.parse import urlparse
    else:
        from urlparse import urlparse
    parsed = urlparse(url)
    if not parsed.path:
        raise ValueError(e + url)

    unc_path = parsed.path.replace("/", "\\")
    server_path = "\\\\{}{}".format(parsed.hostname, unc_path)

    if not parsed.username:
        domain, username = None
    elif ';' in parsed.username:
        domain, username = parsed.username.split(';')
    else:
        domain, username = None, parsed.username
    port = 555 if not parsed.port else int(parsed.port)
    return domain, parsed.hostname, port, username, parsed.password, server_path

# endregion


register_reader('smb', SMBSource)
register_writer('smb', SMBSource)
