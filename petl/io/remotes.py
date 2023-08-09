# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import logging
import sys
from contextlib import contextmanager

from petl.compat import PY3
from petl.io.sources import register_reader, register_writer, get_reader, get_writer

logger = logging.getLogger(__name__)

# region RemoteSource


class RemoteSource(object):
    """Read or write directly from files in remote filesystems.

    This source handles many filesystems that are selected based on the
    protocol passed in the `url` argument.

    The url should be specified in `to..()` and `from...()` functions. E.g.::

        >>> import petl as etl
        >>>
        >>> def example_s3():
        ...     url = 's3://mybucket/prefix/to/myfilename.csv'
        ...     data = b'foo,bar\\na,1\\nb,2\\nc,2\\n'
        ...
        ...     etl.tocsv(data, url)
        ...     tbl = etl.fromcsv(url)
        ...
        >>> example_s3() # doctest: +SKIP
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' | '1' |
        +-----+-----+
        | 'b' | '2' |
        +-----+-----+
        | 'c' | '2' |
        +-----+-----+

    This source uses `fsspec`_ to provide the data transfer with the remote
    filesystem. Check the `Built-in Implementations <fs_builtin>`_ for available
    remote implementations.

    Some filesystem can use `URL chaining <fs_chain>`_ for compound I/O.

    .. note::

        For working this source require `fsspec`_ to be installed, e.g.::

            $ pip install fsspec

        Some remote filesystems require aditional packages to be installed.
        Check  `Known Implementations <fs_known>`_ for checking what packages
        need to be installed, e.g.::

            $ pip install s3fs     # AWS S3
            $ pip install gcsfs    # Google Cloud Storage
            $ pip install adlfs    # Azure Blob service
            $ pip install paramiko # SFTP
            $ pip install requests # HTTP, github

    .. versionadded:: 1.6.0

    .. _fsspec: https://filesystem-spec.readthedocs.io/en/latest/
    .. _fs_builtin: https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations
    .. _fs_known: https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations
    .. _fs_chain: https://filesystem-spec.readthedocs.io/en/latest/features.html#url-chaining
    """

    def __init__(self, url, **kwargs):
        self.url = url
        self.kwargs = kwargs

    def open_file(self, mode="rb"):
        import fsspec
        # auto_mkdir=True can fail in some filesystems or without permission for full path
        # E.g: s3fs tries to create a bucket when writing into a folder does not exists
        fs = fsspec.open(self.url, mode=mode, compression='infer', auto_mkdir=False, **self.kwargs)
        return fs

    @contextmanager
    def open(self, mode="rb"):
        mode2 = mode[:1] + r"b"  # python2
        fs = self.open_file(mode=mode2)
        with fs as source:
            yield source


# registering filesystems with packages installed


def _register_filesystems(only_available=False):
    """Register all known fsspec implementations as remote source."""
    from fsspec.registry import known_implementations, registry
    # https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations
    # https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations
    _register_filesystems_from(known_implementations, only_available)
    # https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.registry.register_implementation
    _register_filesystems_from(registry, only_available)


def _register_filesystems_from(fsspec_registry, only_available):
    """Register each fsspec provider from this registry as remote source."""
    for protocol, spec in fsspec_registry.items():
        missing_deps = isinstance(spec, dict) and "err" in spec
        if missing_deps and only_available:
            # this could lead to only buit-in implementations available
            # Other Known Implementations are reported with 'err' even even
            # the package is installed
            continue
        # When missing a package for fsspec use the available source in petl
        # E.g: fsspec requires `requests` package installed for handling http and https
        # but petl has URLSource that can work with urlib
        has_reader = get_reader(protocol)
        if not missing_deps or has_reader is None:
            register_reader(protocol, RemoteSource)
        has_writer = get_writer(protocol)
        if not missing_deps or has_writer is None:
            register_writer(protocol, RemoteSource)


def _try_register_filesystems():
    try:
        # pylint: disable=unused-import
        import fsspec  # noqa: F401
    except ImportError:
        logger.debug("# Missing fsspec package. Install with: pip install fsspec")
    else:
        try:
            _register_filesystems()
        except Exception as ex:
            raise ImportError("# ERROR: failed to register fsspec filesystems", ex)


if PY3:
    _try_register_filesystems()

# endregion

# region SMBSource


class SMBSource(object):
    """Downloads or uploads to Windows and Samba network drives. E.g.::

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
    """

    def __init__(self, url, **kwargs):
        self.url = url
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode="rb"):
        mode2 = mode[:1] + r"b"  # python2
        source = _open_file_smbprotocol(self.url, mode=mode2, **self.kwargs)
        try:
            yield source
        finally:
            source.close()


def _open_file_smbprotocol(url, mode="rb", **kwargs):

    _domain, host, port, user, passwd, server_path = _parse_smb_url(url)
    import smbclient

    try:
        # register the server with explicit credentials
        if user:
            smbclient.register_session(
                host, username=user, password=passwd, port=port
            )
        # Read an existing file as bytes
        mode2 = mode[:1] + r"b"
        filehandle = smbclient.open_file(server_path, mode=mode2, **kwargs)
        return filehandle

    except Exception as ex:
        raise ConnectionError("SMB error: %s" % ex).with_traceback(sys.exc_info()[2])


def _parse_smb_url(url):
    e = "SMB url must be smb://workgroup;user:password@server:port/share/folder/file.txt: "

    if not url:
        raise ValueError("SMB error: no host given")
    if not url.startswith("smb://"):
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
        domain = None
        username = None
    elif ";" in parsed.username:
        domain, username = parsed.username.split(";")
    else:
        domain, username = None, parsed.username
    port = 445 if not parsed.port else int(parsed.port)
    return domain, parsed.hostname, port, username, parsed.password, server_path


register_reader("smb", SMBSource)
register_writer("smb", SMBSource)

# endregion
