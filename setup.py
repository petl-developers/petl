from __future__ import absolute_import, division, print_function

from setuptools import find_packages, setup

setup(
    name='petl',
    author='Alistair Miles',
    author_email='alimanfoo@googlemail.com',
    maintainer="Juarez Rudsatz",
    maintainer_email="juarezr@gmail.com",
    package_dir={'': '.'},
    packages=find_packages('.'),
    scripts=['bin/petl'],
    url='https://github.com/petl-developers/petl',
    license='MIT License',
    description='A Python package for extracting, transforming and loading '
                'tables of data.',
    long_description=open('README.txt').read(),
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*',
    setup_requires=["setuptools>18.0", "setuptools-scm>1.5.4"],
    extras_require={
        'avro': ['fastavro>=0.24.0'],
        'bcolz': ['bcolz>=1.2.1'],
        'db': ['SQLAlchemy>=1.3.6,<2.0'],
        'hdf5': ['cython>=0.29.13', 'numpy>=1.16.4', 'numexpr>=2.6.9', 
                 'tables>=3.5.2'],
        'http': ['aiohttp>=3.6.2', 'requests'],
        'interval': ['intervaltree>=3.0.2'],
        'numpy': ['numpy>=1.16.4'],
        'pandas': ['pandas>=0.24.2'],
        'remote': ['fsspec>=0.7.4'],
        'smb': ['smbprotocol>=1.0.1'],
        'xls': ['xlrd>=2.0.1', 'xlwt>=1.3.0'],
        'xlsx': ['openpyxl>=2.6.2'],
        'xpath': ['lxml>=4.4.0'],
        'whoosh': ['whoosh'],
    },
    use_scm_version={
        "version_scheme": "guess-next-dev",
        "local_scheme": "dirty-tag",
        "write_to": "petl/version.py",
    },
    classifiers=['Intended Audience :: Developers',
                 'License :: OSI Approved :: MIT License',
                 'Programming Language :: Python :: 2',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3',
                 'Programming Language :: Python :: 3.6',
                 'Programming Language :: Python :: 3.7',
                 'Programming Language :: Python :: 3.8',
                 'Programming Language :: Python :: 3.9',
                 'Programming Language :: Python :: 3.10',
                 'Programming Language :: Python :: 3.11',
                 'Programming Language :: Python :: 3.12',
                 'Programming Language :: Python :: 3.13',
                 'Topic :: Software Development :: Libraries :: Python Modules'
                 ]
)
