from __future__ import print_function, absolute_import, division
from setuptools import setup, find_packages


setup(
    name='petl',
    author='Alistair Miles',
    author_email='alimanfoo@googlemail.com',
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
                 'Topic :: Software Development :: Libraries :: Python Modules'
                 ]
)
