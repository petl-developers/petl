from __future__ import print_function, absolute_import, division


from ast import literal_eval
from distutils.core import setup


def get_version(source='petl/__init__.py'):
    with open(source) as f:
        for line in f:
            if line.startswith('__version__'):
                return literal_eval(line.split('=')[-1].lstrip())
    raise ValueError("__version__ not found")


setup(
    name='petl',
    version=get_version(),
    author='Alistair Miles',
    author_email='alimanfoo@googlemail.com',
    package_dir={'': '.'},
    packages=['petl', 'petl.io', 'petl.transform', 'petl.util',
              'petl.test', 'petl.test.io', 'petl.test.transform',
              'petl.test.util'],
    scripts=['bin/petl'],
    url='https://github.com/alimanfoo/petl',
    license='MIT License',
    description='A Python package for extracting, transforming and loading '
                'tables of data.',
    long_description=open('README.txt').read(),
    classifiers=['Intended Audience :: Developers',
                 'License :: OSI Approved :: MIT License',
                 'Programming Language :: Python :: 2.6',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3.4',
                 'Topic :: Software Development :: Libraries :: Python Modules'
                 ]
)
