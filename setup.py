from ast import literal_eval
from distutils.core import setup


def get_version(source='src/petl/__init__.py'):
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
    package_dir={'': 'src'},
    packages=['petl'],
    scripts=['bin/petl'],
    url='https://github.com/alimanfoo/petl',
    license='MIT License',
    description='A tentative Python module for extracting, transforming and loading tables of data.',
    long_description=open('README.txt').read(),
    classifiers=['Intended Audience :: Developers',
                 'License :: OSI Approved :: MIT License',
                 'Programming Language :: Python',
                 'Topic :: Software Development :: Libraries :: Python Modules'
                 ]
)