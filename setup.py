from distutils.core import setup

setup(
    name='petl',
    version='0.1-SNAPSHOT',
    author='Alistair Miles',
    author_email='alimanfoo@googlemail.com',
    package_dir={'': 'src'},
    packages=['petl'],
    url='http://pypi.python.org/pypi/petl/',
    license='LICENSE.txt',
    description='A tentative Python module for extracting, transforming and loading tables of data.',
    long_description=open('README.txt').read(),
)
