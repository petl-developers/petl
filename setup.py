from distutils.core import setup

setup(
    name='petl',
    version='0.5',
    author='Alistair Miles',
    author_email='alimanfoo@googlemail.com',
    package_dir={'': 'src'},
    packages=['petl'],
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
