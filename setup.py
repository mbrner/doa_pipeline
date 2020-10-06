from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='doa_pipeline',
    version='0.0.1',

    description='PoC for a pipeline engine build with an data oriented architecture.',
    long_description=long_description,

    url='https://github.com/mbrner/doa_pipeline',

    author='Mathis Boerner',
    author_email='mathis.boerner@googlemail.com',

    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.7',
    ],
    keywords='pipeline',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=[
        'sqlalchemy',
        'psycopg2'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)