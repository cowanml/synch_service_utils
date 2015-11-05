__author__ = 'arkilic'
try:
    from setuptools import setup
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup

import versioneer
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='synch_service_utils',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    license="BSD (3-clause)",
    url="https://github.com/nsls-ii/synch_service_utils",
    packages=['synch_service_utils',
              ],
    long_description=read('README.md'),
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 3',
    ],
)
