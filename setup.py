"""Install the boto_iterators module with setuptools."""
import os
from setuptools import setup, find_packages


def fread(filename: str) -> str:
    """
    Open and read a (short) local file given its filename.

    :param filename: the file name.

    :return: the file content, as raw string.
    """
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


setup(
    name='boto_iterators',
    version=fread('VERSION'),
    description=('Boto3 bolts framework to process data using Iterators.'),
    author_email='dev@afacqu.es',
    packages=find_packages(),
    install_requires=fread('requirements.txt').splitlines()
)
