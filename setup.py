# -*- coding: utf-8 -*-

from setuptools import setup

from pydataapi import __version__

packages = ['pydataapi']

with open('requirements.txt') as f:
    requires = f.readlines()

with open('requirements.txt') as f:
    test_requirements = f.readlines()

with open('LICENSE', 'r') as f:
    _license = f.read()

with open('README.md', 'r') as f:
    readme = f.read()

setup(
    name='pydataapi',
    version=__version__,
    description="py-data-api is a user-friendly client for AWS Aurora Serverless's Data API ",
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Koudai Aono',
    author_email='koxudaxi@gmail.com',
    url='https://github.com/koxudaxi/py-data-api',
    packages=packages,
    data_files=[('', ['LICENSE', 'README.md'])],
    package_dir={'pydataapi': 'pydataapi'},
    include_package_data=True,
    install_requires=requires,
    zip_safe=False,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    tests_require=test_requirements,
)
