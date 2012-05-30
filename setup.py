from distribute_setup import use_setuptools
use_setuptools('0.6.15')

from setuptools import setup
setup(
    name='MongoDisco',
    description='MongoDB integration with the Disco map-reduce framework.',
    long_description=file('README.md').read(),
    version='0.8',
    url='https://github.com/10genNYUITP/MongoDisco',
    license='Apache Software License, Version 2.0',
    packages=['mongodisco'],
    install_requires=[
        'pymongo >= 2.1'
    ],
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: Apache Software License',
        'Development Status :: 3 - Alpha',
    ],
)
