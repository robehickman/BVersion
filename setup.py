from setuptools import setup # type:ignore

def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name='BVersion',
    version='0.9',
    description='Centralised version control system for binary files',
    long_description=readme(),

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Topic :: File Management :: File Synchronisation',
        'Topic :: Software Development :: Version Control'
    ],

    keywords='binary file version control',
    url='https://github.com/robehickman/BVersion',
    author='Robert Hickman',
    author_email='robehickman@gmail.com',
    license='MIT',

    packages=[
        'bversion',
        'bversion.http',
        'bversion.storage',
        'bversion.backup'
    ],

    test_suite='nose.collector',
    tests_require=['nose'],

    install_requires=[
        'pysodium==0.7.0.post0',
        'termcolor==1.1.0',
        'typing_extensions'
    ],

    scripts=[
        'cli_tools/bvn',
        'cli_tools/bvn_server',
        'cli_tools/bvn_repo'
    ],

    zip_safe=False
)
