from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name='shttpfs',
    version='0.5',
    description='Client/server Http File Sync Utility',
    long_description=readme(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Topic :: File Management :: File Synchronisation',
    ],
    keywords='file synchronisation synchroniser',
    url='https://github.com/robehickman/simple-http-file-sync',
    author='Robert Hickman',
    author_email='robehickman@gmail.com',
    license='MIT',
    packages=['shttpfs3'],
    test_suite='nose.collector',
    tests_require=['nose'],
    install_requires=[
        'pysodium==0.7.0.post0',
        'termcolor==1.1.0'
    ],
    scripts=['cli_tools/shttpfs3', 'cli_tools/shttpfs_server3'],
    zip_safe=False)

