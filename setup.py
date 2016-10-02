from setuptools import setup

def readme():
    with open('README') as f:
        return f.read()

setup(
    name='shttpfs',
    version='0.1',
    description='Client/server Http File Sync Utility',
    long_description=readme(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Topic :: File Management :: File Synchronisation',
    ],
    keywords='file synchronisation synchroniser',
    url='https://github.com/robehickman/simple-http-file-sync',
    author='Robert Hickman',
    author_email='robehickman@gmail.com',
    license='MIT',
    packages=['shttpfs'],
    test_suite='nose.collector',
    tests_require=['nose'],
    install_requires=[
        'termcolor', 'pynacl', 'scrypt', 'poster', 'flask'
    ],
    scripts=['cli_tools/shttpfs', 'cli_tools/shttpfs_server', 'cli_tools/shttpfs_keygen'],
    zip_safe=False)

