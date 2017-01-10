from setuptools import find_packages
from setuptools import setup

setup(
    name='cassandra-mirror',
    version='0.0.1',
    author='Josh Snyder',
    author_email='josh.snyder@fitbit.com',
    packages=find_packages(),
    entry_points=dict(
    console_scripts=[
            'backup=cassandra_mirror.backup:do_backup',
            'restore=cassandra_mirror.restore:do_restore',
        ]
    ),
)
