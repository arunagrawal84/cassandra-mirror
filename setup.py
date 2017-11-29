from setuptools import find_packages
from setuptools import setup

setup(
    name='cassandra-mirror',
    version='0.1.1',
    author='Josh Snyder',
    author_email='josh@code406.com',
    packages=find_packages(),
    entry_points=dict(
        console_scripts=[
            'backup=cassandra_mirror.backup:do_backup',
            'restore=cassandra_mirror.restore:do_restore',
        ]
    ),
    install_requires=[
        'boto3>=1.4.2',
        'PyYAML>=3.12',
        'plumbum>=1.6.3',
    ],
    dependency_links = [
        'git+https://github.com/hashbrowncipher/plumbum.git@d57e53955536423857be87ec394e6eb376acaddf#egg=plumbum',
    ]
)
