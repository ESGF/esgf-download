
# -*- coding: utf-8 -*-
from setuptools import setup

import codecs

with codecs.open('README.md', encoding="utf-8") as fp:
    long_description = fp.read()
INSTALL_REQUIRES = [
    'MyProxyClient>=2.1.0',
    'aiofiles>=22.1.0',
    'alembic>=1.8.1',
    'click>=8.1.3',
    'click-params==0.3.0',
    'httpx>=0.23.0',
    'nest-asyncio>=1.5.6',
    'pyOpenSSL>=22.1.0',
    'pyyaml>=6.0',
    'tomlkit>=0.11.5',
    'rich>=12.6.0',
    'sqlalchemy>=2.0.0b2',
    'setuptools>=65.4.1',
    'aiostream>=0.4.5',
    'attrs>=22.1.0',
    'cattrs>=22.2.0',
]
ENTRY_POINTS = {
    'console_scripts': [
        'esgpull = esgpull.cli:main',
    ],
}

setup_kwargs = {
    'name': 'esgpull',
    'version': '0.2.0',
    'description': 'ESGF data discovery, download, replication tool',
    'long_description': long_description,
    'license': 'Public',
    'author': '',
    'author_email': 'Sven Rodriguez <srodriguez@ipsl.fr>',
    'maintainer': None,
    'maintainer_email': None,
    'url': '',
    'packages': [
        'esgpull',
        'esgpull.db',
        'esgpull.cli',
    ],
    'package_data': {'': ['*']},
    'long_description_content_type': 'text/markdown',
    'install_requires': INSTALL_REQUIRES,
    'python_requires': '>=3.10',
    'entry_points': ENTRY_POINTS,

}


setup(**setup_kwargs)
