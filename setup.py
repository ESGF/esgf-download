# -*- coding: utf-8 -*-
import codecs

from setuptools import setup

with codecs.open("README.md", encoding="utf-8") as fp:
    long_description = fp.read()
INSTALL_REQUIRES = [
    "MyProxyClient>=2.1.0",
    "aiofiles>=22.1.0",
    "alembic>=1.8.1",
    "click>=8.1.3",
    "click-params>=0.4.0",
    "httpx>=0.23.0",
    "nest-asyncio>=1.5.6",
    "pyOpenSSL>=22.1.0",
    "pydantic>=1.10.2",
    "pyyaml>=6.0",
    "tomlkit>=0.11.5",
    "rich>=12.6.0",
    "sqlalchemy>=1.4.41",
    "tqdm>=4.64.1",
    "setuptools>=65.4.1",
    "aiostream>=0.4.5",
]
ENTRY_POINTS = {
    "console_scripts": [
        "esgpull = esgpull.cli:main",
    ],
}

setup_kwargs = {
    "name": "esgpull",
    "version": "4.0.0",
    "description": "ESGF Data transfer Program",
    "long_description": long_description,
    "license": "Public",
    "author": "",
    "author_email": "Sven Rodriguez <srodriguez@ipsl.fr>",
    "maintainer": "",
    "maintainer_email": "Sven Rodriguez <srodriguez@ipsl.fr>",
    "url": "",
    "packages": [
        "esgpull",
        "esgpull.cli",
    ],
    "package_data": {"": ["*"]},
    "long_description_content_type": "text/markdown",
    "install_requires": INSTALL_REQUIRES,
    "python_requires": ">=3.10",
    "entry_points": ENTRY_POINTS,
}


setup(**setup_kwargs)
