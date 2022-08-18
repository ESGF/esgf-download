import re
from pathlib import Path

from setuptools import setup, find_packages


def get_version():
    version = Path("esgpull", "version.py").read_text()
    return version.split('"')[1]


setup(
    name="esgpull",
    packages=find_packages(),
    package_data={"esgpull": ["py.typed"]},
    include_package_data=True,
    version=get_version(),
    entry_points={"console_scripts": ["esgpull=esgpull.cli:main"]},
    # url='https://github.com/Prodiguer/synda',
    description="ESGF Data transfer Program",
    long_description="This program download files from the Earth System Grid "
    "Federation (ESGF) archive using command line.",
    # zip_safe=False,
    license="Public",
    platforms="Linux",
    maintainer="Sven Rodriguez",
    maintainer_email="srodriguez@ipsl.fr",
    install_requires=[
        "rich",
        "humanize",
        "pyyaml",
        "tqdm",
        "httpx",
        "aiofiles",
        "click",
        "click-params",
        "click-default-group",
        "sqlalchemy",
        "alembic",
        "pandas",  # maybe useless
        "nest_asyncio",  # allow usage in notebooks
    ],
    extras_require={
        "dev": [
            "pytest >= 6.2.4",
            "flake8",
            "sphinx",
            "mypy",
            "sqlalchemy[mypy]",
        ]
    },
)
