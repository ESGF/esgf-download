from setuptools import setup, find_packages
from esgpull import __version__

setup(
    name="esgpull",
    packages=find_packages(),
    package_data={"esgpull": ["py.typed"]},
    include_package_data=True,
    version=__version__,
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
        "sqlalchemy",
        "tqdm",
        "click",
        "click-params",
        "click-default-group",
    ],
    extras_require={
        "dev": ["pytest", "flake8", "sphinx", "mypy", "sqlalchemy[mypy]"]
    },
)
