This document covers a few ways to install `esgpull`, a necessary first step into being able search and download ESGF datasets.

!!! warning "Python version `>= 3.10`"

    `esgpull` requires python `>= 3.10` to run properly.

    This requirement could be relaxed with future releases if requested.


## Using conda

`conda` is the recommended way to install `esgpull`.

 is to create a fresh environment using `python >= 3.10`, then install the package with both `conda-forge` and `ipsl` channels:

```shell title="Install esgpull on a fresh conda environment"
conda create -n esgpull python=3.10
conda activate esgpull
conda install esgpull -c conda-forge -c ipsl
```


## Using pip

Run this command:

```shell title="Install esgpull from pip"
pip install https://github.com/svenrdz/esg-pull.git
```

!!! tip "Initialize the database and working directories"

    The `self install` command sets up the directories and files required for `esgpull` to work correctly.

    It also fetches and stores the full vocabulary of *facets* from ESGF index nodes and store it locally.

    It will take a few minutes to complete but only needs to be run once.

    ```sh
    esgpull self install
    ```

## Get the source code

Esgpull is developed and maintained on GitHub, you can clone the public repository:

```shell
git clone https://github.com/svenrdz/esg-pull.git
```

The source can now be installed using `pip`:

```
cd esg-pull
python -m pip install .
```
