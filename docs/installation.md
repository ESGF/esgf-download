This document covers a few ways to install `esgpull`, a necessary first step into being able search and download ESGF datasets.

## pip + github

Run this command:

```shell title="Install esgpull from pip"
pip install git+ssh://git@github.com/svenrdz/esg-pull
```


!!! warning "Initialize the database and working directories"

    The `init` command sets up the directories and files required for `esgpull` to work correctly.

    It also fetches and stores the full vocabulary of *facets* from ESGF index nodes and store it locally.

    It will take a few minutes to complete but only needs to be run once.

    ```sh
    esgpull init
    ```

## Get the source code

Esgpull is developed and maintained on GitHub, you can clone the public repository:

```shell
git clone git@github.com:svenrdz/esg-pull
```

The source can now be installed using `pip`:

```
cd esg-pull
python -m pip install .
```

### Makefile

You can otherwise install the source using the targets defined in the `Makefile`:

- `make install` for a regular installation,
- `make develop` to install additional dependencies required for contribution
