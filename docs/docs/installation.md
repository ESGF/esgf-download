This document covers a few ways to install `esgpull`, a necessary first step into being able to search and download ESGF datasets.

## Python version

`esgpull` only supports python 3.10 and newer.

!!! note "Supporting lower python versions could be done with future releases, do not hesitate to ask for it."


## Installation

`esgpull` is distributed via PyPI:

```shell
pip install esgpull
esgpull --help
```

For isolated installation, [`uv`](https://github.com/astral-sh/uv) or
[`pipx`](https://github.com/pypa/pipx) are recommended:

```shell
# with uv
uv tool install esgpull
esgpull --help

# alternatively, uvx enables running without explicit installation (comes with uv)
uvx esgpull --help
```

```shell
# with pipx
pipx install esgpull
esgpull --help
```


## Setup

Installing `esgpull` is the first step to using it, but not the only one.

??? warning "Once you have installed the `esgpull` package, make sure to read this section."

    Only a few `esgpull` commands work out of the box, namely `search` and `convert`.

    To use the full set of functionalities, you will need to setup a local install with:

    ```sh
    esgpull self install
    ```

    ## Why do I need to install twice ?

    The reason is that `esgpull` is prevented from writing anything on disk until installed.

    Installing `esgpull` equates choosing a directory in which it is allowed to write anything it needs to run properly. It also creates all the required files/directories in that directory and fetches some metadata from ESGF that is required to run properly.

    ## Multiple installs

    It is possible to have multiple installs, which allows using multiple configurations on the same machine.

    Installing in a directory which is an existing `esgpull` install is possible and intended. It allows sharing a single configuration across multiple users.

    ## Deleting an install

    To delete an `esgpull` install, it is required that this install is active.

    Deleting does not `rm` any file, it only removes the option to use this install.

    ```sh
    esgpull self choose <path/to/install>
    esgpull self delete
    ```

## Configuration

`esgpull` is highly configurable, and it is recommended to take a look at the [configuration](configuration.md) page to learn more about it.
