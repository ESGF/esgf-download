This document covers a few ways to install `esgpull`, a necessary first step into being able search and download ESGF datasets.

## pip + github

Run this command:

```shell
pip install git+ssh://git@github.com/svenrdz/esg-pull
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
