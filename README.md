[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm.fming.dev)

# esgpull - ESGF data management utility

`esgpull` is a tool that simplifies usage of the [ESGF Search API](https://esgf.github.io/esg-search/ESGF_Search_RESTful_API.html) for data discovery, and manages procedures related to downloading and storing files from ESGF.

```py
import esgpull

ctx = esgpull.Context()
ctx.query.project = "CMIP6"
print(ctx.query)
print("Number of CMIP6 datasets:", c.hits)
```

## Features

- Command-line interface
- HTTP download (async multi-file)

## Usage

```console
$ esgpull --help
Usage: esgpull [OPTIONS] COMMAND [ARGS]...

  esgpull is a management utility for files and datasets from ESGF.

Options:
  -V, --version  Show the version and exit.
  -h, --help     Show this message and exit.

Commands:
  autoremove
  config
  download
  facet
  init
  install
  login
  remove
  retry
  search      Search datasets/files on ESGF
  status
  upgrade
```
