# esgpull - ESGF data management utility

[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/rye/main/artwork/badge.json)](https://rye.astral.sh)

`esgpull` is a tool that simplifies usage of the [ESGF Search API](https://esgf.github.io/esg-search/ESGF_Search_RESTful_API.html) for data discovery, and manages procedures related to downloading and storing files from ESGF.

```py
from esgpull import Esgpull, Query

query = Query()
query.selection.project = "CMIP6"
query.options.distrib = True  # default=False
esg = Esgpull()
nb_datasets = esg.context.hits(query, file=False)[0]
nb_files = esg.context.hits(query, file=True)[0]
datasets = esg.context.datasets(query, max_hits=5)
print(f"Number of CMIP6 datasets: {nb_datasets}")
print(f"Number of CMIP6 files: {nb_files}")
for dataset in datasets:
    print(dataset)
```

## Features

- Command-line interface
- HTTP download (async multi-file)

## Installation

Install `esgpull` using pip or conda:

```shell
pip install esgpull
```

```shell
conda install -c conda-forge ipsl::esgpull
```

## Usage

```console
Usage: esgpull [OPTIONS] COMMAND [ARGS]...

  esgpull is a management utility for files and datasets from ESGF.

Options:
  -V, --version  Show the version and exit.
  -h, --help     Show this message and exit.

Commands:
  add       Add queries to the database
  config    View/modify config
  convert   Convert synda selection files to esgpull queries
  download  Asynchronously download files linked to queries
  login     OpenID authentication and certificates renewal
  remove    Remove queries from the database
  retry     Re-queue failed and cancelled downloads
  search    Search datasets and files on ESGF
  self      Manage esgpull installations / import synda database
  show      View query tree
  status    View file queue status
  track     Track queries
  untrack   Untrack queries
  update    Fetch files, link files <-> queries, send files to download...
```

## Useful links
* [ESGF Webinar: An Introduction to esgpull, A Replacement for Synda](https://www.youtube.com/watch?v=xv2RVMd1iCA)


## Contributions

You can use the common github workflow (through pull requests and issues) to contribute.
