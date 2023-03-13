[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm.fming.dev)

# esgpull - ESGF data management utility

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

## Usage

```console
Usage: esgpull [OPTIONS] COMMAND [ARGS]...

  esgpull is a management utility for files and datasets from ESGF.

Options:
  -V, --version  Show the version and exit.
  -h, --help     Show this message and exit.

Commands:
  add       Add one or more queries to the database.
  config
  convert   Convert synda selection files to esgpull queries.
  download
  facet
  login
  remove    Remove queries
  retry
  search    Search datasets and files on ESGF
  self
  show      Show recorded query data
  status
  track     Remove queries
  untrack   Remove queries
  update
```
