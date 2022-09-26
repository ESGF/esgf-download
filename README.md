# esgpull - ESGF data management utility

`esgpull` is a tool that simplifies usage of the [ESGF Search API](https://esgf.github.io/esg-search/ESGF_Search_RESTful_API.html) for data discovery, and manages procedures related to downloading and storing files from ESGF.

```py
import esgpull

c = esgpull.Context()
c.query.project = "CMIP6"
print(c.query)
print("Number of CMIP6 datasets:", c.hits)
```

## Features

- Command-line interface
- HTTP download (async multi-file)

## Usage

```sh
$ esgpull --help
Usage: esgpull [OPTIONS] COMMAND [ARGS]...

  esgpull is a management utility for files and datasets from ESGF.

Options:
  -v, --version  Show the version and exit.
  -h, --help     Show this message and exit.

Commands:
  autoremove
  download
  get
  install
  login
  param
  remove
  retry
  search      Search datasets/files on ESGF
  settings
  upgrade
```


### CLI

```mermaid
classDiagram
  class esgpull {
    -h, --help
    -b, --bootstrap
  }

  class autoremove {
    -h, --help
    -y, --yes
  }
  esgpull --|> autoremove: Prompt [Y/n]
  
  class download {
    -h, --help
    start()
    stop()
    status()
    queue()
    watch()
  }
  esgpull --|> download

  class get {
    -h, --help
  }
  esgpull --|> get: Prompt [Y/n]

  class install {
    -h, --help
  }
  esgpull --|> install: Prompt [Y/n]

  class login {
    -h, --help
  }
  esgpull --|> login

  class param {
    -h, --help
    init()
    update()
    list()
    facet()
  }
  esgpull --|> param

  class remove {
    -h, --help
  }
  esgpull --|> remove: Prompt [Y/n]

  class retry {
    -h, --help
  }
  esgpull --|> retry: Prompt [Y/n]

  class search {
    -h, --help
  }
  esgpull --|> settings

  class settings {
    -h, --help
    show()
    get()
    set()
  }
  esgpull --|> settings

  class upgrade {
    -h, --help
  }
  esgpull --|> upgrade: Prompt [Y/n]
```
