# esgpull - ESGF data management utility

## Introduction

esgpull is a minimal command line utility for local management of ESGF datasets / files.

It helps with discovery, download and version control of datasets uploaded on ESGF nodes.

```python
from esgpull import Storage, StorageMode

## Open a `:memory:` sqlite database
store = Storage(mode=StorageMode.Sqlite, path="path/to/db")
print(store.semver)
# 4.0.0
```

## Features

- Command-line interface
- HTTP download (async multi-file)

## Usage

```sh
~/ipsl/esg-pull master ×
› esgpull --help
Usage: esgpull [OPTIONS] COMMAND [ARGS]...

Options:
  -h, --help  Show this message and exit.

Commands:
  autoremove
  download
  get
  install
  login
  param
  remove
  retry
  upgrade
```


### CLI

[Documentation](https://click.palletsprojects.com/en/8.1.x/#documentation)

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
    show()
    update()
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

  class upgrade {
    -h, --help
  }
  esgpull --|> upgrade: Prompt [Y/n]
```
