`esgpull` is a tool that simplifies usage of the [ESGF Search API](https://esgf.github.io/esg-search/ESGF_Search_RESTful_API.html) for data discovery, and manages procedures related to downloading and storing files from ESGF.

!!! Glossary

    __Dataset__
    : collection of _files_. Described by metadata that follows its project's conventions.

    __Facet__
    : basic element of a dataset's _metadata_. Pair of strings in the form `name:value`, equivalent to a python dictionary's item.


## Initialisation

Before anything, make sure `esgpull` is correctly [installed](installation).
Then you can run this next command to fill the database with all _facets_ that can be found in ESGF index nodes.

```sh title="Initialize facets"
esgpull facet init
```

## Data discovery

Look at the [data discovery](search) page for more information.


## Downloading

Loop at the [download](download) page for more information.

