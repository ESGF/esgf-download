# esgpull

`esgpull` is a management utility for files and datasets from ESGF[^1].

## Features

!!! tip "Search datasets"
    
    `esgpull` includes many ways of searching for data, with **facet** and **free-text** terms together with **options**.

    === "Facet terms"

        Specify exact facet terms with `<name>:<value>` syntax:

        ```sh title="All CMIP6 datasets"
        esgpull search project:CMIP6
        ```
        ```{.markdown .result}
        Found 1493626 datasets.
        ┏━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃  # ┃   size ┃ id                                                                                     ┃
        ┡━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │  0 │  10.2G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.3hr.hfls.gr.v20181203                 │
        │  1 │   7.3G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.3hr.clt.gr.v20181203                  │
        │  2 │  10.3G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.cfadLidarsr532.gr.v20181203    │
        │  3 │   1.5G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clmisr.gr.v20181203            │
        │  4 │ 162.6M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.cllcalipso.gr.v20181203        │
        │  5 │  82.5M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clhcalipso.gr.v20181203        │
        │  6 │  94.7M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clmcalipso.gr.v20181203        │
        │  7 │   1.8G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clcalipso.gr.v20181203         │
        │  8 │ 136.0M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.cltcalipso.gr.v20181203        │
        │  9 │   1.0G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clisccp.gr.v20181203           │
        │ 10 │ 651.6M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.jpdftaureliqmodis.gr.v20181203 │
        │ 11 │ 431.2M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.jpdftaureicemodis.gr.v20181203 │
        │ 12 │ 981.8M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.parasolRefl.gr.v20181203       │
        │ 13 │ 787.5G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.hus.gr.v20181203               │
        │ 14 │ 390.1M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.hfss.gn.v20181203             │
        │ 15 │ 379.8M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.hfls.gn.v20181203             │
        │ 16 │   3.9G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.mc.gn.v20181203               │
        │ 17 │ 398.8M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.prc.gn.v20181203              │
        │ 18 │ 361.7M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.huss.gn.v20181203             │
        │ 19 │ 347.9M │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.hurs.gn.v20181203             │
        └────┴────────┴────────────────────────────────────────────────────────────────────────────────────────┘
        ```

    === "Options"

        Discover possible facet values with `--options <facet name>`:

        ```sh title="All variables for CMIP6 datasets"
        esgpull search project:CMIP6 --options variable_id
        ```
        ```{.sh .markdown .result}
        [
            {
                'variable_id': {
                    'abs550aer': 3398,
                    'agesno': 3084,
                    ...
                    'zsatcalc': 106,
                    'ztp': 4825
                }
            }
        ]
        ```

    === "Free-text search"

        Narrow down the search (or options search) with free-text terms:

        ```sh title="CMIP6 variables for which both 'ocean' and 'temperature' appear in the metadata"
        esgpull search project:CMIP6 --options variable_id "ocean AND temperature"
        ```
        ```{.sh .markdown .result}
        [
            {
                'variable_id': {
                    'bigthetao': 1668,
                    'bigthetaoga': 1692,
                    ...
                    'tosga': 434,
                    'tossq': 388
                }
            }
        ]
        ```

    === "Complete search"

        Show the first 3 datasets from CMIP6 with variable `bigthetao`:

        ```sh title="First 3 CMIP6 datasets with variable 'bigthetao'"
        esgpull search project:CMIP6 variable_id:bigthetao --slice 0:3
        ```
        ```{.markdown .result}
        Found 1668 datasets.
        ┏━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ # ┃  size ┃ id                                                                                        ┃
        ┡━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │ 0 │ 21.3G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.historical.r2i1p1f2.Omon.bigthetao.gn.v20181126        │
        │ 1 │ 57.4G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.piControl.r1i1p1f2.Omon.bigthetao.gn.v20180814         │
        │ 2 │ 12.8G │ CMIP6.CMIP.CNRM-CERFACS.CNRM-ESM2-1.piControl-spinup.r1i1p1f2.Omon.bigthetao.gn.v20180914 │
        └───┴───────┴───────────────────────────────────────────────────────────────────────────────────────────┘
        ```

!!! tip "Asynchronous downloads"
    
    Downloads are done concurrently (up to a maximum), maximizing retrieval speed.

!!! tip "SQLite database"

    Each download is recorded in a SQLite database

<!-- ??? warning "Search datasets" -->

<!--     === "CLI" -->
<!--         ```shell -->
<!--         esgpull search {macro}:GIEC+TEMPERATURE -->

<!--         TADAAA -->
<!--         ``` -->

<!--     === "Python" -->
<!--         ```py -->
<!--         from esgpull.context import Context, SCENARIO_GIEC, TEMPERATURE -->

<!--         c = SCENARIO_GIEC + TEMPERATURE -->
<!--         res = c.search(file=True) -->

<!--         assert c.__class__ == Context -->
<!--         print(help(Context.__add__)) -->
<!--         ``` -->

<!--     > rich.print(TABLEAU DE RESULTAT) -->

<!-- ??? warning "Client/Server model" -->

<!--     Run server once (deploy?), CLI/web-page using RPC to run actions (search/download) -->

## Setup

Run `pip install esgpull` to install the latest release and its dependencies.

Have a look at the [Installation page](/installation) for more ways to install.

## Quickstart

Jump directly to the [Quickstart guide](/quickstart) to get to know how to use `esgpull`.

[^1]: ESGF: [https://pcmdi.llnl.gov/](https://pcmdi.llnl.gov/)
