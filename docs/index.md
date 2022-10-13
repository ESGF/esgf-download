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
        Found 1539146 datasets.
             ╷          ╷                                                                      
           # │     size │ id                                                                   
        ╶────┼──────────┼─────────────────────────────────────────────────────────────────────╴
           0 │  10.9 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.3hr.hfls.gr.v2018…  
           1 │   7.9 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.3hr.clt.gr.v20181…  
           2 │  11.1 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.cfadLidars…  
           3 │   1.6 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clmisr.gr.…  
           4 │ 170.5 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.cllcalipso…  
           5 │  86.5 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clhcalipso…  
           6 │  99.3 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clmcalipso…  
           7 │   1.9 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clcalipso.…  
           8 │ 142.7 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.cltcalipso…  
           9 │   1.1 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.clisccp.gr…  
          10 │ 683.2 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.jpdftaurel…  
          11 │ 452.1 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.jpdftaurei…  
          12 │   1.0 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.parasolRef…  
          13 │ 845.6 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.E3hrPt.hus.gr.v20…  
          14 │ 409.1 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.hfss.gn.v…  
          15 │ 398.2 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.hfls.gn.v…  
          16 │   4.2 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.mc.gn.v20…  
          17 │ 418.1 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.prc.gn.v2…  
          18 │ 379.3 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.huss.gn.v…  
          19 │ 364.8 MB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.amip.r1i1p1f2.CFsubhr.hurs.gn.v…  
             ╵          ╵
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
        Found 1801 datasets.
            ╷         ╷                                                                        
          # │    size │ id                                                                     
        ╶───┼─────────┼───────────────────────────────────────────────────────────────────────╴
          0 │ 22.8 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.historical.r2i1p1f2.Omon.bigtheta…  
          1 │ 61.6 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1.piControl.r1i1p1f2.Omon.bigthetao…  
          2 │ 13.7 GB │ CMIP6.CMIP.CNRM-CERFACS.CNRM-ESM2-1.piControl-spinup.r1i1p1f2.Omon.b…  
            ╵         ╵
        ```

!!! tip "Asynchronous downloads"
    
    Downloads are done concurrently (up to a maximum), maximizing retrieval speed.

!!! tip "SQLite database"

    Each download is recorded in a SQLite database

## Setup

Run `pip install esgpull` to install the latest release and its dependencies.

Have a look at the [Installation page](installation) for more ways to install.

## Quickstart

Jump directly to the [Quickstart guide](quickstart) to get to know how to use `esgpull`.

[^1]: ESGF: [https://pcmdi.llnl.gov/](https://pcmdi.llnl.gov/)
