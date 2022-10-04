With `esgpull`, downloading files is done as a two-step process:

* `install` to fetch the files' metadata and add them to the queue, and discard those that have already been queued or downloaded previously.
* `download start` to fire a *blocking* shell process that downloads all queued files asynchronously.

## Install

Using a `search` command with either _facet_ or _free-text_ terms, through command line arguments or selection files, the list of files to download can (should?) be reduced to a minimal set of required files, avoiding unnecessary use of network as much as possible.

Then downloading those files is trivial, since the `install` syntax is exactly the same as for a `search` command.

```sh title="Search command to find what you want to download"
esgpull search mip_era:CMIP6 experiment_id:historical table_id:fx --file
```
```{.sh .markdown .result}
Found 9 files.
┏━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ # ┃   size ┃ id                                                                                         ┃
┡━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ 0 │  27.3K │ CMIP6.CMIP.IPSL.IPSL-CM5A2-INCA.historical.r1i1p1f1.fx.areacella.gr.v20200729.areacella_f… │
│ 1 │   1.6M │ CMIP6.CMIP.IPSL.IPSL-CM5A2-INCA.historical.r1i1p1f1.fx.zfull.gr.v20200729.zfull_fx_IPSL-C… │
│ 2 │  34.9K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR-INCA.historical.r1i1p1f1.fx.mrsofc.gr.v20210216.mrsofc_fx_IP… │
│ 3 │  31.8K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR-INCA.historical.r1i1p1f1.fx.rootd.gr.v20210216.rootd_fx_IPSL… │
│ 4 │  29.6K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.areacella.gr.v20180803.areacella_fx_I… │
│ 5 │  35.7K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.mrsofc.gr.v20180803.mrsofc_fx_IPSL-CM… │
│ 6 │ 111.1K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.orog.gr.v20190516.orog_fx_IPSL-CM6A-L… │
│ 7 │  33.0K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.rootd.gr.v20180803.rootd_fx_IPSL-CM6A… │
│ 8 │  44.3K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.sftlf.gr.v20180803.sftlf_fx_IPSL-CM6A… │
└───┴────────┴────────────────────────────────────────────────────────────────────────────────────────────┘
```

!!! note "Install command with the same arguments"
        ```sh
        esgpull install mip_era:CMIP6 experiment_id:historical table_id:fx
        ```
        ```{.sh .markdown .result}
        Found 9 files.
        Total size: 2.0M
        Continue? [Y/n]: Y
        Installed 7 new files.
        ```

        Only 7 out of 9 files were installed in this example. That means 2 files were either downloaded or queued already.

!!! warning "Search options"

        Some options that work for the `search` command do not exist for the `install` command.

        That includes the `--file` to display files instead of datasets, since the `install` command assumes only **files** can be downloaded.

        ```sh title="Failing install command"
        esgpull install mip_era:CMIP6 experiment_id:historical table_id:fx -- file
        ```
        ```{.sh .markdown .result}
        Usage: esgpull install [OPTIONS] [FACETS]...
        Try 'esgpull install -h' for help.

        Error: No such option: --file
        ```

## Download process
