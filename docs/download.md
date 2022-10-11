With `esgpull`, downloading files is done as a two-step process:

* `install` to fetch the files' metadata and add them to the queue, and discard those that have already been queued or downloaded previously.
* `download start` to fire a *blocking* shell process that downloads all queued files asynchronously.

## Install

Using a `search` command with either _facet_ or _free-text_ terms, through command line arguments or selection files, the list of files to download can (should?) be reduced to a minimal set of required files, avoiding unnecessary use of network as much as possible.

Then downloading those files is trivial, since the `install` syntax is exactly the same as for a `search` command.

```sh title="Search command to find what you want to download"
esgpull search mip_era:CMIP6 experiment_id:historical table_id:fx member_id:r1i1p1f1
```
```{.markdown .result}
Found 9 datasets.
┏━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ # ┃   size ┃ id                                                                            ┃
┡━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ 0 │  44.3K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.sftlf.gr.v20180803        │
│ 1 │  35.7K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.mrsofc.gr.v20180803       │
│ 2 │  29.6K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.areacella.gr.v20180803    │
│ 3 │  33.0K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.rootd.gr.v20180803        │
│ 4 │ 111.1K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.fx.orog.gr.v20190516         │
│ 5 │  27.3K │ CMIP6.CMIP.IPSL.IPSL-CM5A2-INCA.historical.r1i1p1f1.fx.areacella.gr.v20200729 │
│ 6 │   1.6M │ CMIP6.CMIP.IPSL.IPSL-CM5A2-INCA.historical.r1i1p1f1.fx.zfull.gr.v20200729     │
│ 7 │  34.9K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR-INCA.historical.r1i1p1f1.fx.mrsofc.gr.v20210216  │
│ 8 │  31.8K │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR-INCA.historical.r1i1p1f1.fx.rootd.gr.v20210216   │
└───┴────────┴───────────────────────────────────────────────────────────────────────────────┘
```

!!! note "Install command with the same arguments"
        ```sh
        esgpull install mip_era:CMIP6 experiment_id:historical table_id:fx member_id:r1i1p1f1
        ```
        ```{.markdown .result}
        Found 9 files.
        Total size: 2.0M
        Continue? [Y/n]: Y
        Installed 9 new files.
        ```

!!! warning "Search options"

        Some options that work for the `search` command do not exist for the `install` command.

        That includes the `--file` to display files instead of datasets, since the `install` command assumes only **files** can be downloaded.

        ```sh title="Failing install command"
        esgpull install mip_era:CMIP6 experiment_id:historical table_id:fx --file
        ```
        ```{.markdown .result}
        Error: No such option: --file
        ```

## Download process

After *installing* any number of files, downloading is simple:

```sh
esgpull download start
```
```{.markdown .result}
100%|██████████████████████████████████████████████████████████████████████████████| 1.96M/1.96M [00:00<00:00, 3.31MiB/s]
Downloaded 9 new files for a total size of 2.0M
```
