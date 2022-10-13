With `esgpull`, downloading files is done as a two-step process:

* `install` to fetch the files' metadata and add them to the queue, and discard those that have already been queued or downloaded previously.
* `download start` to fire a *blocking* shell process that downloads all queued files asynchronously.

!!! tip "Example selection file"
        
        To ease on the readability, each command throughout this page uses the following selection files.

        They can be found in the `examples/selection_files` directory.

        ```yaml title="download_example_small.yaml"
        mip_era: CMIP6
        experiment_id: piControl
        table_id: Ofx
        member_id: r1i1p1f1
        ```

        ```yaml title="download_example_large.yaml"
        experiment_id: historical
        member_id: r1i1p1f1
        mip_era: CMIP6
        table_id: Eday
        variable_id: ts
        ```

## Install

Once a `search` command yields satisfying results — ideally the smallest set of files that meets needs — then downloading is trivial using an `install` command, since the `install` syntax is exactly the same as for a `search` command.

```sh title="Example search command"
esgpull search --selection-file download_example_small.yaml
```
```{.markdown .result}
Found 13 datasets.
     ╷          ╷                                                           
   # │     size │ id                                                        
╶────┼──────────┼──────────────────────────────────────────────────────────╴
   0 │   1.5 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.bas…  
   1 │   1.5 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.bas…  
   2 │   1.7 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.hfg…  
   3 │   1.7 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.hfg…  
   4 │   1.7 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.hfg…  
   5 │   1.5 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.are…  
   6 │   1.5 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.are…  
   7 │   1.5 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.are…  
   8 │   1.5 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.bas…  
   9 │   1.6 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.are…  
  10 │   1.5 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.bas…  
  11 │   1.7 MB │ CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.Ofx.hfg…  
  12 │ 329.9 kB │ CMIP6.CMIP.IPSL.IPSL-CM5A2-INCA.piControl.r1i1p1f1.Ofx.…  
     ╵          ╵ 
```

```sh title="Replace search by install"
esgpull install --selection-file download_example_small.yaml
```
```{.markdown .result}
Found 16 files.
Dropped 3 duplicates.
Total size: 19.0 MB
Continue? [Y/n]: 
Installed 13 new files.
```

!!! warning "Search options"

        Some `search` options do not work with the `install` command.

        That includes the `--file` to display files instead of datasets, since `install` assumes only **files** can be downloaded.

        ```sh title="Failing install command"
        esgpull install --selection-file download_example_small.yaml --file
        ```
        ```{.markdown .result}
        Error: No such option: --file
        ```

## Download process

After *installing* any number of files, downloading is simple:

```sh
# Using the following example install command:
esgpull install --selection-file download_example_small.yaml

# Download files with:
esgpull download
```
```{.markdown .result}
[15:18:37] ✓ id:1767 · 329.9 kB · 5.4 MB/s                    esgpull.py:202
           ✓ id:1768 · 1.5 MB · 11.4 MB/s                     esgpull.py:202
           ✓ id:1769 · 1.5 MB · 9.3 MB/s                      esgpull.py:202
[15:18:38] ✓ id:1770 · 1.5 MB · 8.1 MB/s                      esgpull.py:202
           ✓ id:1771 · 1.6 MB · 6.6 MB/s                      esgpull.py:202
           ✓ id:1772 · 1.5 MB · 7.1 MB/s                      esgpull.py:202
           ✓ id:1773 · 1.5 MB · 7.2 MB/s                      esgpull.py:202
           ✓ id:1774 · 1.5 MB · 7.0 MB/s                      esgpull.py:202
           ✓ id:1775 · 1.5 MB · 6.9 MB/s                      esgpull.py:202
           ✓ id:1776 · 1.7 MB · 7.8 MB/s                      esgpull.py:202
           ✓ id:1777 · 1.7 MB · 8.1 MB/s                      esgpull.py:202
           ✓ id:1778 · 1.7 MB · 9.9 MB/s                      esgpull.py:202
           ✓ id:1779 · 1.7 MB · 11.7 MB/s                     esgpull.py:202
  13/13 00:00
Downloaded 13 new files for a total size of 19.0 MB
```

### Configuration

RAM usage for downloads is bounded by the following formula:

```
RAM = Settings::download.max_concurrent * Settings::download.chunk_size
```

### Failed downloads

For each failed download, their status will be set to **error**.

Those can be put back to the download queue, by using the `retry` command.

```sh
esgpull retry --help
```
```{.sh .markdown .result}
Usage: esgpull retry [OPTIONS] [[new|queued|starting|started|pausing|paused|
                     error|cancelled|done]]...
```

!!! tip "Cancelled download"

    If `esgpull` has been stopped with with ++ctrl+c++ while downloading, all incomplete downloads will have the `cancelled` status.

    By default, `retry` will put both **error** and **cancelled** downloads back to the queue.

!!! tip "Unexpected errors"

    Some unexpected errors might break `esgpull`. In this case, the downloads will stay in a transient status **starting**.

    The `retry` command will not send those to the queue by default. It can still be done using either:

    * `esgpull retry starting` to send only those back to the queue
    * `esgpull retry --all` to send every download back to the queue (except `done` downloads of course)
