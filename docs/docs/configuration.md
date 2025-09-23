This page details some of the options available for configuring your `esgpull` installation.

## Configuration

On invocation, `$ esgpull config` will show the base configurations in the terminal.

```shell
$ esgpull config

──────────────────────────────────────────────────────────────────────────────────────────────────────────────────── /home/me/.esgpull/config.toml ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
[paths]
data = "/home/me/.esgpull/data"
db = "/home/me/.esgpull/db"
log = "/home/me/.esgpull/log"
tmp = "/home/me/.esgpull/tmp"

[credentials]
filename = "credentials.toml"

[cli]
page_size = 20

[db]
filename = "esgpull.db"

[download]
chunk_size = 67108864
http_timeout = 20
max_concurrent = 5
disable_ssl = false

[api]
index_node = "esgf-node.ipsl.upmc.fr"
http_timeout = 20
max_concurrent = 5
page_limit = 50

[api.default_options]
distrib = "false"
latest = "true"
replica = "none"
retracted = "false"

[plugins]
enabled = "false"
```

To modify a config item from the command line, the dot-separated path to that item must
be provided as the first argument along with the new value that item should get as the second argument:

```shell
$ esgpull config api.index_node esgf-data.dkrz.de
```
```shell
[api]
index_node = "esgf-data.dkrz.de"

Previous value: esgf-node.ipsl.upmc.fr
```

On first call, this will generate a ``config.toml`` file in the ``~/.esgpull``
directory with only the modified values:

```shell
$ esgpull config api.index_node esgf-data.dkrz.de
```
```shell
[api]
index_node = "esgf-data.dkrz.de"

👍 New config file created at /home/srodriguez/.esgpull_test_config_generate/config.toml.
```

If a user wishes to simply generate the ``config.toml`` file without modifying any values, they
simply must run the following:

```shell
$ esgpull config --generate
```
```shell
👍 Config generated at /home/me/.esgpull/config.toml
```

!!! note "Complete existing config with defaults"

    The `--generate` flag also works when a configuration file already exists.

    In this case, a prompt will ask for permission to fill the existing file
    with default values for all missing options:

    ```shell
    $ esgpull config --generate
    ```
    ```shell
    A config file already exists, fill it with missing defaults? [y/n] (n): y
    👍 Config generated at /home/me/.esgpull/config.toml
    ```


## Login (deprecated)

Login is not longer required and has been removed from `esgpull`.
Existing `auth` folder in esgpull installation folder can be removed safely.

## Plugins

Plugin functionality is disabled by default and can be enabled with:

```shell
$ esgpull config plugins.enabled true
```

Plugin-specific configurations are managed separately through the `esgpull plugins config` command and stored in a dedicated `plugins.toml` file. See the [Plugins page](plugins) for more information.
