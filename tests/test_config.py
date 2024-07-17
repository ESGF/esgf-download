import pytest
import tomlkit

from esgpull.config import Config
from esgpull.exceptions import BadConfigError


def test_from_path(root):
    config = Config.load(root)
    assert config.paths.auth.parent == root
    assert config.paths.data.parent == root
    assert config.paths.db.parent == root
    assert config.paths.tmp.parent == root


def test_deprecated_search_api(root, config_path):
    root.mkdir(parents=True)
    with open(config_path, "w") as f:
        tomlkit.dump({"search": {"http_timeout": 1234}}, f)
    try:
        config = Config.load(root)
        assert config.api.http_timeout == 1234
    except Exception:
        raise
    finally:
        config_path.unlink()


def test_deprecated_search_api_error(root, config_path):
    root.mkdir(parents=True)
    with open(config_path, "w") as f:
        tomlkit.dump(
            {"search": {"http_timeout": 1234}, "api": {"max_concurrent": 1}}, f
        )
    try:
        with pytest.raises(BadConfigError):
            Config.load(root)
    except Exception:
        raise
    finally:
        config_path.unlink()


def test_update_config(root, config_path):
    root.mkdir(parents=True)
    with open(config_path, "w") as f:
        tomlkit.dump({}, f)
    config = Config.load(root)
    config.download.disable_ssl = True
    config.update_item("download.disable_ssl", "false")
    assert config.download.disable_ssl is False
    config.update_item("download.disable_ssl", "true")
    assert config.download.disable_ssl is True
    config.update_item("download.disable_ssl", False)
    assert config.download.disable_ssl is False
    with pytest.raises(ValueError):
        config.update_item("download.disable_ssl", "bad_value")
