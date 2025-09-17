import pytest
import tomlkit

from esgpull.config import Config, ConfigKind
from esgpull.exceptions import BadConfigError


def test_from_path(root):
    config = Config.load(root)
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


def test_generate_creates_serializable_config(root, config_path):
    root.mkdir(parents=True, exist_ok=True)
    config = Config.load(root)

    assert config.kind == ConfigKind.NoFile

    config.generate()

    assert config.kind == ConfigKind.Complete
    assert config_path.is_file()

    with config_path.open("rb") as f:
        doc = tomlkit.load(f)

    assert all(isinstance(value, str) for value in config.raw["paths"].values())
    for key, value in doc["paths"].items():
        assert str(value) == config.raw["paths"][key]


def test_dump_and_set_default_from_partial_config(root, config_path):
    root.mkdir(parents=True, exist_ok=True)
    with config_path.open("w") as f:
        tomlkit.dump(
            {
                "download": {"disable_ssl": True},
                "api": {"max_concurrent": 2},
            },
            f,
        )

    config = Config.load(root)

    assert config.kind == ConfigKind.Partial

    user_values = config.dump(with_defaults=False)
    assert user_values["download"]["disable_ssl"] is True
    assert user_values["api"]["max_concurrent"] == 2
    assert "paths" not in user_values

    old_value = config.set_default("download.disable_ssl")

    assert old_value is True
    assert config.download.disable_ssl is False
    assert "download" not in config.raw
    assert config.dump(with_defaults=False)["api"]["max_concurrent"] == 2
