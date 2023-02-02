from esgpull.config import Config


def test_from_path(root):
    config = Config.load(root)
    assert config.paths.auth.parent == root
    assert config.paths.data.parent == root
    assert config.paths.db.parent == root
    assert config.paths.tmp.parent == root
