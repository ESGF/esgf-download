from esgpull.config import Config


def test_from_path(tmp_path):
    config = Config.load(tmp_path)
    assert config.paths.root == tmp_path
    assert config.paths.auth.parent == tmp_path
    assert config.paths.data.parent == tmp_path
    assert config.paths.db.parent == tmp_path
    assert config.paths.tmp.parent == tmp_path
