from esgpull.settings import Settings


def test_from_path(tmp_path):
    settings = Settings.from_path(tmp_path)
    assert settings.core.paths.root == tmp_path
    assert settings.core.paths.auth.parent == tmp_path
    assert settings.core.paths.data.parent == tmp_path
    assert settings.core.paths.db.parent == tmp_path
    assert settings.core.paths.settings.parent == tmp_path
    assert settings.core.paths.tmp.parent == tmp_path
