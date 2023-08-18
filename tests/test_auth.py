import pytest
from httpx import HTTPStatusError

from esgpull.auth import Auth, Credentials

# from esgpull.config import Config, Paths


@pytest.fixture
def creds():
    return Credentials("esgf-node.ipsl.upmc.fr", "foo", "foobar")


def test_auth_from_path(creds, tmp_path):
    auth = Auth.from_path(tmp_path, creds)
    assert auth.status == auth.Missing
    with pytest.raises(HTTPStatusError):
        auth.renew()


# def test_auth_password_in_env(auth):
