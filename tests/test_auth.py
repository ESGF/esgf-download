import pytest
from httpx import HTTPStatusError

from esgpull.auth import Auth, Credentials


@pytest.fixture
def auth(tmp_path):
    return Auth(tmp_path)


def test_auth(auth):
    assert auth.status == auth.Missing
    creds = Credentials("esgf-node.ipsl.upmc.fr", "foo", "foobar")
    with pytest.raises(HTTPStatusError):
        auth.renew(creds)


# def test_auth_password_in_env(auth):
