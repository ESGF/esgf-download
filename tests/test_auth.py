import pytest

from httpx import HTTPStatusError
from esgpull.auth import Auth, Identity


@pytest.fixture
def auth(tmp_path):
    return Auth(tmp_path)


def test_auth(auth):
    assert auth.status == auth.Missing
    identity = Identity("esgf-node.ipsl.upmc.fr", "foo", "foobar")
    with pytest.raises(HTTPStatusError):
        auth.renew(identity)


# def test_auth_password_in_env(auth):
