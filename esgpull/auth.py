from __future__ import annotations

from enum import Enum, unique
from pathlib import Path
from shutil import rmtree
from typing import Any
from urllib.parse import (
    ParseResult,
    ParseResultBytes,
    urljoin,
    urlparse,
    urlunparse,
)
from xml.etree import ElementTree

import httpx
import tomlkit
from attrs import Factory, define, field
from myproxy.client import MyProxyClient
from OpenSSL import crypto

from esgpull.config import Config
from esgpull.constants import PROVIDERS


class Secret:
    def __init__(self, value: str | None = None) -> None:
        self._value = value

    def get_value(self) -> str | None:
        return self._value

    def __str__(self) -> str:
        if self.get_value() is None:
            return str(None)
        else:
            return "*" * 10

    def __repr__(self) -> str:
        return str(self)


@define
class Credentials:
    provider: str | None = None
    user: str | None = None
    password: Secret = field(default=None, converter=Secret)

    @staticmethod
    def from_config(config: Config) -> Credentials:
        path = config.paths.auth / config.credentials.filename
        return Credentials.from_path(path)

    @staticmethod
    def from_path(path: Path) -> Credentials:
        if path.is_file():
            with path.open() as fh:
                doc = tomlkit.load(fh)
            return Credentials(**doc)
        else:
            return Credentials()

    def write(self, path: Path) -> None:
        if path.is_file():
            raise FileExistsError(path)
        with path.open("w") as f:
            cred_dict = dict(
                provider=self.provider,
                user=self.user,
                password=self.password.get_value(),
            )
            tomlkit.dump(cred_dict, f)

    def parse_openid(self) -> ParseResult | ParseResultBytes | Any:
        if self.provider not in PROVIDERS:
            raise ValueError(f"unknown provider: {self.provider}")
        ns = {"x": "xri://$xrd*($v*2.0)"}
        provider = urlunparse(
            [
                "https",
                self.provider,
                urljoin(PROVIDERS[self.provider], self.user),
                "",
                "",
                "",
            ]
        )
        resp = httpx.get(str(provider))
        resp.raise_for_status()
        root = ElementTree.fromstring(resp.text)
        services = root.findall(".//x:Service", namespaces=ns)
        for service in services:
            t = service.find("x:Type", namespaces=ns)
            if t is None:
                continue
            elif t.text == "urn:esg:security:myproxy-service":
                url = service.find("x:URI", namespaces=ns)
                if url is not None:
                    return urlparse(url.text)
        raise ValueError("did not found host/port")


@unique
class AuthStatus(Enum):
    Valid = ("valid", "green")
    Expired = ("expired", "orange")
    Missing = ("missing", "red")


@define
class Auth:
    cert_dir: Path
    cert_file: Path
    credentials: Credentials = Factory(Credentials)
    __status: AuthStatus | None = field(init=False, default=None, repr=False)

    Valid = AuthStatus.Valid
    Expired = AuthStatus.Expired
    Missing = AuthStatus.Missing

    @staticmethod
    def from_config(
        config: Config, credentials: Credentials = Credentials()
    ) -> Auth:
        return Auth.from_path(config.paths.auth, credentials)

    @staticmethod
    def from_path(
        path: Path, credentials: Credentials = Credentials()
    ) -> Auth:
        cert_dir = path / "certificates"
        cert_file = path / "credentials.pem"
        return Auth(cert_dir, cert_file, credentials)

    @property
    def cert(self) -> str | None:
        if self.status == AuthStatus.Valid:
            return str(self.cert_file)
        else:
            return None

    @property
    def status(self) -> AuthStatus:
        if self.__status is None:
            self.__status = self._get_status()
        return self.__status

    def _get_status(self) -> AuthStatus:
        if not self.cert_file.exists():
            return AuthStatus.Missing
        with self.cert_file.open("rb") as f:
            content = f.read()
        filetype = crypto.FILETYPE_PEM
        pem = crypto.load_certificate(filetype, content)
        if pem.has_expired():
            return AuthStatus.Expired
        return AuthStatus.Valid

    # TODO: review this
    def renew(self) -> None:
        if self.cert_dir.is_dir():
            rmtree(self.cert_dir)
        self.cert_file.unlink(missing_ok=True)
        openid = self.credentials.parse_openid()
        client = MyProxyClient(
            hostname=openid.hostname,
            port=openid.port,
            caCertDir=str(self.cert_dir),
            proxyCertLifetime=12 * 60 * 60,
        )
        creds = client.logon(
            self.credentials.user,
            self.credentials.password.get_value(),
            bootstrap=True,
            updateTrustRoots=True,
            authnGetTrustRootsCall=False,
        )
        with self.cert_file.open("wb") as file:
            for cred in creds:
                file.write(cred)
        self.__status = None
