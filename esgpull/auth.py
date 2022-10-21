from dataclasses import dataclass, field
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
from attrs import define
from myproxy.client import MyProxyClient
from OpenSSL import crypto

from esgpull.constants import PROVIDERS
from esgpull.settings import Settings


class Secret:
    def __new__(cls, value: str | None):
        if value is None:
            return None
        else:
            return super().__new__(cls)

    def __init__(self, value: str) -> None:
        self._value = value

    def get_value(self) -> str:
        return self._value

    def __str__(self) -> str:
        return "*" * 10 if self.get_value() else ""


@define
class Credentials:
    provider: str | None = None
    user: str | None = None
    password: Secret | None = None

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
class AuthStatus(str, Enum):
    Valid = "Valid"
    Expired = "Expired"
    Missing = "Missing"


@dataclass
class Auth:
    settings: Settings
    credentials: Credentials

    cert_dir: Path = field(init=False)
    cert_file: Path = field(init=False)
    __status: AuthStatus | None = field(init=False, default=None)

    Valid = AuthStatus.Valid
    Expired = AuthStatus.Expired
    Missing = AuthStatus.Missing

    def __post_init__(self) -> None:
        self.path = self.settings.core.paths.auth
        self.cert_dir = self.path / "certificates"
        self.cert_file = self.path / "credentials.pem"

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
    def renew(self, creds: Credentials) -> None:
        if self.cert_dir.is_dir():
            rmtree(self.cert_dir)
        self.cert_file.unlink(missing_ok=True)
        openid = creds.parse_openid()
        client = MyProxyClient(
            hostname=openid.hostname,
            port=openid.port,
            caCertDir=str(self.cert_dir),
            proxyCertLifetime=12 * 60 * 60,
        )
        if creds.password is None:
            password = None
        else:
            password = creds.password.get_value()
        credentials = client.logon(
            creds.user,
            password,
            bootstrap=True,
            updateTrustRoots=True,
            authnGetTrustRootsCall=False,
        )
        with self.cert_file.open("wb") as file:
            for cred in credentials:
                file.write(cred)
        self.__status = None
