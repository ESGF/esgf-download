from typing import Any, Optional

from pathlib import Path
from shutil import rmtree
from enum import Enum, auto
from dataclasses import dataclass, field

import httpx
from OpenSSL import crypto
from xml.etree import ElementTree
from myproxy.client import MyProxyClient
from urllib.parse import (
    urlunparse,
    urljoin,
    urlparse,
    ParseResult,
    ParseResultBytes,
)


IDP = "/esgf-idp/openid/"
CEDA_IDP = "/OpenID/Provider/server/"
PROVIDERS = {
    "esg-dn1.nsc.liu.se": IDP,
    "esgf-data.dkrz.de": IDP,
    "ceda.ac.uk": CEDA_IDP,
    "esgf-node.ipsl.upmc.fr": IDP,
    "esgf-node.llnl.gov": IDP,
    "esgf.nci.org.au": IDP,
}


@dataclass(repr=False)
class Identity:
    provider: str
    user: str
    password: str

    def __post_init__(self):
        assert self.provider in PROVIDERS
        # self.__password = self.password
        # self.password = "*" * len(self.__password)

    def parse_openid(self) -> ParseResult | ParseResultBytes | Any:
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


class Status(Enum):
    VALID = auto()
    EXPIRED = auto()
    MISSING = auto()


@dataclass
class Auth:
    path: str | Path
    cert_file: Path = field(init=False)
    __status: Optional[Status] = field(init=False, default=None)

    VALID = Status.VALID
    EXPIRED = Status.EXPIRED
    MISSING = Status.MISSING

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self.cert_file = self.path / "credentials.pem"
        self.cert_dir = self.path / "certificates"

    @property
    def cert(self) -> Optional[str]:
        if self.status == Status.VALID:
            return str(self.cert_file)
        else:
            return None

    @property
    def status(self) -> Status:
        if self.__status is None:
            self.__status = self._get_status()
        return self.__status

    def _get_status(self) -> Status:
        if not self.cert_file.exists():
            return Status.MISSING
        with self.cert_file.open("rb") as f:
            content = f.read()
        filetype = crypto.FILETYPE_PEM
        pem = crypto.load_certificate(filetype, content)
        if pem.has_expired():
            return Status.EXPIRED
        return Status.VALID

    def renew(self, identity: Identity) -> None:
        if self.cert_dir.is_dir():
            rmtree(self.cert_dir)
        self.cert_file.unlink(missing_ok=True)
        openid = identity.parse_openid()
        client = MyProxyClient(
            hostname=openid.hostname,
            port=openid.port,
            caCertDir=str(self.cert_dir),
            proxyCertLifetime=12 * 60 * 60,
        )
        credentials = client.logon(
            identity.user,
            identity.password,
            bootstrap=True,
            updateTrustRoots=True,
            authnGetTrustRootsCall=False,
        )
        with self.cert_file.open("wb") as file:
            for cred in credentials:
                file.write(cred)
        self.__status = None
