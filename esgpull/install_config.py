from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import platformdirs
from typing_extensions import NotRequired, TypedDict

from esgpull.constants import INSTALLS_PATH_ENV, ROOT_ENV
from esgpull.exceptions import AlreadyInstalledName, AlreadyInstalledPath


@dataclass(init=False)
class Install:
    path: Path
    name: str | None = None

    def __init__(self, path: Path | str, name: str | None = None) -> None:
        self.path = Path(path)
        self.name = name

    def asdict(self) -> InstallDict:
        result: InstallDict = {"path": str(self.path)}
        if self.name is not None:
            result["name"] = self.name
        return result


class InstallDict(TypedDict):
    path: str
    name: NotRequired[str]


class InstallConfigDict(TypedDict):
    current: NotRequired[int]
    installs: list[InstallDict]


@dataclass(init=False)
class _InstallConfig:
    path: Path
    current_idx: int | None
    installs: list[Install]

    def __init__(self) -> None:
        self.setup()

    def setup(self, install_path: Path | None = None):
        if install_path is not None:
            user_config_dir = install_path
        elif (env := os.environ.get(INSTALLS_PATH_ENV)) is not None:
            user_config_dir = Path(env)
        else:
            user_config_dir = platformdirs.user_config_path("esgpull")
        self.path = user_config_dir / "installs.json"
        if self.path.is_file():
            with self.path.open() as f:
                content = json.load(f)
            self.current_idx = content.get("current")
            installs = content.get("installs", [])
            self.installs = [Install(**inst) for inst in installs]
        else:
            self.current_idx = None
            self.installs = []

    def fullpath(self, path: Path) -> Path:
        return path.expanduser().resolve()

    @property
    def current(self) -> Install | None:
        env = os.getenv(ROOT_ENV)
        if env is not None:
            return self.installs[int(env)]
        elif self.current_idx is not None:
            return self.installs[self.current_idx]
        else:
            return None

    @property
    def default(self) -> Path:
        return Path.home() / ".esgpull"

    def activate_msg(self, idx: int, commented: bool = False) -> str:
        install = self.installs[idx]
        name = install.name or install.path
        msg = f"""
To choose {install.path} as the current install location:

$ esgpull self choose {name}


To enable {install.path} for the current shell:

$ eval $(esgpull self activate {name})
""".strip()
        if commented:
            lines = msg.splitlines()
            return "\n".join("# " + line for line in lines)
        else:
            return msg

    def activate_needs_eval(self, idx: int) -> str:
        export = f"export {ROOT_ENV}={idx}"
        comment = self.activate_msg(idx, commented=True)
        return "\n".join([export, comment])

    def asdict(self) -> InstallConfigDict:
        result: InstallConfigDict = {
            "installs": [inst.asdict() for inst in self.installs]
        }
        if self.current_idx is not None:
            result["current"] = self.current_idx
        return result

    def add(self, path: Path, name: str | None = None) -> int:
        install = Install(self.fullpath(path), name)
        idx_path = self.index(path=install.path)
        if idx_path > -1:
            raise AlreadyInstalledPath(
                path=install.path,
                msg=self.activate_msg(idx_path),
            )
        if name is not None:
            idx_name = self.index(name=name)
            if idx_name > -1:
                raise AlreadyInstalledName(
                    name=name,
                    msg=self.activate_msg(idx_name),
                )
        self.installs.append(install)
        return len(self.installs) - 1

    def remove_current(self) -> bool:
        if self.current_idx is None:
            return False
        else:
            self.installs.pop(self.current_idx)
            self.current_idx = None
            return True

    def choose(
        self,
        *,
        idx: int | None = None,
        name: str | None = None,
        path: Path | None = None,
    ) -> None:
        if idx is not None:
            self.current_idx = idx
        elif name is not None:
            self.current_idx = self.index(name=name)
        elif path is not None:
            self.current_idx = self.index(path=path)
        else:
            self.current_idx = None
        if self.current_idx == -1:
            self.current_idx = None

    def write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w") as f:
            json.dump(self.asdict(), f)

    def index(
        self,
        *,
        name: str | None = None,
        path: Path | None = None,
    ) -> int:
        if name is not None:
            return self._index_name(name)
        elif path is not None:
            return self._index_path(path)
        else:
            raise ValueError("nothing provided")

    def _index_name(self, name: str) -> int:
        for i, inst in enumerate(self.installs):
            if inst.name is not None and name == inst.name:
                return i
        return -1

    def _index_path(self, path: Path) -> int:
        path = self.fullpath(path)
        for i, inst in enumerate(self.installs):
            if path == inst.path:
                return i
        return -1


InstallConfig = _InstallConfig()
