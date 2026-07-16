"""Server configuration for the JIVE-specific backends.

This isolates all knowledge of remote hosts (name/user/host/path, read from
``computers.toml``) in one small module, out of the core :mod:`experiment` data model.
It is imported only by the modules that legitimately reach JIVE servers — the ``jive``
retrieval/distribution backends and their helpers, and :mod:`tools` for locating
site-installed binaries. The server-agnostic core never imports it.

``computers.toml`` is a local config file, not a network call; a run in ``regular`` or
``sweeps`` mode needs neither this module nor that file.
"""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Server:
    """A remote host entry from ``computers.toml`` (name, user, host, path)."""
    name: str
    user: str
    host: str
    path: Path

    def to_dict(self) -> dict:
        return {'name': self.name, 'user': self.user, 'host': self.host, 'path': str(self.path)}

    @classmethod
    def from_dict(cls, data: dict) -> 'Server':
        return cls(name=data['name'], user=data['user'], host=data['host'], path=Path(data['path']))


class Servers(list[Server]):
    """A list of :class:`Server` objects, indexable by position or by name."""

    def names(self) -> list[str]:
        """Returns a list of all server names."""
        return [server.name for server in self]

    # Deliberate deviation from list's Liskov-substitutable __getitem__: this is a
    # name-or-index collection by design (e.g. servers['ccs']), not fully substitutable
    # for a plain list.
    def __getitem__(self, key: int | str) -> Server:  # type: ignore[override]
        """Get a server by index (int) or by name (str)."""
        if isinstance(key, int):
            return super().__getitem__(key)
        elif isinstance(key, str):
            for server in self:
                if server.name == key:
                    return server
            raise KeyError(f"Server '{key}' not found")
        else:
            raise TypeError(f"Index must be int or str, not {type(key).__name__}")

    def to_dict(self) -> list[dict]:
        return [s.to_dict() for s in self]

    @classmethod
    def from_dict(cls, data: list[dict]) -> 'Servers':
        return cls([Server.from_dict(s) for s in data])


def retrieve_servers() -> Servers:
    """Reads the server configuration from ``computers.toml`` (a local file, no network).

    Searches ``$XDG_CONFIG_HOME/evn`` (or ``~/.config/evn``), then ``~jops/.config/evn``.

    Returns:
        Servers: A list of Server objects.

    Raises:
        FileNotFoundError: If the ``computers.toml`` file cannot be found.
    """
    if (configpath := (Path(os.getenv('XDG_CONFIG_HOME', Path.home())) / 'evn')).exists():
        pass
    elif (configpath := (Path(os.path.expanduser('~jops')) / '.config/evn')).exists():
        pass
    else:
        raise FileNotFoundError("No such file or directory: .config/evn/computers.toml neither "
                                "in local user nor jops")

    with open(configpath / 'computers.toml', 'rb') as f:
        servers = tomllib.load(f)

    return Servers([Server(name=s, user=servers[s]['user'], host=servers[s]['host'],
                           path=Path(servers[s]['path'])) for s in servers])
