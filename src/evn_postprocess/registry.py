"""Minimal name-keyed backend registry shared by retrieval, pipelines, and distribution.

Each plugin family instantiates one :class:`BackendRegistry` with its kind name and
error class; the family's module-level functions (register/available_backends/get_*)
stay as thin delegates so their public APIs are unchanged. Factories are zero-argument
callables, kept lazy so unselected backends never import their dependencies.
"""
from __future__ import annotations

from typing import Callable


class BackendRegistry:
    """Name -> factory registry with uniform unknown-name errors.

    Args:
        kind: The family name used in error messages (e.g. 'retrieval').
        error_cls: The exception class raised on unknown names.
    """

    def __init__(self, kind: str, error_cls: type[Exception]):
        self.kind = kind
        self.error_cls = error_cls
        self._factories: dict[str, Callable[[], object]] = {}

    def register(self, name: str, factory: Callable[[], object]) -> None:
        """Registers *factory* under *name* (overwrites silently)."""
        self._factories[name] = factory

    def unregister(self, name: str) -> None:
        """Removes *name* from the registry (missing names are ignored)."""
        self._factories.pop(name, None)

    def available(self) -> list[str]:
        """Returns the sorted names of all registered backends."""
        return sorted(self._factories)

    def __contains__(self, name: str) -> bool:
        return name in self._factories

    def get(self, name: str):
        """Instantiates the backend *name*.

        Raises:
            error_cls: On an unregistered name, listing the registered ones. A
                registered factory may itself raise (e.g. not-implemented stubs).
        """
        if name not in self._factories:
            raise self.error_cls(f"Unknown {self.kind} backend '{name}'. "
                                 f"Registered backends: {', '.join(self.available())}.")
        return self._factories[name]()
