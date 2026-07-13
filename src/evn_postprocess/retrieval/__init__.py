"""Retrieval backends: how the input files of an experiment are obtained.

This sub-package encapsulates ALL knowledge about where input files come from
(PRD "Retrieval interface"). A backend implements :class:`Retriever`:

  - ``fetch(workdir, expname)`` obtains/locates the .vex file, the .lis files, and the
    optional experiment toml, returning an :class:`InputSet`.
  - ``fetch_station_files(exp)`` obtains the .log/.antabfs station files needed at the
    antab step (decision recorded in docs/issues-refactor.md: this belongs to
    retrieval, not to the pipeline backends).

Built-in backends: ``jive`` (JIVE servers, replicating the historical behaviour),
``none`` (everything already local; never contacts any server), and ``sweeps`` (a
registered but not-yet-implemented placeholder). Backends are looked up by name in a
lazy registry so that selecting ``none`` never imports JIVE-specific machinery. Third
parties can call :func:`register` to add their own backend without touching core code.

The backend name is chosen by the operating mode (see :mod:`evn_postprocess.mode`):
``supsci`` -> ``jive``, ``regular`` -> ``none``, ``sweeps`` -> ``sweeps``. An unknown
name raises :class:`RetrievalError` at selection time, before any workflow step executes.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from loguru import logger


class RetrievalError(RuntimeError):
    """Raised when input files cannot be obtained/located.

    The message always names the missing file(s) and the attempted source.
    Subclasses RuntimeError (operational failure), like the other backend errors;
    the toml/vex input errors subclass ValueError instead.
    """


@dataclass
class InputSet:
    """The input files of an experiment, as located/retrieved by a backend.

    ``lisfiles`` may be empty for backends that create them in a later step (jive);
    the ``none`` backend always fills it (nothing would create them afterwards).
    """
    vexfile: Path
    lisfiles: list[Path] = field(default_factory=list)
    tomlfile: Path | None = None


class Retriever(ABC):
    """Interface every retrieval backend implements. See the module docstring."""
    name: str = ''

    @abstractmethod
    def fetch(self, workdir: Path, expname: str) -> InputSet:
        """Obtains/locates the vex, lis, and toml files for *expname* in *workdir*.

        Raises:
            RetrievalError: Naming exactly which file is missing and where it was
                looked for / how it was attempted to be retrieved.
        """

    @abstractmethod
    def fetch_lisfiles(self, exp) -> bool:
        """Obtains the .lis files of every correlator pass into the working directory.

        Called by the lisfiles workflow step when no local .lis files exist yet
        (jive: create them remotely on ccs and copy them over; none: error, since
        nothing would create them).

        Raises:
            RetrievalError: When the .lis files cannot be obtained.
        """

    @abstractmethod
    def fetch_station_files(self, exp) -> bool:
        """Obtains the .log/.antabfs station files into ``exp.dirs.pipe_temp``.

        Called at the antab step. Missing files for individual stations are warnings
        (stations may legitimately lack them); a completely empty result is an error.

        Raises:
            RetrievalError: When no station files can be obtained at all.
        """

    def fetch_schedule_files(self, exp) -> None:
        """Best-effort fetch of the observing-schedule (.key/.sum) files at initialization.

        A JIVE-only nicety; the default does nothing (regular/none runs have no server).
        Never blocks: any failure is logged and ignored by the implementations.
        """
        return None


# Lazy factories so unselected backends never import their deps (shared registry).
from ..registry import BackendRegistry

_REGISTRY = BackendRegistry('retrieval', RetrievalError)


def register(name: str, factory: Callable[[], Retriever]) -> None:
    """Registers a retrieval backend factory under *name* (overwrites silently)."""
    _REGISTRY.register(name, factory)


def available_backends() -> list[str]:
    """Returns the names of all registered retrieval backends."""
    return _REGISTRY.available()


def get_retriever(name: str) -> Retriever:
    """Instantiates the retrieval backend *name*.

    Raises:
        RetrievalError: On an unregistered name (listing the registered ones).
    """
    return _REGISTRY.get(name)


def _make_none() -> Retriever:
    from .local import NoneRetriever
    return NoneRetriever()


def _make_jive() -> Retriever:
    from .jive import JiveRetriever
    return JiveRetriever()


def _make_sweeps() -> Retriever:
    raise RetrievalError("The 'sweeps' retrieval backend is registered but not implemented "
                         "yet. Use mode 'supsci' (JIVE servers) or 'regular' (files already "
                         "local).")


register('none', _make_none)
register('jive', _make_jive)
register('sweeps', _make_sweeps)
