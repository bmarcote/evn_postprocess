"""Distribution backends: how the finished experiment is delivered.

Encapsulates everything delivery-specific behind :class:`Distributor` (PRD
"Distribution interface"):

  - ``jive`` (default): the historical JIVE delivery — credentials, file protection,
    archive upload, PI letter, station feedback (full extraction is Issue 14).
  - ``none``: nothing is archived and no server is contacted; the workflow simply
    completes with the data left in place.
  - ``sweeps``: registered name for the future SWEEPS delivery; not implemented.

Selection precedence: experiment toml ``[distribution] mode`` > ``"jive"``. Unknown or
unimplemented backends fail with an explicit error at selection time.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable

from loguru import logger


DEFAULT_MODE = 'jive'

# Module-level CLI override (set from main via set_cli_mode); None means "not given".
_CLI_MODE: str | None = None


class DistributionError(RuntimeError):
    """Raised when a distribution backend cannot be selected or delivery fails.

    Subclasses RuntimeError (operational failure), like the other backend errors.
    """


class Distributor(ABC):
    """Interface every distribution backend implements."""
    name: str = ''

    @abstractmethod
    def deliver(self, exp) -> bool:
        """Delivers the finished experiment (summary, credentials, archive, letter...).

        Returns:
            bool: True when the delivery completed (or was deliberately skipped).
        """


from ..registry import BackendRegistry

_REGISTRY = BackendRegistry('distribution', DistributionError)


def register(name: str, factory: Callable[[], Distributor]) -> None:
    """Registers a distribution backend factory under *name* (overwrites silently)."""
    _REGISTRY.register(name, factory)


def available_backends() -> list[str]:
    """Returns the names of all registered distribution backends."""
    return _REGISTRY.available()


def get_distributor(name: str) -> Distributor:
    """Instantiates the distribution backend *name*.

    Raises:
        DistributionError: On an unregistered name (listing the registered ones), or
            on a registered-but-unimplemented backend (e.g. sweeps).
    """
    return _REGISTRY.get(name)


def set_cli_mode(name: str | None) -> None:
    """Sets (and validates) the CLI-provided distribution mode override.

    Raises:
        DistributionError: If *name* is not a registered backend.
    """
    global _CLI_MODE
    if name is not None and name not in _REGISTRY:
        raise DistributionError(f"Unknown distribution backend '{name}'. "
                                f"Registered backends: {', '.join(available_backends())}.")
    _CLI_MODE = name


def selected_mode(exp_toml=None) -> str:
    """Returns the effective distribution mode: CLI > experiment toml [distribution] mode > default."""
    if _CLI_MODE is not None:
        return _CLI_MODE
    if exp_toml is not None and exp_toml.distribution:
        return exp_toml.distribution
    return DEFAULT_MODE


class NoneDistributor(Distributor):
    """No-op backend: nothing is archived, no server is contacted, data stay in place."""
    name = 'none'

    def deliver(self, exp) -> bool:
        logger.info(f"Distribution mode 'none': {exp.expname} is NOT archived; the data "
                    "remain in the experiment directory.")
        return True


def _make_none() -> Distributor:
    return NoneDistributor()


def _make_jive() -> Distributor:
    from .jive import JiveDistributor
    return JiveDistributor()


def _make_sweeps() -> Distributor:
    raise DistributionError("The 'sweeps' distribution backend is registered but not "
                            "implemented yet. Use 'jive' (default) or 'none'.")


register('none', _make_none)
register('jive', _make_jive)
register('sweeps', _make_sweeps)
