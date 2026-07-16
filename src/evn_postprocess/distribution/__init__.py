"""Distribution backends: how the finished experiment is delivered.

Encapsulates everything delivery-specific behind :class:`Distributor` (PRD
"Distribution interface"):

  - ``jive`` (default): the historical JIVE delivery — credentials, file protection,
    archive upload, PI letter, station feedback (full extraction is Issue 14).
  - ``none``: nothing is archived and no server is contacted; the workflow simply
    completes with the data left in place.
  - ``sweeps``: registered name for the future SWEEPS delivery; not implemented.

The backend name is chosen by the operating mode (see :mod:`evn_postprocess.mode`):
``supsci`` -> ``jive``, ``regular`` -> ``none``, ``sweeps`` -> ``sweeps``. Unknown or
unimplemented backends fail with an explicit error at selection time.
"""
from __future__ import annotations

import glob
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable

from loguru import logger

from ..registry import BackendRegistry


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


class NoneDistributor(Distributor):
    """Non-supsci delivery: no archiving, no server contact, no PI letter.

    Instead of delivering anywhere, it verifies the run ended in a known-good state: the
    expected FITS-IDI files exist for every correlator pass. It reports a clear "ready" on
    success or returns False after naming exactly what is missing (PRD story 31). The
    ANTAB Tsys/gain-curve information is attached by the earlier prearchive step; its
    presence is reported best-effort.
    """
    name = 'none'

    def deliver(self, exp) -> bool:
        passes = getattr(exp, 'correlator_passes', None) or []
        if not passes:
            logger.error(f"Distribution mode 'none': no correlator passes are set up for "
                         f"{exp.expname}; nothing to verify. Run the earlier steps first.")
            return False
        missing = []
        for a_pass in passes:
            basename = str(a_pass.fitsidifile)
            if not basename or not glob.glob(f"{basename}*"):
                missing.append(basename or f"(pass {a_pass.msfile})")
        if missing:
            logger.error(f"Distribution mode 'none': the final FITS-IDI files are not in "
                         f"order for {exp.expname}. Missing for pass(es): {', '.join(missing)}. "
                         "Produce them (tconvert/polconvert) before distributing.")
            return False
        # Best-effort note on the ANTAB attachment (done by prearchive/append_antab).
        if not any(Path('.').glob('*.antab')):
            logger.warning(f"No .antab file found for {exp.expname}: verify the Tsys/gain-curve "
                           "information was appended to the FITS-IDI files (prearchive step).")
        logger.info(f"Distribution mode 'none': {exp.expname} is ready — FITS-IDI files present "
                    f"for all {len(passes)} correlator pass(es); nothing archived, data left in place.")
        return True


def _make_sweeps() -> Distributor:
    raise DistributionError("The 'sweeps' distribution backend is registered but not "
                            "implemented yet. Use 'jive' (default) or 'none'.")


# The concrete backend module imports Distributor from this package, so it can only be
# imported here, after the class definitions above.
from .jive import JiveDistributor  # noqa: E402

register('none', NoneDistributor)
register('jive', JiveDistributor)
register('sweeps', _make_sweeps)
