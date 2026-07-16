"""Calibration pipeline backends.

Encapsulates everything pipeline-specific behind :class:`PipelineBackend`
(PRD "Pipeline interface"; code encapsulation only, no OS containers):

  - ``prepare(exp)``: build the pipeline input files from LOCAL files only (the
    station .log/.antabfs acquisition belongs to the retrieval backends).
  - ``run(exp)``: execute the pipeline for all correlator passes.
  - ``collect(exp)``: gather diagnostics/outputs after the run.

Built-in backends: ``aips`` (the EVN.py AIPS pipeline, wrapping the historical pipeline
module), ``none`` (no-op that still satisfies downstream steps), and ``vpipe``
(registered name, not implemented yet). Every operating mode currently runs the ``aips``
pipeline (see :mod:`evn_postprocess.mode`). Unknown or unimplemented backends fail with
an explicit error at selection time.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable

from loguru import logger

from ..registry import BackendRegistry


class PipelineError(RuntimeError):
    """Raised when a pipeline backend cannot be selected or fails to run.

    Subclasses RuntimeError (operational failure), like the other backend errors.
    """


class PipelineBackend(ABC):
    """Interface every calibration-pipeline backend implements."""
    name: str = ''

    @abstractmethod
    def prepare(self, exp) -> bool:
        """Builds the pipeline input files from local files (uvflg, input file, ...)."""

    @abstractmethod
    def run(self, exp) -> bool:
        """Runs the pipeline over all correlated passes."""

    @abstractmethod
    def collect(self, exp) -> bool:
        """Collects diagnostics/outputs after the pipeline run."""


_REGISTRY = BackendRegistry('pipeline', PipelineError)


def register(name: str, factory: Callable[[], PipelineBackend]) -> None:
    """Registers a pipeline backend factory under *name* (overwrites silently)."""
    _REGISTRY.register(name, factory)


def available_backends() -> list[str]:
    """Returns the names of all registered pipeline backends."""
    return _REGISTRY.available()


def get_pipeline(name: str) -> PipelineBackend:
    """Instantiates the pipeline backend *name*.

    Raises:
        PipelineError: On an unregistered name (listing the registered ones), or on a
            registered-but-unimplemented backend (e.g. vpipe).
    """
    return _REGISTRY.get(name)


class NonePipeline(PipelineBackend):
    """No-op backend: skips calibration while keeping downstream steps satisfied."""
    name = 'none'

    def prepare(self, exp) -> bool:
        logger.info(f"Pipeline mode 'none': skipping input preparation for {exp.expname}.")
        return True

    def run(self, exp) -> bool:
        logger.info(f"Pipeline mode 'none': skipping the pipeline run for {exp.expname}.")
        return True

    def collect(self, exp) -> bool:
        logger.info(f"Pipeline mode 'none': no pipeline outputs to collect for {exp.expname}.")
        return True


def _make_vpipe() -> PipelineBackend:
    raise PipelineError("The 'vpipe' pipeline backend is registered but not implemented yet. "
                        "Use 'aips' (default) or 'none'.")


# Concrete backend modules import PipelineBackend from this package, so they can only be
# imported here, after the class definitions above.
from .aips import AipsPipeline  # noqa: E402

register('none', NonePipeline)
register('aips', AipsPipeline)
register('vpipe', _make_vpipe)
