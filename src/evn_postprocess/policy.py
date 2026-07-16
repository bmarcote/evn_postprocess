"""Persistent, machine-readable post-processing decisions for an EVN experiment.

The :class:`Policy` dataclass collects every value that was historically asked
interactively in the ``msops`` dialog (weight threshold, polswap antennas,
1-bit antennas, polconvert antennas, reference antenna override) plus a couple
of run-mode flags. A populated policy file is what makes the workflow runnable
unattended in a batch scheduler.

A policy can be loaded from a small TOML file (see ``Policy.load``) or attached
in code; the same object round-trips through :class:`evn_postprocess.experiment.Experiment`'s
JSON state so it is always available after a resume.

Example ``policy.toml``::

    weight_threshold = 0.85
    polswap          = ["Wb"]
    polconvert       = ["Kt"]
    onebit           = []
    refant           = ["Ef"]
    pause_after      = ["postpipe"]
    skip_archive     = false
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any


@dataclass
class Policy:
    """All the user-facing decisions that the post-processing needs.

    Every field has a sensible default so the workflow can run with an empty
    policy in interactive mode (the dialogs are still asked) or fail fast in
    batch mode (the runner detects missing values via :meth:`requires_input`).

    Attributes:
        weight_threshold: Threshold passed to ``flag_weights``. ``None`` means
            "ask the user" in interactive mode and "fail fast" in batch mode.
        polswap: Antenna codes (capitalised) requiring a polarisation swap.
        polconvert: Antenna codes requiring PolConvert.
        onebit: Antenna codes whose data needs the 1-bit -> 2-bit correction.
        refant: Reference antenna(s), in priority order. Empty means "let the
            workflow auto-pick".
        pause_after: Step names after which the runner should stop and write a
            ``REVIEW_REQUIRED`` marker for human inspection. Defaults to
            ``["postpipe"]`` to preserve the historical pause.
        skip_archive: If True, the ``archive`` step is skipped even when the CLI
            ``-a/--no-archive`` flag is not used.
        batch: If True, the runner refuses to call any interactive dialog and
            exits 0 with ``REVIEW_REQUIRED`` whenever a manual decision is needed.
    """
    weight_threshold: float | None = None
    polswap: list[str] = field(default_factory=list)
    polconvert: list[str] = field(default_factory=list)
    onebit: list[str] = field(default_factory=list)
    refant: list[str] = field(default_factory=list)
    pause_after: list[str] = field(default_factory=lambda: ["postpipe"])
    skip_archive: bool = False
    batch: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Returns a JSON-serializable representation of the policy."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Policy":
        """Builds a Policy from a plain dict, ignoring unknown keys defensively."""
        return cls(**{k: v for k, v in data.items() if k in {f.name for f in cls.__dataclass_fields__.values()}})

    @classmethod
    def load(cls, path: str | Path) -> "Policy":
        """Loads a policy from a TOML file.

        Raises:
            FileNotFoundError: If the file does not exist.
            tomllib.TOMLDecodeError: If the file is not valid TOML.
        """
        path = Path(path)
        with open(path, "rb") as f:
            return cls.from_dict(tomllib.load(f))

    def requires_input(self) -> list[str]:
        """Returns the names of fields that must be filled before a batch run.

        The ``msops`` dialog has historically populated these. If the policy is
        used in batch mode and any of them is still ``None``, the runner should
        stop and ask a human.

        Returns:
            A list of field names that still need a user-supplied value.
        """
        missing: list[str] = []
        if self.weight_threshold is None:
            missing.append("weight_threshold")
        return missing

    def merge(self, **overrides: Any) -> "Policy":
        """Returns a new Policy with the supplied fields replaced.

        Useful for CLI overrides (e.g. ``--refant Ef Mc``) without mutating the
        original loaded object.
        """
        data = self.to_dict()
        data.update({k: v for k, v in overrides.items() if v is not None})
        return Policy.from_dict(data)
