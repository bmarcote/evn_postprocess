"""e-EVN cross-experiment coordination: sibling conventions and synchronisation barriers.

The experiments of an e-EVN run live in sibling directories ``../EXPm`` (PRD story 31).
Two filesystem barriers, both with pause-and-resume semantics (no daemon, PRD story 33):

  (a) the antab step of the run leader (EXP1) requires the FITS-IDI completion marker
      in every sibling directory (a single antab_editor session covers the whole run);
  (b) the pipeline of EXPn (n>1) requires the final .antab files in ../EXP1/.

The completion marker is an explicit file written by the engine when post_polconvert
finishes (PRD Open Q3 decision: file presence alone cannot distinguish partial
FITS-IDI output), named ``{expname.lower()}.fitsidi_ready``.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from loguru import logger


MARKER_SUFFIX = '.fitsidi_ready'


def marker_path(expname: str, directory: Path | None = None) -> Path:
    """Returns the FITS-IDI completion marker path for *expname* in *directory*."""
    return (directory if directory is not None else Path('.')) / f"{expname.lower()}{MARKER_SUFFIX}"


def mark_fitsidi_ready(exp) -> None:
    """Writes the FITS-IDI completion marker for *exp* (idempotent, never blocks)."""
    try:
        marker_path(exp.expname).write_text(
            f"experiment: {exp.expname}\ncompleted: {dt.datetime.now().isoformat()}\n",
            encoding='utf-8')
        logger.debug(f"FITS-IDI completion marker written for {exp.expname}.")
    except OSError as e:
        logger.warning(f"Could not write the FITS-IDI completion marker: {e}")


def sibling_dir(expname: str) -> Path:
    """Returns the conventional sibling directory of experiment *expname* (../EXPNAME)."""
    return Path('..') / expname.upper()


def siblings(exp) -> dict[str, Path]:
    """Returns {expname: directory} for every experiment of the e-EVN run of *exp*.

    The current experiment maps to '.'; the others to their ``../EXPm`` convention.
    For a non-e-EVN experiment the mapping contains only the experiment itself.
    """
    return {name: (Path('.') if name == exp.expname.upper() else sibling_dir(name))
            for name in exp.eEVN_experiments()}


def fitsidi_missing(exp) -> list[str]:
    """Returns the experiments of the run whose FITS-IDI completion marker is absent.

    Barrier (a): the antab step must wait until this list is empty. The current
    experiment is included (its own marker must exist too).
    """
    return [name for name, directory in siblings(exp).items()
            if not marker_path(name, directory).exists()]


def final_antab_available(exp) -> bool:
    """Whether the final .antab files of the run leader (EXP1) exist in ../EXP1/.

    Barrier (b): the antab/pipeline steps of EXPn (n>1) must wait for them. Looks in
    the leader's ``pipeline/in`` directory (where the antab step publishes them) and,
    as fallback, in its top-level directory.
    """
    leader = sibling_dir(exp.eEVNname)
    return any(leader.glob('pipeline/in/*.antab')) or any(leader.glob('*.antab'))


def leader_antab_dir(exp) -> Path:
    """Returns the directory holding the run leader's final antab/uvflg files."""
    leader = sibling_dir(exp.eEVNname)
    return leader / 'pipeline' / 'in' if any(leader.glob('pipeline/in/*.antab')) else leader
