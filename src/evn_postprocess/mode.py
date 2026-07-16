"""Operating mode: who is running evn_postprocess and in what context.

A single mode replaces the three Phase-1 backend-selection flags. It answers one
question -- support-scientist job, regular local user, or the automated SWEEPS
system -- and from that answer derives which retrieval, pipeline, and distribution
backends run (see :func:`backends_for`).

Resolution (see :func:`resolve`): an explicit ``--mode`` wins over the mode persisted
on the experiment, which wins over auto-detection (:func:`detect`). Detection is a
purely local OS check: the login/effective user being ``jops`` or membership of the
OS group ``supsci`` selects ``supsci``; membership of the OS group ``sweeps`` selects
``sweeps``; anything else is ``regular``. A group that does not exist on the machine
counts as "not a member" -- detection never raises.
"""
from __future__ import annotations

import getpass
import grp
import os
from enum import Enum
from typing import NamedTuple

from loguru import logger


# OS group whose members default to the corresponding mode. Missing groups are ignored.
SUPSCI_GROUP = 'supsci'
SWEEPS_GROUP = 'sweeps'
# Login name that always maps to supsci mode (the shared JIVE support-scientist account).
SUPSCI_USER = 'jops'


class Mode(str, Enum):
    """The operating mode. ``str`` base makes it JSON-serialisable as its value."""
    supsci = 'supsci'
    regular = 'regular'
    sweeps = 'sweeps'


class Backends(NamedTuple):
    """The retrieval/pipeline/distribution backend names selected by a mode."""
    retrieval: str
    pipeline: str
    distribution: str


# mode -> the backend names its steps use. The pipeline is the AIPS EVN pipeline in
# every mode; only the input-retrieval and delivery ends differ.
_BACKENDS: dict[Mode, Backends] = {
    Mode.supsci: Backends(retrieval='jive', pipeline='aips', distribution='jive'),
    Mode.regular: Backends(retrieval='none', pipeline='aips', distribution='none'),
    Mode.sweeps: Backends(retrieval='sweeps', pipeline='aips', distribution='sweeps'),
}


def _username() -> str:
    """Returns the current login/effective user name, or '' if it cannot be determined."""
    try:
        return getpass.getuser()
    except Exception:  # getpass raises when no username is resolvable (rare, sandboxes)
        return os.getenv('USER', '')


def _user_groups() -> set[str]:
    """Returns the set of OS group names the current process belongs to.

    Combines the supplementary groups (``os.getgroups``) with the primary group of the
    login user. Unresolvable gids and lookup failures are skipped, never raised.
    """
    names: set[str] = set()
    gids = set(os.getgroups())
    try:
        if _username():
            gids.add(os.stat(os.path.expanduser('~')).st_gid)  # cheap primary-group hint
    except OSError:
        pass
    for gid in gids:
        try:
            names.add(grp.getgrgid(gid).gr_name)
        except (KeyError, OverflowError):
            continue
    # Also honour explicit membership lists (a user may be a member without the gid
    # appearing in os.getgroups if the process did not re-read them).
    for group_name in (SUPSCI_GROUP, SWEEPS_GROUP):
        try:
            if _username() in grp.getgrnam(group_name).gr_mem:
                names.add(group_name)
        except KeyError:
            continue  # group does not exist on this machine
    return names


def detect() -> Mode:
    """Detects the mode from the OS user and group membership (never raises).

    Returns:
        ``supsci`` for user ``jops`` or a member of the ``supsci`` group; ``sweeps`` for
        a member of the ``sweeps`` group; ``regular`` otherwise.
    """
    user = _username()
    groups = _user_groups()
    if user == SUPSCI_USER or SUPSCI_GROUP in groups:
        return Mode.supsci
    if SWEEPS_GROUP in groups:
        return Mode.sweeps
    return Mode.regular


def resolve(cli_mode: Mode | str | None = None, stored_mode: Mode | str | None = None) -> Mode:
    """Resolves the effective mode: explicit CLI > persisted > auto-detected.

    Warns (never raises) when an explicit *cli_mode* overrides a different *stored_mode*,
    so an intentional switch is possible but never silent.

    Args:
        cli_mode: The mode passed on the command line (``--mode``), or None.
        stored_mode: The mode persisted on the experiment state, or None.
    """
    cli = Mode(cli_mode) if cli_mode is not None else None
    stored = Mode(stored_mode) if stored_mode is not None else None
    if cli is not None:
        if stored is not None and cli != stored:
            logger.warning(f"Overriding the stored mode '{stored.value}' with '{cli.value}' "
                           "(from --mode); the new mode is persisted for later runs.")
        return cli
    if stored is not None:
        return stored
    return detect()


def backends_for(mode: Mode | str) -> Backends:
    """Returns the retrieval/pipeline/distribution backend names for *mode*.

    Raises:
        ValueError: If *mode* is not a known mode.
    """
    return _BACKENDS[Mode(mode)]
