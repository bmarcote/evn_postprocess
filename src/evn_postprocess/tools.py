"""Thin adapter layer for external binaries used during post-processing.

Most legacy code paths invoked tools by hardcoded name (``j2ms2``, ``EVN.py``,
``feedback.pl``, ``archive.pl``, ``tConvert``\u2026) and trusted ``$PATH``. That
makes the package brittle: a missing or differently-named binary surfaces as a
cryptic non-zero exit code from :func:`utils.shell_command`. This module gives
each tool a one-line wrapper that:

  * resolves the binary location through (1) an environment-variable override,
    (2) ``computers.toml`` if a server-style entry is defined for it, and
    (3) the system ``$PATH`` as a fallback;
  * runs the binary in the requested working directory using a list-form
    ``subprocess`` call (no shell interpolation), so filenames that contain
    spaces or shell meta-characters can no longer break the call;
  * raises :class:`ToolMissingError` with a clear message if the binary cannot
    be found at all instead of letting a generic ``FileNotFoundError`` bubble
    up from deep inside the workflow.

The wrappers themselves are very small on purpose so that the existing
imperative call sites can be migrated incrementally (this module does *not*
replace :func:`utils.shell_command`; it sits next to it).
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Optional, Sequence
from loguru import logger

from . import servers as _servers


class ToolMissingError(RuntimeError):
    """Raised when a required external binary cannot be located on this machine."""


def resolve(name: str, *, env_var: str | None = None, default: str | None = None) -> str:
    """Returns the absolute path (or PATH-resolvable name) of an external binary.

    Resolution order:
      1. ``env_var`` environment variable, if set.
      2. ``EVN_<NAME>`` environment variable (uppercased binary name).
      3. ``computers.toml`` server entry whose name equals ``name``
         (the ``path`` value is used directly).
      4. ``shutil.which(name)``.
      5. ``default`` if provided, else raise :class:`ToolMissingError`.

    Args:
        name: Canonical binary name (e.g. ``"tConvert"``).
        env_var: Explicit env-var override checked before ``EVN_<NAME>``.
        default: Fallback returned when nothing else resolves; if None and the
            tool is genuinely missing, raise.

    Returns:
        A string suitable for use as the first element of a subprocess call.
    """
    if env_var and (value := os.environ.get(env_var)):
        return value

    auto_env = f"EVN_{name.upper().replace('.', '_').replace('-', '_')}"
    if value := os.environ.get(auto_env):
        return value

    try:
        servers = _servers.retrieve_servers()
        if name in servers.names():
            return str(servers[name].path)
    except (FileNotFoundError, KeyError):
        pass

    if which := shutil.which(name):
        return which

    if default is not None:
        return default

    raise ToolMissingError(
        f"Could not find {name!r} (looked at env {auto_env}, computers.toml, and $PATH). "
        "Set the env var or add an entry to your computers.toml."
    )


def run(name: str, args: Sequence[str], *, cwd: Path | str | None = None,
        timeout: float | int | None = None,
        env: Optional[dict[str, str]] = None,
        check: bool = True) -> subprocess.CompletedProcess:
    """Runs an external tool with structured arguments.

    Always uses ``shell=False`` to avoid argument-quoting bugs. ``cwd`` is
    explicit so tasks no longer rely on a process-wide ``os.chdir``.

    Args:
        name: Canonical binary name; resolved via :func:`resolve`.
        args: Arguments to pass to the binary (no shell expansion is performed).
        cwd: Working directory for the call; ``None`` keeps the current one.
        timeout: Wall-clock timeout in seconds.
        env: Environment overrides (added on top of ``os.environ``).
        check: If True, raise :class:`subprocess.CalledProcessError` on non-zero
            exit. Set False if the caller wants to inspect the return code.

    Returns:
        The completed process. Stdout and stderr are captured as strings.
    """
    bin_path = resolve(name)
    cmd = [bin_path, *map(str, args)]
    full_env = {**os.environ, **(env or {})}
    cwd_str = str(cwd) if cwd is not None else None
    logger.info(f"[bold]> {' '.join(cmd)}[/bold]" + (f"  (cwd={cwd_str})" if cwd_str else ""))
    return subprocess.run(cmd, cwd=cwd_str, env=full_env, timeout=timeout, check=check,
                          capture_output=True, text=True)


# ----------------------------------------------------------------------------
# Convenience wrappers for the tools used most often.
# Each wrapper is a one-liner around `run`; their job is to give the rest of
# the codebase a stable, typed API and to centralise the binary names.
# ----------------------------------------------------------------------------

def tconvert(args: Iterable[str], cwd: Path | str | None = None, **kwargs):
    """Runs ``tConvert`` with the given arguments."""
    return run("tConvert", list(args), cwd=cwd, **kwargs)


def j2ms2(args: Iterable[str], cwd: Path | str | None = None, **kwargs):
    """Runs ``j2ms2`` with the given arguments."""
    return run("j2ms2", list(args), cwd=cwd, **kwargs)


def evnpy(args: Iterable[str], cwd: Path | str | None = None, **kwargs):
    """Runs ``EVN.py`` (the EVN pipeline driver) with the given arguments."""
    return run("EVN.py", list(args), cwd=cwd, **kwargs)


def feedback(args: Iterable[str], cwd: Path | str | None = None, **kwargs):
    """Runs ``feedback.pl`` with the given arguments."""
    return run("feedback.pl", list(args), cwd=cwd, **kwargs)


def archive_pl(args: Iterable[str], cwd: Path | str | None = None, **kwargs):
    """Runs ``archive.pl`` with the given arguments."""
    return run("archive.pl", list(args), cwd=cwd, **kwargs)


def antab_editor(args: Iterable[str], cwd: Path | str | None = None, **kwargs):
    """Runs ``antab_editor.py`` with the given arguments."""
    return run("antab_editor.py", list(args), cwd=cwd, **kwargs)
