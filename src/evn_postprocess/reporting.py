"""Per-step output on three separate channels.

Every workflow step speaks on three independent sinks, each with a single job:

  1. **Rich terminal** (:func:`announce`): a concise, colourful status line for the
     operator following the run.
  2. **loguru debug file** (``logs/logging_messages.log``): the verbose internal record,
     configured by the engine; kept out of the operator's way.
  3. **Replayable command log** (``logs/commands.sh``, :func:`record_command`): the exact
     local command(s) each step ran, one shell-runnable line per command with a per-step
     comment header, so a support scientist can replay any step by hand.

The three are deliberately separate: changing one (e.g. terminal verbosity) never
touches the others. All three live under ``logs/`` in the experiment working directory.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

from loguru import logger
from rich.console import Console


LOGS_DIRNAME = 'logs'
DEBUG_LOG_NAME = 'logging_messages.log'   # loguru DEBUG/INFO sink
COMMAND_LOG_NAME = 'commands.sh'          # replayable shell commands

_console = Console(highlight=False)

# The step currently executing, used to head each block of recorded commands. Set by the
# engine before every step (see workflow.run_workflow); None outside a step.
_current_step: str | None = None
# The last step header written to commands.sh, so the header is emitted once per step.
_last_recorded_step: str | None = None


def logs_dir(base: Path | str | None = None) -> Path:
    """Returns (creating if needed) the ``logs/`` directory under *base* (cwd by default)."""
    directory = (Path(base) if base is not None else Path('.')) / LOGS_DIRNAME
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def debug_log_path(base: Path | str | None = None) -> Path:
    """Returns the path of the loguru debug file (``logs/logging_messages.log``)."""
    return logs_dir(base) / DEBUG_LOG_NAME


def command_log_path(base: Path | str | None = None) -> Path:
    """Returns the path of the replayable command log (``logs/commands.sh``)."""
    return logs_dir(base) / COMMAND_LOG_NAME


def set_current_step(name: str | None) -> None:
    """Records which step is executing, so :func:`record_command` can head its block."""
    global _current_step
    _current_step = name


def announce(message: str, *, style: str = 'bold cyan') -> None:
    """Prints a concise, colourful terminal message for the operator (never raises).

    This is the operator-facing channel only; verbose detail belongs in the loguru
    debug file, and reproducible commands in the command log.
    """
    try:
        _console.print(message, style=style)
    except Exception:  # a stray markup char must never abort a step
        print(message)


def record_command(command: str, step: str | None = None) -> None:
    """Appends a replayable local command to ``logs/commands.sh`` (best-effort).

    A per-step comment header is written the first time a command is recorded for a step,
    so the file reads top-to-bottom as a manual runbook. Failure to write is logged at
    debug level and never interrupts the step.

    Args:
        command: The exact shell command line that was run.
        step: The step name; defaults to the current step set by the engine.
    """
    global _last_recorded_step
    command = command.strip()
    if not command:
        return
    step = step if step is not None else _current_step
    try:
        path = command_log_path()
        new_file = not path.exists()  # must be evaluated before open() creates the file
        with open(path, 'a', encoding='utf-8') as handle:
            if new_file:
                handle.write("#!/bin/sh\n")
                handle.write("# Replayable local commands run by evn_postprocess, in order.\n")
                handle.write(f"# Started {_dt.datetime.now():%Y-%m-%d %H:%M:%S}.\n")
            if step and step != _last_recorded_step:
                handle.write(f"\n# --- step: {step} ---\n")
                _last_recorded_step = step
            handle.write(command + "\n")
    except OSError as e:
        logger.debug(f"Could not record command to {COMMAND_LOG_NAME}: {e}")
