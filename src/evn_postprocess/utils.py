import os
import re
import sys
import time
import datetime as _dt
import subprocess
import threading
from pathlib import Path
from typing import Optional, TextIO, Union
from loguru import logger
import astropy.units as u

# The line in the PI letter after which automatic remarks are inserted. Shared
# single source of truth for process.update_piletter (weight/PolConvert remarks)
# and distribution.jive._apply_comments_to_letter (review comments).
PILETTER_REMARKS_ANCHOR = 'Further remarks:'

# Used by format_remote_path to recognise ``{obsdate.strftime('FMT')}`` patterns,
# with an optional trailing ``.lower()`` / ``.upper()`` method call (used e.g. by the
# vlbeer path ``{obsdate.strftime('%b%y').lower()}`` to get a lowercase month name).
_OBSDATE_STRFTIME_RE = re.compile(r"\{obsdate\.strftime\(([\"'])(.+?)\1\)(?:\.(lower|upper)\(\))?\}")


def format_remote_path(template: str, *, obsdate: Optional[_dt.date] = None,
                       expname: Optional[str] = None) -> str:
    """Substitutes ``{expname}`` and ``{obsdate.strftime('FMT')}`` placeholders in a path string.

    This replaces the previous use of :func:`eval` on f-string-shaped TOML values
    in :mod:`io` and :mod:`pipeline`, which had the dual problem of executing
    arbitrary Python from the configuration file and producing opaque ``NameError``s
    on typos. The new implementation only understands the two placeholder shapes
    actually used by ``computers.toml``:

      * ``{expname}`` \u2014 verbatim substitution of the experiment code.
      * ``{obsdate.strftime('FMT')}`` \u2014 strftime applied to the observation date,
        with an optional trailing ``.lower()`` / ``.upper()`` (e.g. vlbeer's
        ``{obsdate.strftime('%b%y').lower()}`` to produce a lowercase month name).

    Anything else is left untouched, which means any typo in the TOML surfaces as a
    later clean error from the SCP/SSH call instead of a low-level eval crash.

    Args:
        template: Raw path template (typically ``str(server.path)``).
        obsdate: Observation date used by the strftime placeholder.
        expname: Experiment code used by the ``{expname}`` placeholder.

    Returns:
        The fully substituted path.
    """
    out = template
    if expname is not None:
        out = out.replace("{expname}", expname)
    if obsdate is not None:
        def _sub(match: re.Match) -> str:
            formatted = obsdate.strftime(match.group(2))
            method = match.group(3)
            if method == "lower":
                return formatted.lower()
            if method == "upper":
                return formatted.upper()
            return formatted
        out = _OBSDATE_STRFTIME_RE.sub(_sub, out)
    return out

# Default behaviour for remote/SSH-based commands.
# These knobs are intentionally generous (real EVN file moves can take a while)
# but bounded so a stuck network never wedges the pipeline.
DEFAULT_SSH_TIMEOUT_S = int(os.environ.get("EVN_SSH_TIMEOUT_S", "60"))
DEFAULT_SCP_TIMEOUT_S = int(os.environ.get("EVN_SCP_TIMEOUT_S", "600"))
DEFAULT_SSH_RETRIES = int(os.environ.get("EVN_SSH_RETRIES", "2"))
DEFAULT_SSH_BACKOFF_S = float(os.environ.get("EVN_SSH_BACKOFF_S", "3.0"))

# OpenSSH connect-time options to avoid host-key prompts in non-interactive runs
# and to fail fast instead of hanging on a dead host.
_SSH_BASE_OPTS = [
    "-o", f"ConnectTimeout={DEFAULT_SSH_TIMEOUT_S}",
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=accept-new",
]

def notify(title: str, body: str = "") -> None:
    """Send a desktop notification via terminal escape sequences.

    Works over SSH by writing escape sequences that the local terminal emulator
    interprets. Supports iTerm2 (OSC 9), VTE-based terminals (OSC 777), and
    a BEL fallback for any terminal that maps bell to a notification.
    Automatically wraps sequences in a DCS passthrough when running inside tmux.

    No-op when stderr is not a TTY (e.g. when running inside a batch scheduler
    or a CI job): the escape sequences would otherwise pollute job logs.

    Args:
        title: Short notification title (e.g. "EVN Post-Processing").
        body: Longer notification body text. If empty, only the title is shown.
    """
    if not sys.stderr.isatty():
        return

    msg = f"{title}: {body}" if body else title

    # OSC 9  — iTerm2, Windows Terminal
    osc9 = f"\033]9;{msg}\a"
    # OSC 777 — VTE-based terminals (GNOME Terminal, Tilix, etc.)
    osc777 = f"\033]777;notify;{title};{body}\a"

    in_tmux = "TMUX" in os.environ

    for seq in (osc9, osc777):
        if in_tmux:
            # DCS passthrough: \ePtmux;\e<sequence>\e\\
            seq = f"\033Ptmux;\033{seq}\033\\"
        sys.stderr.write(seq)

    sys.stderr.write("\a")  # BEL fallback
    sys.stderr.flush()


def scp(originpath: str, destpath: str, timeout: Optional[Union[float, int]] = None,
        retries: int = DEFAULT_SSH_RETRIES, **kwargs) -> bool:
    """Runs ``scp originpath destpath`` with sane defaults for unattended use.

    Adds a connect-timeout, BatchMode (no password prompts), and a small
    retry-with-backoff so transient network blips don't fail an entire step.

    Args:
        originpath: Source path (can include user@host: prefix for remote).
        destpath: Destination path.
        timeout: Wall-clock timeout for the *whole* transfer in seconds.
            Defaults to ``DEFAULT_SCP_TIMEOUT_S``.
        retries: Number of retries on ValueError/TimeoutExpired (default 2).
            The original attempt counts as the first try.

    Returns:
        True on success.

    Raises:
        ValueError: If every attempt fails with a non-zero exit code.
        subprocess.TimeoutExpired: If every attempt times out.
    """
    if timeout is None:
        timeout = DEFAULT_SCP_TIMEOUT_S
    cmd = ["scp"] + _SSH_BASE_OPTS + [originpath, destpath]
    logger.info(f"[bold]> {' '.join(cmd)}[/bold]")

    last_exc: Optional[BaseException] = None
    for attempt in range(1, retries + 2):
        try:
            process = subprocess.run(cmd, timeout=timeout, **kwargs)
            if process.returncode == 0:
                return True
            last_exc = ValueError(
                f"ERROR: could not retrieve {destpath} from {originpath} "
                f"(exit {process.returncode}, attempt {attempt}/{retries + 1})"
            )
        except subprocess.TimeoutExpired as exc:
            last_exc = exc
            logger.warning(f"scp timed out after {timeout}s (attempt {attempt}/{retries + 1})")

        if attempt <= retries:
            time.sleep(DEFAULT_SSH_BACKOFF_S * attempt)

    assert last_exc is not None  # control-flow guarantee
    raise last_exc


def rsync(originpaths: Union[str, list[str]], destpath: str,
          timeout: Optional[Union[float, int]] = None,
          retries: int = DEFAULT_SSH_RETRIES, **kwargs) -> bool:
    """Runs ``rsync`` over SSH with the same unattended defaults as :func:`scp`.

    Preferred over :func:`scp` for the MS files: they are directory trees of many
    files (``-a`` recurses and preserves them) and very large, so ``--partial``
    lets an interrupted transfer resume instead of starting over.

    Args:
        originpaths: Source path or list of source paths (a ``user@host:`` prefix
            is allowed for a remote source). Remote globs are expanded by rsync.
        destpath: Destination path (``user@host:`` prefix allowed).
        timeout: Wall-clock timeout for the whole transfer. Defaults to
            ``DEFAULT_SCP_TIMEOUT_S``.
        retries: Retries on non-zero exit / timeout (the first attempt counts as one).

    Returns:
        True on success.

    Raises:
        ValueError: If every attempt fails with a non-zero exit code.
        subprocess.TimeoutExpired: If every attempt times out.
    """
    if timeout is None:
        timeout = DEFAULT_SCP_TIMEOUT_S
    sources = [originpaths] if isinstance(originpaths, str) else list(originpaths)
    cmd = ["rsync", "-a", "--partial", "-e", "ssh " + " ".join(_SSH_BASE_OPTS), *sources, destpath]
    logger.info(f"[bold]> {' '.join(cmd)}[/bold]")

    last_exc: Optional[BaseException] = None
    for attempt in range(1, retries + 2):
        try:
            process = subprocess.run(cmd, timeout=timeout, **kwargs)
            if process.returncode == 0:
                return True
            last_exc = ValueError(
                f"ERROR: rsync of {sources} to {destpath} failed "
                f"(exit {process.returncode}, attempt {attempt}/{retries + 1})"
            )
        except subprocess.TimeoutExpired as exc:
            last_exc = exc
            logger.warning(f"rsync timed out after {timeout}s (attempt {attempt}/{retries + 1})")

        if attempt <= retries:
            time.sleep(DEFAULT_SSH_BACKOFF_S * attempt)

    assert last_exc is not None  # control-flow guarantee
    raise last_exc


def ssh(computer: str, commands: str, shell: bool = False,
        stdout: Optional[int] = subprocess.PIPE,
        stderr: Optional[int] = subprocess.PIPE,
        timeout: Optional[Union[float, int]] = None) -> str | None:
    """Runs ``ssh computer commands`` and returns the captured stdout (UTF-8).

    The OpenSSH ``ConnectTimeout`` and ``BatchMode`` options are always set so the
    call fails fast instead of hanging on a dead host or prompting interactively.
    The wall-clock ``timeout`` parameter additionally bounds the total command
    duration. A single retry is performed on TimeoutExpired.

    Args:
        computer: Host to connect to (``user@host`` accepted).
        commands: Shell command(s) to execute on the remote host.
        shell: Whether to use shell mode. Defaults to False.
        stdout: Standard output redirection. Defaults to ``subprocess.PIPE``.
        stderr: Standard error redirection. Defaults to ``subprocess.PIPE``.
        timeout: Wall-clock timeout. Defaults to ``DEFAULT_SSH_TIMEOUT_S * 4``
            (long enough for typical remote greps but bounded).

    Returns:
        Captured stdout decoded as UTF-8, or None if there was no output.

    Raises:
        ValueError: If the ssh command returns a non-zero exit code.
        subprocess.TimeoutExpired: If both attempts exceed the timeout.
    """
    if timeout is None:
        timeout = DEFAULT_SSH_TIMEOUT_S * 4
    cmd = ["ssh", *_SSH_BASE_OPTS, computer, commands]
    logger.info(f"[bold]> ssh {computer} {commands}[/bold]")

    last_timeout: Optional[subprocess.TimeoutExpired] = None
    for attempt in range(1, DEFAULT_SSH_RETRIES + 2):
        try:
            result = subprocess.run(cmd, shell=shell, stdout=stdout, stderr=stderr,
                                    timeout=timeout)
            if result.returncode != 0:
                raise ValueError(
                    f"Error code {result.returncode} when running ssh {computer}: {commands}."
                    f" stderr={result.stderr.decode('utf-8', errors='replace') if result.stderr else ''!r}"
                )
            if result.stdout is None:
                return None
            return result.stdout.decode('utf-8') if isinstance(result.stdout, (bytes, bytearray)) else None
        except subprocess.TimeoutExpired as exc:
            last_timeout = exc
            logger.warning(f"ssh timed out after {timeout}s (attempt {attempt}/{DEFAULT_SSH_RETRIES + 1})")
            if attempt <= DEFAULT_SSH_RETRIES:
                time.sleep(DEFAULT_SSH_BACKOFF_S * attempt)
                continue
            assert last_timeout is not None
            raise last_timeout
    return None


def open_unique_log(logfile: Union[str, Path]) -> tuple[TextIO, Path]:
    """Atomically opens a new log file for writing, never overwriting an existing one.

    The first run uses ``logfile`` verbatim (e.g. ``logs/tconvert.log``); if that
    name already exists a numbered sibling is used instead
    (``logs/tconvert-2.log``, ``logs/tconvert-3.log``, ...). The file is created
    with ``O_CREAT | O_EXCL`` so concurrent callers — e.g. the parallel correlator
    passes run by :func:`process.j2ms2` and :func:`process.tconvert` — each get
    their own distinct file with no race.

    Args:
        logfile: Desired log path. The parent directory is created if missing.

    Returns:
        A ``(file_object, path)`` tuple. The caller owns the file object and must
        close it.
    """
    logfile = Path(logfile)
    logfile.parent.mkdir(parents=True, exist_ok=True)
    candidate = logfile
    counter = 2
    while True:
        try:
            fd = os.open(candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            return os.fdopen(fd, "w", encoding="utf-8"), candidate
        except FileExistsError:
            candidate = logfile.with_name(f"{logfile.stem}-{counter}{logfile.suffix}")
            counter += 1


def shell_command(command: str, parameters: Optional[Union[str, list]] = None, shell: bool = True,
                  bufsize: int = -1, stdout: Optional[int] = subprocess.PIPE,
                  stderr: Optional[int] = subprocess.PIPE,
                  stderr_warn_re: Optional[re.Pattern] = None,
                  logfile: Optional[Union[str, Path]] = None,
                  echo: bool = True) -> str:
    """Runs the provided command in the shell and streams its output live.

    Both stdout and stderr are captured and echoed to the terminal as the command runs:
    stdout is printed plain, stderr is printed in red (ANSI). On non-zero exit code, a
    concise ValueError is raised (the detailed error output has already been shown to the
    user via the streaming above, so the exception message stays short).

    Args:
        command (str): Command to execute.
        parameters (Optional[Union[str, list]]): Command parameters as string or list.
        shell (bool): Whether to use shell mode. Default True.
        bufsize (int): Buffer size for subprocess. Default -1.
        stdout (Optional[int]): Kept for API compatibility; output is always streamed.
        stderr (Optional[int]): Kept for API compatibility; errors are always streamed in red.
        stderr_warn_re (Optional[re.Pattern]): If given, stderr lines matching this pattern
            are printed in yellow (treated as warnings) instead of red. Non-matching stderr
            lines remain red. Use only when warnings can be reliably told apart from errors;
            when None (default) all stderr is red, as before.
        logfile (Optional[Union[str, Path]]): If given, the combined stdout/stderr of the
            command is also written (plain, without the terminal colour codes) to this file.
            An existing file is never overwritten: a numbered sibling is used instead (see
            :func:`open_unique_log`), so each run gets its own file. Failure to open the log
            is non-fatal — a warning is emitted and the command still runs.
        echo (bool): When True (default) the output is streamed live to the terminal. Set
            False to run quietly: output is still captured and teed to ``logfile``, but not
            echoed to stdout/stderr. Use for background/parallel runs (e.g. the auxiliary
            lag-space MS) so they do not garble the foreground command's real-time output.

    Returns:
        str: Concatenated stdout from the command (UTF-8).

    Raises:
        ValueError: If the command exits with a non-zero return code.
    """
    del stdout, stderr  # API compat; new behavior always streams both

    if isinstance(parameters, list):
        full_shell_command = [command] + parameters
    else:
        full_shell_command = [command] if parameters is None else [command, parameters]

    cmd_str = ' '.join(full_shell_command)
    logger.info(f"[bold]> {cmd_str}[/bold]")
    # Record the exact command into logs/commands.sh so a step can be replayed by hand
    # (see evn_postprocess.reporting). Lazy import keeps utils free of package cycles.
    from . import reporting
    reporting.record_command(cmd_str)

    # Optionally tee the combined stdout/stderr to a log file. Both pump threads
    # write to the same handle, so a lock keeps their lines from interleaving
    # mid-line. Opening the log must never break the actual command, hence the
    # best-effort try/except.
    log_fh: Optional[TextIO] = None
    log_lock = threading.Lock()
    if logfile is not None:
        try:
            log_fh, log_path = open_unique_log(logfile)
            log_fh.write(f"# command: {cmd_str}\n")
            log_fh.write(f"# started: {_dt.datetime.now():%Y-%m-%d %H:%M:%S}\n\n")
            log_fh.flush()
            logger.info(f"Logging output of '{command}' to {log_path}")
        except OSError as e:
            logger.warning(f"Could not open log file {logfile}: {e}; continuing without it.")
            log_fh = None

    process = subprocess.Popen(cmd_str, shell=shell, bufsize=bufsize,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def _pump(stream, chunks, out_stream, red: bool, warn_re: Optional[re.Pattern] = None):
        """Read lines from stream, append to chunks, echo to out_stream.

        stdout is echoed plain. stderr is echoed red, except lines matching *warn_re*
        (when provided), which are echoed yellow (warnings). The plain text (no colour
        codes) is also appended to the log file when one is open.
        """
        try:
            for raw in iter(stream.readline, b''):
                text = raw.decode('utf-8', errors='replace')
                chunks.append(text)
                if log_fh is not None:
                    with log_lock:
                        log_fh.write(text)
                        log_fh.flush()
                if not echo:
                    continue  # quiet run: captured + logged above, but not streamed to terminal
                if red and warn_re is not None and warn_re.search(text):
                    out_stream.write(f"\033[33m{text}\033[0m")  # yellow: recognised warning
                elif red:
                    out_stream.write(f"\033[31m{text}\033[0m")  # red: error / unclassified
                else:
                    out_stream.write(text)
                out_stream.flush()
        finally:
            stream.close()

    t_out = threading.Thread(target=_pump, args=(process.stdout, stdout_chunks, sys.stdout, False))
    t_err = threading.Thread(target=_pump, args=(process.stderr, stderr_chunks, sys.stderr, True, stderr_warn_re))
    t_out.start()
    t_err.start()
    try:
        process.wait()
        t_out.join()
        t_err.join()
    finally:
        if log_fh is not None:
            try:
                log_fh.write(f"\n# finished: {_dt.datetime.now():%Y-%m-%d %H:%M:%S} "
                             f"(exit code {process.returncode})\n")
            except (OSError, ValueError):
                pass  # disk full / already closed: the captured output above is what matters
            log_fh.close()

    if process.returncode != 0:
        had_output = bool(stdout_chunks) or bool(stderr_chunks)
        if had_output:
            raise ValueError(f"'{command}' exited with code {process.returncode} (see output above).")
        raise ValueError(f"'{cmd_str}' exited with code {process.returncode} (no output).")

    return ''.join(stdout_chunks)


def remote_file_exists(host: str, path: str,
                       timeout: Optional[Union[float, int]] = None) -> bool:
    """Checks if a file/glob exists on the remote host.

    Wraps ``ssh host ls path`` with the same connect-timeout/BatchMode options as
    :func:`ssh` so the call cannot hang on a dead host. ``ls`` (rather than
    ``test -f``) is used because the original implementation needs to also accept
    glob patterns.

    Args:
        host: Remote host (``user@host`` accepted).
        path: Path or glob to check on the remote host.
        timeout: Wall-clock timeout. Defaults to ``DEFAULT_SSH_TIMEOUT_S``.

    Returns:
        True if at least one file matched, False if not.

    Raises:
        ConnectionError: If the SSH connection itself failed (exit code != 0/1/2).
    """
    if timeout is None:
        timeout = DEFAULT_SSH_TIMEOUT_S
    cmd = ["ssh", *_SSH_BASE_OPTS, host, f"ls {path}"]
    try:
        status = subprocess.call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 timeout=timeout)
    except subprocess.TimeoutExpired:
        raise ConnectionError(f"SSH to {host} timed out after {timeout}s while checking {path}")

    if status == 0:
        logger.info(f"File {path} in {host} exists.")
        return True
    if status in (1, 2):
        logger.info(f"File {path} in {host} does not exist.")
        return False

    raise ConnectionError(f"SSH connection to {host} failed (exit code {status}).")


def grep_remote_file(host: str, remote_file: str, word: str,
                     timeout: Optional[Union[float, int]] = None) -> str:
    """Runs a grep on a remote file via ssh and returns stdout (UTF-8).

    Args:
        host: Remote host (``user@host`` accepted).
        remote_file: Path to the file on the remote host.
        word: Word/pattern to search for.
        timeout: Wall-clock timeout. Defaults to ``DEFAULT_SSH_TIMEOUT_S``.

    Returns:
        Captured stdout decoded as UTF-8.

    Raises:
        ValueError: If the grep command fails.
        subprocess.TimeoutExpired: If the SSH session exceeds the timeout.
    """
    if timeout is None:
        timeout = DEFAULT_SSH_TIMEOUT_S
    cmd = ["ssh", *_SSH_BASE_OPTS, host, f"grep {word} {remote_file}"]
    logger.info(f"> grep {word} {remote_file}")
    process = subprocess.run(cmd, shell=False, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, timeout=timeout)
    if process.returncode not in (0, 1):  # 1 == 'no matches', not an error here
        raise ValueError(
            f"Errorcode {process.returncode} when searching for {word} in "
            f"{remote_file} from {host}: {process.stderr.decode('utf-8', errors='replace')!r}"
        )

    return process.stdout.decode('utf-8')


def station_1bit_in_vix(vexfile: str | Path) -> bool:
    """Checks if there is any station in the vex file that recorded at 1 bit.
    Note that this/these station(s) may or may not have recorded at 1 bit in this experiment,
    but only at other moment of the run.

    Args:
        vexfile (str | Path): Path to the VEX file to check.

    Returns:
        bool: True if at least one station recorded at 1 bit, False otherwise.

    Raises:
        FileNotFoundError: If the VEX file is not found.
    """
    output = subprocess.call(["grep", "1bit", str(vexfile) if isinstance(vexfile, Path) else vexfile], shell=False, stdout=subprocess.PIPE)
    if output == 0:
        # There is at least one station recording at 1 bit.
        logger.info(f"There is at least one station recording at 1 bit in {vexfile}.")
        return True
    elif output == 1:
        return False
    else:
        # File not found
        raise FileNotFoundError(f"{vexfile} file not found.")


def extract_tail_standardplots_output(stdplt_output: str) -> str:
    """Given a full log output from standardplots, it returns only the last bits that contain
    the information provided by the "r" command.

    Args:
        stdplt_output (str): Full log output from standardplots.

    Returns:
        str: Extracted tail containing the "r" command output.
    """
    last_lines = []
    for a_line in stdplt_output.split('\n')[::-1]:
        # All "r" output lines always start with those messages
        # (listTimeRage: , listSources: , listAntennas: , listFreqs: ):
        if 'list' in a_line:
            last_lines.append(f"# {a_line}")
        elif 'ms: Current' in a_line:
            # We are already done for this output
            break

    last_lines.append('\n')
    logger.info('\n'.join(last_lines[::-1]))
    return '\n'.join(last_lines[::-1])


def space_available(path) -> u.Quantity:
    """Returns the available space in the disk where the given path is located.

    Args:
        path: Path to check (string or Path object).

    Returns:
        astropy.units.Quantity: Available space in gigabytes.
    """
    results = os.statvfs(path)
    return (u.Quantity(results.f_frsize*results.f_bavail, unit=u.b)).to(u.Gb)
