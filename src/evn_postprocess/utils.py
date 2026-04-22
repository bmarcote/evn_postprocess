import os
import sys
import subprocess
import threading
from pathlib import Path
from typing import Optional, Union
from rich import print as rprint
from loguru import logger
import astropy.units as u

def notify(title: str, body: str = "") -> None:
    """Send a desktop notification via terminal escape sequences.

    Works over SSH by writing escape sequences that the local terminal emulator
    interprets. Supports iTerm2 (OSC 9), VTE-based terminals (OSC 777), and
    a BEL fallback for any terminal that maps bell to a notification.
    Automatically wraps sequences in a DCS passthrough when running inside tmux.

    Args:
        title: Short notification title (e.g. "EVN Post-Processing").
        body: Longer notification body text. If empty, only the title is shown.
    """
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


def scp(originpath: str, destpath: str, timeout: Optional[Union[float,int]] = None, **kwargs) -> bool:
    """Does a scp from originpath to destpath. If the process returns an error,
    then it raises ValueError.

    Args:
        originpath (str): Source path (can include user@host: prefix for remote).
        destpath (str): Destination path.
        timeout (Optional[Union[float, int]]): Timeout in seconds for the scp command. Default None.

    Returns:
        bool: True if the scp command succeeded.

    Raises:
        ValueError: If the scp command returns a non-zero exit code.
    """
    # rprint(f"[bold]> scp {originpath} {destpath}[/bold]")
    # CHANGED FOR THE JEX CALL. LIKELY PUT THIS BACK AND RUN DIRECTLY .RUN IN JEXP
    #for key, value in zip(('shell', 'stdout', 'stderr'), (False, None, subprocess.PIPE)):
    #    if key not in kwargs:
    #        kwargs[key] = value
    logger.info(f"[bold]> scp {originpath} {destpath}[/bold]")
    process = subprocess.run(["scp", originpath, destpath], timeout=timeout, **kwargs)
    if process.returncode != 0:
        raise ValueError(f"ERROR: could not retrieve {destpath} from {originpath}.")

    return True


def ssh(computer: str, commands: str, shell: bool = False, stdout: Optional[int] = subprocess.PIPE,
        stderr: Optional[int] = subprocess.PIPE) -> str | None:
    """Sends a ssh command to the indicated computer.
    The output is expected to be in UTF-8 format.

    Args:
        computer (str): Computer/host to connect to (can include user@ prefix).
        commands (str): Command(s) to execute on the remote computer.
        shell (bool): Whether to use shell mode. Default False.
        stdout (Optional[int]): Standard output redirection. Default subprocess.PIPE.
        stderr (Optional[int]): Standard error redirection. Default subprocess.PIPE.

    Returns:
        str | None: Output from the command in UTF-8 format, or None if no output.

    Raises:
        ValueError: If the ssh command returns a non-zero exit code.
    """
    logger.info(f"[bold]> ssh {computer} {commands}[/bold]")
    process = subprocess.Popen(["ssh", computer, commands], shell=shell, stdout=stdout,
                               stderr=stderr)
    if (process.returncode != 0) and (process.returncode is not None):
        raise ValueError(f"Error code {process.returncode} when running " \
                         f"ssh {computer}:{commands} in ccs.")

    return process.communicate()[0].decode('utf-8') if process.communicate()[0] is not None else None


def shell_command(command: str, parameters: Optional[Union[str, list]] = None, shell: bool = True,
                  bufsize: int = -1, stdout: Optional[int] = subprocess.PIPE,
                  stderr: Optional[int] = subprocess.PIPE) -> str:
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

    process = subprocess.Popen(cmd_str, shell=shell, bufsize=bufsize,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def _pump(stream, chunks, out_stream, red: bool):
        """Read lines from stream, append to chunks, echo to out_stream (red if requested)."""
        try:
            for raw in iter(stream.readline, b''):
                text = raw.decode('utf-8', errors='replace')
                chunks.append(text)
                if red:
                    out_stream.write(f"\033[31m{text}\033[0m")
                else:
                    out_stream.write(text)
                out_stream.flush()
        finally:
            stream.close()

    t_out = threading.Thread(target=_pump, args=(process.stdout, stdout_chunks, sys.stdout, False))
    t_err = threading.Thread(target=_pump, args=(process.stderr, stderr_chunks, sys.stderr, True))
    t_out.start()
    t_err.start()
    process.wait()
    t_out.join()
    t_err.join()

    if process.returncode != 0:
        had_output = bool(stdout_chunks) or bool(stderr_chunks)
        if had_output:
            raise ValueError(f"'{command}' exited with code {process.returncode} (see output above).")
        raise ValueError(f"'{cmd_str}' exited with code {process.returncode} (no output).")

    return ''.join(stdout_chunks)


def remote_file_exists(host: str, path: str) -> bool:
    """Checks if a file or path exists in a remote computer.

    Args:
        host (str): Remote host to check (can include user@ prefix).
        path (str): Path to check on the remote host.

    Returns:
        bool: True if the file/path exists, False otherwise.

    Raises:
        Exception: If SSH connection to the host fails.
    """
    # Test does not work if finds multiple files.
    # status = subprocess.call(['ssh', host, f"test -f {path}"])
    status = subprocess.call(['ssh', host, f"ls {path}"],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if status == 0:
        logger.info(f"File {path} in {host} exists.")
        return True
    elif (status == 1) or (status == 2):
        logger.info(f"File {path} in {host} does not exist.")
        return False

    raise Exception(f"SSH connection to {host} failed.")


def grep_remote_file(host: str, remote_file: str, word: str) -> str:
    """Runs a grep in a file located in a remote host and returns the result.

    Args:
        host (str): Remote host (can include user@ prefix).
        remote_file (str): Path to the file on the remote host.
        word (str): Word/pattern to search for.

    Returns:
        str: Output from grep command in UTF-8 format.

    Raises:
        ValueError: If there is a problem accessing the host or file.
    """
    cmd = f"grep {word} {remote_file}"
    logger.info(f"> grep {word} {remote_file}")
    process = subprocess.Popen(["ssh", host, cmd], shell=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    output = process.communicate()[0].decode('utf-8')
    if process.returncode != 0:
        raise ValueError(f"Errorcode {process.returncode} when searching " \
                         f"for {word} in {remote_file} from {host}.")

    return output


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
