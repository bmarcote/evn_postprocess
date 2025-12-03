import os
import sys
import subprocess
from pathlib import Path
from typing import Optional, Union
from rich import print as rprint
import astropy.units as u

def scp(originpath: str, destpath: str, timeout: Optional[Union[float,int]] = None) -> bool:
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
    rprint(f"[bold]> scp {originpath} {destpath}[/bold]")
    process = subprocess.run(["scp", originpath, destpath], shell=False,
                              stdout=None, stderr=subprocess.PIPE, timeout=timeout)
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
    rprint(f"\n[bold]> ssh {computer} {commands}[/bold]")
    process = subprocess.Popen(["ssh", computer, commands], shell=shell, stdout=stdout,
                               stderr=stderr)
    # logger.info(output)
    if (process.returncode != 0) and (process.returncode is not None):
        raise ValueError(f"Error code {process.returncode} when running " \
                         f"ssh {computer}:{commands} in ccs.")

    return process.communicate()[0].decode('utf-8') if process.communicate()[0] is not None else None


def shell_command(command: str, parameters: Optional[Union[str, list]] = None, shell: bool = True,
                  bufsize: int = -1, stdout: Optional[int] = subprocess.PIPE,
                  stderr: Optional[int] = subprocess.PIPE) -> str:
    """Runs the provided command in the shell with some arguments if necessary.
    Parameters must be either a single string or a list, if provided.

    Args:
        command (str): Command to execute.
        parameters (Optional[Union[str, list]]): Command parameters as string or list. Default None.
        shell (bool): Whether to use shell mode. Default True.
        bufsize (int): Buffer size for subprocess. Default -1.
        stdout (Optional[int]): Standard output redirection. Default subprocess.PIPE.
        stderr (Optional[int]): Standard error redirection. Default subprocess.PIPE.

    Returns:
        str: Output of the command in UTF-8 encoding.

    Raises:
        ValueError: If the command returns a non-zero exit code.
    """
    if isinstance(parameters, list):
        full_shell_command = [command] + parameters
    else:
        full_shell_command = [command] if parameters is None else [command, parameters]

    rprint(f"\n[bold]> {' '.join(full_shell_command)}[/bold]")
    process = subprocess.Popen(' '.join(full_shell_command), shell=shell,
                               stdout=stdout, stderr=stderr, bufsize=bufsize)
    output_lines = []
    while process.poll() is None:
        if process.stdout is not None:
            out = process.stdout.readline().decode('utf-8')
            output_lines.append(out)
            sys.stdout.write(out)
            sys.stdout.flush()

    if (process.returncode != 0) and (process.returncode is not None):
        raise ValueError(f"Error code {process.returncode} when running " \
                         f"{command} {parameters} in ccs.")

    return '\n'.join(output_lines)


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
        return True
    elif (status == 1) or (status == 2):
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
