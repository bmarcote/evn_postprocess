"""The 'jive' retrieval backend: fetch input files from the JIVE servers.

Owns ALL JIVE-server knowledge for input acquisition (ccs for vex/lis, vlbeer for the
station .log/.antabfs files, per the decision recorded in docs/issues-refactor.md).
Imported only when the 'jive' backend is selected. Delegation to the historical io/
lisfiles helpers remains for the vex/lis transport; the vlbeer fetch lives here.
"""
from __future__ import annotations

import glob
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from loguru import logger
from rich import print as rprint

from . import InputSet, RetrievalError, Retriever
from .. import experiment_state
from ..lisfiles import LAG_TAG


def fetch_from_vlbeer(exp, server) -> bool:
    """Retrieves the antabfs, log, and flag files from vlbeer into antenna_files/.

    Moved verbatim from the historical pipeline module (audit finding: vlbeer
    knowledge belongs to retrieval). Missing/timed-out file classes are warnings, not
    errors (stations may legitimately lack them). Also flags which antennas have
    log/antabfs files and normalises the non-standard ',opacity_corrected' POLY tag
    so antab_editor can parse the files.
    """
    from .. import utils

    def fetch_file(ext: str):
        try:
            s_formatted = utils.format_remote_path(str(server.path), obsdate=exp.obsdate)
            utils.scp(f"{server.user}@{server.host}:{Path(s_formatted) / f'{exp.expname.lower()}*{ext}'}",
                      str(exp.dirs.pipe_temp) + "/", timeout=120)
        except subprocess.TimeoutExpired:
            rprint(f"[bold yellow]Could not retrieve the {ext} files from vlbeer.[/bold yellow]")
            logger.warning(f"Could not retrieve {ext} files from vlbeer")
        except ValueError:
            rprint(f"[bold yellow]Could not find the {ext} files in vlbeer.[/bold yellow]")
            logger.warning(f"Could not find {ext} files in vlbeer")

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_file, a_file) for a_file in ('antabfs', 'log', 'flag')]
        for future in futures:
            future.result()

    for ext in ('antabfs', 'log'):
        for a_file in list(exp.dirs.pipe_temp.glob(f"{exp.expname.lower()}*{ext}")):
            ant = a_file.name.split('.')[0].replace(f"{exp.expname.lower()}", '').split('_')[0].capitalize()
            try:
                if ext == 'log':
                    exp.antennas[ant].logfsfile = True
                elif ext == 'antabfs':
                    exp.antennas[ant].antabfsfile = True
            except ValueError:
                # Likely the antenna has a different name, or is an e-EVN antenna that
                # participated in the run but not in this particular experiment.
                rprint(f"[yellow]The antenna '{ant}' has a log file but is not part of "
                       "this experiment. Just ignoring this and continuing...[/yellow]")

    logger.debug(f"\n# Log files found for:\n# {', '.join(exp.antennas.logfsfile)}")
    if len(set(exp.antennas.names) - set(exp.antennas.logfsfile)) > 0:
        logger.debug("# Missing files for: "
                     f"{', '.join((set(exp.antennas.names) - set(exp.antennas.logfsfile)).intersection(set(exp.antennas.observed)))}\n")
    else:
        logger.debug("# No missing log files for any station that observed.\n")

    logger.debug(f"# Antab files found for:\n# {', '.join(exp.antennas.antabfsfile)}")
    if len(set(exp.antennas.names) - set(exp.antennas.antabfsfile)) > 0:
        logger.debug("# Missing files for: "
                     f"{', '.join((set(exp.antennas.names) - set(exp.antennas.antabfsfile)).intersection(set(exp.antennas.observed)))}\n")
    else:
        logger.debug("# No missing antab files for any station that observed.\n")

    # In case of high-freq observations, some stations added the "opacity_corrected"
    # flag to the POLY= line, against any standard... Remove it so antab_editor (later)
    # can work fine, keeping a comment line as the record.
    for antabfs_file in exp.dirs.pipe_temp.glob(f"{exp.expname.lower()}*.antabfs"):
        with open(antabfs_file, 'r') as f:
            content = f.read()

        if ',opacity_corrected' in content:
            new_lines = []
            for line in content.split('\n'):
                if ',opacity_corrected' in line:
                    new_lines.append(line.replace(',opacity_corrected', ''))
                    new_lines.append('! opacity_corrected')
                else:
                    new_lines.append(line)

            with open(antabfs_file, 'w') as f:
                f.write('\n'.join(new_lines))

            antenna = antabfs_file.name.split('.')[0].replace(f"{exp.expname.lower()}_", '').split('_')[0].capitalize()
            if antenna in exp.antennas:
                exp.antennas[antenna].opacity = True

    exp.store()
    return True


class JiveRetriever(Retriever):
    """Backend replicating the historical JIVE behaviour (vex from ccs, station files
    from vlbeer). Requires a computers.toml server configuration."""
    name = 'jive'

    @staticmethod
    def _servers():
        from .. import experiment
        try:
            return experiment.retrieve_servers()
        except FileNotFoundError as e:
            raise RetrievalError(
                "Retrieval mode 'jive' requires the computers.toml server configuration "
                "(expected in ~/.config/evn/ or the local .config directory). "
                f"Not found: {e}") from e

    def fetch(self, workdir: Path, expname: str) -> InputSet:
        """Locates the vex locally or fetches it (with the .piletter) from the servers.

        The .lis files are not fetched here: they are created/retrieved by the
        lisfiles workflow step (full extraction into this backend is Issue 5).

        Raises:
            RetrievalError: When the vex file cannot be obtained.
        """
        workdir = Path(workdir)
        from .. import io
        from ..inputs import find_local_vex
        vexfile = find_local_vex(expname, workdir)
        if vexfile is None:
            servers = self._servers()
            if not io.get_init_files(expname, servers):
                raise RetrievalError(
                    f"Could not retrieve the vex file for {expname} from the correlator "
                    f"server ({servers['ccs'].host}). For e-EVN experiments, copy the vex "
                    f"of the run (EXP1) into {workdir.resolve()} first.")
            vexfile = find_local_vex(expname, workdir)
            if vexfile is None:
                raise RetrievalError(f"Retrieval reported success but no vex file is "
                                     f"present for {expname} in {workdir.resolve()}.")
        lisfiles = sorted(Path(f) for f in glob.glob(str(workdir / f"{expname.lower()}*.lis"))
                          if LAG_TAG not in f)
        tomlfile = experiment_state.toml_path_for(expname, workdir)
        self._prefill_pi(expname, tomlfile)
        return InputSet(vexfile=vexfile, lisfiles=lisfiles,
                        tomlfile=tomlfile if tomlfile.exists() else None)

    @staticmethod
    def _prefill_pi(expname: str, tomlfile: Path) -> bool:
        """Pre-fills the [pi] section of the experiment toml when trivially available.

        PRD Open Q4 decision: retrieval fills PI info when a JIVE-internal source is
        trivially available; the distribution-time prompt remains the safety net. With
        .expsum and .jexp gone there is currently no structured internal source (the
        .piletter is free text), so this is a documented no-op hook: implement it here
        when such a source (e.g. an internal API/DB) becomes available.

        Returns:
            bool: True when PI info was written into the toml (currently never).
        """
        return False

    def fetch_lisfiles(self, exp) -> bool:
        """Creates the .lis files remotely on ccs and copies them to the workdir.

        Replicates the historical behaviour (lisfiles.create_lis_files +
        lisfiles.get_lis_files), which uses the timeout-configured ssh/scp helpers.

        Raises:
            RetrievalError: When the remote creation or the copy fails.
        """
        from .. import lisfiles as _lisfiles
        if not _lisfiles.create_lis_files(exp):
            raise RetrievalError(f"Could not create the .lis files for {exp.expname} on the "
                                 "correlator server (ccs).")
        if not _lisfiles.get_lis_files(exp):
            raise RetrievalError(f"Could not copy the .lis files for {exp.expname} from the "
                                 "correlator server (ccs).")
        return True

    def fetch_station_files(self, exp) -> bool:
        """Fetches the .log/.antabfs files from vlbeer into ``exp.dirs.pipe_temp``.

        Raises:
            RetrievalError: When the vlbeer server is not configured.
        """
        servers = self._servers()
        try:
            vlbeer = servers['vlbeer']
        except KeyError as e:
            raise RetrievalError(f"Server 'vlbeer' missing from computers.toml: {e}") from e
        return fetch_from_vlbeer(exp, vlbeer)
