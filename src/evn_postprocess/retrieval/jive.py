"""The 'jive' retrieval backend: fetch input files from the JIVE servers.

Owns ALL JIVE-server knowledge for input acquisition and every outbound ssh/scp for it
(ccs for the vex and .lis files, the piletters host for the .piletter, vlbeer for the
station .log/.antabfs and the .key/.sum schedule files, and the VLBA cal fetch). No other
module in the package performs input-side server access -- the server-agnostic core and
the ``none``/``sweeps`` modes never import this file. Imported only when the 'jive'
backend is selected.
"""
from __future__ import annotations

import glob
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from loguru import logger
from rich import print as rprint

from . import InputSet, RetrievalError, Retriever
from .. import experiment_state
from .. import utils
from ..lisfiles import LAG_TAG


# --------------------------------------------------------------------------------------
# Server-touching transport functions. Relocated here (Phase 2, Issue 4) so that every
# outbound ssh/scp for input acquisition lives inside the JIVE retrieval backend, out of
# the shared core modules. Logic is unchanged from the historical io/lisfiles/pipeline.
# --------------------------------------------------------------------------------------

def get_init_files(expname: str, servers, eEVNname: str | None = None) -> bool:
    """Retrieves the .vix (or .vox) vex file of the experiment, plus the .piletter.

    The vex file is the only hard requirement (all experiment metadata derives from it);
    the .piletter is best-effort for the later distribution stage and its absence only
    logs a warning.

    Returns:
        bool: True if the vex file is present locally after the call.
    """
    eEVNname = expname if eEVNname is None else eEVNname
    piletter_server = servers['piletters']
    piletter_path = Path(f"{expname.lower()}.piletter")
    main_vex = Path(f"{expname.upper()}.vix")

    def fetch_piletter():
        if not piletter_path.exists():
            utils.scp(f"{piletter_server.user}@{piletter_server.host}:{piletter_server.path / piletter_path}", '.')
            logger.debug(f"{piletter_path.name} was not found. Retrieved from {piletter_server.host}.")
        else:
            logger.debug(f"{piletter_path.name} already exists")

    def fetch_vix_or_vox():
        ccs_server = servers['ccs']
        base_path = Path(str(ccs_server.path).format(expname=eEVNname))
        remote_host = f"{ccs_server.user}@{ccs_server.host}"
        if main_vex.exists():
            logger.debug(f"{expname.upper()}.vix already exists.")
            return True

        for ext in ['vox', 'vix']:
            file_path = Path(f"{eEVNname.lower()}.{ext}")
            if not file_path.exists():
                if utils.remote_file_exists(remote_host, base_path / file_path):
                    utils.scp(f"{remote_host}:{base_path / file_path}", '.')
                    logger.debug(f"{file_path} was not found. Retrieved from {remote_host}.")
                else:
                    continue
            try:
                main_vex.symlink_to(file_path)
                logger.debug(f"Symlink {file_path} -> {main_vex} created.")
            except FileExistsError:
                logger.error(f"{expname.lower()} vix/vox file not found in {remote_host}. "
                             "It may have a non-standard name.")
                return False
            return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {'piletter': executor.submit(fetch_piletter),
                   'vex': executor.submit(fetch_vix_or_vox)}
        try:
            futures['piletter'].result()
        except Exception as e:  # best-effort: only needed at distribution time
            logger.warning(f"Could not retrieve {piletter_path.name} (continuing): {e}")
        futures['vex'].result()

    return main_vex.exists()


def get_vlbeer_sched_files(expname: str, obsdate, server) -> bool:
    """Retrieves the .key and .sum observing files from vlbeer (best-effort)."""
    files = [Path(f"{expname.lower()}.key"), Path(f"{expname.lower()}.sum")]

    def fetch_file(a_file: Path):
        if a_file.exists():
            logger.debug(f"{a_file.name} already exists.")
            return
        try:
            s_formatted = utils.format_remote_path(str(server.path), obsdate=obsdate)
            utils.scp(f"{server.user}@{server.host}:{Path(s_formatted) / a_file}", ".", timeout=120)
            logger.debug(f"Retrieved {a_file.name} from vlbeer")
        except subprocess.TimeoutExpired:
            rprint(f"[bold yellow]Could not retrieve {a_file.name} from vlbeer.[/bold yellow]")
            a_file.unlink(missing_ok=True)
            logger.warning(f"Could not retrieve {a_file.name} from vlbeer (timeout)")
        except ValueError:
            rprint(f"[bold yellow]Could not find {a_file.name} in vlbeer.[/bold yellow]")
            a_file.unlink(missing_ok=True)
            logger.warning(f"Could not find {a_file.name} in vlbeer")

    with ThreadPoolExecutor(max_workers=2) as executor:
        for future in [executor.submit(fetch_file, a_file) for a_file in files]:
            future.result()
    return all([p.exists() for p in files])


def lis_files_in_ccs(exp, server) -> bool:
    """Returns whether .lis files already exist in the experiment directory in ccs."""
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    return utils.remote_file_exists(f"{server.user}@{server.host}",
                                    str(Path(str(server.path).format(expname=eEVNname)) / f"{eEVNname.lower()}*.lis"))


def create_lis_files(exp) -> bool:
    """Creates the .lis files remotely on ccs (make_lis)."""
    from .. import servers as _servers
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    server = _servers.retrieve_servers()['ccs']
    if not lis_files_in_ccs(exp, server):
        logger.info("Creating lis file...")
        utils.ssh(f"{server.user}@{server.host}",
                  f"cd {Path(str(server.path).format(expname=eEVNname))};/ccs/bin/make_lis -e {eEVNname}")
    return True


def get_lis_files(exp) -> bool:
    """Copies the .lis files from ccs and normalises them for this experiment."""
    from .. import servers as _servers
    from .. import lisfiles as _lisfiles
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    server = _servers.retrieve_servers()['ccs']
    if len(_lisfiles._pass_lisfiles(f"{eEVNname.lower()}*.lis")) == 0:
        utils.scp(f"{server.user}@{server.host}:"
                  + str(Path(str(server.path).format(expname=eEVNname)) / f"{eEVNname.lower()}*.lis"), '.')

    for a_lis in _lisfiles._pass_lisfiles("*.lis"):
        _lisfiles.split_lis_cont_line(exp, a_lis)

    # e-EVN runs may need the lis files renamed to this experiment.
    if eEVNname != exp.expname:
        for a_lis in _lisfiles._pass_lisfiles("*.lis"):
            if exp.expname.lower() not in a_lis:
                _lisfiles.update_lis_file(a_lis, eEVNname, exp.expname)
            os.rename(a_lis, a_lis.replace(eEVNname.lower(), exp.expname.lower()))
    return True


def get_vlba_antab(exp):
    """Retrieves the VLBA cal (antab) files and gains into the archive temp folder."""
    rprint("[bold yellow]get_vlba_antab not implemented yet. You need to get the VLBA "
           "antab files manually.[/bold yellow]")
    raise NotImplementedError
    if exp.expname.lower()[0] != 'g':
        return True
    cd = f"cd /data/pipe/{exp.expname.lower()}/temp/"
    utils.ssh('jops@archive.jive.eu', ';'.join([cd, "scp jops@eee:/data0/tsys/vlba_gains.key ."]))
    utils.ssh('jops@archive.jive.eu', ';'.join([cd, "scp jops@ccs:/ccs/var/log2vex/logexp_date/"
                                                f"{exp.expname.upper()}_{exp.obsdate.strftime('%Y%m%d')}"
                                                f"/{exp.expname.lower()}cal.vlba ."]))
    return True


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
        from .. import servers
        try:
            return servers.retrieve_servers()
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
        from ..inputs import find_local_vex
        vexfile = find_local_vex(expname, workdir)
        if vexfile is None:
            servers = self._servers()
            if not get_init_files(expname, servers):
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
        if not create_lis_files(exp):
            raise RetrievalError(f"Could not create the .lis files for {exp.expname} on the "
                                 "correlator server (ccs).")
        if not get_lis_files(exp):
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

    def fetch_schedule_files(self, exp) -> None:
        """Best-effort fetch of the .key/.sum schedule files from vlbeer (JIVE nicety).

        Their absence must never block a run, so every failure (no computers.toml, server
        unreachable, files missing) is logged and swallowed. This keeps the only place that
        knows about vlbeer for schedule files inside the JIVE retrieval backend.
        """
        try:
            servers = self._servers()
            get_vlbeer_sched_files(exp.expname if exp.eEVNname is None else exp.eEVNname,
                                   exp.obsdate, servers['vlbeer'])
        except (RetrievalError, FileNotFoundError, KeyError, ValueError, RuntimeError) as e:
            logger.warning(f"Could not retrieve the .key/.sum files from vlbeer (continuing): {e}")
