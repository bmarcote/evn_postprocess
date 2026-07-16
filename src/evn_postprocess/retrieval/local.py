"""The 'none' retrieval backend: all input files are already on disk.

Never contacts any server. Validation-only: every file the workflow will need must
already exist locally, and any missing one produces a RetrievalError that names it and
states that this mode does not create/retrieve files.
"""
from __future__ import annotations

import glob
from pathlib import Path

from loguru import logger

from . import InputSet, RetrievalError, Retriever
from .. import experiment_state
from ..inputs import find_local_vex
from ..lisfiles import LAG_TAG


class NoneRetriever(Retriever):
    """Backend for fully-local runs (external users, tests): validates, never fetches."""
    name = 'none'

    def fetch(self, workdir: Path, expname: str) -> InputSet:
        """Locates the local vex, lis, and (optional) toml files, validating presence.

        Raises:
            RetrievalError: Naming the missing vex or lis file(s) and stating that
                retrieval mode 'none' does not create or download files.
        """
        workdir = Path(workdir)
        vexfile = find_local_vex(expname, workdir)
        if vexfile is None:
            raise RetrievalError(
                f"No vex file ({expname.upper()}.vix/.vex/.vox/.vax) found in {workdir.resolve()}. "
                f"Retrieval mode 'none' does not create or download files: place the vex file "
                f"there (for e-EVN experiments, copy it from the ../EXP1 directory).")
        pattern = str(workdir / f"{expname.lower()}*.lis")
        lisfiles = sorted(Path(f) for f in glob.glob(pattern) if LAG_TAG not in f)
        if not lisfiles:
            raise RetrievalError(
                f"No .lis files matching {pattern} found. Retrieval mode 'none' does not "
                f"create or download files: place the .lis file(s) of every correlator pass "
                f"in {workdir.resolve()}.")
        tomlfile = experiment_state.toml_path_for(expname, workdir)
        logger.debug(f"Local inputs for {expname}: {vexfile.name}, "
                     f"{len(lisfiles)} lis file(s), toml "
                     f"{'present' if tomlfile.exists() else 'absent'}.")
        return InputSet(vexfile=vexfile, lisfiles=lisfiles,
                        tomlfile=tomlfile if tomlfile.exists() else None)

    def fetch_lisfiles(self, exp) -> bool:
        """Never creates .lis files: reaching this point means none exist locally.

        Raises:
            RetrievalError: Always — naming the expected pattern and stating that
                mode 'none' does not create files.
        """
        raise RetrievalError(
            f"No local .lis files matching {exp.expname.lower()}*.lis and retrieval mode "
            f"'none' does not create them. Place the .lis file(s) of every correlator "
            f"pass in the experiment directory.")

    def fetch_station_files(self, exp) -> bool:
        """Validates that the .antabfs/.log station files are already in antenna_files/.

        Individual stations without files produce warnings (they may legitimately lack
        them); a directory without any .antabfs file at all is an error, since the
        ANTAB cannot be built from nothing and this mode will not download anything.

        Raises:
            RetrievalError: When no .antabfs file exists in ``exp.dirs.pipe_temp``.
        """
        tempdir = Path(exp.dirs.pipe_temp)
        if not list(tempdir.glob('*.antabfs')):
            raise RetrievalError(
                f"No .antabfs files found in {tempdir.resolve()}. Retrieval mode 'none' does "
                f"not download station files: place the .antabfs (and .log) files there.")
        for ant in exp.antennas.observed:
            if not list(tempdir.glob(f"{ant.lower()}*.antabfs")):
                logger.warning(f"No .antabfs file for station {ant} in {tempdir} "
                               f"(mode 'none': it will not be downloaded).")
            if not list(tempdir.glob(f"{ant.lower()}*.log")):
                logger.warning(f"No .log file for station {ant} in {tempdir} "
                               f"(mode 'none': it will not be downloaded).")
        return True
