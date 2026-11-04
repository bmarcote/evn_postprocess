"""Auxiliary lag-space ("lag spectrum") products of an experiment.

A lag-space MS is built from the first correlator pass, restricted to the calibrator sources,
ONLY to compute the per-scan antenna SNR and the polarization diagnostics
(``process.compute_lag_snr``). It is not a correlator pass and must never be treated as one:
it is kept in ``exp.lag_pass``, never in ``exp.correlator_passes``, and all its files (the
``.lis``, the MS and the log of the j2ms2 run) live in their own directory, :data:`LAG_DIR`.
Pass discovery only looks at the experiment directory, so it cannot see them, and j2ms2 runs
from inside :data:`LAG_DIR`, so its outputs cannot land next to those of the passes.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from loguru import logger
from . import experiment, utils


# Directory, relative to the experiment directory, holding EVERY auxiliary lag-space product:
# the {expname}-lag.lis, the {expname}-lag.ms and the log of the j2ms2 run that creates it.
# These are NOT correlator passes: the lag MS is only used to compute per-scan antenna SNR and
# the polarization diagnostics (process.compute_lag_snr). Keeping them out of the experiment
# directory means no pass discovery (``{expname}*.lis``, ``*.ms``), and no external tool run on
# the .lis files there, can ever take them for a correlator pass.
LAG_DIR = Path("lag-spectrum")

# Suffix tagging the lag-space products. Only a safety net now: the layout before LAG_DIR left
# {expname}-lag.lis / {expname}-lag.ms in the experiment directory (they are moved into LAG_DIR
# by migrate_legacy_products), and until then pass discovery must keep skipping them.
LAG_TAG = "-lag."


def lisfile_path(exp: experiment.Experiment) -> Path:
    """Path of the auxiliary lag-space .lis file (``lag-spectrum/{expname}-lag.lis``)."""
    return LAG_DIR / f"{exp.expname.lower()}-lag.lis"


def ms_path(exp: experiment.Experiment) -> Path:
    """Path of the auxiliary lag-space MS (``lag-spectrum/{expname}-lag.ms``)."""
    return LAG_DIR / f"{exp.expname.lower()}-lag.ms"


def migrate_legacy_products(exp: experiment.Experiment) -> None:
    """Moves the lag-space products of the previous layout into :data:`LAG_DIR`.

    Experiments processed before :data:`LAG_DIR` have ``{expname}-lag.ms`` and
    ``{expname}-lag.lis`` in the experiment directory. That .lis was a copy of the first pass
    with only the MS name changed, so it still named that pass's FITS-IDI output: whatever ran
    on it (tConvert, a loop over ``*.lis``) wrote the lag data into the pass's IDI files. The MS
    is moved (a rename: no data copied) and the .lis is moved and rebuilt from the first pass,
    so it names its own IDI file and the paths are valid from inside :data:`LAG_DIR`.
    ``exp.lag_pass`` is pointed at the new location. Does nothing when there is nothing to move.
    """
    old_ms = Path(f"{exp.expname.lower()}-lag.ms")
    old_lis = Path(f"{exp.expname.lower()}-lag.lis")
    if not old_ms.exists() and not old_lis.exists():
        return

    LAG_DIR.mkdir(exist_ok=True)
    new_ms, new_lis = ms_path(exp), lisfile_path(exp)
    for old, new in ((old_ms, new_ms), (old_lis, new_lis)):
        if not old.exists():
            continue
        if new.exists():
            logger.warning(f"Both {old} and {new} exist: keeping {new} and leaving {old} in place "
                           "(it is not a correlator pass and is ignored). Remove it by hand.")
        else:
            shutil.move(str(old), str(new))
            logger.info(f"Moved the auxiliary lag-space {old} into {LAG_DIR}/.")

    if new_lis.exists() and exp.correlator_passes and exp.correlator_passes[0].lisfile.exists():
        try:
            create_lisfile(exp, exp.correlator_passes[0])
        except ValueError as e:
            logger.warning(f"Could not rebuild {new_lis} ({e}); it is kept as moved, isolated "
                           f"in {LAG_DIR}/.")

    if exp.lag_pass is not None:
        exp.lag_pass.lisfile = new_lis
        exp.lag_pass.msfile = new_ms


def create_lisfile(exp: experiment.Experiment, source_pass: experiment.CorrelatorPass) -> Path:
    """Creates ``lag-spectrum/{expname}-lag.lis``, a copy of the given pass configured to
    produce the lag-space MS (``lag-spectrum/{expname}-lag.ms``).

    ``j2ms2`` ignores the ``-o output`` option when an input ``.lis`` file is supplied via
    ``-v`` (the output MS name is taken from the ``.lis`` header instead). To control the
    lag-space MS name we therefore duplicate the source ``.lis`` file and rewrite its header:
    the output MS becomes ``{expname}-lag.ms`` and the output FITS-IDI ``{expname}-lag.IDI``
    (never the name of the pass's own IDI files, so that nothing run on this file can write into
    them). The file is meant to be run from inside :data:`LAG_DIR` (see :func:`run_j2ms2`), so
    the outputs land there, and every relative path in it (the ``.cor`` correlator outputs and
    the ``.vix`` file) is rewritten to be relative to that directory. Absolute paths are not an
    option: ``j2ms2`` prepends its working directory to the ``.cor`` paths even when absolute.

    Args:
        exp (experiment.Experiment): Experiment object (provides the experiment name).
        source_pass (experiment.CorrelatorPass): The pass whose .lis file is duplicated
            (normally the first/main correlator pass).

    Returns:
        Path: The path to the created ``lag-spectrum/{expname}-lag.lis`` file.

    Raises:
        ValueError: If the output MS name of *source_pass* is not found in the .lis header.
    """
    lag_lisfile = lisfile_path(exp)
    lag_msname = ms_path(exp).name
    lag_idiname = f"{exp.expname.lower()}-lag.IDI"
    old_msname = source_pass.msfile.name

    with open(source_pass.lisfile, 'r') as f:
        lines = f.readlines()

    def _from_lag_dir(path: str) -> str:
        return path if os.path.isabs(path) else os.path.relpath(path, LAG_DIR)

    # Fields are matched as whitespace-delimited tokens, so the spacing of the file is kept.
    ms_re = re.compile(rf'(?<!\S){re.escape(old_msname)}(?!\S)')
    # The FITS-IDI output: '{exp}_N_1.IDI', or '{ms}.UVF' before the lisfiles step renames it.
    idi_re = re.compile(r'(?<!\S)\S+\.(?:IDI|UVF)(?!\S)')
    vex_re = re.compile(r'(?<!\S)[^\s"]+\.(?:vix|vex)(?!\S)', re.IGNORECASE)
    job_re = re.compile(r'^(\s*[+-]\s+)(\S+)')
    ms_found = False
    for i, line in enumerate(lines):
        if line.lstrip()[:1] in ('+', '-'):
            # Job line: '+ <path>.cor  - No0001 ... <file>.vix ...'.
            line = job_re.sub(lambda m: m.group(1) + _from_lag_dir(m.group(2)), line, count=1)
            lines[i] = vex_re.sub(lambda m: _from_lag_dir(m.group(0)), line)
        elif line.strip() and not line.lstrip().startswith('#'):
            # Header line: '<EXP> <file>.vix <mode> <output>.ms <output>.IDI'.
            line, n_ms = ms_re.subn(lag_msname, line)
            ms_found = ms_found or n_ms > 0
            line = idi_re.sub(lag_idiname, line)
            lines[i] = vex_re.sub(lambda m: _from_lag_dir(m.group(0)), line)

    if not ms_found:
        raise ValueError(f"The output MS {old_msname} is not named in the header of "
                         f"{source_pass.lisfile}: cannot build the lag-space .lis file from it.")

    LAG_DIR.mkdir(exist_ok=True)
    with open(lag_lisfile, 'w') as f:
        f.writelines(lines)

    return lag_lisfile


def run_j2ms2(exp: experiment.Experiment) -> bool:
    """Creates the lag-space MS (``lag-spectrum/{expname}-lag.ms``) from the first pass.

    Builds its .lis (:func:`create_lisfile`), registers it as ``exp.lag_pass`` and runs j2ms2
    from inside :data:`LAG_DIR`, restricted to the calibrator sources. Quiet: the output goes
    only to ``lag-spectrum/j2ms2-lag.log`` (echo=False), so it does not garble the terminal
    stream of the correlator passes it runs in parallel with. Non-fatal: a failure is logged
    and the partial MS removed. Nothing is done if the MS already exists, with ``--no-lag``,
    or when the first pass has no .lis file.

    Returns:
        bool: Whether a lag-space MS was created by this call.
    """
    migrate_legacy_products(exp)
    lag_ms = ms_path(exp)
    if exp.no_lag:
        logger.info("--no-lag set: skipping creation of the lag-space MS.")
        return False
    if lag_ms.exists() or not exp.correlator_passes or not exp.correlator_passes[0].lisfile.exists():
        return False

    try:
        lag_lisfile = create_lisfile(exp, exp.correlator_passes[0])
        exp.lag_pass = experiment.CorrelatorPass(lisfile=lag_lisfile, msfile=lag_ms,
                                                 fitsidifile="", pipeline=False)
        cal_sources = exp.sources.fringefinder + exp.sources.calibrator
        lag_args = ["-v", lag_lisfile.name, "-d", "frequency"]
        if cal_sources:
            lag_args.append(f"fo:filter/source={','.join(cal_sources)}")
        if not exp.eEVNname:
            lag_args.append("fo:nosquash_source_table")
        utils.shell_command("j2ms2", lag_args, shell=True, stdout=None, stderr=subprocess.STDOUT,
                            bufsize=0, logfile=LAG_DIR / "j2ms2-lag.log", echo=False, cwd=LAG_DIR)
    except BaseException as e:
        # The MS did not exist before (checked above): whatever j2ms2 left is partial.
        if lag_ms.exists():
            shutil.rmtree(lag_ms, ignore_errors=True)
        if not isinstance(e, Exception):
            raise
        logger.warning(f"Lag-space MS creation failed (non-fatal): {e}")
        return False

    logger.info(f"Created lag-space MS: {lag_ms}")
    return True
