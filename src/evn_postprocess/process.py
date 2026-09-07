#! /usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the eee computer.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.

"""
from __future__ import annotations
import os
import re
import glob
import shlex
import string
import random
from typing import Optional
from pathlib import Path
from itertools import product
from datetime import datetime, timedelta
import shutil
import subprocess
import numpy as np
from loguru import logger
from astropy import units as u
from astropy.io import fits
from rich import print as rprint
from rich import progress
from concurrent.futures import ThreadPoolExecutor, as_completed
from . import experiment, utils, mstools
from . import lisfiles
from . import reporting
from . import plotting
# polconvert_main kept for future use once version compatibility is resolved.
# from .scripts.polconvert import main as polconvert_main
from .scripts import find_idi_with_time as find_idi_mod


# --- Automatic polarization diagnostics (from the lag MS) -------------------------------
# Only these antennas are checked for the "linear polarization -> PolConvert" case (they are
# the ones known to potentially record linear polarization). Polswap is checked for all.
_POLCONVERT_CANDIDATES: tuple[str, ...] = ('Ef', 'T6', 'Ur', 'Gm', 'Gt', 'At', 'Yy', 'Me')
# An antenna's fringe must reach at least this lag SNR (max over polarizations, on a
# fringe-finder scan) before we trust its parallel/cross-hand amplitude ratio.
_POL_MIN_SNR: float = 7.0
# cross-hand / parallel-hand amplitude ratio decision thresholds:
#   ratio >= _POLSWAP_RATIO          -> RL,LR dominate -> R/L swapped (polswap)
#   _LINEAR_RATIO_LOW <= ratio < _POLSWAP_RATIO and antenna is a candidate
#                                    -> all four products comparable -> linear pol (polconvert)
#   ratio < _LINEAR_RATIO_LOW        -> normal circular feeds, no action
_POLSWAP_RATIO: float = 2.5
_LINEAR_RATIO_LOW: float = 0.5
_PARALLEL_POLS: frozenset[str] = frozenset({'RR', 'LL', 'XX', 'YY'})
_CROSS_POLS: frozenset[str] = frozenset({'RL', 'LR', 'XY', 'YX'})

# The stderr lines of getdata.pl that are notes rather than failures, shown yellow instead
# of red. Everything getdata.pl reports through perl's warn(): the "**** Warning: ..." it
# labels itself (and ssh's "Warning: Permanently added ..."), plus the "Ignoring"/"Skipping"
# notes for job-list lines and subdirectories it does not recognise.
_GETDATA_WARN_RE = re.compile(r"warning|^(?:Ignoring|Skipping) ", re.IGNORECASE)

# From more than this many correlator passes, tConvert shows a progress bar instead of
# leaving the terminal silent: with the passes converting concurrently their own output is
# muted, and a long multi-phase-centre run would otherwise give no sign of how far it is.
_TCONVERT_PROGRESS_MIN_PASSES: int = 5

# _TCONVERT_BIN = "tConvert"  # This will be the one to use once we certify the following one works
_TCONVERT_BIN = "/home/verkout/src/jive-casa/build-reftime_assert_fail/apps/tConvert/tConvert"

# PolConvert nearly always dies in its own teardown ("double free or corruption",
# "malloc(): invalid next size", or the PyQt5 libqsvgicon.so symbol lookup error) *after* it
# has computed and written the solution, so its exit code says nothing about whether it
# worked: an attempt is judged by the files it left behind (see _fringe_peak_ratios). Only a
# run that died before writing a complete result is retried, up to this many extra times.
_POLCONVERT_RETRIES: int = 3

# A converted solution is accepted when, in every IF, the parallel-to-cross fringe-peak
# amplitude ratio (RR+LL)/(RL+LR) on the reference baseline exceeds this value. A failed/linear
# solution leaves the four products comparable (ratio ~1); a real conversion lifts it well above.
_POLCONVERT_MIN_RATIO: float = 2

# --- PolConvert solution search --------------------------------------------------------
# An antenna only joins the solve if its fringe on the solve scan reaches this lag SNR. It is
# lower than _POL_MIN_SNR because that one gates the *diagnosis* of linear feeds (where a wrong
# call is costly), while here a baseline just has to carry usable signal.
_POLCONVERT_SOLVE_MIN_SNR: float = 3.0

# Minutes trimmed off the scan before solving: antennas are often still settling at the start.
# A scan must last longer than this for the trim to leave a usable range.
_POLCONVERT_TRIM_MIN: int = 1

# Reference antennas tried per scan, strongest fringe first. Two rather than one because the
# solutions found by hand on ES123D and ES123F used the runner-up, and rather than all of them
# because looping over every candidate is what used to make a hopeless search take hours.
_POLCONVERT_MAX_REFANTS: int = 2

# Parameter space of the search, tried in this nesting order for each (scan, refant, time
# range). The doweights are ordered by how often they produced the solutions found by hand;
# 0.0001 (ES123D, RS005A) and 1 (EY054) are last because they are needed only rarely.
_POLCONVERT_DOWEIGHTS: tuple[float, ...] = (0.1, 0.01, 0.001, 0.0001, 1.0)
_POLCONVERT_TIMEAVGS_S: tuple[int, ...] = (10, 20, 30, 60)
_POLCONVERT_CHANAVGS: tuple[int, ...] = (8, 16, 32)

# Ceiling on the whole search, so widening it above cannot turn a hopeless run into an
# overnight one: the full space is 3 time ranges x 2 refants x 60 parameter sets per scan,
# and one attempt costs ~30-60 s. The combinations are ordered best-first, so the cap only
# ever cuts into the least likely tail.
_POLCONVERT_MAX_ATTEMPTS: int = 150


def archive(exp: experiment.Experiment) -> bool:
    """Runs the archive command for all -auth, -stnd, -fits,...
    """
    # Compress all figures from standardplots if they haven't been yet
    if len(glob.glob("*.ps")) > 0:
        # This avoids issues as it seems like gzip freezes when overwriting the same files
        if len(glob.glob("*.ps.gz")) > 0:
            utils.shell_command("rm -rf", "*ps.gz", shell=True)

        utils.shell_command("gzip", "*ps", shell=True)

    if exp.credentials is not None:
        utils.shell_command("archive.pl", ["-auth", "-e", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}",
                                                 "-n", exp.credentials.username, "-p", exp.credentials.password])
        logger.info(f"archive.pl -auth {exp.expname}_{exp.obsdate.strftime('%y%m%d')}")
    else:
        assert len(glob.glob("*_*.auth")) == 0, 'No credentials stored but auth file found'

    utils.shell_command("archive.pl", ["-stnd", "-e", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}", "*ps.gz"])
    utils.shell_command("archive.pl", ["-stnd", "-e", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}",
                                       f"{exp.expname.lower()}.piletter"])
    utils.shell_command("archive.pl", ["-fits", "-e", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}", "*IDI*"])
    return True


def getdata(exp: experiment.Experiment) -> bool:
    """Gets the data from all existing .lis files from the given experiment.

    Args:
        exp (experiment.Experiment): Experiment object with correlator passes.

    Returns:
        bool: True if data was retrieved successfully.
    """
    try:
        def _fetch_pass(a_pass):
            try:
                if not a_pass.lisfile.exists():
                    logger.error(f"LIS file not found: {a_pass.lisfile}")
                    return False

                cmd_args = ["-proj", exp.eEVNname if exp.eEVNname is not None else exp.expname,
                            "-lis", a_pass.lisfile.name]
                # getdata.pl (and the scp calls it makes) write warnings to stderr that are not
                # errors, most notably ssh's "Warning: Permanently added '<host>' ... known hosts"
                # and getdata's own "**** Warning: ...". Those are explicitly labelled "warning",
                # so colour them yellow. So are the "Ignoring <job line>" and "Skipping <subdir>"
                # notes it emits through perl's warn() for every job-list line it does not
                # recognise — harmless, but there can be hundreds of thousands of them on a
                # multi-phase-centre run, and a screen of red reads like a failed step. Genuine
                # errors (perl die messages, "Could not open ...", scp failures) match none of
                # these and stay red.
                utils.shell_command("getdata.pl", cmd_args, shell=True,
                                    stdout=None, stderr=subprocess.STDOUT, bufsize=0,
                                    stderr_warn_re=_GETDATA_WARN_RE,
                                    logfile=exp.dirs.logs / "getdata.log")
                return True
            except Exception as e:
                logger.opt(exception=True).error(f"Error fetching the data for "
                                                 f"{a_pass.lisfile.name}: {e}")
                return False

        if len(exp.correlator_passes) == 0:
            rprint("[bold yellow]No correlator passes found to fetch[/bold yellow]")
            return True

        with ThreadPoolExecutor(utils.pass_workers(len(exp.correlator_passes),
                                                   utils.MAX_PASS_IO_WORKERS)) as pool:
            results = list(pool.map(_fetch_pass, exp.correlator_passes))

        if not all(results):
            logger.error(f"Failed to fetch data for {(len(results) - sum(results))} passes")
            return False

        return True
    except Exception as e:
        logger.opt(exception=True).error(f"Unexpected error in getdata: {e}")
        return False


def j2ms2(exp: experiment.Experiment) -> bool:
    """Runs j2ms2 on all existing .lis files from the given experiment.
    If the MS to produce already exists, then it will not generate it again.

    Args:
        exp (experiment.Experiment): Experiment object with correlator passes.

    Returns:
        bool: True if all MS files were created successfully.

    Raises:
        IOError: If there is not enough disk space to create the MS files.
    """
    try:
        # Check disk space
        try:
            du_result = subprocess.run("du -sc */*.cor*", shell=True, capture_output=True, text=True)
            if du_result.returncode != 0:
                logger.warning("Could not estimate disk space usage, proceeding anyway")
            else:
                if utils.space_available(Path.cwd()) <= 1.2*u.kbit*int(du_result.stdout.split()[-2]):
                    rprint("\n\n[bold red]There is no enough space in the computer to create the MS file[/bold red]")
                    raise IOError("Not enough disk space to create the MS file.")
        except (ValueError, IndexError, subprocess.SubprocessError) as e:
            logger.warning(f"Could not check disk space: {e}, proceeding anyway")

        if not exp.correlator_passes:
            logger.error("No correlator passes found for j2ms2")
            return False

        def _j2ms2_correlator_pass(args: tuple[experiment.Experiment, experiment.CorrelatorPass]) -> bool:
            exp, a_pass = args
            try:
                if not a_pass.lisfile.exists():
                    logger.error(f"LIS file not found: {a_pass.lisfile}")
                    return False

                if os.path.isdir(a_pass.msfile):
                    logger.debug(f"MS file already exists: {a_pass.msfile}")
                    return True

                j2ms2_args = ["-v", str(a_pass.lisfile)]
                if not exp.eEVNname:
                    j2ms2_args.append("fo:nosquash_source_table")

                utils.shell_command("j2ms2", j2ms2_args, shell=True, stdout=None, stderr=subprocess.STDOUT, bufsize=0,
                                    logfile=exp.dirs.logs / "j2ms2.log")
                return True
            except Exception as e:
                logger.opt(exception=True).error(f"Error running j2ms2 for "
                                                 f"{a_pass.lisfile.name}: {e}")
                return False

        with ThreadPoolExecutor(utils.pass_workers(len(exp.correlator_passes),
                                                   utils.MAX_PASS_IO_WORKERS)) as pool:
            ms_futures = [pool.submit(_j2ms2_correlator_pass, (exp, p)) for p in exp.correlator_passes]

            # Create lag-space MS from first pass in parallel (for signal detection).
            # j2ms2 ignores '-o' when given an input .lis via '-v', so we build a dedicated
            # '{expname}-lag.lis' that already names the lag MS, and restrict it to the
            # calibrator sources via the 'fo:filter/source=...' directive.
            lag_ms = Path(f"{exp.expname.lower()}-lag.ms")
            lag_future = None
            if exp.no_lag:
                logger.info("--no-lag set: skipping creation of the lag-space MS.")
            elif not lag_ms.exists() and exp.correlator_passes[0].lisfile.exists():
                lag_lisfile = lisfiles.create_lag_lisfile(exp, exp.correlator_passes[0])
                # Register the lag pass as a dedicated, separate product (NOT a correlator
                # pass). It is kept out of exp.correlator_passes on purpose so it is never
                # counted as a real pass (multi_phase_center, pipeline input, msops, ...);
                # it exists solely for the per-scan antenna SNR computation (compute_lag_snr).
                exp.lag_pass = experiment.CorrelatorPass(
                    lisfile=lag_lisfile, msfile=lag_ms, fitsidifile="", pipeline=False)
                cal_sources = exp.sources.fringefinder + exp.sources.calibrator
                lag_args = ["-v", str(lag_lisfile), "-d", "frequency"]
                if cal_sources:
                    lag_args.append(f"fo:filter/source={','.join(cal_sources)}")
                if not exp.eEVNname:
                    lag_args.append("fo:nosquash_source_table")
                # Quiet: the lag MS run goes only to its log file (echo=False), so its
                # output does not garble the foreground pass's real-time terminal stream
                # while both run in parallel (Issue 7).
                lag_future = pool.submit(utils.shell_command, "j2ms2", lag_args,
                    shell=True, stdout=None, stderr=subprocess.STDOUT, bufsize=0,
                    logfile=exp.dirs.logs / "j2ms2-lag.log", echo=False)

            ms_results = [f.result() for f in ms_futures]
            if lag_future is not None:
                try:
                    lag_future.result()
                    logger.info(f"Created lag-space MS: {lag_ms}")
                except Exception as e:
                    logger.warning(f"Lag-space MS creation failed (non-fatal): {e}")

        return all(ms_results)
    except Exception as e:
        logger.opt(exception=True).error(f"Unexpected error in j2ms2: {e}")
        return False


def update_ms_expname(exp: experiment.Experiment) -> bool:
    """For e-EVN experiments, where the .vex-file experiment name does not match the actual
    experiment name, this one must be updated in the created MS file(s).

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if experiment names were updated successfully.
    """
    if (exp.eEVNname is not None) and (exp.eEVNname != exp.expname):
        with ThreadPoolExecutor(utils.pass_workers(len(exp.correlator_passes))) as executor:
            futures = [executor.submit(mstools.change_project_name, a_pass.msfile, exp.expname)
                       for a_pass in exp.correlator_passes]
            for fut in futures:
                fut.result()
        logger.info(f"Renamed project in MS to {exp.expname}")

    return True


def get_metadata_from_ms(exp: experiment.Experiment) -> bool:
    """Extracts metadata from MS files and populates the experiment object.

    Args:
        exp (experiment.Experiment): Experiment object to populate with MS metadata.

    Returns:
        bool: True if metadata was extracted successfully.
    """
    def _get_ms_metadata(exp: experiment.Experiment, a_pass: experiment.CorrelatorPass):
        try:
            ms = mstools.Ms(a_pass.msfile, runstats=True)
            for ant in ms.antennas:
                if ant.name not in a_pass.antennas:
                    # Convert mstools.Antenna to experiment.Antenna
                    exp_ant = experiment.Antenna(name=ant.name, observed=ant.observed,
                                                 subbands=ant.subbands, weights=ant.weights,
                                                 polconvert=ant.polconvert, polswap=ant.polswap,
                                                 onebit=ant.onebit, logfsfile=ant.logfsfile,
                                                 antabfsfile=ant.antabfsfile)
                    a_pass.antennas.append(exp_ant)
                else:
                    a_pass.antennas[ant.name].observed = ant.observed
                    a_pass.antennas[ant.name].subbands = ant.subbands
                    a_pass.antennas[ant.name].weights = ant.weights

            a_pass.freqsetup = experiment.Subbands(subbands=ms.freqsetup.nspw, channels=ms.freqsetup.nchan,
                                                   frequency=ms.freqsetup.meanfreq, bandwidth=ms.freqsetup.bandwidth,
                                                   polarizations=ms.freqsetup.polarizations)

            # Copy sources from MS to correlator pass
            a_pass.sources = experiment.Sources()
            for src in ms.sources:
                if src.name in exp.sources.names:
                    existing_source = exp.sources[src.name]
                    exp_src = experiment.Source(name=src.name, coordinates=src.coordinates,
                                               type=existing_source.type, protected=existing_source.protected,
                                               intent=src.intent)
                else:
                    exp_src = experiment.Source(name=src.name, coordinates=src.coordinates,
                                               type=experiment.SourceType.other, protected=False,
                                               intent=src.intent)
                a_pass.sources.append(exp_src)
        except Exception as e:
            logger.error(f"Error reading MS metadata from {a_pass.msfile}: {e}")
            raise

        # Populate a_pass.scans from exp.scans (VEX) + ms.scans (observed antennas).
        # exp.scans has scanno as str like "No0001"; ms.scans keys are ints like 1.
        vex_scanno_map = {int(s.scanno.replace('No', '')): s for s in exp.scans}
        a_pass.scans = experiment.Scans()
        for ms_scanno, ms_antennas in ms.scans.items():
            if ms_scanno in vex_scanno_map:
                vex_scan = vex_scanno_map[ms_scanno]
                a_pass.scans.append(experiment.Scan(scanno=vex_scan.scanno, starttime=vex_scan.starttime,
                                                    duration_s=vex_scan.duration_s, source=vex_scan.source,
                                                    stations_scheduled=vex_scan.stations_scheduled,
                                                    stations_observed=tuple(sorted(ms_antennas))))
            else:
                logger.warning(f"MS scan {ms_scanno} in {a_pass.msfile.name} has no matching VEX scan")

    def _update_mpc_pass(a_pass: experiment.CorrelatorPass):
        a_pass.antennas = exp.correlator_passes[0].antennas
        a_pass.sources = exp.correlator_passes[0].sources
        a_pass.freqsetup = exp.correlator_passes[0].freqsetup
        a_pass.scans = exp.correlator_passes[0].scans

    logger.debug(f"get_metadata_from_ms: {len(exp.correlator_passes)} passes, spectral_line={exp.spectral_line}")
    if len(exp.correlator_passes) > 1 and not exp.spectral_line:
        # then this is just a multiphase center with all setups identical. Do not loop
        # through all MSs.
        logger.debug("Using MPC path - extracting metadata from first pass only")
        _get_ms_metadata(exp, exp.correlator_passes[0])
        with ThreadPoolExecutor(utils.pass_workers(len(exp.correlator_passes) - 1)) as executor:
            for fut in [executor.submit(_update_mpc_pass, a_pass) for a_pass in exp.correlator_passes[1:]]:
                fut.result()
    else:
        logger.debug("Using standard path - extracting metadata from all passes")
        for a_pass in exp.correlator_passes:
            _get_ms_metadata(exp, a_pass)

    for exp_scan, ps in zip(exp.scans, exp.correlator_passes[0].scans):
        if ps.scanno == exp_scan.scanno:
            exp_scan.stations_observed = tuple(sorted(ps.stations_observed))

    # Antennas scheduled (from VEX) but absent from every MS get observed=False.
    for ant_name in exp.antennas.names:
        exp.antennas[ant_name].observed = any((ant_name in a_pass.antennas) and a_pass.antennas[ant_name].observed \
                                              for a_pass in exp.correlator_passes)
        if ant_name in exp.correlator_passes[0].antennas:
            exp.antennas[ant_name].subbands = exp.correlator_passes[0].antennas[ant_name].subbands
            exp.antennas[ant_name].weights = exp.correlator_passes[0].antennas[ant_name].weights

    # Also add any antenna that appeared in the MS but was not in VEX
    for a_pass in exp.correlator_passes:
        for ant in a_pass.antennas:
            if ant.name not in exp.antennas:
                exp.antennas.append(experiment.Antenna(name=ant.name, observed=ant.observed,
                                                       subbands=ant.subbands))

    # Pick a default reference antenna if none is set yet
    if not exp.refant:
        total_scans = len(exp.correlator_passes[0].scans)
        scan_counts = {ant: sum(1 for s in exp.correlator_passes[0].scans if ant in s.stations_observed)
                       for ant in exp.antennas.observed}
        full_coverage = {ant for ant, count in scan_counts.items() if count == total_scans}

        priority_ants = ('Ef', 'Ys', 'O8', 'Gb', 'At', 'Pt')
        primary = next((a for a in priority_ants if a in exp.antennas and exp.antennas[a].observed), None)

        if primary and primary in full_coverage:
            exp.refant = [primary]
        elif primary:
            exp.refant = [primary] + [a for a in priority_ants if a != primary and a in exp.antennas.observed] + \
                         [a for a in exp.antennas.observed if a not in priority_ants]
        else:
            exp.refant = list(exp.antennas.observed)

        logger.info(f"Auto-selected reference antenna(s): {', '.join(exp.refant)}")

    logger.info(f"Antennas observed: {', '.join(exp.antennas.observed)}")
    logger.info(f"Antennas NOT observed: {', '.join(n for n in exp.antennas.names if n not in exp.antennas.observed)}")
    if exp.refant:
        logger.info(f"Reference antenna: {exp.refant[0]}")

    exp.store()
    return True


def _derive_pol_diagnostics(ff_amp_sum: dict[int, np.ndarray], ff_amp_cnt: dict[int, int],
                            ff_best_snr: dict[int, float], ant_names: list[str],
                            pol_labels: list[str]) -> dict:
    """Turn the accumulated fringe-finder per-antenna polarization amplitudes into findings.

    For each antenna with a detected fringe (lag SNR >= _POL_MIN_SNR) we compare the mean
    parallel-hand (RR/LL) amplitude against the mean cross-hand (RL/LR) amplitude over the
    fringe-finder scans:

      * cross >> parallel               -> the R/L feeds are swapped (polswap),
      * all four products comparable     -> linear polarization, needs PolConvert
                                            (only for the _POLCONVERT_CANDIDATES antennas),
      * parallel dominates               -> normal circular feeds, no action.

    Returns a dict with the per-antenna measurements and the polswap/polconvert antenna lists.
    """
    parallel_idx = [i for i, l in enumerate(pol_labels) if l in _PARALLEL_POLS]
    cross_idx = [i for i, l in enumerate(pol_labels) if l in _CROSS_POLS]
    candidates = {c.upper() for c in _POLCONVERT_CANDIDATES}

    diag: dict = {'analyzed': False, 'polswap': [], 'polconvert': [], 'antennas': {}}
    # Need full polarization (both parallel- and cross-hand products) and at least one
    # measured antenna; otherwise the comparison is impossible (e.g. dual-pol-only data).
    if not (parallel_idx and cross_idx and ff_amp_cnt):
        return diag

    diag['analyzed'] = True
    for aidx, amp_sum in sorted(ff_amp_sum.items()):
        cnt = ff_amp_cnt.get(aidx, 0)
        if cnt == 0:
            continue
        name = ant_names[aidx]
        mean_amp = amp_sum / cnt
        par = float(np.mean(mean_amp[parallel_idx]))
        crs = float(np.mean(mean_amp[cross_idx]))
        snr = round(float(ff_best_snr.get(aidx, 0.0)), 1)
        ratio = (crs / par) if par > 0 else None
        decision = 'undetermined'
        if snr >= _POL_MIN_SNR and ratio is not None:
            if ratio >= _POLSWAP_RATIO:
                decision = 'polswap'
                diag['polswap'].append(name)
            elif ratio >= _LINEAR_RATIO_LOW and name.upper() in candidates:
                decision = 'polconvert'
                diag['polconvert'].append(name)
            else:
                decision = 'normal'
        diag['antennas'][name] = {'parallel': round(par, 4), 'cross': round(crs, 4),
                                  'ratio': round(ratio, 3) if ratio is not None else None,
                                  'snr': snr, 'decision': decision}
    return diag


def compute_lag_snr(exp: experiment.Experiment) -> bool:
    """Compute lag-space SNR and polarization diagnostics per antenna from the lag MS.

    The lag MS (``j2ms2 -d frequency``) stores the complex cross-power spectrum
    per integration/subband. For each cross-correlation baseline this Fourier
    transforms the spectrum over the frequency (channel) axis into delay (lag)
    space, where a fringe is a sharp peak, and incoherently averages the |lag|
    spectra of all that baseline's rows. The per-pol SNR of the averaged spectrum
    (fringe peak / robust noise) is then taken; the maximum SNR per antenna (over
    its baselines) is stored per scan and polarization into ``exp.lag_snr``.

    In the same pass, for the fringe-finder scans it accumulates the per-antenna
    fringe-peak amplitude of each polarization product and derives automatic
    polarization findings (polswap / polconvert), stored in ``exp.pol_diagnostics``
    (see :func:`_derive_pol_diagnostics`).

    SNR is estimated as peak / (1.4826 × MAD) over the averaged delay spectrum.
    The FFT-before-magnitude and per-baseline incoherent averaging are both
    essential: taking |DATA| first, or a per-row maximum, makes pure noise read
    as a detection (every antenna SNR > 7).

    For the fringe-finder scans it additionally records, per antenna, the parallel-hand
    fringe-peak amplitude in each IF (``exp.lag_bandpass``), reported alongside the PolConvert
    reference antenna as a bandpass-flatness diagnostic (see :func:`_refant_bandpass_scatter`).

    Args:
        exp: Experiment object. Results stored in ``exp.lag_snr``, ``exp.lag_bandpass``
            and ``exp.pol_diagnostics``.

    Returns:
        True if computation succeeded (including when lag MS is absent).
    """
    # Use the dedicated lag pass if registered (see j2ms2); fall back to the conventional
    # name so the step still works on experiments processed before lag_pass was tracked.
    lag_ms = exp.lag_pass.msfile if exp.lag_pass is not None else Path(f"{exp.expname.lower()}-lag.ms")
    if not lag_ms.exists():
        logger.warning(f"Lag MS {lag_ms} not found. Skipping lag SNR computation.")
        return True


    with mstools.misc.table(lag_ms) as ms:
        with mstools.misc.table(ms.getkeyword('ANTENNA')) as t:
            ant_names = list(t.getcol('NAME'))
        with mstools.misc.table(ms.getkeyword('POLARIZATION')) as t:
            pol_labels = [mstools.misc.Stokes(ct).name for ct in t.getcol('CORR_TYPE')[0]]
        with mstools.misc.table(ms.getkeyword('FIELD')) as t:
            field_names = list(t.getcol('NAME'))
        with mstools.misc.table(ms.getkeyword('SPECTRAL_WINDOW')) as t:
            n_spw = t.nrows()
        # FIELD_IDs that correspond to fringe-finder sources (used for the pol diagnostics).
        ff_names = set(exp.sources.fringefinder)  # built once, checked per field
        ff_field_ids = {fid for fid, name in enumerate(field_names) if name in ff_names}

        # Per-baseline accumulators. The lag MS holds the *complex* cross-power spectrum per
        # integration/subband; a fringe is a sharp peak in DELAY (lag) space, i.e. the Fourier
        # transform of that spectrum over the frequency (channel) axis. The fringe delay lives
        # in the spectral *phase*, so we must FFT each spectrum over the channel axis BEFORE
        # taking the magnitude — taking |DATA| first (as the old code did) only leaves a smooth
        # bandpass with no delay peak, and its scale-invariant peak/MAD cannot tell a strong
        # fringe from pure noise. We then incoherently average the |lag| spectra of all rows
        # (subbands/integrations) of each baseline so the noise floor settles to a stable, low
        # level; a per-row maximum would instead let the largest of thousands of noise samples
        # cross the detection threshold (the bug that made every antenna read SNR > 7).
        # acc_sum[(scan, a1, a2)] = running sum of |FFT_freq(DATA)|, shape (nchan, npol).
        acc_sum: dict[tuple[int, int, int], np.ndarray] = {}
        acc_cnt: dict[tuple[int, int, int], int] = {}
        acc_ff: dict[tuple[int, int, int], bool] = {}
        # Per-IF (per spectral window) accumulators, kept ONLY for fringe-finder baselines so
        # the memory stays small. They feed the per-antenna bandpass (amplitude vs IF) used to
        # pick the PolConvert reference antenna. Keyed by (scan, a1, a2, ddid) so each IF stays
        # separate; summing these back over ddid would reproduce acc_sum exactly.
        acc_if_sum: dict[tuple[int, int, int, int], np.ndarray] = {}
        acc_if_cnt: dict[tuple[int, int, int, int], int] = {}

        for start, nrow in mstools.misc.chunkert(0, len(ms), 1000):
            ants1 = ms.getcol('ANTENNA1', startrow=start, nrow=nrow)
            ants2 = ms.getcol('ANTENNA2', startrow=start, nrow=nrow)
            scans = ms.getcol('SCAN_NUMBER', startrow=start, nrow=nrow)
            fields = ms.getcol('FIELD_ID', startrow=start, nrow=nrow)
            # DATA_DESC_ID indexes the spectral window (IF); on EVN lag MSs ddid == spw index.
            ddids = ms.getcol('DATA_DESC_ID', startrow=start, nrow=nrow)

            cross = ants1 != ants2
            if not np.any(cross):
                continue

            # FFT over the frequency (channel) axis -> delay/lag space, then magnitude.
            data = ms.getcol('DATA', startrow=start, nrow=nrow)  # complex (nrow, nchan, npol)
            lag = np.abs(np.fft.fft(data[cross], axis=1)).astype(np.float32)  # (n_cross, nchan, npol)

            cross_scans, cross_a1, cross_a2 = scans[cross], ants1[cross], ants2[cross]
            cross_fields, cross_ddids = fields[cross], ddids[cross]
            for i in range(len(cross_scans)):
                bl_key = (int(cross_scans[i]), int(cross_a1[i]), int(cross_a2[i]))
                is_ff_row = int(cross_fields[i]) in ff_field_ids
                if bl_key not in acc_sum:
                    acc_sum[bl_key] = lag[i].copy()
                    acc_cnt[bl_key] = 1
                    acc_ff[bl_key] = is_ff_row
                else:
                    acc_sum[bl_key] += lag[i]
                    acc_cnt[bl_key] += 1
                if is_ff_row:
                    ifkey = (bl_key[0], bl_key[1], bl_key[2], int(cross_ddids[i]))
                    if ifkey not in acc_if_sum:
                        acc_if_sum[ifkey] = lag[i].copy()
                        acc_if_cnt[ifkey] = 1
                    else:
                        acc_if_sum[ifkey] += lag[i]
                        acc_if_cnt[ifkey] += 1

    # Reduce each baseline's mean |lag| spectrum to a per-pol SNR (fringe peak / robust noise)
    # and a per-pol fringe-peak amplitude, then keep, for each (scan, antenna), the strongest
    # baseline. Max-over-baselines is safe here because every baseline's noise floor is now a
    # stable ~few-sigma value after the incoherent average, so a dead antenna stays well below
    # the threshold while a detection on any baseline lifts the antenna above it.
    best_snr: dict[tuple[int, int], np.ndarray] = {}
    ff_amp_sum: dict[int, np.ndarray] = {}
    ff_amp_cnt: dict[int, int] = {}
    ff_best_snr: dict[int, float] = {}
    for (scan, a1i, a2i), spec_sum in acc_sum.items():
        spec = spec_sum / acc_cnt[(scan, a1i, a2i)]      # mean |lag| spectrum (nchan, npol)
        peak = np.max(spec, axis=0)                       # (npol,) fringe-peak amplitude per pol
        noise = 1.4826 * np.median(np.abs(spec - np.median(spec, axis=0)), axis=0)
        snr = np.divide(peak, noise, out=np.zeros_like(peak, dtype=float), where=noise > 0)
        is_ff = acc_ff[(scan, a1i, a2i)]
        snr_max = float(np.max(snr)) if is_ff else 0.0
        for aidx in (a1i, a2i):
            ant_key = (scan, aidx)
            if ant_key not in best_snr:
                best_snr[ant_key] = snr.copy()
            else:
                np.maximum(best_snr[ant_key], snr, out=best_snr[ant_key])
            if is_ff:
                if aidx not in ff_amp_sum:
                    ff_amp_sum[aidx] = np.zeros(spec.shape[1])
                    ff_amp_cnt[aidx] = 0
                    ff_best_snr[aidx] = 0.0
                ff_amp_sum[aidx] += peak
                ff_amp_cnt[aidx] += 1
                if snr_max > ff_best_snr[aidx]:
                    ff_best_snr[aidx] = snr_max

    # Convert to nested dict: {scan_str: {ant_name: {pol: snr}}}
    lag_snr: dict[str, dict[str, dict[str, float]]] = {}
    for (scan, aidx), snr_arr in best_snr.items():
        scan_str = str(scan)
        if scan_str not in lag_snr:
            lag_snr[scan_str] = {}
        lag_snr[scan_str][ant_names[aidx]] = {p: round(float(snr_arr[i]), 1) for i, p in enumerate(pol_labels)}

    # Per-antenna bandpass on the fringe-finder scans: the parallel-hand (RR/LL) fringe-peak
    # amplitude in each IF, taken on the antenna's strongest baseline for that IF. The PolConvert
    # reference antenna is chosen on fringe strength, and this is logged next to it as a
    # bandpass-flatness diagnostic; see _refant_bandpass_scatter.
    parallel_idx = [i for i, l in enumerate(pol_labels) if l in _PARALLEL_POLS]
    bp_amp: dict[tuple[int, int], np.ndarray] = {}  # (scan, antenna) -> per-IF amplitude
    for (scan, a1i, a2i, ddid), spec_sum in acc_if_sum.items():
        spec = spec_sum / acc_if_cnt[(scan, a1i, a2i, ddid)]   # mean |lag| spectrum (nchan, npol)
        peak = np.max(spec, axis=0)                            # (npol,) fringe-peak per pol
        par = float(np.mean(peak[parallel_idx])) if parallel_idx else float(np.max(peak))
        for aidx in (a1i, a2i):
            ant_key = (scan, aidx)
            if ant_key not in bp_amp:
                bp_amp[ant_key] = np.full(n_spw, np.nan, dtype=float)
            cur = bp_amp[ant_key][ddid]
            if np.isnan(cur) or par > cur:        # keep the strongest baseline per IF
                bp_amp[ant_key][ddid] = par

    lag_bandpass: dict[str, dict[str, list]] = {}
    for (scan, aidx), amps in bp_amp.items():
        # None (not NaN) for IFs without data, so the result stays valid JSON.
        lag_bandpass.setdefault(str(scan), {})[ant_names[aidx]] = \
            [round(float(x), 5) if np.isfinite(x) else None for x in amps]

    exp.lag_snr = lag_snr
    exp.lag_bandpass = lag_bandpass
    exp.pol_diagnostics = _derive_pol_diagnostics(ff_amp_sum, ff_amp_cnt, ff_best_snr,
                                                  ant_names, pol_labels)
    logger.info(f"Lag SNR computed for {len(lag_snr)} scans from {lag_ms}")
    if exp.pol_diagnostics.get('analyzed'):
        pd = exp.pol_diagnostics
        logger.info(f"Polarization diagnostics: polswap={pd['polswap'] or 'none'}, "
                    f"polconvert={pd['polconvert'] or 'none'} "
                    f"(from {len(pd['antennas'])} antennas on fringe-finder scans).")
    exp.store()
    return True


def standardplots(exp: experiment.Experiment, do_weights=True) -> bool:
    """Runs the standardplots on the specified experiment using Jplot.

    For each pipelinable correlator pass, discovers all scans containing the
    fringe-finder sources and creates per-scan plots (with refant fallback).
    The scan number is embedded in every output filename.

    Args:
        exp (experiment.Experiment): Experiment object.
        do_weights (bool): Whether to include weight plots. Default True.

    Returns:
        bool: True if standardplots completed successfully, False otherwise.
    """

    if not exp.refant:
        logger.error("No reference antenna set. Use 'postprocess edit refant <ANT>' first.")
        return False

    refant = exp.refant[0]
    counter = 0
    for a_pass in exp.correlator_passes:
        try:
            if not a_pass.pipeline:
                continue

            calsources = a_pass.sources.fringefinder if a_pass.sources else exp.sources.fringefinder

            if not calsources:
                logger.error(f"No fringe-finder sources found for {a_pass.msfile.name}. "
                       "Set them with 'postprocess edit fringefinder <SRC>'.")
                return False

            counter += 1
            logger.info(f"standardplots {a_pass.msfile.name} refant={refant} "
                        f"calsrc={','.join(calsources)} weights={do_weights and counter == 1}")

            plotter = plotting.Jplot(ms=str(a_pass.msfile.name), refant=refant, calsrc=','.join(calsources),
                            weight_plots=(do_weights and counter == 1))

            if not plotter.create_plot(sources=calsources):
                logger.error(f"Standardplots failed for {a_pass.msfile.name}")
                return False

            # Retrieve the summary into a log file
            logger.info(utils.shell_command("echo", [f'"ms {a_pass.msfile.name};r"', "|", "jplotter"],
                                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT))
        except Exception as e:
            logger.opt(exception=True).error(f"Standardplots reported an error: {e}")
            return False

    return True


def print_exp(exp: experiment.Experiment, display_in_terminal: bool = True) -> bool:
    """Shows in the terminal all metadata related to the given experiment.
    """
    return exp.print_blessed(outputfile='notes.md', display_in_terminal=display_in_terminal)


def open_standardplot_files(exp) -> bool:
    """Converts PS plots to PNG, then launches a web dashboard for reviewing them.

    The dashboard shows experiment metadata (same as print_blessed), a scan overview
    table, and a plot viewer with selectors for plot type and scan number.
    The server runs until the user presses Ctrl+C.

    Args:
        exp: experiment.Experiment object.

    Returns:
        bool: True after the dashboard server is stopped by the user.
    """

    standardplots = []
    for plot_type in ('weight', 'auto', 'cross', 'ampphase', 'amptime'):
        standardplots += glob.glob(f"{exp.expname.lower()}*{plot_type}*.ps")

    if len(standardplots) == 0:
        raise FileNotFoundError(f"Standardplots for {exp.expname} not found but expected.")

    plotting.convert_ps_to_png(exp.dirs.plots, exp.expname.lower())
    # rprint("\n[bold yellow]Take a look at the produced standard plots:[/bold yellow]")
    # rprint(f"[yellow]{'\n- '.join([aplot for aplot in standardplots])}"
    #        "\nOpening the dashboard in your browser...[/yellow]")
    plotting.serve_dashboard(exp, exp.dirs.plots)
    return True


def open_pipeline_dashboard(exp) -> bool:
    """Launches the web dashboard after the pipeline has run, with the pipeline feedback
    page shown as a new "Pipeline" tab on top of the standard plots.

    Reuses the same dashboard served before msops (see :func:`open_standardplot_files`),
    so the user only needs the SSH tunnel printed by the server to review both the
    standard plots and the pipeline feedback page in their browser.

    Args:
        exp: experiment.Experiment object.

    Returns:
        bool: True after the dashboard server is stopped by the user.
    """
    plotting.serve_dashboard(exp, exp.dirs.plots, pipeline_dir=exp.dirs.pipe_out)
    return True


def onebit(exp: experiment.Experiment) -> bool:
    """In case some stations recorded at 1 bit, scales 1-bit data to correct for
    quantization losses in all MS associated with the given experiment name.

    Args:
        exp (experiment.Experiment): Experiment object with antenna information.

    Returns:
        bool: True if scaling was applied successfully, None if user intervention needed.
    """
    # Sanity check
    if len(exp.antennas.onebit) > 0:
        with ThreadPoolExecutor(utils.pass_workers(len(exp.correlator_passes))) as executor:
            futures = [executor.submit(mstools.scale1bit, a_pass.msfile, exp.antennas.onebit)
                      for a_pass in exp.correlator_passes]
            for fut in futures:
                fut.result()  # Propagate any exceptions
        logger.info(f"scale1bit {' '.join(exp.antennas.onebit)}")
    elif utils.station_1bit_in_vix(exp.vixfile):
        logger.error(f"Traces of 1bit station found in {exp.vixfile} "
                     "but no station specified to be corrected.")
        return False

    return True


def ysfocus(exp: experiment.Experiment) -> bool:
    """Fix mount types for Yebes and Hobart antennas.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if mount types were fixed successfully.
    """
    if ('Ys' not in exp.antennas.names) and ('Ho' not in exp.antennas.names) and ('Hb' not in exp.antennas.names):
        return True

    def _fix_mounts(a_pass):
        if 'Ys' in exp.antennas.names:
            logger.info(f"Fixing yebes mount for {a_pass.msfile}")
            mstools.fix_yebes_mount(a_pass.msfile)
        if ('Ho' in exp.antennas.names) or ('Hb' in exp.antennas.names):
            logger.info(f"Fixing hobart mount for {a_pass.msfile}")
            mstools.fix_hobart_mount(a_pass.msfile)

    with ThreadPoolExecutor(utils.pass_workers(len(exp.correlator_passes))) as executor:
        for fut in [executor.submit(_fix_mounts, a_pass) for a_pass in exp.correlator_passes]:
            fut.result()  # Propagate any exceptions

    return True


def _swapped_scans(exp: experiment.Experiment, antenna: str) -> list[tuple[experiment.Scan, bool]]:
    """The scans where *antenna* has enough signal, each flagged as swapped or not.

    Reads the per-polarization lag SNRs already in ``exp.lag_snr``: a swapped antenna
    shows its signal in the cross-hand products (RL, LR) instead of the parallel-hand
    ones (RR, LL). Scans too weak to decide (or without full polarization) are left out.

    Args:
        exp: Experiment object.
        antenna: Antenna name.

    Returns:
        [(scan, swapped)] in observing order; empty when the lag analysis has no usable
        data for this antenna (e.g. --no-lag).
    """
    scans = []
    for scan in exp.scans:
        snr = exp.lag_snr.get(str(_scan_number(scan)), {}).get(antenna, {})
        parallel = [v for pol, v in snr.items() if pol in _PARALLEL_POLS]
        cross = [v for pol, v in snr.items() if pol in _CROSS_POLS]
        if parallel and cross and max(snr.values()) >= _POL_MIN_SNR:
            scans.append((scan, np.mean(cross) >= _POLSWAP_RATIO * np.mean(parallel)))
    return scans


def polswap_check(exp: experiment.Experiment) -> bool:
    """Works out, per antenna, over which time range the polarization swap applies.

    A station that swapped its polarizations often fixes it partway through the run, so
    swapping the whole observation would corrupt the part that was already correct. For
    every antenna marked for polswap this compares the first and last scans with enough
    signal (see :func:`_swapped_scans`):

      * both swapped -> the swap covers the whole observation;
      * exactly one change of state -> the swap covers everything before (or after) the
        scan where it changed, which becomes the end (or start) time;
      * no scan swapped, or several changes of state -> a warning, and the whole
        observation is swapped (the operator decides from the log).

    The resulting range is stored in ``exp.pol_diagnostics`` (as ISO strings, so it stays
    JSON-serializable) and applied by :func:`polswap`.

    Args:
        exp: Experiment object.

    Returns:
        True (informational: it never fails the step).
    """
    for antenna in exp.antennas.polswap:
        scans = _swapped_scans(exp, antenna)
        changes = [i for i in range(1, len(scans)) if scans[i][1] != scans[i - 1][1]]
        start = end = None
        if not scans:
            logger.warning(f"polswap {antenna}: no scan has enough signal to tell when the swap "
                           "applies; swapping the whole observation.")
        elif len(changes) > 1:
            logger.warning(f"polswap {antenna}: the polarizations change back and forth "
                           f"({len(changes)} times); swapping the whole observation. Check the "
                           f"scans {', '.join(scans[i][0].scanno for i in changes)}.")
        elif not any(swapped for _, swapped in scans):
            logger.warning(f"polswap {antenna}: no scan actually looks swapped, but the antenna "
                           "is marked for polswap; swapping the whole observation.")
        elif changes:
            # One change of state: the swap covers everything on the side that is swapped.
            if scans[0][1]:
                end = scans[changes[0]][0].starttime
            else:
                start = scans[changes[0]][0].starttime
            logger.info(f"polswap {antenna}: the polarizations change at scan "
                        f"{scans[changes[0]][0].scanno}; swapping "
                        f"{'until' if end else 'from'} "
                        f"{(end or start).strftime('%d/%m/%Y %H:%M:%S')} UTC.")
        else:
            logger.info(f"polswap {antenna}: swapped in all {len(scans)} checked scans; "
                        "swapping the whole observation.")

        exp.pol_diagnostics.setdefault('antennas', {}).setdefault(antenna, {})['polswap_range'] = \
            [t.isoformat() if t else None for t in (start, end)]
    return True


def polswap_range(exp: experiment.Experiment, antenna: str) -> tuple[Optional[datetime], Optional[datetime]]:
    """The (start, end) time range over which *antenna* must be swapped.

    ``None`` on either side means unbounded, i.e. from the beginning / until the end of
    the observation. Set by :func:`polswap_check`; unset means the whole observation.
    """
    times = exp.pol_diagnostics.get('antennas', {}).get(antenna, {}).get('polswap_range')
    return tuple(datetime.fromisoformat(t) if t else None for t in (times or [None, None]))


def polswap(exp: experiment.Experiment) -> bool:
    """Swaps the polarizations of the flagged antennas in every MS of the experiment.

    Each antenna is swapped only over the time range :func:`polswap_check` found it to be
    swapped in (the whole observation unless the station fixed it partway through).

    Args:
        exp (experiment.Experiment): Experiment object with polswap antenna information.

    Returns:
        bool: True if the polarization swap was applied successfully.
    """
    if not exp.antennas.polswap:
        return True

    polswap_check(exp)

    def _polswap_pass(a_pass):
        for antenna in exp.antennas.polswap:
            mstools.polswap(a_pass.msfile, antenna, *polswap_range(exp, antenna))

    with ThreadPoolExecutor(utils.pass_workers(len(exp.correlator_passes))) as executor:
        for fut in [executor.submit(_polswap_pass, a_pass) for a_pass in exp.correlator_passes]:
            fut.result()  # Propagate any exceptions

    logger.info(f"polswap {', '.join(exp.antennas.polswap)}")
    exp.store()
    return True


def flag_weights(exp: experiment.Experiment) -> bool:
    """Flags visibilities based on weight thresholds for all correlator passes.

    Skips passes where flag_weights was already applied with the same threshold
    (i.e. flagged_weights.percentage != -1).

    Args:
        exp (experiment.Experiment): Experiment object with flagged_weights information.

    Returns:
        bool: True if weight flagging was applied successfully.
    """
    def _flag_weights_pass(a_pass):
        if a_pass.flagged_weights.percentage >= 0:
            logger.info(f"flag_weights: {a_pass.msfile.name} already flagged with "
                        f"threshold={a_pass.flagged_weights.threshold} "
                        f"({a_pass.flagged_weights.percentage:.2f}% non-zero flagged). Skipping.")
            return

        total_vis, pct_total, pct_nonzero = mstools.flag_weights(a_pass.msfile, a_pass.flagged_weights.threshold)
        a_pass.flagged_weights.percentage = pct_nonzero
        logger.info(f"flag_weights: {a_pass.msfile.name} threshold={a_pass.flagged_weights.threshold}\n"
                f"# {pct_total:.2f}% total flagged, {pct_nonzero:.2f}% non-zero weights flagged\n")

    # TODO: check if this is IO or CPU bound
    with ThreadPoolExecutor(utils.pass_workers(len(exp.correlator_passes))) as executor:
        for fut in [executor.submit(_flag_weights_pass, a_pass) for a_pass in exp.correlator_passes]:
            fut.result()  # Propagate any exceptions
    return True


def _du_kbytes(path: Path | str) -> int:
    """Returns the disk usage of *path* in kilobytes via ``du -s``.

    The previous implementation called ``subprocess.run("du -s ...", shell=True)``,
    decoded the stdout, and indexed straight into ``split()[0]`` with no error
    handling. A flaky shell or an empty/unicode-error stdout would crash the step
    with an opaque ``IndexError``/``ValueError``. This helper returns 0 (and logs
    a warning) on failure so the surrounding logic can pick a sane default chunk
    size instead of aborting tconversion.
    """
    try:
        result = subprocess.run(["du", "-s", str(path)], capture_output=True, text=True, timeout=120)
    except (subprocess.TimeoutExpired, OSError) as e:
        logger.warning(f"`du -s {path}` failed: {e}; defaulting size estimate to 0 kB.")
        return 0
    if result.returncode != 0:
        logger.warning(f"`du -s {path}` exited with {result.returncode}; "
                       f"stderr={result.stderr.strip()!r}; defaulting size estimate to 0 kB.")
        return 0
    parts = result.stdout.split()
    if not parts:
        logger.warning(f"`du -s {path}` produced no output; defaulting size estimate to 0 kB.")
        return 0
    try:
        return int(parts[0])
    except ValueError:
        logger.warning(f"Could not parse `du -s {path}` output {result.stdout!r}; "
                       "defaulting size estimate to 0 kB.")
        return 0


def _tconvert_chunk_arg(a_pass: experiment.CorrelatorPass) -> str:
    """Returns the ``chunk_size=...`` tConvert option for a pass, scaled to the IDI size.

    Args:
        a_pass: Correlator pass whose MS size drives the chunk size.

    Returns:
        The ``chunk_size=<n>GB`` string to pass to tConvert via ``-o``.

    Raises:
        IOError: If the >4 TB case would not fit in the current directory.
    """
    # The size difference between internal MS and FITS-IDI is around 1.55
    idi_size = 1.55 * u.kbit * _du_kbytes(a_pass.msfile)
    if idi_size < 20*u.Gb:
        return "chunk_size=4GB"
    elif idi_size < 4*u.Tb:
        return "chunk_size=8GB"
    else:
        if utils.space_available(Path.cwd()) <= 1.1*idi_size:
            raise IOError("Not enough disk space to create the FITS-IDI files.")
        return f"chunk_size={int(idi_size.to(u.Tb).value)}GB"


def tconvert(exp: experiment.Experiment) -> bool:
    """Runs tConvert on all correlator passes to create FITS-IDI files from the MS.

    Selects chunk_size based on the estimated FITS-IDI size. Passes whose FITS-IDI files
    already exist are skipped, so the step is idempotent and can be re-run on its own.

    The passes run concurrently under the same ceiling as :func:`j2ms2`
    (``utils.MAX_PASS_IO_WORKERS``): one tConvert subprocess per pass, bounded by the disk
    throughput they share rather than by the cores. tConvert is verbose about its progress,
    and several of those streams interleaved on one terminal are unreadable, so the live
    output is kept only while a single pass is converting; from two upwards each pass writes
    to its own ``logs/tconvert.log`` sibling instead (the log names the .lis file it ran).
    Past ``_TCONVERT_PROGRESS_MIN_PASSES`` passes a Rich progress bar replaces that silence,
    showing how many are done and how long the rest should take.

    A pass that fails does not abandon the others: every failure is collected and they are
    all named together at the end, and the step then reports the failure.

    Args:
        exp: Experiment object.

    Returns:
        True if all passes converted successfully.

    Raises:
        IOError: If a pass would not fit in the current directory (see
            :func:`_tconvert_chunk_arg`).
    """
    passes = [a_pass for a_pass in exp.correlator_passes
              if len(glob.glob(f"{a_pass.fitsidifile}*")) == 0]
    if not passes:
        logger.info("FITS-IDI files already exist for every correlator pass. Skipping tConvert.")
        return True

    # Resolved before any conversion starts: the chunk size is where the "does this still
    # fit on disk" check lives, and a pass that cannot fit has to stop the step outright
    # rather than raise out of a worker with the other conversions already running.
    chunk_args = [_tconvert_chunk_arg(a_pass) for a_pass in passes]
    echo = len(passes) == 1
    show_bar = len(passes) > _TCONVERT_PROGRESS_MIN_PASSES

    def _tconvert_pass(a_pass: experiment.CorrelatorPass, chunk_arg: str) -> None:
        # With the bar up this line is the one thing that would scroll it away, and the bar
        # already says how many passes are done; it stays in the debug log either way.
        logger.log('DEBUG' if show_bar else 'INFO',
                   f"tConvert: {a_pass.lisfile.name} -> {a_pass.fitsidifile}*")
        utils.shell_command(_TCONVERT_BIN, ["-v", a_pass.lisfile.name, "-o", chunk_arg],
                            stdout=None, stderr=subprocess.STDOUT,
                            logfile=exp.dirs.logs / "tconvert.log", echo=echo)

    if not echo:
        logger.info(f"Converting {len(passes)} correlator passes at once; their output goes "
                    f"to {exp.dirs.logs / 'tconvert.log'} (one file per pass) instead of the "
                    "terminal, where the streams would be interleaved.")

    # One pass failing must not hide the others: every failure is collected and they are all
    # reported together once the conversions that did work have finished.
    errors: list[str] = []
    with ThreadPoolExecutor(utils.pass_workers(len(passes), utils.MAX_PASS_IO_WORKERS)) as pool:
        futures = {pool.submit(_tconvert_pass, a_pass, chunk): a_pass
                   for a_pass, chunk in zip(passes, chunk_args)}
        with progress.Progress(progress.SpinnerColumn(),
                               progress.TextColumn("[progress.description]{task.description}"),
                               progress.BarColumn(), progress.MofNCompleteColumn(),
                               progress.TextColumn("passes"), progress.TimeElapsedColumn(),
                               progress.TimeRemainingColumn(),
                               disable=not show_bar) as bar:
            task = bar.add_task("[green]tConvert", total=len(passes))
            for future in as_completed(futures):
                a_pass = futures[future]
                try:
                    future.result()
                except Exception as e:
                    errors.append(f"{a_pass.lisfile.name} -> {a_pass.fitsidifile}*: {e}")
                bar.advance(task)

    if errors:
        logger.error(f"tConvert failed on {len(errors)} of the {len(passes)} correlator "
                     f"pass(es) (the rest converted; see {exp.dirs.logs / 'tconvert.log'}):")
        for error in errors:
            logger.error(f"    {error}")
        return False
    return True


def _get_all_fringefinder_scans(exp: experiment.Experiment) -> list[experiment.Scan]:
    """Get all fringe-finder scans sorted by number of observing antennas (descending).

    Args:
        exp: Experiment object.

    Returns:
        List of Scan objects on fringe-finder sources, sorted by observed station count.
    """
    ff_sources = exp.sources.fringefinder
    if not ff_sources:
        return []

    ff_scans = []
    for scan in exp.scans:
        if scan.source in ff_sources:
            ff_scans.append(scan)

    # Sort by number of observed stations (descending)
    ff_scans.sort(key=lambda s: len(s.stations_observed) if s.stations_observed else len(s.stations_scheduled),
                  reverse=True)

    return ff_scans


def _aips_timerange(start: datetime, end: datetime, obsdate) -> list[int]:
    """Convert a start/end datetime pair to the AIPS 8-element time range.

    AIPS format: ``[day, hour, minute, second, day, hour, minute, second]`` where day counts
    from the beginning of the observation (0 on the first day).

    Args:
        start: Range start.
        end: Range end.
        obsdate: Observation start date (datetime.date), the origin of the day counter.

    Returns:
        8-element list in AIPS time format.
    """
    obs_midnight = datetime.combine(obsdate, datetime.min.time())

    def _to_aips(t: datetime) -> list[int]:
        days, rem = divmod(int((t - obs_midnight).total_seconds()), 86400)
        hours, rem = divmod(rem, 3600)
        return [days, hours, rem // 60, rem % 60]

    return _to_aips(start) + _to_aips(end)


def _polconvert_time_ranges(scan: experiment.Scan, obsdate) -> list[list[int]]:
    """Tentative time ranges to solve the PolConvert bandpass on, best-first, for one scan.

    All three candidates avoid the first minute of the scan, where antennas are frequently
    still settling: the last minute alone (short, and the most stable part of the scan), then
    the minute around the middle of the scan (the part several of the solutions found by hand
    used, and the one that avoids antennas slewing away early), then everything after that
    first minute. On a short scan these collapse into each other, so duplicates are dropped;
    a scan no longer than the trim contributes its full range unchanged.

    Args:
        scan: Scan object (starttime + duration_s as scheduled in the vex).
        obsdate: Observation start date (datetime.date).

    Returns:
        List of 8-element AIPS time ranges, in the order they should be tried.
    """
    trim = timedelta(minutes=_POLCONVERT_TRIM_MIN)
    start = scan.starttime
    end = scan.starttime + timedelta(seconds=scan.duration_s)
    if scan.duration_s <= trim.total_seconds():
        return [_aips_timerange(start, end, obsdate)]
    middle = start + (end - start) / 2
    ranges = [_aips_timerange(end - trim, end, obsdate),
              _aips_timerange(middle - trim / 2, middle + trim / 2, obsdate),
              _aips_timerange(start + trim, end, obsdate)]
    return [list(r) for r in dict.fromkeys(tuple(r) for r in ranges)]


def _pc_ants(names) -> list[str]:
    """Antenna names as PolConvert needs them: upper case, the way FITS-IDI spells them.

    ``exp.antennas`` carries the mixed-case vex spelling ('Ef', 'Jb'), while the ANTENNA and
    ARRAY_GEOMETRY tables of a FITS-IDI hold 'EF', 'JB'. PolConvert matches the names it is
    given against those tables literally, so every antenna handed to it goes through here —
    and so does every antenna *reported* about a run, so what is logged cannot drift from
    what was actually passed.
    """
    return [name.upper() for name in names]


def _aips_timerange_str(time_range: list[int]) -> str:
    """The AIPS 8-element time range as a readable ``d/hh:mm:ss - d/hh:mm:ss``.

    The raw ``[0, 17, 0, 0, 0, 17, 5, 0]`` that PolConvert wants says very little to whoever
    is reading the log to work out which part of which scan was tried.
    """
    if len(time_range) != 8:
        return str(time_range)
    day0, day1 = time_range[0], time_range[4]
    stamps = [f"{d}/{h:02d}:{m:02d}:{sec:02d}" if (day0 or day1) else f"{h:02d}:{m:02d}:{sec:02d}"
              for d, h, m, sec in (time_range[:4], time_range[4:])]
    return " - ".join(stamps)


def _write_polconvert_template(exp: experiment.Experiment, ref_idi: str, lin_ants: list, refant: str,
                               exclude_ants: list, do_ifs: list, time_range: list, chan_avg: int,
                               time_avg: int, solve_weight: float, logdir: str,
                               output_file: Path = Path('polconvert_inputs.toml')) -> Path:
    """Write the PolConvert input TOML file from the template with the given parameters.

    Values are formatted as valid TOML: strings are single-quoted, lists use TOML array syntax.
    The written file can be passed directly to 'polconvert.py <file> --compute' or '--apply'.

    Args:
        exp: Experiment object (used for expname in the idi_files wildcard).
        ref_idi: Resolved FITS-IDI filename containing the fringe-finder scan.
        lin_ants: Antenna names observing linear polarization.
        refant: Reference antenna name.
        exclude_ants: Antennas to exclude during computation.
        do_ifs: IF numbers to process (1-indexed, AIPS convention).
        time_range: AIPS-format time range (8-element int list).
        chan_avg: Channel averaging for bandpass solution.
        time_avg: Time averaging in seconds.
        solve_weight: Weight of circular antennas relative to linear ones in the solve.
        logdir: Directory for PolConvert log files.
        output_file: Path to write the TOML file.

    Returns:
        Path to the written TOML file.
    """
    template_path = Path(__file__).parent / 'templates' / 'polconvert_inputs.toml.template'
    template = template_path.read_text()
    content = template.format(
        expname=exp.expname.lower(),
        ref_idi=ref_idi,
        linants=str(_pc_ants(lin_ants)),
        refant=repr(refant.upper()),
        exclude_ants=str(_pc_ants(exclude_ants)),
        do_if=str(do_ifs),
        time_range=str(time_range),
        chanavg=chan_avg,
        timeavg=time_avg,
        solve_weight=solve_weight,
        logdir=repr(logdir),
    )
    output_file.write_text(content)
    logger.info(f"Written PolConvert input file to {output_file}")
    return output_file


def _scan_number(scan: experiment.Scan) -> Optional[int]:
    """Integer scan number from a VEX scanno like ``'No0018'`` (-> 18), or None.

    The lag-MS SNR/bandpass dictionaries are keyed by the MS scan number as a string, which
    matches this integer (verified: lag MS scan 18 == VEX No0018), so this is how a VEX scan
    is looked up in ``exp.lag_snr`` / ``exp.lag_bandpass``.
    """
    try:
        return int(re.sub(r'\D', '', scan.scanno))
    except (ValueError, TypeError):
        return None


def _ant_scan_snr(exp: experiment.Experiment, ant: str, scan_key: str) -> float:
    """Best-polarization lag SNR of *ant* on one scan (0.0 when it has no lag data).

    Args:
        exp: Experiment object (reads ``exp.lag_snr``).
        ant: Antenna name.
        scan_key: MS scan number as a string (see :func:`_scan_number`).

    Returns:
        The maximum SNR over polarizations, or 0.0 if the antenna or the scan is unknown.
    """
    snrs = exp.lag_snr.get(scan_key, {}).get(ant, {})
    return max(snrs.values()) if snrs else 0.0


def _scan_lag_score(exp: experiment.Experiment, scan: experiment.Scan) -> tuple[int, float]:
    """Score a fringe-finder scan from the lag-MS SNR: ``(#antennas detected, summed SNR)``.

    An antenna counts as detected when its best-polarization lag SNR on the scan reaches
    ``_POL_MIN_SNR``. Scans absent from ``exp.lag_snr`` (e.g. never correlated, like the early
    e-MERLIN-only block of EZ041A) score ``(0, 0.0)`` and rank last. This steers the selection
    to a scan that truly has fringes on many antennas, rather than one merely *scheduled* on
    many antennas (the old station-count heuristic, which picked a non-correlated scan).
    """
    snum = _scan_number(scan)
    per_ant = exp.lag_snr.get(str(snum), {}) if snum is not None else {}
    n_det, total = 0, 0.0
    for snr_by_pol in per_ant.values():
        best = max(snr_by_pol.values()) if snr_by_pol else 0.0
        if best >= _POL_MIN_SNR:
            n_det += 1
            total += best
    return n_det, round(total, 1)


def _rank_fringefinder_scans(exp: experiment.Experiment) -> list[experiment.Scan]:
    """Fringe-finder scans ranked best-first for PolConvert.

    Primary key: number of antennas with a detected fringe on the scan (lag SNR);
    secondary key: the summed SNR of those antennas. Falls back to the scheduled-station
    ordering only when no lag SNR is available at all (e.g. ``--no-lag`` runs).
    """
    ff_scans = [s for s in exp.scans if s.source in exp.sources.fringefinder]
    if not ff_scans:
        return []
    if not exp.lag_snr:
        return _get_all_fringefinder_scans(exp)
    return sorted(ff_scans, key=lambda s: _scan_lag_score(exp, s), reverse=True)


def _refant_bandpass_scatter(exp: experiment.Experiment, ant: str, scan_key: str,
                             ifs: list[int]) -> float:
    """Coefficient of variation (std/mean) of an antenna's per-IF amplitude on a scan.

    Lower means a flatter bandpass across the IFs PolConvert has to convert. Returns ``inf``
    when the per-IF amplitudes are unavailable, so antennas with bandpass data are always
    preferred over those without.
    """
    amps = exp.lag_bandpass.get(scan_key, {}).get(ant)
    if not amps:
        return float('inf')
    vals = [amps[i] for i in ifs if i < len(amps) and amps[i] is not None]
    if len(vals) < 2:
        return float('inf')
    arr = np.asarray(vals, dtype=float)
    mean = float(np.mean(arr))
    return float(np.std(arr) / mean) if mean > 0 else float('inf')


def _polconvert_refants(exp: experiment.Experiment, lin_ants: list[str], subbands: set[int],
                        scan_key: str) -> list[str]:
    """Reference antennas for the PolConvert solve, strongest fringe on the solve scan first.

    A candidate must be (1) observed, (2) NOT one of the linear antennas being converted (the
    conversion cannot reference itself), and (3) cover every IF that has to be converted. They
    are ranked by their lag SNR on this scan; the experiment ``refant`` order breaks ties and
    decides on its own when no lag data is available. At most ``_POLCONVERT_MAX_REFANTS`` are
    returned, so the second one is a fallback rather than the start of a long sweep.

    Args:
        exp: Experiment object.
        lin_ants: Linear-polarization antennas being converted.
        subbands: IFs (0-indexed) that have to be converted.
        scan_key: MS scan number of the solve scan, as a string.

    Returns:
        The reference antenna names to try, best first; empty when no antenna qualifies.
    """
    candidates = [a.name for a in exp.antennas
                  if a.observed and a.name not in lin_ants and subbands.issubset(set(a.subbands))]
    priority = {name: i for i, name in enumerate(exp.refant or [])}
    candidates.sort(key=lambda a: (_ant_scan_snr(exp, a, scan_key),
                                   -priority.get(a, len(priority))), reverse=True)
    return candidates[:_POLCONVERT_MAX_REFANTS]


def _polconvert_solve_scans(exp: experiment.Experiment, lin_ants: list[str]) -> list[experiment.Scan]:
    """Scans on which the PolConvert bandpass can be solved, best-first.

    The fringe-finder scans where at least one linear antenna being converted shows a strong
    fringe (lag SNR >= ``_POL_MIN_SNR``); if no fringe-finder scan qualifies, the phase
    calibrator scans that do. Both are ordered by :func:`_scan_lag_score` (most antennas
    detected, then summed SNR). Without any lag data nothing can be filtered, so the plain
    fringe-finder ranking is returned.

    Args:
        exp: Experiment object.
        lin_ants: Linear-polarization antennas being converted.

    Returns:
        Scans to try, in order; empty when no scan shows a fringe on the linear antenna(s).
    """
    if not exp.lag_snr:
        return _rank_fringefinder_scans(exp)

    def _with_linear_fringe(sources: list[str]) -> list[experiment.Scan]:
        scans = [s for s in exp.scans if s.source in sources
                 and max((_ant_scan_snr(exp, a, str(_scan_number(s))) for a in lin_ants),
                         default=0.0) >= _POL_MIN_SNR]
        return sorted(scans, key=lambda s: _scan_lag_score(exp, s), reverse=True)

    if ff_scans := _with_linear_fringe(exp.sources.fringefinder):
        return ff_scans
    if cal_scans := _with_linear_fringe(exp.sources.calibrator):
        logger.info("No fringe-finder scan shows a strong fringe on the linear antenna(s); "
                    "falling back to the phase-calibrator scans.")
        return cal_scans
    return []


def _polconvert_exclude_ants(exp: experiment.Experiment, lin_ants: list[str], refant: str,
                             subbands: set[int], scan_key: Optional[str]) -> list[str]:
    """Antennas to leave out of the PolConvert solve.

    Dropped: antennas that did not observe, those that did not record every IF the linear
    antenna covers, and those whose fringe on the solve scan is below
    ``_POLCONVERT_SOLVE_MIN_SNR``. The reference antenna and the linear antennas being
    converted are always kept, whatever their coverage or SNR.

    Args:
        exp: Experiment object.
        lin_ants: Linear-polarization antennas being converted.
        refant: Reference antenna of the solve.
        subbands: IFs (0-indexed) that have to be converted.
        scan_key: MS scan number of the solve scan, as a string. The SNR filter is skipped
            when it is None or the scan has no lag data at all (e.g. a ``--no-lag`` run),
            since every antenna would then read as 0.0 and be excluded.

    Returns:
        Sorted list of antenna names to exclude.
    """
    # None whenever the SNR filter must be skipped, so the lookup below stays well-typed.
    snr_key = scan_key if (scan_key is not None and exp.lag_snr.get(scan_key)) else None
    exclude: list[str] = []
    for ant in exp.antennas:
        if not ant.observed:
            exclude.append(ant.name)
        elif ant.name in lin_ants or ant.name == refant:
            continue
        elif not subbands.issubset(set(ant.subbands)):
            exclude.append(ant.name)
        elif snr_key is not None and _ant_scan_snr(exp, ant.name, snr_key) < _POLCONVERT_SOLVE_MIN_SNR:
            exclude.append(ant.name)
    return sorted(set(exclude))


def _fringe_peak_ratios(logdir: str, n_ifs: int) -> list[float]:
    """Per-IF ``(RR+LL)/(RL+LR)`` of a finished ``--compute``, or ``[]`` if it did not finish.

    Each ``FRINGE.PEAKS_IF*_SCAN_*.dat`` (one per converted IF) lists the normalized
    fringe-peak amplitude of RR, LL, RL and LR on the reference baseline. A real conversion
    concentrates power in the parallel hands; a failed/linear solution leaves the four
    products comparable (ratio ~1).

    This is what says whether an attempt ran, because PolConvert's exit code does not: it
    almost always dies in its own teardown after writing everything. A run counts as finished
    only when it left the complete set that it writes at the very end -- the gains file plus
    one readable peaks file per IF -- so a crash halfway through cannot be mistaken for a
    solution that merely converted badly.

    Args:
        logdir: The log folder given to PolConvert.
        n_ifs: Number of IFs the attempt was asked to convert.

    Returns:
        One ratio per IF, in file order, or an empty list when the run left no full result.
    """
    peaks_dir = Path(logdir) / 'FRINGE.PEAKS'
    if not (Path(logdir) / 'polconvert.gains').exists() or not peaks_dir.exists():
        return []

    ratios: list[float] = []
    for dat_file in sorted(peaks_dir.glob('FRINGE.PEAKS_IF*_SCAN_*.dat')):
        content = dat_file.read_text()  # read once, searched per polarization
        a = {pol: float(m.group(1)) for pol in ('RR', 'LL', 'RL', 'LR')
             if (m := re.search(rf'{pol}:\s*([\d.eE+-]+)\s*;', content))}
        if len(a) != 4:
            continue
        cross = a['RL'] + a['LR']
        ratios.append((a['RR'] + a['LL']) / cross if cross > 0 else float('inf'))

    return ratios if len(ratios) == n_ifs else []


# PolConvert plots through matplotlib, whose default backend on this machine is 'qtagg'.
# Loading it pulls in a PyQt5 Qt5 plugin that dies with "symbol lookup error: ...
# libqsvgicon.so: undefined symbol: _ZdlPvm" and takes the interpreter down with it (rc=127).
# The post-processing is headless anyway, so the child runs on the non-interactive Agg
# backend, which still writes the PNGs PolConvert produces and keeps Qt out of the process.
# It does not make the child exit cleanly — PolConvert then aborts on its own corrupted heap
# instead — which is why nothing here reads the exit code.
_POLCONVERT_ENV: dict[str, str] = {'MPLBACKEND': 'Agg'}


def _run_polconvert_cli(template_file: Path, mode: str) -> int:
    """Run ``polconvert.py <template> <mode>`` once, in a child process.

    Deliberately a child process rather than an in-process import. PolConvert corrupts its own
    heap and dies of it, and a SIGSEGV/SIGABRT cannot be held by ``try``/``except``: it
    terminates the interpreter, so in-process it would take the whole post-processing down
    with it. Isolated in a child, the very same crash is only a return code.

    That return code is reported but never acted on: the crash happens in the teardown that
    follows a completed run, so it marks good solutions as failures. What the run left on disk
    is the verdict (:func:`_fringe_peak_ratios`, :func:`_polconvert_apply`).

    The child's stdout/stderr are inherited rather than captured, so its progress — and the
    fringe-SNR table it prints when it finishes — is visible as it runs instead of surfacing
    (or not) at the end. polconvert.py keeps its own detailed ``PolConvert-{mode}.log`` in the
    log directory regardless.

    Returns:
        The child's exit code, for logging only (negative when a signal killed it).
    """
    return subprocess.run(['polconvert.py', str(template_file), mode],
                          env={**os.environ, **_POLCONVERT_ENV}).returncode


def _record_polconvert_command(template_file: Path, mode: str) -> None:
    """Records one ``polconvert.py`` invocation in ``logs/commands.sh``.

    Called once per outcome rather than once per launch: the search overwrites the same input
    file on every attempt, so recording all of them would fill the runbook with hundreds of
    identical lines pointing at a file that no longer holds those parameters. Recorded when a
    combination is accepted, and once when the search gives up — in both cases the file left
    on disk is the one the recorded command would read.
    """
    reporting.record_command(shlex.join(['polconvert.py', str(template_file), mode]))


def _polconvert_compute(template_file: Path, logdir: str, n_ifs: int) -> list[float]:
    """Solve one PolConvert combination and return its per-IF fringe-peak ratios.

    Retries only a run that died before writing a complete solution — the crashes that happen
    mid-solve are transient, while the far more common teardown crash leaves everything on
    disk and needs no retry at all.

    Args:
        template_file: The input TOML written for this combination.
        logdir: The log folder PolConvert writes into.
        n_ifs: Number of IFs being converted.

    Returns:
        One ratio per IF, or an empty list when no attempt produced a complete solution.
    """
    for attempt in range(1, _POLCONVERT_RETRIES + 2):
        rc = _run_polconvert_cli(template_file, '--compute')
        if ratios := _fringe_peak_ratios(logdir, n_ifs):
            return ratios
        logger.warning(f"PolConvert wrote no complete solution for the {n_ifs} IFs (it exited "
                       f"{rc}) [attempt {attempt}/{_POLCONVERT_RETRIES + 1}].")
        # It never reached the summary it prints on its own; render whatever fringe SNRs it
        # did manage to write, so a crash still says something.
        _log_fringe_snr_table(logdir)
    logger.warning("PolConvert died before writing a solution every time with these "
                   "parameters; trying the next combination.")
    return []


def _polconvert_apply(template_file: Path, idi_files: list[str]) -> bool:
    """Apply an accepted solution to every FITS-IDI file, judged by the files it produces.

    Args:
        template_file: The input TOML of the accepted combination.
        idi_files: The FITS-IDI files that must come out converted.

    Returns:
        True once every file has its ``.PCONVERT`` counterpart, False if some never appear.
    """
    for attempt in range(1, _POLCONVERT_RETRIES + 2):
        rc = _run_polconvert_cli(template_file, '--apply')
        missing = [f for f in idi_files if not Path(f + '.PCONVERT').exists()]
        if not missing:
            return True
        logger.warning(f"PolConvert did not convert {len(missing)} of {len(idi_files)} FITS-IDI "
                       f"files (it exited {rc}) [attempt {attempt}/{_POLCONVERT_RETRIES + 1}]: "
                       f"{', '.join(missing)}.")
    return False


def _log_fringe_snr_table(logdir: str) -> None:
    """Prints the per-IF fringe-SNR table for the attempt that just ran.

    ``polconvert.py`` prints this table itself when it finishes, so this is only for the runs
    that died before getting there: it renders the same table, in-process, from whatever
    ``FRINGE.PEAKS`` files PolConvert had already written. Importing that module is cheap and
    safe — it imports PolConvert (and matplotlib, and Qt) only inside its own ``main()``, so
    nothing of that chain is pulled in here.

    Never raises: a summary that cannot be built must not be what ends an attempt.
    """
    try:
        from evn_support.polconvert import print_fringe_snr_table
        print_fringe_snr_table(logdir)
    except Exception as e:  # ImportError, or anything the summary itself trips over
        logger.debug(f"No fringe-SNR summary for this attempt ({e}); the raw values, if any, "
                     f"are in {Path(logdir) / 'FRINGE.PEAKS'}.")


def polconvert(exp: experiment.Experiment) -> bool:
    """Run PolConvert locally, auto-selecting the scan, time range and reference antenna.

    Linear-polarization antennas (``exp.antennas.polconvert``) are converted to circular. The
    search is bounded (``_POLCONVERT_MAX_ATTEMPTS``) and ordered best-first:

      * scans: fringe-finder scans where a linear antenna actually shows a strong fringe,
        falling back to the phase calibrators (:func:`_polconvert_solve_scans`);
      * reference antenna: the two circular full-band antennas with the strongest fringe
        (:func:`_polconvert_refants`), with weak / partial-band antennas excluded from the
        solve (:func:`_polconvert_exclude_ants`);
      * time ranges: up to three per scan, the last minute, the middle minute and everything
        after the first minute (:func:`_polconvert_time_ranges`);
      * solution parameters: ``doweight`` x time averaging x channel averaging
        (``_POLCONVERT_DOWEIGHTS`` x ``_POLCONVERT_TIMEAVGS_S`` x ``_POLCONVERT_CHANAVGS``).

    Each combination runs ``polconvert.py --compute`` and is judged by what it wrote, never by
    its exit code (see :func:`_run_polconvert_cli`): a complete solution is accepted when the
    FRINGE.PEAKS ``(RR+LL)/(RL+LR)`` ratio reaches ``_POLCONVERT_MIN_RATIO`` in *every* IF. The
    first accepted solution is applied to every FITS-IDI file with ``--apply``; otherwise the
    search moves on to the next parameter set, then the next time range, then the next
    reference antenna, then the next scan.

    Args:
        exp: Experiment object.

    Returns:
        True if a good conversion was produced (or PolConvert is not needed), else False.
    """
    if not exp.antennas.polconvert:
        logger.info("PolConvert is not required.")
        return True

    if len(glob.glob('*IDI*.PCONVERT')) > 0:
        logger.info("PolConvert output files already exist. Skipping.")
        return True

    lin_ants = [a for a in exp.antennas.polconvert]
    subbands = set().union(*(set(exp.antennas[p].subbands) for p in lin_ants))
    if not subbands:
        logger.error("Linear antennas have no recorded subbands; cannot run PolConvert.")
        return False
    do_ifs = [i + 1 for i in sorted(subbands)]

    idi_files = sorted(glob.glob(f"{exp.expname.lower()}_*_1.IDI*"))
    if not idi_files:
        logger.error("No FITS-IDI files found for PolConvert.")
        return False

    scans = _polconvert_solve_scans(exp, lin_ants)
    if not scans:
        logger.error(f"No fringe-finder or phase-calibrator scan shows a fringe on {', '.join(lin_ants)}; "
                     "cannot solve PolConvert automatically.")
        return False

    logdir = 'polconvert_logs'
    tried = 0
    for scan in scans:
        scan_key = str(_scan_number(scan))
        refants = _polconvert_refants(exp, lin_ants, subbands, scan_key)
        if not refants:
            logger.warning(f"No circular reference antenna covers all IFs on scan {scan.scanno}.")
            continue

        n_det, snr_sum = _scan_lag_score(exp, scan)
        for refant in refants:
            exclude_ants = _polconvert_exclude_ants(exp, lin_ants, refant, subbands, scan_key)
            scatter = _refant_bandpass_scatter(exp, refant, scan_key, sorted(subbands))
            logger.info(f"PolConvert: scan {scan.scanno} on {scan.source} ({n_det} antennas "
                        f"detected, SNR sum {snr_sum}); linants={_pc_ants(lin_ants)}, "
                        f"refant={refant.upper()} (SNR {_ant_scan_snr(exp, refant, scan_key):.1f}, "
                        f"bandpass scatter {scatter:.3f}), exclude={_pc_ants(exclude_ants)}, "
                        f"IFs={do_ifs}.")

            for time_range in _polconvert_time_ranges(scan, exp.obsdate):
                ref_idi = find_idi_mod.find_idi_with_time(idi_files=idi_files,
                                                          aipstime=time_range[:4], verbose=False)
                if ref_idi is None:
                    logger.debug(f"No FITS-IDI covers {time_range[:4]} on scan {scan.scanno}; skipping.")
                    continue

                for solve_weight, time_avg, chan_avg in product(_POLCONVERT_DOWEIGHTS,
                                                                _POLCONVERT_TIMEAVGS_S,
                                                                _POLCONVERT_CHANAVGS):
                    if tried >= _POLCONVERT_MAX_ATTEMPTS:
                        _record_polconvert_command(Path('polconvert_inputs.toml'), '--compute')
                        logger.error(f"PolConvert reached its budget of {_POLCONVERT_MAX_ATTEMPTS} "
                                     f"attempts without a good solution. Inspect {logdir}, adjust "
                                     "polconvert_inputs.toml, and run it manually.")
                        return False

                    template_file = _write_polconvert_template(exp, ref_idi, lin_ants, refant,
                                                               exclude_ants, do_ifs, time_range,
                                                               time_avg=time_avg, chan_avg=chan_avg,
                                                               solve_weight=solve_weight, logdir=logdir)
                    tried += 1
                    # Every attempt says what it is trying before it runs: a search that ends
                    # without converging is otherwise a single failure line with no record of the
                    # combinations it went through, or of which one came closest.
                    logger.info(f"PolConvert --compute [attempt {tried}]: scan {scan.scanno} "
                                f"({scan.source}), {_aips_timerange_str(time_range)}, "
                                f"refant={refant.upper()}, linants={_pc_ants(lin_ants)}, "
                                f"exclude={_pc_ants(exclude_ants)}, IFs={do_ifs}, ref_idi={ref_idi}, "
                                f"doweight={solve_weight}, timeavg={time_avg}s, chanavg={chan_avg}.")

                    ratios = _polconvert_compute(template_file, logdir, len(do_ifs))
                    if not ratios:
                        continue

                    worst = min(ratios)
                    logger.info(f"PolConvert (RR+LL)/(RL+LR) per IF: min={worst:.1f}, "
                                f"median={float(np.median(ratios)):.1f} "
                                f"(need >= {_POLCONVERT_MIN_RATIO} in every IF).")
                    if worst < _POLCONVERT_MIN_RATIO:
                        logger.info(f"Scan {scan.scanno} {_aips_timerange_str(time_range)}: no good "
                                    f"solution with doweight={solve_weight}, timeavg={time_avg}s, "
                                    f"chanavg={chan_avg}; trying the next combination.")
                        continue

                    logger.info(f"Good PolConvert solution: scan {scan.scanno}, refant "
                                f"{refant.upper()}, time range {_aips_timerange_str(time_range)}, "
                                f"doweight={solve_weight}, timeavg={time_avg}s, chanavg={chan_avg}. "
                                "Applying it to all FITS-IDI files.")
                    _record_polconvert_command(template_file, '--compute')
                    _record_polconvert_command(template_file, '--apply')
                    if not _polconvert_apply(template_file, idi_files):
                        logger.error("PolConvert --apply left FITS-IDI files unconverted after a "
                                     "good --compute. Stopping.")
                        return False

                    exp.store()
                    return True

    if tried:
        _record_polconvert_command(Path('polconvert_inputs.toml'), '--compute')
    logger.error(f"PolConvert could not reach a good solution after {tried} attempt(s) over "
                 f"{len(scans)} scan(s). Inspect {logdir}, adjust polconvert_inputs.toml, "
                 "and run it manually.")
    return False


def post_polconvert(exp: experiment.Experiment) -> Optional[bool]:
    """Converts PCONVERTed FITS-IDI files to MS and creates verification plots.

    Imports the .PCONVERT FITS-IDI files into a new MS using casatasks, then
    runs standardplots (cross) on it and converts the resulting PS files to PNG,
    overriding any previous plot images.

    Args:
        exp: Experiment object.

    Returns:
        True if completed or not needed, False on error.
    """
    if not exp.antennas.polconvert:
        return True

    if len(glob.glob('*IDI*.PCONVERT')) == 0:
        return True

    cwd = Path.cwd()
    pconverted_idi = list(cwd.glob('*IDI*.PCONVERT'))

    # Convert PCONVERTed FITS-IDI files to MS and create verification plots
    if any('_1_1' in pp.name for pp in pconverted_idi):
        pconv_ms = exp.correlator_passes[0].msfile.name.replace('.ms', '-pconv.ms')
        idi_files = [str(idi) for idi in sorted(pconverted_idi) if '_1_1' in idi.name]

        pconv_ms_path = cwd / pconv_ms
        if pconv_ms_path.exists():
            shutil.rmtree(pconv_ms_path)

        # Imported here, not at module level: casatasks only loads once casacore has, and
        # this is the single place that needs it, so a broken CASA cannot make the whole
        # package unimportable.
        import casatasks
        casatasks.importfitsidi(vis=pconv_ms, fitsidifile=idi_files, constobsid=True,
                                scanreindexgap_s=8.0, specframe='GEO')
        logger.info(f"Created {pconv_ms} from {len(idi_files)} PCONVERT IDI files.")

        if not exp.refant:
            logger.error("No reference antenna set for polconvert verification plots.")
            return False

        calsources = exp.sources.fringefinder
        if not calsources:
            logger.error("No fringe-finder sources found for polconvert verification.")
            return False

        plotter = plotting.Jplot(ms=pconv_ms, refant=exp.refant[0], calsrc=','.join(calsources))
        plotter.create_plot(sources=calsources, plots=['cross'])

        # Rename pconv plot files to standard names so they override the previous ones
        for stdplot_file in glob.glob('*-pconv*.ps'):
            Path(stdplot_file).rename(stdplot_file.replace('-pconv', ''))

        # Convert PS plots to PNG images, overriding previous ones
        plotting.convert_ps_to_png(exp.dirs.plots, exp.expname.lower())
        logger.info("PolConvert verification plots created and converted to images.")

    logger.info("PolConvert post-processing complete.")
    exp.store()
    return True


def post_post_polconvert(exp: experiment.Experiment) -> bool:
    """Copies original FITS-IDI files to idi_ori/ and renames PCONVERT files.

    Preserves the original IDI files in a backup directory, then renames the
    .PCONVERT output files to standard IDI names so downstream tools can find them.

    Args:
        exp: Experiment object.

    Returns:
        True if completed or not needed.
    """
    if not exp.antennas.polconvert:
        return True

    if len(glob.glob('*IDI*.PCONVERT')) == 0:
        return True

    cwd = Path.cwd()
    idi_ori = cwd / 'idi_ori'
    idi_ori.mkdir(exist_ok=True)

    # Copy original FITS-IDI files to idi_ori/
    for an_idi in cwd.glob('*.IDI*'):
        if '.PCONVERT' not in an_idi.name:
            shutil.copy2(str(an_idi), str(idi_ori / an_idi.name))

    logger.info(f"Copied original IDI files to {idi_ori}")

    # Rename PCONVERT files to standard IDI names
    pconverted_idi = list(cwd.glob('*IDI*.PCONVERT'))
    for an_idi in pconverted_idi:
        an_idi.rename(cwd / an_idi.name.replace('.PCONVERT', ''))

    logger.info(f"Renamed {len(pconverted_idi)} PCONVERT files to standard names")
    return True


def set_credentials(exp: experiment.Experiment) -> bool:
    """Sets the credentials for the given experiment.

    NMEs (see :func:`experiment.is_nme`) carry no proprietary data, so no credentials are
    set. Otherwise recovers from an existing .auth file, or generates a new random password.

    Args:
        exp: Experiment object.

    Returns:
        True if credentials were set or not needed, False on error.
    """
    if experiment.is_nme(exp.expname):
        logger.info(f"{exp.expname} is an NME. No authentication set.")
        return True

    auth_files = glob.glob("*_*.auth")
    if len(auth_files) == 1:
        username, password = auth_files[0].split('.')[0].split('_')
        exp.credentials = experiment.Credentials(username=username, password=password)
        logger.info(f"Recovered credentials from {auth_files[0]}")
    elif len(auth_files) > 1:
        logger.error("More than one .auth file found in the directory.")
        return False
    else:
        password = "".join(random.sample((string.digits + string.ascii_letters), 12))
        exp.credentials = experiment.Credentials(username=exp.expname.lower(), password=password)
        auth_file = Path(f"{exp.credentials.username}_{exp.credentials.password}.auth")
        auth_file.touch()
        logger.info(f"Created credentials: {auth_file.name}")

    return True


def protect_experiment_files(exp: experiment.Experiment) -> bool:
    """Sets source protection in the EVN Archive using auth_pipe.py.

    Args:
        exp: Experiment object.

    Returns:
        True if protection was set or not needed, False on error.
    """
    protected_sources = [s.name for s in exp.sources if s.protected]
    if not protected_sources:
        logger.info("No protection required for this experiment.")
        return True

    # Only the archived source data is protected here ('-p source'); the pipeline
    # products are protected by the archive itself.
    try:
        utils.shell_command("auth_pipe.py", ["-e", f"{exp.expname.upper()}_{exp.obsdate.strftime('%y%m%d')}",
                            "-s", ' '.join(protected_sources), "-p", "source"])
    except ValueError:
        logger.error(f"Could not protect experiment files in archive for {exp.expname.upper()}.")
        return False

    logger.info(f"Protected sources in the archive: {', '.join(protected_sources)}")
    return True


def has_Tsys(fitsfile) -> bool:
    """Check if a FITS-IDI file has the SYSTEM_TEMPERATURE table.

    Args:
        fitsfile: Path to the FITS-IDI file.

    Returns:
        True if the table is present.
    """
    with fits.open(fitsfile) as hdu:
        return 'SYSTEM_TEMPERATURE' in hdu


def has_GC(fitsfile) -> bool:
    """Check if a FITS-IDI file has the GAIN_CURVE table.

    Args:
        fitsfile: Path to the FITS-IDI file.

    Returns:
        True if the table is present.
    """
    with fits.open(fitsfile) as hdu:
        return 'GAIN_CURVE' in hdu


def check_consistency(fitsfile, verbose: bool = True) -> bool:
    """Check if a FITS-IDI file has the required Tsys and GC tables.

    Args:
        fitsfile: FITS-IDI file path (str or Path). Should be the first IDI file
            (e.g. exp_1_1.IDI1) as subsequent files are not expected to have the tables.
        verbose: Log the check results.

    Returns:
        True if all expected tables are present.
    """
    if isinstance(fitsfile, str):
        fitsfile = Path(fitsfile)

    if not fitsfile.exists():
        raise FileNotFoundError(f"The FITS-IDI file {fitsfile} could not be found.")

    all_good = True
    if has_Tsys(fitsfile):
        if verbose:
            logger.info(f"{fitsfile} has SYSTEM_TEMPERATURE table.")
    else:
        if verbose:
            logger.warning(f"{fitsfile} does not have SYSTEM_TEMPERATURE table.")
        all_good = False

    if has_GC(fitsfile):
        if verbose:
            logger.info(f"{fitsfile} has GAIN_CURVE table.")
    else:
        if verbose:
            logger.warning(f"{fitsfile} does not have GAIN_CURVE table.")
        all_good = False

    return all_good



def append_antab(exp: experiment.Experiment) -> bool:
    """Appends Tsys and GC information from the ANTAB file into the FITS-IDI files.

    Reads ANTAB files from exp.dirs.pipe_in and applies them to the FITS-IDI files
    in the current working directory by calling append_tsys.py and append_gc.py.

    Args:
        exp: Experiment object.

    Returns:
        True if Tsys/GC information was appended or already present, False on error.
    """
    fits2check = glob.glob(f"{exp.expname.lower()}_*_*.IDI1") + glob.glob(f"{exp.expname.lower()}_*_*.IDI")
    if not fits2check:
        logger.error("Could not find FITS-IDI files to append Tsys/GC.")
        return False

    if all(check_consistency(f, verbose=False) for f in fits2check):
        logger.info("ANTAB information already appended into the FITS-IDI files.")
        return True

    antabfiles = sorted(exp.dirs.pipe_in.glob(f"{exp.expname.lower()}*.antab"))
    if not antabfiles:
        logger.error(f"No ANTAB files found in {exp.dirs.pipe_in}.")
        return False

    idifiles = sorted(
        glob.glob(f"{exp.expname.lower()}_*_1.IDI*"),
        key=lambda s: [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', s)]
    )
    if not idifiles:
        logger.error("No FITS-IDI files found.")
        return False

    def _parse_pass(filename):
        i0 = filename.index('_')
        return int(filename[i0+1:(i0 + 1 + filename[i0+1:].index('_'))])

    def _run_append(antabfile, idi_list):
        antabfile = str(antabfile)
        for pc in sorted(set(_parse_pass(idi) for idi in idi_list)):
            pc_files = [idi for idi in idi_list if _parse_pass(idi) == pc]
            logger.debug(f"Running append_tsys.py {antabfile} {' '.join(pc_files)}")
            proc = subprocess.Popen(["append_tsys.py", "--replace", antabfile, *pc_files],
                                    stdout=None, stderr=subprocess.STDOUT)
            proc.wait()
        for idifile in [idi for idi in idi_list if idi.endswith('.IDI1') or idi.endswith('IDI')]:
            logger.debug(f"Running append_gc.py {antabfile} {idifile}")
            proc = subprocess.Popen(["append_gc.py", "--replace", antabfile, idifile],
                                    stdout=None, stderr=subprocess.STDOUT)
            proc.wait()

    if len(antabfiles) == 1:
        _run_append(antabfiles[0], idifiles)
    else:
        for i, antabfile in enumerate(antabfiles):
            _run_append(antabfile, [idi for idi in idifiles if f"_{i+1}_1.IDI" in idi])

    if not all(check_consistency(f) for f in fits2check):
        logger.error("The Tsys/GC could not be imported into the FITS-IDI files.")
        return False

    return True


<<<<<<< HEAD
=======
def antenna_feedback(exp: experiment.Experiment) -> bool:
    """Reminds the user to report antenna issues via Mattermost and RedMine.

    Args:
        exp: Experiment object.

    Returns:
        True always.
    """
    body = ("[bold]Update the database with observed issues:[/bold]\n\n"
            "  1. Type [bold cyan]/feedback[/bold cyan] in Mattermost to bookkeep antenna issues.\n"
            "  2. Update JIVE RedMine:\n"
            "     [link=https://jrm.jive.nl/projects/science-support/news]"
            "https://jrm.jive.nl/projects/science-support/news[/link]")
    Console().print(Panel(body, title="[bold yellow]Station Feedback[/bold yellow]",
                          border_style="yellow", padding=(1, 2)))
    return True


>>>>>>> d499383ef8bc92d1fc9aaf2ee21dd4b1b9e1d214
def nme_report(exp: experiment.Experiment) -> bool:
    """Reminds the user to write the NME report if applicable.

    Args:
        exp: Experiment object.

    Returns:
        True always.
    """
    if experiment.is_nme(exp.expname):
        logger.info(f"{exp.expname} is an NME — time to write the NME Report "
                    "(create_nme_report_template.py, then upload it to the EVN wiki and "
                    "inform evntech@jive.eu).")
    else:
        logger.info(f"Experiment {exp.expname} done.")

    return True


def aggregate_sources_from_passes(exp: experiment.Experiment) -> None:
    """Aggregates all sources from all correlator passes into the global experiment sources.

    This ensures that exp.sources contains all sources from all passes, not just
    the ones from VEX/jexp files.

    Args:
        exp (experiment.Experiment): Experiment object to update with aggregated sources.
    """
    for a_pass in exp.correlator_passes:
        if a_pass.sources:
            for source in a_pass.sources:
                # Add source to global experiment sources if not already present
                if source.name not in exp.sources.names:
                    exp.sources.append(source)
                else:
                    # Update existing source type if this pass has more specific information
                    existing_source = exp.sources[source.name]
                    # Prefer non-'other' types
                    if existing_source.type == experiment.SourceType.other and source.type != experiment.SourceType.other:
                        existing_source.type = source.type
                    # Update protected status if this source is protected
                    if source.protected and not existing_source.protected:
                        existing_source.protected = True
