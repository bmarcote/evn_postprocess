#! /usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the eee computer.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.

"""
import os
import re
import glob
import string
import random
import traceback
from typing import Optional, Union
from pathlib import Path
from collections import defaultdict
from datetime import timedelta
import shutil
import subprocess
import numpy as np
from loguru import logger
from astropy import units as u
from astropy.io import fits
from rich import print as rprint
from rich.panel import Panel
from rich.console import Console
from concurrent.futures import ThreadPoolExecutor
from . import experiment, utils, mstools
from .plotting import convert_ps_to_png, serve_dashboard
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

# --- tConvert configuration ------------------------------------------------------------
# _TCONVERT_BIN = "tConvert"  # This will be the one to use once we certify the following one works
_TCONVERT_BIN = "/home/verkout/src/jive-casa/build-reftime_assert_fail/apps/tConvert/tConvert"

# Temporary workaround (see _tconvert_in_eee): the system tConvert is currently broken, so by
# default the tconvert step runs on eee instead. Where the MS / FITS-IDI files are staged there:
_EEE_TCONVERT_TEMP = Path("/data0/temp")
# A few very large MS files move over the network here, so the usual 10-minute transfer/run
# bounds are far too tight; give them generous (env-overridable) ceilings.
_EEE_RSYNC_TIMEOUT_S = int(os.environ.get("EVN_EEE_RSYNC_TIMEOUT_S", str(6 * 3600)))
_EEE_TCONVERT_TIMEOUT_S = int(os.environ.get("EVN_EEE_TCONVERT_TIMEOUT_S", str(8 * 3600)))

# --- PolConvert configuration ----------------------------------------------------------
# PolConvert is run locally (see :func:`polconvert`). It occasionally crashes with a
# segmentation fault (a known upstream bug under revision); because it runs in a subprocess,
# a crash returns a negative exit code instead of killing post-processing, so the same
# attempt is simply retried up to this many extra times before moving on.
_POLCONVERT_SEGFAULT_RETRIES: int = 2
# A converted solution is accepted when, in every IF, the parallel-to-cross fringe-peak
# amplitude ratio (RR+LL)/(RL+LR) on the reference baseline exceeds this value. A failed/linear
# solution leaves the four products comparable (ratio ~1); a real conversion lifts it well above.
_POLCONVERT_MIN_RATIO: float = 3.0
# The reference antenna is chosen as the flattest-bandpass antenna among the *well-detected*
# ones: only antennas whose lag SNR reaches this fraction of the best candidate's SNR compete
# on flatness. Without the gate, flatness (a coefficient of variation) is dominated by noise on
# weak antennas, which would pick a low-SNR station over a strong, equally-flat one.
_POLCONVERT_REFANT_SNR_FRACTION: float = 0.5
# Default bandpass-solution parameters written into the PolConvert input file.
_POLCONVERT_CHANAVG: int = 16
_POLCONVERT_TIMEAVG_S: int = 20
_POLCONVERT_SOLVE_WEIGHT: float = 0.1


def update_pipelinable_passes(exp, pipelinable: Union[list, dict]) -> None:
    """Updates the attribute of the CorrelatorPasses from exp to define
    if the specific pass should run in the pipeline or not.

    Args:
        exp (experiment.Experiment): Experiment object.
        pipelinable (Union[list, dict]): Either a list of bool (same length as exp.correlator_passes)
            or a dict with lisfile as key and bool as value.
    
    Returns:
        None
    """
    if isinstance(pipelinable, list):
        assert len(pipelinable) == len(exp.correlator_passes)
        for i, is_pipelinable in enumerate(pipelinable):
            assert isinstance(is_pipelinable, bool)
            exp.correlator_passes[i].pipeline = is_pipelinable
    elif isinstance(pipelinable, dict):
        for a_lisfile in pipelinable:
            for a_exppass in exp.correlator_passes:
                if a_exppass.lisfile == a_lisfile:
                    a_exppass.pipeline = pipelinable[a_lisfile]
                    break



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
    """Gets the data into eee from all existing .lis files from the given experiment.
    
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
                # so colour them yellow; genuine errors (perl die messages, scp failures) are not
                # labelled that way and stay red.
                utils.shell_command("getdata.pl", cmd_args, shell=True,
                                    stdout=None, stderr=subprocess.STDOUT, bufsize=0,
                                    stderr_warn_re=re.compile(r"warning", re.IGNORECASE),
                                    logfile=exp.dirs.logs / "getdata.log")
                return True
            except Exception as e:
                logger.error(f"Error fetching data for {a_pass.lisfile.name}: {e}")
                traceback.print_exc()
                return False

        if len(exp.correlator_passes) == 0:
            rprint("[bold yellow]No correlator passes found to fetch[/bold yellow]")
            return True

        with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes), 4)) as pool:
            results = list(pool.map(_fetch_pass, exp.correlator_passes))
            
        if not all(results):
            failed_count = len(results) - sum(results)
            logger.error(f"Failed to fetch data for {failed_count} passes")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Unexpected error in getdata: {e}")
        traceback.print_exc()
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
                cor_size = int(du_result.stdout.split()[-2])
                available_space = utils.space_available(Path.cwd())
                if available_space <= 1.2*u.kbit*cor_size:
                    rprint("\n\n[bold red]There is no enough space in the computer to create " \
                           "the MS file[/bold red]")
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
                logger.error(f"Error running j2ms2 for {a_pass.lisfile.name}: {e}")
                traceback.print_exc()
                return False

        with ThreadPoolExecutor(max_workers=10) as pool:
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
                from . import lisfiles
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
                lag_future = pool.submit(utils.shell_command, "j2ms2", lag_args,
                    shell=True, stdout=None, stderr=subprocess.STDOUT, bufsize=0,
                    logfile=exp.dirs.logs / "j2ms2-lag.log")

            ms_results = [f.result() for f in ms_futures]
            if lag_future is not None:
                try:
                    lag_future.result()
                    logger.info(f"Created lag-space MS: {lag_ms}")
                except Exception as e:
                    logger.warning(f"Lag-space MS creation failed (non-fatal): {e}")

        return all(ms_results)
    except Exception as e:
        logger.error(f"Unexpected error in j2ms2: {e}")
        traceback.print_exc()
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
        with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes), 10)) as executor:
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
        with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes)-1, 10)) as executor:
            futures = [executor.submit(_update_mpc_pass, a_pass) for a_pass in exp.correlator_passes[1:]]
            for fut in futures:
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
    fringe-peak amplitude in each IF (``exp.lag_bandpass``), which drives the PolConvert
    reference-antenna choice (flattest bandpass; see :func:`_rank_polconvert_refants`).

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

    ff_names = set(exp.sources.fringefinder)

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
                key = (int(cross_scans[i]), int(cross_a1[i]), int(cross_a2[i]))
                is_ff_row = int(cross_fields[i]) in ff_field_ids
                if key not in acc_sum:
                    acc_sum[key] = lag[i].copy()
                    acc_cnt[key] = 1
                    acc_ff[key] = is_ff_row
                else:
                    acc_sum[key] += lag[i]
                    acc_cnt[key] += 1
                if is_ff_row:
                    ifkey = (key[0], key[1], key[2], int(cross_ddids[i]))
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
        med = np.median(spec, axis=0)
        noise = 1.4826 * np.median(np.abs(spec - med), axis=0)
        snr = np.divide(peak, noise, out=np.zeros_like(peak, dtype=float), where=noise > 0)
        is_ff = acc_ff[(scan, a1i, a2i)]
        snr_max = float(np.max(snr)) if is_ff else 0.0
        for aidx in (a1i, a2i):
            key = (scan, aidx)
            if key not in best_snr:
                best_snr[key] = snr.copy()
            else:
                np.maximum(best_snr[key], snr, out=best_snr[key])
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
    # reference antenna is later chosen as the non-linear antenna whose amplitudes vary least
    # across IFs (flattest bandpass); see _rank_polconvert_refants.
    parallel_idx = [i for i, l in enumerate(pol_labels) if l in _PARALLEL_POLS]
    bp_amp: dict[tuple[int, int], np.ndarray] = {}  # (scan, antenna) -> per-IF amplitude
    for (scan, a1i, a2i, ddid), spec_sum in acc_if_sum.items():
        spec = spec_sum / acc_if_cnt[(scan, a1i, a2i, ddid)]   # mean |lag| spectrum (nchan, npol)
        peak = np.max(spec, axis=0)                            # (npol,) fringe-peak per pol
        par = float(np.mean(peak[parallel_idx])) if parallel_idx else float(np.max(peak))
        for aidx in (a1i, a2i):
            key = (scan, aidx)
            if key not in bp_amp:
                bp_amp[key] = np.full(n_spw, np.nan, dtype=float)
            cur = bp_amp[key][ddid]
            if np.isnan(cur) or par > cur:        # keep the strongest baseline per IF
                bp_amp[key][ddid] = par

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
    from .plotting import Jplot

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

            plotter = Jplot(ms=str(a_pass.msfile.name), refant=refant, calsrc=','.join(calsources),
                            weight_plots=(do_weights and counter == 1))

            if not plotter.create_plot(sources=calsources):
                logger.error(f"Standardplots failed for {a_pass.msfile.name}")
                return False

            # Retrieve the summary into a log file
            logger.info(utils.shell_command("echo", [f'"ms {a_pass.msfile.name};r"', "|", "jplotter"],
                                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT))
        except Exception:
            logger.error("Standardplots reported an error!")
            traceback.print_exc()
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

    convert_ps_to_png(exp.dirs.plots, exp.expname.lower())
    # rprint("\n[bold yellow]Take a look at the produced standard plots:[/bold yellow]")
    # rprint(f"[yellow]{'\n- '.join([aplot for aplot in standardplots])}"
    #        "\nOpening the dashboard in your browser...[/yellow]")
    serve_dashboard(exp, exp.dirs.plots)
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
    serve_dashboard(exp, exp.dirs.plots, pipeline_dir=exp.dirs.pipe_out)
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
        with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes), 4)) as executor:
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

    with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes), 4)) as executor:
        futures = [executor.submit(_fix_mounts, a_pass) for a_pass in exp.correlator_passes]
        for fut in futures:
            fut.result()  # Propagate any exceptions

    return True


def polswap(exp: experiment.Experiment) -> bool:
    """Swaps the polarization of the given antennas for all associated MS files
    to the given experiment.
    
    Args:
        exp (experiment.Experiment): Experiment object with polswap antenna information.
    
    Returns:
        bool: True if polarization swap was applied successfully.
    """
    if len(exp.antennas.polswap) > 0:
        def _polswap_pass(a_pass):
            for antenna in exp.antennas.polswap:
                mstools.polswap(a_pass.msfile, antenna)

        with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes), 4)) as executor:
            futures = [executor.submit(_polswap_pass, a_pass) for a_pass in exp.correlator_passes]
            for fut in futures:
                fut.result()  # Propagate any exceptions
        logger.info(f"polswap {','.join(exp.antennas.polswap)}")
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
    with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes), 4)) as executor:
        futures = [executor.submit(_flag_weights_pass, a_pass) for a_pass in exp.correlator_passes]
        for fut in futures:
            fut.result()  # Propagate any exceptions
    return True


def update_piletter(exp: experiment.Experiment) -> bool:
    """Updates the PI letter by changing two things:
    - Removing the trailing epoch-related character in the experiment name.
    - Adding the weightthreshold that was used and how much data were flagged.
    """
    if exp.correlator_passes[0].flagged_weights is None:
        weightthreshold: Union[int, float] = -1
        flaggeddata: Union[int, float] = -1
    else:
        weightthreshold = float(exp.correlator_passes[0].flagged_weights.threshold)
        flaggeddata = float(exp.correlator_passes[0].flagged_weights.percentage)

    polconvert_written = subprocess.call(["grep", "Martí-Vidal,",
                                          f"{exp.expname.lower()}.piletter"],
                                         shell=False, stdout=subprocess.PIPE) == 0
    with open(f"{exp.expname.lower()}.piletter", 'r') as orifile:
        with open(f"{exp.expname.lower()}.piletter~", 'w') as destfile:
            for a_line in orifile.readlines():
                tmp_line = a_line
                if ('derived from the following EVN project code(s):' in tmp_line) and \
                   (exp.expname[-1].isalpha()):
                    tmp_line = tmp_line.replace(exp.expname, exp.expname[:-1])

                if ('***SuppSci:' not in tmp_line) and ('there is one***' not in tmp_line):
                    if '***weight cutoff***' in tmp_line:
                        tmp_line = tmp_line.replace('***weight cutoff***', f"{weightthreshold:.2}")

                    if '***percent flagged***' in tmp_line:
                        tmp_line = tmp_line.replace('***percent flagged***', f"{flaggeddata:.2}")

                    for ant in exp.correlator_passes[0].antennas:
                        if (f"{ant.name.capitalize()}:" in tmp_line) and (not ant.observed):
                            tmp_line = tmp_line.replace(f"{ant.name.capitalize()}:",
                                                    f"{ant.name.capitalize()}: Could not observe.")

                    destfile.write(tmp_line)
                    if (utils.PILETTER_REMARKS_ANCHOR in tmp_line) and (not polconvert_written):
                        # The polconvert paragraph, the bandwidth-limitation paragraph, and
                        # the opacity paragraph are independent: each may apply on its own.
                        # Previously the bandwidth and opacity blocks were nested inside
                        # `if len(polconvert) > 0`, which silently suppressed them whenever
                        # no antenna required PolConvert.
                        if len(exp.antennas.polconvert) > 0:
                            destfile.write("\n")
                            if len(exp.antennas.polconvert) > 1:
                                s = f"s {', '.join(exp.antennas.polconvert[:-1])} and " \
                                    f"{exp.antennas.polconvert[-1]} "
                            else:
                                s = f" {exp.antennas.polconvert[0]} "

                            destfile.write(f"- Note that the antenna{s}originally observed linear "
                                           "polarizations, which were transformed to circular "
                                           "ones during post-processing via the PolConvert "
                                           "program (Martí-Vidal, et al. 2016, A&A,587, A143). "
                                           "Thanks to this correction, you can automatically "
                                           "recover the absolute EVPA value when using the "
                                           "antenna as reference station during fringe-fitting.\n")

                        ants_bw = {}
                        if len(set([cp.freqsetup.subbands for cp in exp.correlator_passes])) == 1:
                            for antenna in exp.correlator_passes[0].antennas:
                                if 0 < len(antenna.subbands) < \
                                        exp.correlator_passes[0].freqsetup.subbands:
                                    # In case the antenna observed a consecutive number of subbands
                                    ant_sbs = np.array(antenna.subbands)
                                    ant_sbs[1:] = ant_sbs[1:] - ant_sbs[:-1]
                                    if (ant_sbs[1:] == 1).all():
                                        ants_bw[antenna.name] = \
                                              [f"{min(antenna.subbands)+1}-{max(antenna.subbands)+1}"]
                                    else:
                                        ants_bw[antenna.name] = [f"{antenna.subbands}"]
                        else:
                            for antenna in exp.correlator_passes[0].antennas:
                                for i,a_pass in enumerate(exp.correlator_passes):
                                    if 0 < len(antenna.subbands) < a_pass.freqsetup.subbands:
                                        if antenna.name not in ants_bw:
                                            ant_sbs = np.array(antenna.subbands)
                                            ant_sbs[1:] = ant_sbs[1:] - ant_sbs[:-1]
                                            if (ant_sbs[1:] == 1).all():
                                                ants_bw[antenna.name] = [
                                                        f"{min(antenna.subbands)+1}-"
                                                        f"{max(antenna.subbands)+1} "
                                                        f"(in correlator pass #{i+1})"]
                                            else:
                                                ants_bw[antenna.name] = [f"{antenna.subbands} "
                                                                 f"(in correlator pass #{i+1})"]
                                        else:
                                            ants_bw[antenna.name].append( \
                                                f"{min(antenna.subbands)+1}-" \
                                                f"{max(antenna.subbands)+1} "
                                                f"(in correlator pass #{i+1})")

                        if len(ants_bw) > 0:
                            ants_bw_r = defaultdict(list)
                            for ant in ants_bw:
                                for sb_range in ants_bw[ant]:
                                    ants_bw_r[sb_range].append(ant)

                            s = "- Note that "
                            for i,ant_r in enumerate(ants_bw_r):
                                if i == 0:
                                    s += f"{', '.join(ants_bw_r[ant_r])} only observed " \
                                         f"subbands {ant_r}, "
                                elif i== len(ants_bw_r)-1:
                                    s += f"and {', '.join(ants_bw_r[ant_r])} subbands {ant_r}, "
                                else:
                                    s += f"{', '.join(ants_bw_r[ant_r])} subbands {ant_r}, "

                            s += "due to their local bandwidth limitations.\n"
                            destfile.write(s)

                        if len(exp.antennas.opacity) >= 1:
                            s = "- Note that the data from the antenna"
                            s_end = (" have been corrected for opacity in the Tsys/Gain Curve "
                                     "measurements.\n")
                            if len(exp.antennas.opacity) > 1:
                                s += f"s {', '.join(exp.antennas.opacity[:-1])} and " \
                                     f"{exp.antennas.opacity[-1]}"
                            else:
                                s += f" {exp.antennas.opacity[0]}"
                            destfile.write(s + s_end)

    os.rename(f"{exp.expname.lower()}.piletter~", f"{exp.expname.lower()}.piletter")
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

    Selects chunk_size based on estimated IDI file size. Skips passes where
    FITS-IDI files already exist.

    When ``exp.tconvert_in_eee`` is set (the default; toggled by
    ``postprocess --no-tConvert-in-eee``) each pass is converted on eee instead, as a
    temporary workaround for a broken local tConvert (see :func:`_tconvert_pass_in_eee`).
    Passes are independent, so they run in parallel.

    Args:
        exp: Experiment object.

    Returns:
        True if all passes converted successfully.
    """
    passes = [a_pass for a_pass in exp.correlator_passes
              if len(glob.glob(f"{a_pass.fitsidifile}*")) == 0]
    if not passes:
        return True

    if not exp.tconvert_in_eee:
        for a_pass in passes:
            utils.shell_command(_TCONVERT_BIN, ["-v", a_pass.lisfile.name, "-o",
                                                _tconvert_chunk_arg(a_pass)],
                                stdout=None, stderr=subprocess.STDOUT,
                                logfile=exp.dirs.logs / "tconvert.log")
        return True

    server = experiment.retrieve_servers()['eee']
    remote = f"{server.user}@{server.host}"
    with ThreadPoolExecutor(max_workers=min(len(passes), 4)) as pool:
        futures = [pool.submit(_tconvert_pass_in_eee, exp, remote, a_pass) for a_pass in passes]
        return all(future.result() for future in futures)


def _tconvert_pass_in_eee(exp: experiment.Experiment, remote: str,
                          a_pass: experiment.CorrelatorPass) -> bool:
    """Converts one correlator pass to FITS-IDI by running tConvert on eee.

    Temporary workaround for the broken local tConvert: copies the pass MS (and its
    small .lis) to ``<remote>:/data0/temp/<lisname>/``, runs the very same tConvert
    command there, copies the produced FITS-IDI files back into the current directory,
    and finally removes the remote temp directory (even if a step failed). Each pass
    uses its own remote sub-directory so several passes can run concurrently.

    Args:
        exp: Experiment object (used for the log directory).
        remote: ``user@host`` of eee.
        a_pass: Correlator pass to convert.

    Returns:
        True on success.
    """
    chunk_arg = _tconvert_chunk_arg(a_pass)
    remote_dir = _EEE_TCONVERT_TEMP / a_pass.lisfile.stem
    try:
        utils.ssh(remote, f"rm -rf {remote_dir} && mkdir -p {remote_dir}")
        # rsync (not scp): the MS is a directory tree of many files, and rsync both moves
        # such trees faster and can resume a partial transfer (--partial) of these very
        # large files. The .lis is tiny and goes in the same call as the MS.
        utils.rsync([str(a_pass.msfile), str(a_pass.lisfile)], f"{remote}:{remote_dir}/",
                    timeout=_EEE_RSYNC_TIMEOUT_S)

        # Run from inside the temp dir so the relative MS / FITS-IDI names in the .lis resolve.
        cmd = f"cd {remote_dir} && /eee/bin/tConvert -v {a_pass.lisfile.name} -o {chunk_arg}"
        output = utils.ssh(remote, cmd, stderr=subprocess.STDOUT, timeout=_EEE_TCONVERT_TIMEOUT_S)
        log_fh, log_path = utils.open_unique_log(exp.dirs.logs / "tconvert.log")
        try:
            log_fh.write(output or "")
        finally:
            log_fh.close()
        logger.debug(f"tConvert (eee) output for {a_pass.lisfile.name} written to {log_path}")

        # Bring the (several) FITS-IDI files this pass produced back to the current directory.
        utils.rsync(f"{remote}:{remote_dir}/{a_pass.fitsidifile}*", ".",
                    timeout=_EEE_RSYNC_TIMEOUT_S)
    finally:
        utils.ssh(remote, f"rm -rf {remote_dir}")

    return True


def _find_best_fringefinder_scan(exp: experiment.Experiment) -> Optional[experiment.Scan]:
    """Find the fringe-finder scan observed by the most antennas.

    Args:
        exp: Experiment object.

    Returns:
        The Scan with the most observed stations on a fringe-finder source, or None.
    """
    ff_sources = exp.sources.fringefinder
    if not ff_sources:
        return None

    best_scan = None
    best_count = 0
    for scan in exp.scans:
        if scan.source in ff_sources:
            n_observed = len(scan.stations_observed) if scan.stations_observed else len(scan.stations_scheduled)
            if n_observed > best_count:
                best_count = n_observed
                best_scan = scan

    return best_scan


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


def _scan_to_aips_timerange(scan: experiment.Scan, obsdate, trim_start_min: int = 1, trim_end_min: int = 0) -> list[int]:
    """Convert a scan's time range to AIPS format with optional trimming.

    AIPS format: [day_start, hour, minute, second, day_end, hour_end, minute_end, second_end]
    where day is days since the beginning of the observation (0 if same day).

    Args:
        scan: Scan object with starttime (datetime) and duration_s (int).
        obsdate: Observation start date (datetime.date).
        trim_start_min: Minutes to remove from the scan start.
        trim_end_min: Minutes to remove from the scan end.

    Returns:
        8-element list in AIPS time format.
    """
    from datetime import datetime as dt
    obs_midnight = dt.combine(obsdate, dt.min.time())
    start = scan.starttime + timedelta(minutes=trim_start_min)
    end = scan.starttime + timedelta(seconds=scan.duration_s) - timedelta(minutes=trim_end_min)

    def _to_aips(t):
        total_sec = int((t - obs_midnight).total_seconds())
        days = total_sec // 86400
        remainder = total_sec % 86400
        hours = remainder // 3600
        remainder = remainder % 3600
        minutes = remainder // 60
        seconds = remainder % 60
        return [days, hours, minutes, seconds]

    return _to_aips(start) + _to_aips(end)


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
        linants=str([a.upper() for a in lin_ants]),
        refant=repr(refant.upper()),
        exclude_ants=str([a.upper() for a in exclude_ants]),
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


def _rank_polconvert_refants(exp: experiment.Experiment, lin_ants: list[str],
                             subbands: set[int], scan_key: str) -> list[str]:
    """PolConvert reference-antenna candidates, best-first.

    A candidate must be (1) observed, (2) NOT one of the linear antennas being converted
    (otherwise the conversion would reference itself), and (3) cover every IF (subband) that
    has to be converted. The best reference is then the flattest bandpass (smallest per-IF
    amplitude scatter) *among the well-detected candidates* — those whose lag SNR reaches
    ``_POLCONVERT_REFANT_SNR_FRACTION`` of the best candidate's SNR. Gating on SNR first is
    essential: the flatness metric is noise-dominated for weak antennas, so without it a
    low-SNR station can edge out a strong, equally-flat one (e.g. on EZ041A pure flatness picks
    Jb at SNR 113 over Mc at SNR 419). Below-gate candidates are kept as lower-priority
    fallbacks ordered by SNR; the experiment ``refant`` order is the final tie-breaker. Without
    any lag data the result degrades gracefully to that ``refant`` order.
    """
    ifs = sorted(subbands)
    snr_by_ant = exp.lag_snr.get(scan_key, {})

    def _snr(ant: str) -> float:
        d = snr_by_ant.get(ant, {})
        return max(d.values()) if d else 0.0

    candidates = [a.name for a in exp.antennas
                  if a.observed and a.name not in lin_ants and subbands.issubset(set(a.subbands))]
    if not candidates:
        return []
    priority = {name: i for i, name in enumerate(exp.refant or [])}
    gate = max(_POL_MIN_SNR, _POLCONVERT_REFANT_SNR_FRACTION * max(_snr(a) for a in candidates))

    def _key(ant: str):
        sensitive = _snr(ant) >= gate
        # Sensitive antennas first, ranked by flatness; the rest after, ranked by SNR.
        return (0 if sensitive else 1,
                _refant_bandpass_scatter(exp, ant, scan_key, ifs) if sensitive else 0.0,
                -_snr(ant), priority.get(ant, len(priority)))

    candidates.sort(key=_key)
    return candidates


def _polconvert_exclude_ants(exp: experiment.Experiment, lin_ants: list[str], refant: str,
                             subbands: set[int]) -> list[str]:
    """Antennas to exclude from the PolConvert solve: those not observed, plus observed ones
    (other than the reference or a linear antenna) that did not record every IF to convert."""
    exclude: list[str] = []
    for ant in exp.antennas:
        if not ant.observed:
            exclude.append(ant.name)
        elif ant.name not in lin_ants and ant.name != refant \
                and not subbands.issubset(set(ant.subbands)):
            exclude.append(ant.name)
    return sorted(set(exclude))


def _check_fringe_peaks(logdir: str = 'polconvert_logs') -> bool:
    """Whether the PolConvert FRINGE.PEAKS indicate a good conversion.

    Each ``FRINGE.PEAKS_*.dat`` (one per IF) lists the normalized fringe-peak amplitude of RR,
    LL, RL and LR on the reference baseline. A real conversion concentrates power in the
    parallel hands, so we require ``(RR+LL)/(RL+LR) >= _POLCONVERT_MIN_RATIO`` in every IF; a
    failed/linear solution leaves the four products comparable (ratio ~1).
    """
    peaks_dir = Path(logdir) / 'FRINGE.PEAKS'
    dat_files = sorted(peaks_dir.glob('*.dat')) if peaks_dir.exists() else []
    if not dat_files:
        logger.warning(f"No FRINGE.PEAKS/*.dat files in {logdir}; cannot assess the solution.")
        return False

    ratios: list[float] = []
    for dat_file in dat_files:
        content = dat_file.read_text()
        a = {pol: float(m.group(1)) for pol in ('RR', 'LL', 'RL', 'LR')
             if (m := re.search(rf'{pol}:\s*([\d.eE+-]+)\s*;', content))}
        if len(a) != 4:
            continue
        cross = a['RL'] + a['LR']
        ratios.append((a['RR'] + a['LL']) / cross if cross > 0 else float('inf'))

    if not ratios:
        logger.warning("Could not parse any FRINGE.PEAKS amplitudes.")
        return False

    worst = min(ratios)
    logger.info(f"PolConvert (RR+LL)/(RL+LR) per IF: min={worst:.1f}, "
                f"median={float(np.median(ratios)):.1f} (need >= {_POLCONVERT_MIN_RATIO} in every IF).")
    return worst >= _POLCONVERT_MIN_RATIO


def _run_polconvert_cli(template_file: Path, mode: str) -> int:
    """Run ``polconvert.py <template> <mode>`` locally, retrying transient segfaults.

    PolConvert occasionally dies with SIGSEGV (a known upstream bug) for reasons unrelated to
    the inputs. Because it runs in a subprocess a crash returns a negative exit code instead of
    killing post-processing, so the identical command is simply retried up to
    ``_POLCONVERT_SEGFAULT_RETRIES`` extra times. Returns the final exit code (0 on success).
    """
    attempts = _POLCONVERT_SEGFAULT_RETRIES + 1
    rc = 1
    for attempt in range(1, attempts + 1):
        result = subprocess.run(['polconvert.py', str(template_file), mode],
                                capture_output=True, text=True)
        rc = result.returncode
        if rc == 0:
            return 0
        if rc < 0:  # killed by a signal (e.g. -11 = SIGSEGV): transient, retry
            logger.warning(f"polconvert.py {mode} crashed (signal {-rc}) "
                           f"[attempt {attempt}/{attempts}]; retrying.")
            continue
        logger.warning(f"polconvert.py {mode} failed (rc={rc}): {result.stderr.strip()[-500:]}")
        break
    return rc


def polconvert(exp: experiment.Experiment) -> bool:
    """Run PolConvert locally, auto-selecting the scan and reference antenna.

    Linear-polarization antennas (``exp.antennas.polconvert``) are converted to circular using:

      * the fringe-finder scan with the most detected antennas and the highest lag SNR
        (:func:`_rank_fringefinder_scans`), and
      * a reference antenna that is circular (not one of the linear antennas), records every IF
        to convert, and has the flattest bandpass across those IFs
        (:func:`_rank_polconvert_refants`).

    For each (scan, reference) candidate it runs ``polconvert.py --compute`` (retrying transient
    segfaults), checks the FRINGE.PEAKS ``(RR+LL)/(RL+LR)`` ratio per IF, and on success applies
    the solution to every FITS-IDI file with ``--apply``. A candidate that does not yield a good
    solution falls through to the next reference antenna, then the next scan.

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

    ff_scans = _rank_fringefinder_scans(exp)
    if not ff_scans:
        logger.error("No fringe-finder scans found for PolConvert.")
        return False

    logdir = 'polconvert_logs'
    tried = 0
    for scan in ff_scans:
        scan_key = str(_scan_number(scan))
        time_range = _scan_to_aips_timerange(scan, exp.obsdate)
        ref_idi = find_idi_mod.find_idi_with_time(idi_files=idi_files, aipstime=time_range[:4],
                                                  verbose=False)
        if ref_idi is None:
            logger.debug(f"No FITS-IDI covers scan {scan.scanno} ({time_range[:4]}); skipping.")
            continue

        refants = _rank_polconvert_refants(exp, lin_ants, subbands, scan_key)
        if not refants:
            logger.warning(f"No circular reference antenna covers all IFs on scan {scan.scanno}.")
            continue

        n_det, snr_sum = _scan_lag_score(exp, scan)
        logger.info(f"PolConvert: scan {scan.scanno} on {scan.source} "
                    f"({n_det} antennas detected, SNR sum {snr_sum}); "
                    f"reference-antenna order: {', '.join(refants)}.")

        for refant in refants:
            exclude_ants = _polconvert_exclude_ants(exp, lin_ants, refant, subbands)
            scatter = _refant_bandpass_scatter(exp, refant, scan_key, sorted(subbands))
            logger.info(f"PolConvert attempt: linants={lin_ants}, refant={refant} "
                        f"(bandpass scatter {scatter:.3f}), exclude={exclude_ants}, IFs={do_ifs}.")
            template_file = _write_polconvert_template(
                exp, ref_idi, lin_ants, refant, exclude_ants, do_ifs, time_range,
                _POLCONVERT_CHANAVG, _POLCONVERT_TIMEAVG_S, _POLCONVERT_SOLVE_WEIGHT, logdir)
            tried += 1

            if _run_polconvert_cli(template_file, '--compute') != 0:
                continue
            if not _check_fringe_peaks(logdir):
                logger.info(f"Solution with refant {refant} on scan {scan.scanno} is not good "
                            "enough; trying the next reference antenna.")
                continue

            logger.info(f"Good PolConvert solution: scan {scan.scanno}, refant {refant}. "
                        "Applying it to all FITS-IDI files.")
            if _run_polconvert_cli(template_file, '--apply') != 0:
                logger.error("PolConvert --apply failed after a good --compute. Stopping.")
                return False
            exp.store()
            return True

    logger.error(f"PolConvert could not reach a good solution after {tried} attempt(s) over "
                 f"{len(ff_scans)} fringe-finder scan(s). Inspect {logdir} or run it manually.")
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

        import casatasks
        casatasks.importfitsidi(vis=pconv_ms, fitsidifile=idi_files, constobsid=True, scanreindexgap_s=8.0, specframe='GEO')
        logger.info(f"Created {pconv_ms} from {len(idi_files)} PCONVERT IDI files.")

        if not exp.refant:
            logger.error("No reference antenna set for polconvert verification plots.")
            return False

        calsources = exp.sources.fringefinder
        if not calsources:
            logger.error("No fringe-finder sources found for polconvert verification.")
            return False

        from .plotting import Jplot
        plotter = Jplot(ms=pconv_ms, refant=exp.refant[0], calsrc=','.join(calsources))
        plotter.create_plot(sources=calsources, plots=['cross'])

        # Rename pconv plot files to standard names so they override the previous ones
        for stdplot_file in glob.glob('*-pconv*.ps'):
            Path(stdplot_file).rename(stdplot_file.replace('-pconv', ''))

        # Convert PS plots to PNG images, overriding previous ones
        convert_ps_to_png(exp.dirs.plots, exp.expname.lower())
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

    For NME or test experiments (name starts with 'N' or 'F'), no credentials are set.
    Otherwise recovers from an existing .auth file, or generates a new random password.

    Args:
        exp: Experiment object.

    Returns:
        True if credentials were set or not needed, False on error.
    """
    if exp.expname.upper()[0] in ('N', 'F'):
        logger.info(f"{exp.expname} is an NME or test experiment. No authentication set.")
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
        possible_char = string.digits + string.ascii_letters
        password = "".join(random.sample(possible_char, 12))
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

    archive_exp = f"{exp.expname.upper()}_{exp.obsdate.strftime('%y%m%d')}"
    # Protect both the archived source data ("source") and the pipeline results/plots
    # ("pipe") for the protected sources (typically the targets). Missing the "pipe"
    # protection would leave the pipeline data for the target publicly accessible.
    for protection in ("source", "pipe"):
        try:
            utils.shell_command("auth_pipe.py", ["-e", archive_exp,
                                "-s", ' '.join(protected_sources), "-p", protection])
        except ValueError:
            logger.error(f"Could not protect experiment {protection} files in archive.")
            return False

    logger.info(f"Protected sources (source and pipeline data): {', '.join(protected_sources)}")
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
    fits2check = glob.glob(f"{exp.expname.lower()}_*_*.IDI1") + \
                 glob.glob(f"{exp.expname.lower()}_*_*.IDI")
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
        i1 = i0 + 1 + filename[i0+1:].index('_')
        return int(filename[i0+1:i1])

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
            pass_files = [idi for idi in idifiles if f"_{i+1}_1.IDI" in idi]
            _run_append(antabfile, pass_files)

    if not all(check_consistency(f) for f in fits2check):
        logger.error("The Tsys/GC could not be imported into the FITS-IDI files.")
        return False

    return True


def create_piletter_auth(exp: experiment.Experiment) -> bool:
    """Creates a copy of the PI letter with download credentials inserted.

    Copies {expname}.piletter to {expname}.piletter_auth and inserts the
    archive download credentials as a new paragraph right after the line
    ending the second paragraph ("...EVN Pipeline plots and products.").
    Does nothing if no credentials are set.

    Args:
        exp: Experiment object.

    Returns:
        True if the file was created or no credentials are set, False on error.
    """
    if exp.credentials is None or exp.credentials.password is None:
        logger.debug("No credentials set; skipping .piletter_auth creation.")
        return True

    piletter = Path(f"{exp.expname.lower()}.piletter")
    piletter_auth = Path(f"{exp.expname.lower()}.piletter_auth")

    if not piletter.exists():
        logger.error(f"{piletter} not found; cannot create {piletter_auth.name}.")
        return False

    marker = "EVN Pipeline plots and products."
    credentials_block = (
        "\nTo access the data, use the following credentials:\n"
        f"  username: {exp.credentials.username}\n"
        f"  password: {exp.credentials.password}\n"
    )

    with open(piletter, 'r') as f:
        lines = f.readlines()

    inserted = False
    with open(piletter_auth, 'w') as f:
        for line in lines:
            f.write(line)
            if not inserted and marker in line:
                f.write(credentials_block)
                inserted = True

    if not inserted:
        logger.warning(f"Marker '{marker}' not found in {piletter.name}; "
                       f"appending credentials at the end of {piletter_auth.name}.")
        with open(piletter_auth, 'a') as f:
            f.write(credentials_block)

    logger.info(f"Created {piletter_auth.name} with download credentials.")
    return True


def send_letters(exp: experiment.Experiment) -> bool:
    """Creates the authenticated PI letter (if needed), archives it, and
    reminds the user to send it to the PIs.

    Args:
        exp: Experiment object.

    Returns:
        True always.
    """
    has_auth = exp.credentials is not None and exp.credentials.password is not None
    if has_auth:
        if not create_piletter_auth(exp):
            return False

    piletter_name = f"{exp.expname.lower()}.piletter{'_auth' if has_auth else ''}"
    utils.shell_command("archive.pl", ["-stnd", "-e", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}",
                                       piletter_name])
    body = f"[bold]Send[/bold] [bold green]{piletter_name}[/bold green] [bold]to [/bold]" \
           f"[bold cyan]{', '.join(p.name for p in exp.pi)}[/bold cyan]: " \
           f"[bold]{', '.join(p.email for p in exp.pi)}[/bold]" \
           f"\nAnd CC [cyan]jops@jive.eu[/cyan]"
    Console().print(Panel(body, title="[bold yellow]Send the PI Letter[/bold yellow]",
                          border_style="yellow", padding=(1, 2)))
    return True


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


def nme_report(exp: experiment.Experiment) -> bool:
    """Reminds the user to write the NME report if applicable.

    Args:
        exp: Experiment object.

    Returns:
        True always.
    """
    if exp.expname[0].upper() == 'N':
        logger.info("This is an NME — time to write the NME Report.")
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
