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
from itertools import product
from . import experiment, utils, mstools
from .plotting import convert_ps_to_png, serve_dashboard
# polconvert_main kept for future use once version compatibility is resolved.
# from .scripts.polconvert import main as polconvert_main
from evn_support import find_idi_with_time as find_idi_mod


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
                utils.shell_command("getdata.pl", cmd_args, shell=True,
                                    stdout=None, stderr=subprocess.STDOUT, bufsize=0)
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
                    
                utils.shell_command("j2ms2", j2ms2_args, shell=True, stdout=None, stderr=subprocess.STDOUT, bufsize=0)
                return True
            except Exception as e:
                logger.error(f"Error running j2ms2 for {a_pass.lisfile.name}: {e}")
                traceback.print_exc()
                return False

        with ThreadPoolExecutor(max_workers=10) as pool:
            results = pool.map(_j2ms2_correlator_pass, product([exp,], exp.correlator_passes))

        return all(results)
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
                    if ('Further remarks:' in tmp_line) and (not polconvert_written):
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

                        s = "- Note that the data from the antenna"
                        s_end = " have been corrected for opacity in the Tsys/Gain Curve " \
                                "measurements."
                        if len(exp.antennas.opacity) > 1:
                            s += f"s {', '.join(exp.antennas.opacity[:-1])} and " \
                                 f"{exp.antennas.opacity[-1]}"
                            destfile.write(s + s_end)
                        elif len(exp.antennas.opacity) == 1:
                            s += f" {exp.antennas.opacity[0]}"
                            destfile.write(s + s_end)

    os.rename(f"{exp.expname.lower()}.piletter~", f"{exp.expname.lower()}.piletter")
    return True


def tconvert(exp: experiment.Experiment) -> bool:
    """Runs tConvert on all correlator passes to create FITS-IDI files from the MS.

    Selects chunk_size based on estimated IDI file size. Skips passes where
    FITS-IDI files already exist.

    Args:
        exp: Experiment object.

    Returns:
        True if all passes converted successfully.
    """
    # tConvert_bin = 'tConvert'
    tConvert_bin = '/home/verkout/src/jive-casa/build-reftime_assert_fail/apps/tConvert/tConvert'
    for a_pass in exp.correlator_passes:
        if len(glob.glob(f"{a_pass.fitsidifile}*")) > 0:
            continue

        # The size difference between internal MS and FITS-IDI is around 1.55
        idi_size = 1.55*u.kbit*int(subprocess.run(f"du -s {str(a_pass.msfile)}", shell=True,
                                                  capture_output=True).stdout.decode().split()[0])

        if idi_size < 20*u.Gb:
            utils.shell_command(tConvert_bin, ["-v", a_pass.lisfile.name, "-o", "chunk_size=4GB"],
                                stdout=None, stderr=subprocess.STDOUT)
        elif idi_size < 4*u.Tb:
            utils.shell_command(tConvert_bin, ["-v", a_pass.lisfile.name, "-o", "chunk_size=8GB"],
                                stdout=None, stderr=subprocess.STDOUT)
        else:
            if utils.space_available(Path.cwd()) <= 1.1*idi_size:
                raise IOError("Not enough disk space to create the FITS-IDI files.")

            utils.shell_command(tConvert_bin, ["-v", a_pass.lisfile.name, "-o",
                                f"chunk_size={int(idi_size.to(u.Tb).value)}GB"],
                                stdout=None, stderr=subprocess.STDOUT)

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


def _check_fringe_peaks(logdir: str = 'polconvert_logs') -> bool:
    """Check FRINGE.PEAKS .dat files to verify PolConvert solution quality.

    Each .dat file corresponds to one subband and contains lines like
    ``POL: <amplitude> ; <snr>`` for RR, LL, RL, LR.

    Two checks are applied:
      1. Per-subband amplitude ratio ``(RR + LL) / (RL + LR) > 5``.
      2. Across subbands, the ``(RR + LL)`` and ``(RL + LR)`` SNR values must
         each stay within 40 % of their respective median.

    Args:
        logdir: Path to the polconvert log directory.

    Returns:
        True if the solution quality is acceptable.
    """
    peaks_dir = Path(logdir) / 'FRINGE.PEAKS'
    if not peaks_dir.exists():
        logger.warning(f"FRINGE.PEAKS directory not found in {logdir}")
        return False

    dat_files = sorted(peaks_dir.glob('*.dat'))
    if not dat_files:
        logger.warning("No .dat files found in FRINGE.PEAKS directory")
        return False

    # Parse amplitude and SNR for every polarization in each subband
    amps: list[dict[str, float]] = []
    snrs: list[dict[str, float]] = []
    for dat_file in dat_files:
        content = dat_file.read_text()
        a, s = {}, {}
        for pol in ('RR', 'LL', 'RL', 'LR'):
            match = re.search(rf'{pol}:\s*([\d.eE+-]+)\s*;\s*([\d.eE+-]+)', content)
            if not match:
                break
            a[pol], s[pol] = float(match.group(1)), float(match.group(2))
        if len(a) != 4:
            logger.debug(f"Could not extract all polarization values from {dat_file.name}")
            continue
        amps.append(a)
        snrs.append(s)

    if not amps:
        logger.warning("No valid .dat files found in FRINGE.PEAKS")
        return False

    # Check 1: amplitude ratio (RR+LL)/(RL+LR) > 5 per subband
    for i, a in enumerate(amps):
        cross = a['RL'] + a['LR']
        if cross <= 0:
            continue
        ratio = (a['RR'] + a['LL']) / cross
        if ratio < 5:
            logger.info(f"Subband {i}: (RR+LL)/(RL+LR)={ratio:.1f} — below 5x threshold")
            return False

    # Check 2: SNR consistency across subbands (each value within 40% of median)
    snr_parallel = np.array([s['RR'] + s['LL'] for s in snrs])
    snr_cross = np.array([s['RL'] + s['LR'] for s in snrs])
    for label, values in (('RR+LL', snr_parallel), ('RL+LR', snr_cross)):
        med = np.median(values)
        if med <= 0:
            continue
        dev = np.abs(values - med) / med
        if np.any(dev > 0.4):
            worst = int(np.argmax(dev))
            logger.info(f"SNR {label} subband {worst}: {values[worst]:.1f} deviates "
                        f"{dev[worst]*100:.0f}% from median {med:.1f} (>40% threshold)")
            return False

    return True


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


def polconvert(exp: experiment.Experiment) -> bool:
    """Run PolConvert automatically with iterative parameter tuning.

    Finds fringe-finder scans and iteratively tries different scans combined with
    different solve_weight / time_avg / time_range combinations until
    FRINGE.PEAKS quality checks pass. Then applies the solution to all IDI files.

    Args:
        exp: Experiment object.

    Returns:
        True if PolConvert succeeded or not needed, False if no good solution found.
    """
    if not exp.antennas.polconvert:
        logger.info("PolConvert is not required.")
        return True

    if len(glob.glob('*IDI*.PCONVERT')) > 0:
        logger.info("PolConvert output files already exist. Skipping.")
        return True

    # Get all fringe-finder scans sorted by quality (most observed antennas first)
    ff_scans = _get_all_fringefinder_scans(exp)
    if not ff_scans:
        logger.error("No fringe-finder scans found for PolConvert.")
        return False

    logger.info(f"Found {len(ff_scans)} fringe-finder scan(s) to try")

    # Common parameters
    lin_ants = [a for a in exp.antennas.polconvert]
    refant = exp.refant[0] if exp.refant else None
    if not refant:
        logger.error("No reference antenna set for PolConvert.")
        return False

    subbands_to_polconvert = set().union(*(exp.antennas[p].subbands for p in lin_ants))
    exclude_ants: list[str] = []
    for ant in exp.antennas:
        if not ant.observed:
            exclude_ants.append(ant.name)
        elif ant.name not in lin_ants and ant.name != refant:
            if not subbands_to_polconvert.issubset(set(ant.subbands)):
                exclude_ants.append(ant.name)

    exclude_ants = sorted(set(exclude_ants))
    do_ifs = [i+1 for i in sorted(list(subbands_to_polconvert))]
    idi_files = sorted(glob.glob(f"{exp.expname.lower()}_*_1.IDI*"))
    if not idi_files:
        logger.error("No FITS-IDI files found for PolConvert.")
        return False

    logdir = 'polconvert_logs'

    # Iterative parameter search: scans, solve_weight, time_avg, and time trimming
    solve_weights = [0.1, 0.01, 0.001]
    time_avgs = [20, 30, 60]
    trim_configs = [(1, 0), (2, 1), (3, 2)]

    # Loop over different fringe-finder scans
    for scan_idx, scan in enumerate(ff_scans, start=1):
        n_stations = len(scan.stations_observed) if scan.stations_observed else len(scan.stations_scheduled)
        logger.info(f"Trying scan {scan_idx}/{len(ff_scans)}: scan {scan.scanno} on {scan.source} "
                    f"({n_stations} stations, {scan.duration_s}s duration)")

        for trim_start, trim_end in trim_configs:
            time_range = _scan_to_aips_timerange(scan, exp.obsdate, trim_start, trim_end)

            ref_idi = find_idi_mod.find_idi_with_time(
                idi_files=sorted(idi_files), aipstime=time_range[:4], verbose=False)
            if ref_idi is None:
                logger.warning(f"No IDI file contains time with trim=({trim_start},{trim_end})min. Skipping.")
                continue

            for solve_weight in solve_weights:
                for time_avg in time_avgs:
                    logger.info(f"PolConvert attempt: solve_weight={solve_weight}, time_avg={time_avg}, "
                                f"trim=({trim_start},{trim_end})min")
                    template_file = _write_polconvert_template(
                        exp, ref_idi, lin_ants, refant, exclude_ants, do_ifs,
                        time_range, 16, time_avg, solve_weight, logdir)
                    result = subprocess.run(['polconvert.py', str(template_file), '--compute'],
                                           capture_output=True, text=True)
                    if result.returncode != 0:
                        logger.warning(f"PolConvert compute failed (rc={result.returncode}): {result.stderr}")
                        return False

                    if _check_fringe_peaks(logdir):
                        logger.info(f"PolConvert solution quality check passed using scan {scan.scanno}! "
                                    f"Parameters stored in {template_file}.")

                        logger.info("Applying PolConvert solution to all IDI files...")
                        result = subprocess.run(['polconvert.py', str(template_file), '--apply'],
                                               capture_output=True, text=True)
                        if result.returncode != 0:
                            logger.error(f"PolConvert apply failed (rc={result.returncode}): {result.stderr}")
                            return False

                        exp.store()
                        return True

                    logger.info("Solution quality insufficient, trying next parameters...")

        if scan_idx < len(ff_scans):
            logger.info(f"No good solution found with scan {scan.scanno}, moving to next scan...")

    logger.error(f"PolConvert could not reach a good solution with any of the {len(ff_scans)} fringe-finder scan(s) "
                 f"and parameter combinations.")
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

    try:
        utils.shell_command("auth_pipe.py", ["-e", f"{exp.expname.upper()}_{exp.obsdate.strftime('%y%m%d')}",
                            "-s", ' '.join(protected_sources), "-p", "source"])
        logger.info(f"Protected sources: {', '.join(protected_sources)}")
    except ValueError:
        logger.error("Could not protect experiment files in archive.")
        return False

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
    """Creates a copy of the PI letter with download credentials appended.

    Copies {expname}.piletter to {expname}.piletter_auth and appends the
    archive download credentials. Does nothing if no credentials are set.

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

    shutil.copy2(piletter, piletter_auth)
    with open(piletter_auth, 'a') as f:
        f.write(f"\nTo access the data, use the EVN Data Archive ({exp.archive_page})\n")
        f.write("with the following credentials:\n")
        f.write(f"  username: {exp.credentials.username}\n")
        f.write(f"  password: {exp.credentials.password}\n")

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
