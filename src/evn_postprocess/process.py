#! /usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the eee computer.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.

"""
import os
import glob
import string
import random
import traceback
from importlib import resources
from typing import Optional, Union
from pathlib import Path
from collections import defaultdict
import subprocess
import numpy as np
from loguru import logger
from astropy import units as u
from rich import print as rprint
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from itertools import product
from . import experiment
from . import utils
from . import mstools


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
        utils.shell_command("archive.pl", ["-auth", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}",
                                                 "-n", exp.credentials.username, "-p", exp.credentials.password])
    else:
        assert len(glob.glob("*_*.auth")) == 0, 'No credentials stored but auth file found'

    utils.shell_command("archive.pl", ["-stnd", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}", "*ps.gz"])
    utils.shell_command("archive.pl", ["-stnd", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}",
                                       f"{exp.expname.lower()}.piletter"])
    utils.shell_command("archive.pl", ["-fits", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}", "*IDI*"])
    return True


def getdata(exp: experiment.Experiment) -> bool:
    """Gets the data into eee from all existing .lis files from the given experiment.
    
    Args:
        exp (experiment.Experiment): Experiment object with correlator passes.
    
    Returns:
        bool: True if data was retrieved successfully.
    """
    def _fetch_pass(a_pass):
        utils.shell_command("getdata.pl",
                            ["-proj", exp.eEVNname if exp.eEVNname is not None else exp.expname,
                             "-lis", a_pass.lisfile.name],
                            shell=True,
                            stdout=None,
                            stderr=subprocess.STDOUT,
                            bufsize=0)

    if len(exp.correlator_passes) == 0:
        rprint("[bold yellow]No correlator passes found to fetch[/bold yellow]")
        return True

    with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes), 4)) as pool:
        results = pool.map(_fetch_pass, exp.correlator_passes)

    return True


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
    if utils.space_available(Path.cwd()) <= 1.2*u.kbit*int(subprocess.run(
                                               "du -sc */*.cor*", shell=True,
                                               capture_output=True).stdout.decode().split()[-2]):
        rprint("\n\n[bold red]There is no enough space in the computer to create " \
               "the MS file[/bold red]")
        raise IOError("Not enough disk space to create the MS file.")

    def _j2ms2_correlator_pass(args: tuple[experiment.Experiment, experiment.CorrelatorPass]) -> bool:
        exp, a_pass = args
        if not os.path.isdir(a_pass.msfile):
            utils.shell_command("j2ms2", ["-v", str(a_pass.lisfile)] + ([] if exp.eEVNname else ["fo:nosquash_source_table"]),
                                shell=True, stdout=None, stderr=subprocess.STDOUT, bufsize=0)

        return True

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = pool.map(_j2ms2_correlator_pass, product([exp,], exp.correlator_passes))

    return all(results)


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

    return True


def get_metadata_from_ms(exp: experiment.Experiment) -> bool:
    """Extracts metadata from MS files and populates the experiment object.
    
    Args:
        exp (experiment.Experiment): Experiment object to populate with MS metadata.
    
    Returns:
        bool: True if metadata was extracted successfully.
    """
    def _get_ms_metadata(exp: experiment.Experiment, a_pass: experiment.CorrelatorPass):
        ms = mstools.Ms(a_pass.msfile, runstats=True)
        rprint(f"Antennas in the MS: {ms.antennas}")
        for ant in ms.antennas:
            if ant.name not in a_pass.antennas:
                a_pass.antennas.append(ant)

            a_pass.antennas[ant.name].observed = ant.observed
            a_pass.antennas[ant.name].subbands = ant.subbands
            a_pass.antennas[ant.name].weights = ant.weights
        
        a_pass.freqsetup = experiment.Subbands(subbands=ms.freqsetup.nspw, channels=ms.freqsetup.nchan,
                                               frequency=ms.freqsetup.meanfreq, bandwidth=ms.freqsetup.bandwidth,
                                               polarizations=ms.freqsetup.polarizations)
        # TODO: Fix the ms.scans
        #for scanno in ms.scans:
        #    scannumbers = [int(s.scanno.replace('No', '')) for s in a_pass.scans]
        #    print(f"SCANNUMBERS: {scannumbers}")
        #    a_pass.scans[scannumbers.index(scanno)].stations_observed = list(ms.scans[scanno])
        
    def _update_mpc_pass(a_pass: experiment.CorrelatorPass):
            a_pass.antennas = exp.correlator_passes[0].antennas
            a_pass.sources = exp.correlator_passes[0].sources
            a_pass.freqsetup = exp.correlator_passes[0].freqsetup

    if len(exp.correlator_passes) > 1 and not exp.spectral_line:
        # then this is just a multiphase center with all setups identical. Do not loop
        # through all MSs.
        _get_ms_metadata(exp, exp.correlator_passes[0])
        with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes)-1, 10)) as executor:
            futures = [executor.submit(_update_mpc_pass, a_pass) for a_pass in exp.correlator_passes[1:]]
            for fut in futures:
                fut.result()
    else:
        with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes), 10)) as executor:
            futures = [executor.submit(_get_ms_metadata, exp, a_pass) 
                    for a_pass in exp.correlator_passes]
            for fut in futures:
                fut.result()

    exp.store()
    return True


def standardplots(exp: experiment.Experiment, do_weights=True) -> bool:
    """Runs the standardplots on the specified experiment using a reference antenna
    and sources to be picked for the auto- and cross-correlations.
    
    Args:
        exp (experiment.Experiment): Experiment object.
        do_weights (bool): Whether to include weight plots. Default True.
    
    Returns:
        bool: True if standardplots completed successfully, False otherwise.
    """
    # TODO: to be fully rewritten
    # To run for all correlator passes that will be pipelined.
    # Then once all of them finish, open the plots and ask user.
    counter = 0
    for a_pass in exp.correlator_passes:
        try:
            if a_pass.pipeline:
                if exp.refant:
                    refant = exp.refant[0] if len(exp.refant) == 1 else f"'{'|'.join(exp.refant)}'"
                else:
                    for ant in ('Ef', 'O8', 'Ys', 'Mc', 'Gb', 'At', 'Pt'):
                        if (ant in a_pass.antennas) and (a_pass.antennas[ant].observed):
                            refant = ant
                            break

                        raise ValueError("Couldn't find a good reference antenna for standardplots. "
                                        "Please specify it manually.")
                counter += 1
                if (counter == 1) and do_weights:
                    utils.shell_command("standardplots",
                                        ["-weight", a_pass.msfile.name, refant, ','.join(exp.sources.fringefinder)],
                                        stdout=None, stderr=subprocess.STDOUT)
                else:
                    utils.shell_command("standardplots",
                                        [a_pass.msfile.name, refant, ','.join(exp.sources.fringefinder)],
                                        stdout=None, stderr=subprocess.STDOUT)

                # Runs again jplotter but only to retrieve the summary into the output
                output = utils.shell_command("echo",
                                             [f'"ms {a_pass.msfile.name};r"', "|", "jplotter"],
                                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                with open('logs/standardplots.log', 'a') as f:
                    f.write(output)

        except Exception:
            rprint("\n\n[red]Standardplots reported an error![/red]")
            # TODO: these tracebacks should be one level above (in app.py)
            traceback.print_exc()
            return False

    return True


def print_exp(exp: experiment.Experiment, display_in_terminal: bool = True) -> bool:
    """Shows in the terminal all metadata related to the given experiment.
    """
    return exp.print_blessed(outputfile='notes.md', display_in_terminal=display_in_terminal)


def open_standardplot_files(exp) -> Optional[bool]:
    """Calls gv to open all plots generated by standardplots.
    """
    standardplots = []
    for plot_type in ('weight', 'auto', 'cross', 'ampphase'):
        standardplots += glob.glob(f"{exp.expname.lower()}*{plot_type}*.ps")
    # standardplots = glob.glob(f"{exp.expname.lower()}*.ps")

    if len(standardplots) == 0:
        raise FileNotFoundError(f"Standardplots for {exp.expname} not found but expected.")

    if exp.silent_mode:
        rprint("[bold yellow]You did not want me to open the plots. " \
               "You shall do it manually[/bold yellow]")
        print("Take a look at the produced standard plots:")
        print("\n".join([f"- {a_plot}" for a_plot in standardplots]))
        print("[green]Execute me again after that to continue the post-process.[/green]")
        return None

    try:
        for a_plot in standardplots:
            utils.shell_command("gv", a_plot, stdout=None, stderr=subprocess.STDOUT)
    except Exception as e:
        print(f"WARNING: Plots could not be opened. Do it manually.\nError: {e}.")
        return None

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
    elif utils.station_1bit_in_vix(exp.vixfile):
        print(f"\n\n[red]{'#'*10}\n#Traces of 1bit station found in {exp.vixfile} "
              "but no station specified to be corrected.[/red]\n\n")
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
            mstools.fix_yebes_mount(a_pass.msfile)
        if ('Ho' in exp.antennas.names) or ('Hb' in exp.antennas.names):
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
    return True


def flag_weights(exp: experiment.Experiment) -> bool:
    """Flags visibilities based on weight thresholds for all correlator passes.
    
    Args:
        exp (experiment.Experiment): Experiment object with flagged_weights information.
    
    Returns:
        bool: True if weight flagging was applied successfully.
    """
    def _flag_weights_pass(a_pass):
        total_vis, pct_total, pct_nonzero = mstools.flag_weights(
            a_pass.msfile, 
            a_pass.flagged_weights.threshold
        )
        a_pass.flagged_weights.percentage = pct_nonzero
        exp.log(f"flag_weights: {a_pass.msfile.name} threshold={a_pass.flagged_weights.threshold}\n"
                f"# {pct_total:.2f}% total flagged, {pct_nonzero:.2f}% non-zero weights flagged\n")

    with ThreadPoolExecutor(max_workers=min(len(exp.correlator_passes), 4)) as executor:
        futures = [executor.submit(_flag_weights_pass, a_pass) for a_pass in exp.correlator_passes]
        for fut in futures:
            fut.result()  # Propagate any exceptions
    return True


def update_piletter(exp) -> bool:
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
                        if len(set([cp.freqsetup.n_subbands for cp in exp.correlator_passes])) == 1:
                            for antenna in exp.correlator_passes[0].antennas:
                                if 0 < len(antenna.subbands) < \
                                        exp.correlator_passes[0].freqsetup.n_subbands:
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
                                    if 0 < len(antenna.subbands) < a_pass.freqsetup.n_subbands:
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


def tconvert(exp) -> bool:
    """Runs tConvert in all MS files available in the directory
    """
    for a_pass in exp.correlator_passes:
        if len(glob.glob(f"{a_pass.fitsidifile}*")) > 0:
            continue

        # The size difference between internal MS and FITS-IDI is around 1.55
        idi_size = 1.55*u.kbit*int(subprocess.run(f"du -s {str(a_pass.msfile)}", shell=True,
                                                  capture_output=True).stdout.decode().split()[0])

        if idi_size < 20*u.Gb:
            utils.shell_command("tConvert", ["-v", a_pass.lisfile.name, "-o", "chunk_size=4GB"],
                                      stdout=None, stderr=subprocess.STDOUT)
        elif idi_size < 4*u.Tb:
            utils.shell_command("tConvert", ["-v", a_pass.lisfile.name,
                                                   "-o", "chunk_size=8GB"],
                                      stdout=None, stderr=subprocess.STDOUT)
        else:
            if utils.space_available(exp.cwd) <= 1.1*idi_size:
                rprint("\n\n[bold red]There is no enough space in the computer to create " \
                       "the FITS-IDI files[/bold red]")
                raise IOError("Not enough disk space to create the FITS-IDI files.")

            utils.shell_command("tConvert", ["-v", a_pass.lisfile.name, "-o",
                                      f"chunk_size={int(idi_size.to(u.Tb).value)}GB"],
                                      stdout=None, stderr=subprocess.STDOUT)

    return True


def prepare_polconvert(exp: experiment.Experiment, output_template: Path = Path('polconvert_inputs.toml')) -> bool:
    """Checks if PolConvert is required for any antenna.
    In that case, prepares the templates for running it and (potentially in the future?)
    will run it. For now it just requests the user to run it manually.
    """
    with resources.as_file(resources.files("templates").joinpath("polconvert_inputs.toml.template")) as template:
        template_content = Path(template).read_text()

    logger.debug("Creating the template file for polconvert (polconvert_inputs.toml)")

    # Use only antennas that observed the same subbands as the antennas to PolConvert (at least)
    subbands_to_polconvert = set().union(*(exp.antennas[p].subbands for p in exp.antennas.polconvert))
    include_ants = []
    for ant in exp.antennas:
        if (ant not in exp.antennas.polconvert) and subbands_to_polconvert.issubset(set(ant.subbands)) and ant.observed:
                include_ants.append(ant.name)

    template_content = template_content.format(expname=exp.expname.lower(), refant=f"'{exp.refant[0].upper()}'",
                                               linants=f"[{', '.join([f"'{ant.upper()}'" for ant in exp.antennas.polconvert])}]",
                                               exclude_ants=f"[{', '.join([f"'{ant.name.upper()}'" for ant in exp.antennas \
                                                                           if ant.name not in include_ants])}]",
                                               do_if=f"[{', '.join([str(s) for s in subbands_to_polconvert])}]",
                                               time_range=[],
                                               chanavg=16, timeavg=30, solve_weight=0.001)

    output_template.write_text(template_content)
    return output_template.exists()


def polconvert(exp) -> Optional[bool]:
    """Checks if PolConvert is required for any antenna.
    In that case, prepares the templates for running it and (potentially in the future?)
    will run it. For now it just requests the user to run it manually.
    """
    if len(exp.antennas.polconvert) > 0:
        polconv_inp = Path('./polconvert_inputs.toml')
        if not polconv_inp.exists():
            exp.log("cp ~/polconvert/polconvert_inputs.toml ./polconvert_inputs.toml")
            utils.shell_command('cp', ['/home/jops/polconvert/polconvert_inputs.toml',
                                      './polconvert_inputs.toml'], shell=True, stdout=None)

            with open(polconv_inp, 'r') as pcfile:
                pccontent = pcfile.read()

            pccontent.replace("expname_1_1.IDI*", f"{exp.expname.lower()}_1_1.IDI*")
            pccontent.replace("'T6'", ', '.join([f"'{ant.upper()}'" for ant in \
                              exp.antennas.polconvert]))
            pccontent.replace("'EF'", f"'{exp.refant[0].upper()}'")

            excl_ants = []
            for ant in exp.antennas:
                if (ant.name != exp.refant[0]) and (ant not in exp.antennas.polconvert):
                    if (not ant.observed):
                        excl_ants.append(ant.name.upper())

                    # I exclude all antennas that did not observe all subbands as the antenas
                    # to PolConvert
                    for pant in exp.antennas.polconvert:
                        if not set(exp.antennas[pant].subbands).issubset(set(ant.subbands)):
                            excl_ants.append(ant.name.upper())

            pccontent.replace("'IR', 'CM', 'DE'", ', '.join([f"'{a}'" for a in excl_ants]))

            with open(polconv_inp, 'w') as pcfile:
                pcfile.write(pccontent)


        if len(exp.antennas.polconvert) > 1:
            verbose_polconv_ants = ', '.join(exp.antennas.polconvert[:-1]) + ' and ' + \
                                    exp.antennas.polconvert[-1]
        else:
            verbose_polconv_ants = exp.antennas.polconvert[0]

        rprint("\n\n[red bold]PolConvert needs to be run manually for " \
               f"{verbose_polconv_ants}.[/red bold]\n")
        print("You would find the input template in the current directory.")
        print("Edit it manually and then run it with:\n")
        rprint("[bold]> polconvert.py  polconvert_inputs.toml[/bold]")
        rprint("\n\n[red bold]Once PolConvert has run, re-run me as ('postprocess')[/red bold]\n\n")
        # Keep the following as it will require a manual interaction
        exp.last_step = 'tconvert'
        return None
    else:
        exp.log("# PolConvert is not required.")
        # dialog_text = "PolConvert is required.\n"
        # dialog_text += f"Please run it manually for {','.join(exp.polconvert_antennas)}."
        # dialog_text += "Once you are done (all FITS properly corrected), press Continue."
        # dialog.warning_dialog(dialog_text)
    return True


def post_polconvert(exp) -> Optional[bool]:
    """Assumes that PolConvert has run, creating the new (corrected) files *IDI*.PCONVERT.
    This function (if indeed PolConvert had run) would move all converted files to the
    standard name (keeping the original ones in a folder (./unconverted_idi_files/),
    and runs again standardplots to confirm that the conversion has been loaded properly.
    """
    if len(exp.antennas.polconvert) == 0:
        return True

    if len(glob.glob('*IDI*.PCONVERT')) == 0:
        # Files would be expected but then let's assume the user already renamed them
        return True

    idi_ori = Path(exp.cwd / 'idi_ori/')
    idi_ori.mkdir(exist_ok=True)

    for an_idi in Path(exp.cwd).glob('*.IDI*'):
        if '.PCONVERT' not in an_idi.name:
            an_idi.rename(idi_ori / an_idi.name)

    pconverted_idi = list(Path(exp.cwd).glob('*IDI*.PCONVERT'))
    for an_idi in pconverted_idi:
        # Path(an_idi.name.replace('.PCONVERT', '')).rename(idi_ori / an_idi.name)
        an_idi.rename(an_idi.name.replace('.PCONVERT', ''))

    exp.log("mkdir idi_ori")
    exp.log("mv *IDI *IDI? *IDI?? *IDI??? *IDI???? idi_ori/")
    exp.log("zmv '(*).PCONVERT' '$1'")
    # Creates a new MS with the PolConverted-data in order to plot it
    # to check if the conversion run properly
    if any(['_1_1' in pp.name for pp in pconverted_idi]):
        _ = utils.shell_command("idi2ms.py", ['--delete',
                              f"{exp.correlator_passes[0].msfile.name.replace('.ms', '-pconv.ms')}",
                              ','.join([idi.name.replace('.PCONVERT', '') for idi in pconverted_idi \
                                        if '_1_1' in idi.name])])
        if exp.refant is not None:
            refant = exp.refant[0] if len(exp.refant) == 1 else f"({'|'.join(exp.refant)})"
        else:
            for ant in ('EF', 'O8', 'YS', 'MC', 'GB', 'AT', 'PT'):
                if (ant in exp.antennas.names) and (exp.antennas[ant].observed):
                    refant = ant
                    break
            raise ValueError("Could not find a good reference antenna for standardplots. "
                             "Please specify it manually.")

        _ = utils.shell_command("standardplots",
                          [f"{exp.correlator_passes[0].msfile.name.replace('.ms', '-pconv.ms')}",
                           refant, ','.join(exp.sources_stdplot)], stdout=None,
                           stderr=subprocess.STDOUT)

        for a_plot in glob.glob(f"{exp.expname.lower()}-*-pconv-cross*.ps"):
            utils.shell_command("gv", a_plot, stdout=None, stderr=subprocess.STDOUT)

    exp.last_step = 'post_polconvert'
    rprint("\n\n[bold green]If PolConvert worked fine, re-run me to continue. " \
           "Otherwise fix it manually before.[/bold green]\n")
    return None


def post_post_polconvert(exp) -> bool:
    """When PolConvert run properly and the user continued, it checks if the standardplots from the
    converted MS (exp-pconv.ms) exist and then rename those plots to the usual name.
    """
    if len(exp.antennas.polconvert) == 0:
        return True

    stdplot_files = glob.glob('*-pconv*.ps')
    if len(stdplot_files) > 0:
        for stdplot_file in stdplot_files:
            Path(stdplot_file).rename(stdplot_file.replace('-pconv', ''))

    return True


def set_credentials(exp) -> bool:
    """Sets the credentials for the given experiment.
    In case of an NME or test, it does not set any credential.
    Otherwise, it will take the credentials from a .auth file if already exists,
    or creates such file iwth a new password.
    """
    if (exp.expname.upper()[0] == 'N') or (exp.expname.upper()[0] == 'F'):
        rprint(f"\n[green][bold]NOTE:[/bold] {exp.expname} is an NME or test experiment.\n"
               "No authentification will be set.[/green]")
    elif len(glob.glob("*_*.auth")) == 1:
        # Some credentials are already in place.
        exp.set_credentials(*glob.glob("*_*.auth")[0].split('.')[0].split('_'))

    elif len(glob.glob("*_*.auth")) > 1:
        raise ValueError("More than one .auth file found in the directory.")
    else:
        possible_char = string.digits + string.ascii_letters
        exp.set_credentials(username=exp.expname.lower(),
                            password="".join(random.sample(possible_char, 12)))
        utils.shell_command("touch",
                                  f"{exp.credentials.username}_{exp.credentials.password}.auth")
        exp.log(f"touch {exp.credentials.username}_{exp.credentials.password}.auth")

    return True


def protect_experiment_files(exp: experiment.Experiment) -> bool:
    """Sets the protection status for pipeline plots in the EVN Archive
    
    This function uses the auth_pipe.py script to set protection status for various 
    pipeline plot types (BANDPASS, CPOL, FRING_DELAY, etc.) for the entire experiment.
    
    Args:
        exp: Experiment object containing experiment metadata
        
    Returns:
        bool: True if protection was successfully set, False otherwise
    """
    if not [s.name for s in exp.sources if s.protected]:
        logger.info("No protection required for this experiment.")
        return True

    try:
        utils.shell_command("auth_pipe.py", ["-e", exp.expname, "-s", [s.name for s in exp.sources if s.protected],
                            "-p", "source"])
    except ValueError:
        logger.error("Could not protect experiment files in archive")
        return False

    return True


def has_Tsys(fitsfile):
    """Checks if the FITS-IDI file has the SYSTEM_TEMPERATURE table.
    """
    with fits.open(fitsfile) as hdu:
        return 'SYSTEM_TEMPERATURE' in hdu


def has_GC(fitsfile):
    """Checks if the FITS-IDI file has the GAIN_CURVE table.
    """
    with fits.open(fitsfile) as hdu:
        return 'GAIN_CURVE' in hdu


def check_consistency(fitsfile, verbose=True):
    """Check if all FITS-IDI files associated to an experiment has the right
    tables that they should have.

    Arguments
        - fitsfile : str
            FITS-IDI file name to check. It should be the first FITS-IDI file
            in case there are multiple (e.g. only exp_1_1.IDI1 even if there are
            multiple *.IDIn, n > 1).
            The rest of files would be not expected to have the tables.

    Returns
        - bool whenever everything is as expected.
    """
    if isinstance(fitsfile, str):
        fitsfile = Path(fitsfile)

    if not fitsfile.exists():
        raise FileNotFoundError(f"The FITS-IDI file {fitsfile} could not be found.")

    all_good = True
    if has_Tsys(fitsfile):
        if verbose:
            pprint(f"[green]{fitsfile} has SYSTEM_TEMPERATURE table.[/green]")
    else:
        if verbose:
            pprint(f"[red]{fitsfile} does not have SYSTEM_TEMPERATURE table.[/red]")

        all_good = False

    if has_GC(fitsfile):
        if verbose:
            pprint("[green]Has GAIN_CURVE table.[/green]")
    else:
        if verbose:
            pprint("[red]Does not have GAIN_CURVE table.[/red]")

        all_good = False

    return all_good



def append_antab(exp) -> bool:
    """Appends the Tsys and GC information from the experiment ANTAB file into the FITS-IDI files.
    It will also re-archive the files.

    If the ANTAB file is already present in the directory, it will assume that the information
    was already appended.
    """
    fits2check = glob.glob(f"{exp.expname.lower()}_*_*.IDI1") + \
                 glob.glob(f"{exp.expname.lower()}_*_*.IDI")
    assert len(fits2check) > 0, "Could not find FITS-IDI to append Tsys/GC!"

    if (not all([check_consistency(a_fits, verbose=False) \
                 for a_fits in fits2check])) \
                 or (len(glob.glob(f"{exp.expname.lower()}*.antab")) == 0):
        utils.shell_command("append_antab_idi.py", "-r", shell=True, stdout=None)
        exp.log('append_antab_idi.py')
        if not all([check_consistency(a_fits) for a_fits in fits2check]):
            # As now everything should be OK. Means that something failed.
            rprint("\n\n[red bold]The Tsys/GC could not be imported into the FITS-IDI.[/red bold]")
            return False
    else:
        rprint("[green]ANTAB information already appended into the FITS-IDI files.[/green]")

    return True


def send_letters(exp) -> bool:
    """Remembers you to update the PI letter and send it , and the pipeletter, to the PIs.
    Finally, it runs parsePIletter.
    """
    print("\n\n\n")
    rprint("[center][bold red] --- Send the PI letter --- [/bold red][/center]")
    pi = "\n"
    if isinstance(exp.piname, list):
        for a_piname,an_email in zip(exp.piname, exp.email):
            pi += f"{a_piname.capitalize()}: {an_email}\n"
    else:
        pi += f"{exp.piname.capitalize()}: {exp.email}\n"

    rprint(f"[green]Send the file [bold]{exp.expname.lower()}.piletter"
           f"{'_auth' if exp.credentials.password is not None else ''}[/bold] to " + pi + \
           "and CCing jops@jive.eu.[/green]")
    return True


def antenna_feedback(exp) -> bool:
    rprint("\n[center][bold red] --- Also update the database with the observed issues "
           "--- [/bold red][/center]")
    rprint("[bold]Now it is also time to bookkeep the issues that you may have "
           "seen in the antennas by typing '/feedback' in Mattermost.[/bold]\n")
    rprint("[bold]Also go to the JIVE RedMine to write down the relevant issues with "
           "particular antennas[/bold]:")
    rprint("https://jrm.jive.nl/projects/science-support/news\n\n")
    return True


def nme_report(exp) -> bool:
    if exp.expname[0] == 'N':
        # This is a NME.
        rprint("[center][bold red]Now it is time to write the NME Report..."
               "Good luck![/bold red][/center]")
    else:
        rprint("[center][bold]Experiment done![/bold][/center]\n")
        print("You may have a coffee/tea after finishing the last tasks!")

    return True
