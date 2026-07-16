from __future__ import annotations

import os
import re
import glob
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from . import experiment, utils
from . import process  # cycle: process imports this module; both only use each other at call time


# Suffix tagging the auxiliary lag-space products ({expname}-lag.lis / {expname}-lag.ms).
# These are NOT correlator passes: the lag MS is only used to compute per-scan antenna SNR,
# so the lag .lis must be excluded from pass discovery (otherwise checklis/j2ms2/msops would
# treat it as an extra pass and operate on it).
LAG_TAG = "-lag."


def _pass_lisfiles(pattern: str) -> list[str]:
    """Sorted .lis files matching *pattern*, excluding the auxiliary lag-space .lis file."""
    return sorted(f for f in glob.glob(pattern) if LAG_TAG not in f)


def update_lis_file(lisfilename: str | Path, oldexp: str, newexp: str) -> None:
    """Updates the lis file (the header lines) referring to an experiment named oldexp
    to newexp. Note that it does not replace all references to oldexp as some of them
    would point to correlator output files that would keep the name.
    
    Args:
        lisfilename (str | Path): Path to the lis file to update.
        oldexp (str): Old experiment name to replace.
        newexp (str): New experiment name.
    
    Returns:
        None
    """
    with open(lisfilename, 'r') as lisfile:
        lisfilelines = lisfile.readlines()
        for i, aline in enumerate(lisfilelines):
            if aline[0] not in ('+', '-'):
                lisfilelines[i] = aline.replace(oldexp, newexp)
                lisfilelines[i] = lisfilelines[i].replace(oldexp.lower(), newexp.lower())
                lisfilelines[i] = lisfilelines[i].replace(f"{newexp.lower()}.vix",
                                                          f"{newexp.upper()}.vix")

    with open(lisfilename, 'w') as lisfile:
        lisfile.write(''.join(lisfilelines))


def create_lag_lisfile(exp: experiment.Experiment, source_pass: experiment.CorrelatorPass) -> Path:
    """Creates a ``{expname}-lag.lis`` copy of the given pass configured to produce the
    lag-space MS (``{expname}-lag.ms``).

    ``j2ms2`` ignores the ``-o output`` option when an input ``.lis`` file is supplied via
    ``-v`` (the output MS name is taken from the ``.lis`` header instead). To control the
    lag-space MS name we therefore duplicate the source ``.lis`` file and rewrite the output
    MS field in its header line(s).

    Args:
        exp (experiment.Experiment): Experiment object (provides the experiment name).
        source_pass (experiment.CorrelatorPass): The pass whose .lis file is duplicated
            (normally the first/main correlator pass).

    Returns:
        Path: The path to the created ``{expname}-lag.lis`` file.
    """
    lag_lisfile = Path(f"{exp.expname.lower()}-lag.lis")
    lag_msname = f"{exp.expname.lower()}-lag.ms"
    old_msname = source_pass.msfile.name

    with open(source_pass.lisfile, 'r') as f:
        lines = f.readlines()

    # The header line(s) are the ones not starting with the +/- job markers. Replace the
    # output MS name (matched as a whitespace-delimited token so other fields, e.g. the
    # .vix or .IDI names, are left untouched).
    token_re = re.compile(rf'(?<!\S){re.escape(old_msname)}(?!\S)')
    for i, line in enumerate(lines):
        if line.lstrip()[:1] not in ('+', '-') and '.ms' in line:
            lines[i] = token_re.sub(lag_msname, line)

    with open(lag_lisfile, 'w') as f:
        f.writelines(lines)

    return lag_lisfile


def split_lis_cont_line(exp: experiment.Experiment, fulllisfile: str | Path) -> None:
    """Given a lis file, it checks if there are jobs set as prod_cont and prod_line.
    If not, it does nothing. Otherwise, it splits the lis file into two lis files,
    one for the continuum pass and another one for the line pass.
    
    Args:
        exp (experiment.Experiment): Experiment object to update with spectral line flag.
        fulllisfile (str | Path): Path to the lis file to check and potentially split.
    
    Returns:
        None
    """
    # Checks that there are more than one PROD pass
    n_prods = set()
    with open(fulllisfile) as f_full:
        for a_fileline in f_full.readlines():
            temp = a_fileline.split()
            if 'PROD' in temp:
                n_prods.add(temp[temp.index('PROD') + 1])

    # TODO: possible problems if > 2 ?
    if ('prod_line' in n_prods) and (len(n_prods) > 1):
        print('This is a spectral line experiment with line and continuum passes.')
        lis_cont = str(fulllisfile).replace('.lis', '_cont.lis')
        with open(lis_cont, 'w') as f_cont, open(str(fulllisfile).replace('.lis', '_line.lis'), 'w') as f_line:
            with open(fulllisfile) as f_full:
                for a_fileline in f_full.readlines():
                    if a_fileline[0].strip() not in ('+', '-'):
                        f_cont.write(a_fileline.replace('.ms', '_cont.ms'))
                        f_line.write(a_fileline.replace('.ms', '_line.ms'))
                    else:
                        if 'prod_line' in a_fileline:
                            f_line.write(a_fileline)
                            f_cont.write(a_fileline.replace('+', '-'))
                        else:
                            f_line.write(a_fileline.replace('+', '-'))
                            f_cont.write(a_fileline)

        os.remove(fulllisfile)

def _process_single_lisfile(args):
    """Helper function to process a single lisfile in parallel.
    
    Args:
        args: Tuple of (index, lisfile_path, expname, thereis_line, i_lines_done)
    
    Returns:
        experiment.CorrelatorPass or None if no .ms line found
    """
    i, a_lisfile, expname, thereis_line, i_lines_done = args
    
    with open(a_lisfile, 'r') as lisfile:
        for a_lisline in lisfile.readlines():
            if '.ms' in a_lisline:  # The header line
                # there is only one .ms input there
                msname = [elem.strip() for elem in a_lisline.split() if '.ms' in elem][0]
                # In case the output FITS IDI name has already been set
                if '.IDI' in a_lisline:
                    fitsidiname = [elem.strip() for elem in a_lisline.split() if '.IDI' in elem][0]
                    to_pipeline = True if ((fitsidiname.split('_')[-2] == '1') or thereis_line) else False
                else:
                    if thereis_line:
                        if '_line' in a_lisfile:
                            fitsidiname = f"{expname.lower()}_{2*i_lines_done + 2}_1.IDI"
                        else:
                            fitsidiname = f"{expname.lower()}_{2*i_lines_done + 1}_1.IDI"

                        to_pipeline = True if i_lines_done == 0 else False
                    else:
                        fitsidiname = f"{expname.lower()}_{i+1}_1.IDI"
                        to_pipeline = True if (i == 0) else False

                # Replaces the old *.UVF string in the .lis file with the FITS IDI
                # file name to generate in this pass.
                if '.UVF' in a_lisline:
                    utils.shell_command('sed',
                                        ['-i', f"'s/{msname}.UVF/{fitsidiname}/g'", str(a_lisfile)],
                                        shell=True, bufsize=-1)
                
                return experiment.CorrelatorPass(Path(a_lisfile), Path(msname), fitsidiname,
                                                to_pipeline)
    
    return None


def get_passes_from_lisfiles(exp: experiment.Experiment) -> bool:
    """Gets all .lis files in the directory, which imply different correlator passes.
    Appends this information to the current experiment (exp object),
    together with the MS file associated for each of them.
    
    This function processes lisfiles in parallel using ThreadPoolExecutor.
    
    Args:
        exp (experiment.Experiment): Experiment object to update with correlator pass information.
    
    Returns:
        bool: True if passes were successfully extracted and stored.
    """
    # Sort the .lis files alphabetically so that downstream pass numbering is
    # deterministic across machines and reruns. The previous unsorted glob.glob()
    # call could produce a different order from the file-system, which silently
    # broke pass-to-IDI assignments for spectral-line experiments.
    lisfiles = _pass_lisfiles(f"{exp.expname.lower()}*.lis")
    thereis_line = True if '_line' in ''.join(lisfiles) else False
    
    # Prepare arguments for parallel processing
    # Calculate i_lines_done for each file index
    args_list = []
    i_lines_done = 0
    for i, a_lisfile in enumerate(lisfiles):
        args_list.append((i, a_lisfile, exp.expname, thereis_line, i_lines_done))
        if thereis_line and (i % 2 == 0) and (i > 0):
            i_lines_done += 1
    
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(_process_single_lisfile, args_list))

    new_passes = [result for result in results if result is not None]

    # Preserve already-extracted MS metadata for passes that are unchanged. Rebuilding the
    # passes from the .lis files (e.g. on a reload triggered when the user edits the .lis
    # set) otherwise resets freqsetup/antennas/sources/scans to empty, which silently breaks
    # later steps that need them (e.g. comment_tasav -> "No frequency setup available").
    # We match on the .lis and .ms file names; if those are unchanged we carry the metadata
    # over, so reloading the pass list is non-destructive for passes that did not change.
    previous = {(p.lisfile.name, p.msfile.name): p for p in exp.correlator_passes}
    for a_pass in new_passes:
        old = previous.get((a_pass.lisfile.name, a_pass.msfile.name))
        if old is not None and old.freqsetup is not None:
            a_pass.freqsetup = old.freqsetup
            a_pass.antennas = old.antennas
            a_pass.sources = old.sources
            a_pass.scans = old.scans
            a_pass.flagged_weights = old.flagged_weights

    exp.correlator_passes = new_passes

    # Aggregate sources from all correlator passes into the global experiment sources
    process.aggregate_sources_from_passes(exp)
    
    exp.store()
    return True


def _check_single_lisfile(args):
    """Helper function to check a single lisfile in parallel.
    
    Args:
        args: Tuple of (correlator_pass, is_multi_phase_center)
    
    Returns:
        bool: True if this pass is valid, False if issues are found.
    """
    a_pass, is_multi_phase_center = args
    
    # checklis.py (external, /home/jops/opt/evn_support) uses non-raw regex strings that
    # emit noisy SyntaxWarnings on Python >= 3.12. The script still works (the sequences are
    # interpreted literally), and we cannot edit it, so silence the warning for this call.
    output = utils.shell_command("PYTHONWARNINGS=ignore::SyntaxWarning checklis.py",
                                 a_pass.lisfile.name, shell=True)
    # The output has the form:
    #      First scan = X
    #       {errors if any otherwise no extra lines}
    #      Last scan = Y
    # removing any possible trailing empty line:
    temp = [o for o in output.split('\n') if len(o) > 0]
    
    if is_multi_phase_center:
        # For multi-phase center experiments, we expect to miss some scans in the different passes
        return not len([t for t in temp if '**** Skipped' not in t]) > 2
    else:
        return (not (len(temp) > 2)) and (all(['No scans in' not in t for t in temp]))


def check_lisfiles(exp: experiment.Experiment) -> bool:
    """Checks the existing .lis files to spot possible issues.
    If at least one of the .lis files reports a possible issue (e.g. duplicated scans,
    missing scans, etc), it will return False. Otherwise it will return True.
    
    Additionally, ensures that all .lis files have different names, different output 
    msfile names, and different fitsidinames for consistency and robustness.
    
    This function processes lisfiles in parallel using ThreadPoolExecutor.
    
    Args:
        exp (experiment.Experiment): Experiment object with correlator passes to check.
    
    Returns:
        bool: True if all lis files are valid and have unique names, False if issues are found.
    """
    is_multi_phase_center = len(exp.correlator_passes) > 2 if exp.spectral_line else len(exp.correlator_passes) > 1
    args_list = [(a_pass, is_multi_phase_center) for a_pass in exp.correlator_passes]
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(_check_single_lisfile, args_list))
    
    # Original checks - ensure all individual lis file checks pass
    if not all(results):
        logger.error("One or more .lis files failed validation checks")
        return False
    
    # Check for unique msfile names
    msfile_names = [a_pass.msfile.name for a_pass in exp.correlator_passes]
    if len(msfile_names) != len(set(msfile_names)):
        duplicate_msfiles = [name for name in msfile_names if msfile_names.count(name) > 1]
        logger.error(f"Duplicate msfile names found: {set(duplicate_msfiles)}")
        return False
    
    # Enhanced checks - ensure unique .lis file names
    lisfile_names = [a_pass.lisfile.name for a_pass in exp.correlator_passes]
    if len(lisfile_names) != len(set(lisfile_names)):
        duplicate_lisfiles = [name for name in lisfile_names if lisfile_names.count(name) > 1]
        logger.error(f"Duplicate .lis file names found: {set(duplicate_lisfiles)}")
        return False
    
    # Enhanced checks - ensure unique fitsidinames
    fitsidinames = [a_pass.fitsidifile for a_pass in exp.correlator_passes]
    if len(fitsidinames) != len(set(fitsidinames)):
        duplicate_fitsidinames = [name for name in fitsidinames if fitsidinames.count(name) > 1]
        logger.error(f"Duplicate FITS IDI names found: {set(duplicate_fitsidinames)}")
        return False
    
    logger.debug("All .lis files passed consistency checks with unique names")
    return True
