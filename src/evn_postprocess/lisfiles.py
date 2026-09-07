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


# checklis.py prints these two lines for a healthy .lis file; every other line it writes
# reports an issue that the operator has to look at.
_CHECKLIS_OK_PREFIXES = ('First scan', 'Last scan')

# How each issue reported by checklis.py is described to the operator. 'skipping' scans are
# expected in multi-phase-center runs (each pass only keeps its own scans), so they are a
# warning; 'duplicated' data and anything else are hard errors that need a manual fix.
_LIS_ISSUE_ADVICE = {'duplicated': "duplicated data (this MUST be fixed manually in the .lis file(s))",
                     'skipping': "skipped scans (this may well be right, but double check them manually)",
                     'other': "other errors (they need to be checked, and fixed manually)"}

# Maximum number of .lis file names quoted per issue in the summary. Multi-phase-center
# experiments can have dozens of passes, and the full list would bury the message.
_MAX_LISFILES_LISTED = 8


def _classify_checklis_output(output: str) -> dict[str, list[str]]:
    """Splits the raw checklis.py output of one .lis file into the issues it reports.

    Args:
        output (str): Raw stdout of `checklis.py {file}.lis`.

    Returns:
        dict[str, list[str]]: The reported lines keyed by issue type ('skipping',
        'duplicated', 'other'). Every key is always present, with an empty list when
        that issue was not reported.
    """
    issues: dict[str, list[str]] = {'duplicated': [], 'skipping': [], 'other': []}
    for line in output.split('\n'):
        text = line.strip()
        if (len(text) == 0) or text.startswith(_CHECKLIS_OK_PREFIXES):
            continue

        if 'skip' in text.lower():
            issues['skipping'].append(text)
        elif 'duplicat' in text.lower():
            issues['duplicated'].append(text)
        else:
            issues['other'].append(text)

    return issues


def _check_single_lisfile(a_pass: experiment.CorrelatorPass) -> tuple[str, dict[str, list[str]]]:
    """Runs checklis.py on a single .lis file (called in parallel) and classifies its output.

    Args:
        a_pass (experiment.CorrelatorPass): The correlator pass whose .lis file is checked.

    Returns:
        tuple[str, dict[str, list[str]]]: The .lis file name, and the issues it reported as
        returned by :func:`_classify_checklis_output`. A checklis.py that cannot run at all
        is reported as an 'other' issue, so it is never silently ignored.
    """
    # checklis.py (external, /home/jops/opt/evn_support) uses non-raw regex strings that
    # emit noisy SyntaxWarnings on Python >= 3.12. The script still works (the sequences are
    # interpreted literally), and we cannot edit it, so silence the warning for this call.
    # echo=False: the passes run in parallel, so their raw outputs would interleave in the
    # terminal; the operator gets the aggregated summary from check_lisfiles_report instead.
    try:
        output = utils.shell_command("PYTHONWARNINGS=ignore::SyntaxWarning checklis.py",
                                     a_pass.lisfile.name, shell=True, echo=False)
    except Exception as e:
        logger.error(f"checklis.py could not be run on {a_pass.lisfile.name}: {e}")
        return a_pass.lisfile.name, {'duplicated': [], 'skipping': [], 'other': [f"checklis.py failed: {e}"]}

    return a_pass.lisfile.name, _classify_checklis_output(output)


def _natural_key(name: str) -> list[tuple[int, object]]:
    """Sort key that orders embedded numbers numerically (so _2.lis comes before _10.lis).

    Args:
        name (str): File name to sort.

    Returns:
        list[tuple[int, object]]: Alternating text/number chunks, each tagged with its kind
        so that a number is never compared against a text chunk.
    """
    return [(0, int(chunk)) if chunk.isdigit() else (1, chunk) for chunk in re.split(r'(\d+)', name)]


def _summarize_lisfiles(lisfile_names: list[str]) -> str:
    """Renders the list of .lis files affected by one issue, truncated when there are many.

    Args:
        lisfile_names (list[str]): Names of the .lis files reporting the same issue.

    Returns:
        str: Text of the form '3 .lis file(s): a.lis, b.lis, c.lis', with the names beyond
        _MAX_LISFILES_LISTED replaced by a '... (+N more)' tail.
    """
    listed = ', '.join(sorted(lisfile_names, key=_natural_key)[:_MAX_LISFILES_LISTED])
    remaining = len(lisfile_names) - _MAX_LISFILES_LISTED
    if remaining > 0:
        listed = f"{listed} ... (+{remaining} more)"

    return f"{len(lisfile_names)} .lis file(s): {listed}"


def check_lisfiles_report(exp: experiment.Experiment) -> tuple[bool, str]:
    """Checks the existing .lis files and reports what is wrong with them, if anything.

    Runs checklis.py on every correlator pass in parallel and classifies what it reports
    (skipped scans, duplicated data, anything else). It also verifies that the passes have
    unique .lis, MS and FITS-IDI names, as repeated names would make later steps overwrite
    each other's products.

    Skipped scans are the one tolerated issue, and only in multi-phase-center experiments,
    where each pass legitimately keeps a subset of the scans: they are still reported so the
    operator can double check them.

    Args:
        exp (experiment.Experiment): Experiment object with the correlator passes to check.

    Returns:
        tuple[bool, str]: (all_ok, message). 'message' is empty when there is nothing to
        report; otherwise it is a multi-line, operator-facing summary saying which .lis
        files show which issue and what has to be done about it. Note that a message can
        also come with all_ok=True (tolerated skipped scans), as a warning.
    """
    is_multi_phase_center = len(exp.correlator_passes) > 2 if exp.spectral_line else len(exp.correlator_passes) > 1
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(_check_single_lisfile, exp.correlator_passes))

    # Which .lis files reported each issue (the full lines only go to the log file).
    files_with: dict[str, list[str]] = {issue: [] for issue in _LIS_ISSUE_ADVICE}
    for lisfile_name, issues in results:
        for issue, lines in issues.items():
            if len(lines) > 0:
                files_with[issue].append(lisfile_name)
                logger.debug(f"checklis.py on {lisfile_name} reported {issue}: {' | '.join(lines)}")

    # Repeated names across passes are always a manual fix (later steps would clobber files).
    repeated_names = []
    for label, names in (('.lis file', [a_pass.lisfile.name for a_pass in exp.correlator_passes]),
                         ('MS', [a_pass.msfile.name for a_pass in exp.correlator_passes]),
                         ('FITS-IDI', [str(a_pass.fitsidifile) for a_pass in exp.correlator_passes])):
        repeated = sorted({name for name in names if names.count(name) > 1})
        if len(repeated) > 0:
            repeated_names.append(f"repeated {label} names across the correlator passes "
                                  f"(this MUST be fixed manually): {', '.join(repeated)}")

    blocking = [issue for issue in ('duplicated', 'other') if len(files_with[issue]) > 0]
    if (len(files_with['skipping']) > 0) and (not is_multi_phase_center):
        blocking.append('skipping')

    reported = [f"  - {_LIS_ISSUE_ADVICE[issue]}: {_summarize_lisfiles(files_with[issue])}"
                for issue in ('duplicated', 'skipping', 'other') if len(files_with[issue]) > 0]
    reported += [f"  - {text}" for text in repeated_names]
    if len(reported) == 0:
        logger.debug("All .lis files passed the checklis and unique-name consistency checks.")
        return True, ''

    all_ok = (len(blocking) == 0) and (len(repeated_names) == 0)
    if all_ok:
        header = ("Only skipped scans were reported by checklis, which is expected in a "
                  "multi-phase-center experiment like this one. Please verify the .lis file(s) "
                  "to see if they are OK:")
    else:
        header = "Issues found in the .lis file(s). Please verify the .lis file(s) to see if they are OK:"

    return all_ok, '\n'.join([header] + reported)


def check_lisfiles(exp: experiment.Experiment) -> bool:
    """Checks the existing .lis files to spot possible issues, logging what it finds.

    Thin wrapper around :func:`check_lisfiles_report` for the callers that only need the
    verdict (e.g. `postprocess exec checklis`). The workflow step uses the report directly,
    so it can pass the summary on to the operator on every channel.

    Args:
        exp (experiment.Experiment): Experiment object with correlator passes to check.

    Returns:
        bool: True if all the .lis files are valid and have unique names, False otherwise.
    """
    all_ok, message = check_lisfiles_report(exp)
    if len(message) > 0:
        if all_ok:
            logger.warning(message)
        else:
            logger.error(message)

    return all_ok
