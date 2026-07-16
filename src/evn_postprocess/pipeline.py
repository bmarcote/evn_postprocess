#! /usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the pipe computer.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.
"""
import os
import re
import glob
import shutil
import subprocess
import traceback
from importlib import resources
from loguru import logger
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from rich import print as rprint
from . import utils
from . import lisfiles
from . import comment_tasav
from . import feedback


def run_antab_editor(exp) -> bool:
    """Opens antab_editor.py for the given experiment.

    For an e-EVN run (several experiments correlated together) the editor is run
    once from the main experiment, passing the associated experiments via ``-a``
    together with the path to their FITS-IDI files, so a single, consistent set of
    Tsys/gain tables is produced for the whole session.

    Returns:
        bool: True once the editor exits successfully (the editor itself runs
        interactively; this function only manages the working directory).
        False if the associated e-EVN experiments do not have their FITS-IDI files yet.
    """
    if (exp.eEVNname is not None) and (exp.expname != exp.eEVNname):
        rprint(f"[bold red]This experiment {exp.expname} is part of the e-EVN run {exp.eEVNname}[/bold red].\n"
              "[red]You should only run antab_editor.py from the main e-EVN experiment run (including the "
              "rest of the run experiments).\nRun it manually in case you indeed want to run it here.[/red]")
        raise ValueError("antab_editor.py should only be run from the main e-EVN experiment run")

    # For an e-EVN run, gather the other experiments observed in the same session.
    # Computed before chdir, while the .vix file is reachable from the experiment root.
    other_exps = []
    if exp.eEVNname is not None:
        other_exps = [e for e in exp.eEVN_experiments() if e.upper() != exp.expname.upper()]

    original_cwd = os.getcwd()
    os.chdir(exp.dirs.pipe_temp)
    try:
        # The associated experiments must already be correlated (FITS-IDI present) so
        # antab_editor can read their Tsys information. Their data live in a sibling
        # experiment directory, i.e. ../../<EXP>/ relative to the antenna_files dir.
        missing_idi = [e for e in other_exps if not glob.glob(f"../../{e}/{e.lower()}*.IDI*")]
        if missing_idi:
            rprint(f"[bold red]Cannot run antab_editor.py for the e-EVN run {exp.eEVNname}:[/bold red] "
                   f"[red]no FITS-IDI files found for {', '.join(missing_idi)}.[/red]\n"
                   "[red]Correlate (and produce the FITS-IDI of) those experiments first.[/red]")
            return False

        assoc_args = ["-a", *other_exps] if other_exps else []
        assoc_paths = [f"../../{e}/" for e in other_exps]

        if '_line' in ''.join(lisfiles._pass_lisfiles(f"{exp.expname.lower()}*.lis")):
            utils.shell_command("antab_editor.py",
                                ["-e", exp.expname.lower(), *assoc_args, "-f", "..", "-l", *assoc_paths],
                                shell=True, stdout=None)
        else:
            utils.shell_command("antab_editor.py",
                                ["-e", exp.expname.lower(), *assoc_args, "-p", "1", "-f", "..", *assoc_paths],
                                shell=True, stdout=None)

        if len(missing_antabs := [a.name for a in exp.antennas if not a.antabfsfile]) > 0:
            rprint(f"[red]Note that you are missing ANTAB files from: {', '.join(missing_antabs)}[/red]")
    finally:
        os.chdir(original_cwd)
    return True


def create_uvflg(exp) -> bool:
    """Produces the combined uvflg file containing the full flagging from all telescopes.
    """
    if len(glob.glob(str(exp.dirs.pipe_temp / "*.uvflg"))) > 0:
        logger.info("uvflg files already created. Skipping.")
        return True

    if len(glob.glob(str(exp.dirs.pipe_temp / "*.log"))) == 0:
        logger.error("No log files found in the temp directory.")
        rprint("[bold red]ERROR:[/bold red] [red]No log files found in the temp directory.[/red]")
        return False
    original_cwd = os.getcwd()
    os.chdir(exp.dirs.pipe_temp)
    utils.shell_command("uvflgall.sh")

    # Check which observed antennas are missing .uvflgfs files and supplement
    # them with a-priori flagging from the experiment .flag file (from vlbeer).
    antennas_with_uvflgfs = {
        Path(f).stem.replace(exp.expname.lower(), '').upper()
        for f in glob.glob("*.uvflgfs")
    }
    missing_antennas = {a.upper() for a in exp.antennas.observed} - antennas_with_uvflgfs
    if missing_antennas:
        logger.info(f"Antennas missing .uvflgfs files: {', '.join(sorted(missing_antennas))}")
        flag_files = glob.glob(f"{exp.expname.lower()}*.flag")
        if flag_files:
            flag_file = Path(flag_files[0])
            with open(flag_file, 'r') as fh:
                flag_lines = fh.readlines()
            for ant in sorted(missing_antennas):
                ant_lines = [l for l in flag_lines if f"antenna='{ant}'" in l]
                if ant_lines:
                    uvflgfs_out = Path(f"{exp.expname.lower()}{ant.lower()}.uvflgfs")
                    with open(uvflgfs_out, 'w') as fh:
                        fh.write(f"! A-priori flagging for {ant} from {flag_file.name}\n")
                        fh.write("opcode='FLAG'\n")
                        fh.write("dtimrang = 1   timeoff=0\n")
                        fh.writelines(ant_lines)
                    logger.info(f"Created {uvflgfs_out.name} from {flag_file.name} for {ant}")
                else:
                    logger.debug(f"No flagging entries found for {ant} in {flag_file.name}")
        else:
            rprint(f"[yellow]Antennas {', '.join(sorted(missing_antennas))} are missing .uvflgfs files "
                   f"and no .flag file was found in the temp directory.[/yellow]")
            logger.warning(f"Missing .uvflgfs for {', '.join(sorted(missing_antennas))} "
                           "and no .flag file available")

    utils.shell_command("cat", ["*uvflgfs", ">", f"{exp.expname.lower()}.uvflg"])
    if len(pipepass := [apass.pipeline for apass in exp.correlator_passes if apass.pipeline]) > 1:
        for p in range(1, len(pipepass) + 1):
            shutil.copy(f"{exp.expname.lower()}.uvflg",
                        f"{exp.expname.lower()}_{p}.uvflg")

    os.chdir(original_cwd)
    return True


def create_input_file(exp) -> bool:
    """Copies the template of an input file for the EVN Pipeline
    and modifies the standard parameters.
    """
    if len(glob.glob(str(exp.dirs.pipe_in / "*.antab"))) == 0:
        for antabfile in exp.dirs.pipe_temp.glob("*.antab"):
            shutil.copy(antabfile, exp.dirs.pipe_in / antabfile.name)

    if len(glob.glob(str(exp.dirs.pipe_in / "*.uvflg"))) == 0:
        for uvflgfile in exp.dirs.pipe_temp.glob("*.uvflg"):
            shutil.copy(uvflgfile, exp.dirs.pipe_in / uvflgfile.name)

    original_cwd = os.getcwd()
    os.chdir(exp.dirs.pipe_in)
    if len(pipepasses := [apass.pipeline for apass in exp.correlator_passes if apass.pipeline]) > 1:
        # Only fan out from the unnumbered .antab/.uvflg into per-pass copies if the
        # unnumbered file actually exists. Previously the .uvflg copy was unguarded,
        # so a perfectly valid setup with already-numbered files (e.g. testexp_1.uvflg
        # and testexp_2.uvflg already present) crashed with FileNotFoundError.
        if Path(f"{exp.expname.lower()}.antab").exists():
            for p in range(1, len(pipepasses) + 1):
                shutil.copy(f"{exp.expname.lower()}.antab", f"{exp.expname.lower()}_{p}.antab")

        if Path(f"{exp.expname.lower()}.uvflg").exists():
            for p in range(1, len(pipepasses) + 1):
                shutil.copy(f"{exp.expname.lower()}.uvflg", f"{exp.expname.lower()}_{p}.uvflg")

    # Copy and modify the pipeline input template. We read the template once and
    # then format it per pass to avoid re-opening the same file (and to make the
    # function trivially mockable in tests via importlib.resources.read_text).
    template_path = resources.files("evn_postprocess.templates").joinpath("pipeline.inp.txt.template")
    template_text = template_path.read_text()
    pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]

    for i, apass in enumerate(pipepasses, 1):
        if len(pipepasses) > 1:
            inp_filename = Path(f"{exp.expname.lower()}_{i}.inp.txt")
        else:
            inp_filename = Path(f"{exp.expname.lower()}.inp.txt")

        if inp_filename.exists():
            continue

        template_content = template_text
        
        replacements = {
            '{expname}': exp.expname.lower() if len(pipepasses) == 1 else f"{exp.expname.lower()}_{i}",
            '{userno}': subprocess.run(['aips_userno.py', exp.supsci.lower()], 
                                       capture_output=True, text=True).stdout.strip() or '100',
            '{refant}': exp.refant[0] if len(exp.refant) > 0 else '',
            '{plotref}': exp.refant[0],
            '{bpass}': ', '.join(apass.sources.fringefinder),
            '{dophaseref}': '' if apass.sources.calibrator else '#',
            # '{phaseref}': ', '.join(apass.sources.calibrator),
            # '{target}': ', '.join(apass.sources.target),
            '{target}': ', '.join(apass.sources.target),
            '{phaseref}': ', '.join([apass.sources.calibrator_for_target(tgt) for tgt in apass.sources.target]) if apass.sources.calibrator else '',
            '{dosolint}': '#' if apass.sources.calibrator else '',
            '{solint}': '2',
            # doprimarybeam/setup_station are only relevant for multi-phase-center
            # (wide-field) experiments. For a single-pass experiment they are commented
            # out via the {do_primarybeam} prefix so they never take effect.
            '{do_primarybeam}': '' if exp.multi_phase_center else '#',
            '{doprimarybeam}': '1' if exp.multi_phase_center else '-1',
            '{setup_station}': exp.refant[0] if len(exp.refant) > 0 else '',
            '{do_all_sources}': '' if exp.multi_phase_center else '#',
            '{all_sources}': ', '.join(set(apass.sources.target + apass.sources.fringefinder + apass.sources.calibrator)) if exp.multi_phase_center else '',
            }
        
        for placeholder, value in replacements.items():
            logger.debug(f"Pipeline input file - replaced {placeholder} with {value}")
            template_content = template_content.replace(placeholder, value)
        
        with open(inp_filename, 'w') as f:
            f.write(template_content)
        
        logger.debug(f"Created pipeline input file: {inp_filename}")

    os.chdir(original_cwd)
    return True


def run_pipeline(exp) -> bool:
    """Runs the EVN Pipeline
    """
    original_cwd = os.getcwd()
    try:
        logger.debug('# Running the pipeline...')
        
        if not exp.dirs.pipe_in.exists():
            logger.error(f"Pipeline input directory does not exist: {exp.dirs.pipe_in}")
            return False
            
        os.environ['PIPEFITS'] = str(original_cwd)
        os.chdir(exp.dirs.pipe_in)
        pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
        
        if not pipepasses:
            logger.error("No pipeline passes found")
            return False
            
        logger.info(f"Setting the PIPEFITS environment variable to {os.environ.get('PIPEFITS')}")
        if len(pipepasses) > 1:
            with ProcessPoolExecutor() as executor:
                futures = [executor.submit(utils.shell_command, "EVN.py", [f"{exp.expname.lower()}_{i}.inp.txt"], stdout=None) 
                           for i in range(1, len(pipepasses) + 1)]
                for i, future in enumerate(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Pipeline pass {i+1} failed: {e}")
                        traceback.print_exc()
                        return False
        else:
            utils.shell_command("EVN.py", [f"{exp.expname.lower()}.inp.txt"], stdout=None) #subprocess.PIPE)

        return True
    except Exception as e:
        logger.error(f"Unexpected error running pipeline: {e}")
        traceback.print_exc()
        return False
    finally:
        # Always restore the working directory. Several early-return paths above
        # (no pipeline passes, a failed pass) previously left the process in
        # exp.dirs.pipe_in, corrupting the cwd for every later step.
        try:
            os.chdir(original_cwd)
        except OSError:
            pass


def comment_tasav_files(exp) -> bool:
    """Creates the comment and tasav files after the EVN Pipeline has run.
    """
    try:
        # Check if files already exist
        comment_files = list(exp.dirs.pipe_out.glob(f"{exp.expname.lower()}*.comment"))

        if comment_files and list(exp.dirs.pipe_in.glob(f"{exp.expname.lower()}*.tasav.txt")):
            logger.debug("Comment and tasav files already exist. Skipping.")
            return True

        if not exp.dirs.pipe_in.exists():
            logger.error(f"Pipeline input directory does not exist: {exp.dirs.pipe_in}")
            return False

        pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]

        if not pipepasses:
            logger.error("No pipeline passes found")
            return False

        if len(pipepasses) > 1:
            for p, ppass in enumerate(pipepasses, start=1):
                if not ppass.freqsetup:
                    logger.warning(f"No frequency setup for pipeline pass {p}")
                    continue
                is_line = ppass.freqsetup.channels >= 512
                comment_tasav.create_comment_and_tasav(exp, f"{exp.expname.lower()}_{p}", is_line)
        else:
            if not exp.correlator_passes[0].freqsetup:
                logger.error("No frequency setup available")
                return False
            is_line = exp.correlator_passes[0].freqsetup.channels >= 512
            comment_tasav.create_comment_and_tasav(exp, exp.expname.lower(), is_line)

        return True
    except Exception as e:
        logger.error(f"Unexpected error creating comment/tasav files: {e}")
        traceback.print_exc()
        return False


def pipeline_feedback(exp) -> bool:
    """Generates the pipeline-feedback HTML page(s) after the EVN Pipeline has run.

    This is the in-tree Python port of the historical ``feedback.pl`` script
    (see :mod:`evn_postprocess.feedback`). For multi-pass experiments one page is
    produced per pass (``{expname}_{p}.html``), otherwise a single ``{expname}.html``.
    """
    pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
    sources = [s.name for s in exp.sources]
    # Network Monitoring Experiments (and the e-EVN test experiments) use the NME-formatted
    # feedback page. These are identified by the experiment name starting with 'N' or 'F'.
    is_nme = exp.expname[:1].upper() in ('N', 'F')

    # Always regenerate the feedback page(s): remove any pre-existing feedback HTML for this
    # experiment first. This guarantees the page reflects the latest products/comments on
    # every run, and clears stale pages (e.g. a {expname}_2.html left over from a previous
    # multi-pass run that is now single-pass). The match is restricted to the feedback page
    # naming ({expname}.html / {expname}_<n>.html) so other HTML in pipe_out is left alone.
    page_re = re.compile(rf"^{re.escape(exp.expname.lower())}(_\d+)?\.html$")
    for old_page in exp.dirs.pipe_out.glob(f"{exp.expname.lower()}*.html"):
        if page_re.match(old_page.name):
            logger.debug(f"Removing existing feedback page before regenerating: {old_page.name}")
            old_page.unlink()

    if len(pipepasses) > 1:
        for p in range(1, len(pipepasses) + 1):
            feedback.generate_feedback_page(f"{exp.expname.lower()}_{p}", sources=sources,
                                            nme=is_nme, contact=exp.supsci, directory=exp.dirs.pipe_out)
    else:
        feedback.generate_feedback_page(exp.expname.lower(), sources=sources,
                                        nme=is_nme, contact=exp.supsci, directory=exp.dirs.pipe_out)
    return True


def archive(exp) -> bool:
    """Archives the EVN Pipeline results.
    """
    original_cwd = os.getcwd()
    try:
        for folder in (exp.dirs.pipe_in, exp.dirs.pipe_out):
            os.chdir(folder)
            utils.shell_command("archive.pl", ["-pipe", "-e", f"{exp.expname.upper()}_{exp.obsdate.strftime('%y%m%d')}"], stdout=None)
            os.chdir(original_cwd)
    finally:
        os.chdir(original_cwd)

    return True


# Here there should be a dialog about checking pipeline results, do them manually...

def ampcal(exp) -> bool:
    """Runs the ampcal.sh script to incorporate the gain corrections into the Grafana database.
    """
    original_cwd = os.getcwd()  # must be read before the chdir so the finally can restore it
    try:
        os.chdir(exp.dirs.pipe_out)
        utils.shell_command("ampcal.sh")
    finally:
        os.chdir(original_cwd)
    return True

