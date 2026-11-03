#! /usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the pipe computer.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.
"""
import os
import re
import shlex
import glob
import shutil
import subprocess
import traceback
from importlib import resources
from loguru import logger
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from rich import print as rprint
from . import experiment
from . import utils
from . import tools
from . import comment_tasav
from . import feedback


# antab_editor.py exits 127 when its window is closed, which is how a normal session ends —
# not a failure, and not the shell's "command not found" it usually means. It is accepted, but
# never silently: shell_command logs a warning for it, and the operator is asked afterwards
# whether the .antab it produced is good enough to go on (see workflow.antfiles).
ANTAB_EDITOR_OK_RETURNCODES: tuple[int, ...] = (127,)


def _link_vixfile(exp) -> None:
    """Symlinks the experiment .vix into ``antenna_files/``, beside the station files.

    antab_editor.py runs from that directory, so having the vex schedule there means it —
    and anyone working in there by hand — can reach it without stepping back out of the
    directory. The link is relative, so it survives the experiment directory being moved.

    The link is named after the *lowercase* experiment name, which is what antab_editor.py
    looks for; ``exp.vixfile`` is the uppercase name in the experiment root (itself usually
    a symlink to the lowercase file), so its name cannot simply be reused here.

    Never fails the step: a missing vex, or the name already taken in ``antenna_files/``,
    is a warning and nothing more.
    """
    link = exp.dirs.pipe_temp / f"{exp.expname.lower()}{exp.vixfile.suffix}"
    if link.is_symlink() and not link.exists():
        logger.warning(f"Replacing dangling symlink {link} (pointed to a missing file).")
        link.unlink()
    elif link.exists():
        return

    if not exp.vixfile.exists():
        logger.warning(f"No {exp.vixfile} found to link into {exp.dirs.pipe_temp}.")
        return
    try:
        link.symlink_to(os.path.relpath(exp.vixfile.resolve(), link.parent.resolve()))
        logger.debug(f"Linked {exp.vixfile} into {exp.dirs.pipe_temp}.")
    except OSError as e:
        logger.warning(f"Could not link {exp.vixfile} into {exp.dirs.pipe_temp}: {e}")


def run_antab_editor(exp) -> bool:
    """Opens antab_editor.py for the given experiment.

    For an e-EVN run (several experiments correlated together) the editor is run
    once from the main experiment, passing the associated experiments via ``-a``
    together with the path to their FITS-IDI files, so a single, consistent set of
    Tsys/gain tables is produced for the whole session.

    The experiment .vix is linked into ``antenna_files/`` first, so the vex schedule sits
    beside the station files the editor works on (see :func:`_link_vixfile`).

    Returns:
        bool: True once the editor has been run (the editor itself runs interactively; this
        function only manages the working directory). Exit code 127, which is what closing
        the editor window produces, counts as a normal end of session.
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

    _link_vixfile(exp)
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
        # -f takes every FITS-IDI directory (this experiment's, '..', and its e-EVN siblings), so
        # it must come last: -p is nargs='+' of ints and would otherwise swallow the sibling paths.
        # The spectral-line check uses the correlator passes: the .lis files are in the
        # experiment folder, not in antenna_files where this runs.
        pass_args = ["-l"] if exp.spectral_line else ["-p", "1"]
        cmd = ["antab_editor.py", "-e", exp.expname.lower(), *assoc_args, *pass_args, "-f", "..", *assoc_paths]

        try:
            utils.shell_command("antab_editor.py", cmd[1:],
                                shell=True, stdout=None, ok_returncodes=ANTAB_EDITOR_OK_RETURNCODES)
        except ValueError as exc:
            manual = ' '.join(shlex.quote(str(p)) for p in cmd)
            raise ValueError(
                f"{exc}\nTo re-run manually: cd {shlex.quote(str(exp.dirs.pipe_temp))} && {manual}"
            ) from exc

        if len(missing_antabs := [a.name for a in exp.antennas if not a.antabfsfile]) > 0:
            rprint(f"[red]Note that you are missing ANTAB files from: {', '.join(missing_antabs)}[/red]")
    finally:
        os.chdir(original_cwd)
    return True


# A flagging command names its antenna as ant_name='XX' (uvflg.pl, vlbeer .flag), or with the AIPS
# UVFLG adverb antennas= (any abbreviation down to ANTE=, as used in hand-written flags). The name is
# matched case-insensitively: uvflg.pl writes 'EF' while vlbeer writes 'Ef'.
_FLAG_ANT_RE = re.compile(r"\b(?:ant_name|ante\w*)\s*=\s*'?([A-Za-z0-9]+)'?", re.IGNORECASE)
# First line of a .uvflgfs written by supplement_uvflgfs_from_flag (so a re-run can tell it from a
# .log-derived one and refresh it).
_FALLBACK_MARK = "! A-priori flagging for"
# Every .uvflgfs starts with these settings so its lines never depend on the file before it in
# the combined .uvflg (same lines uvflg.pl writes).
_UVFLGFS_HEADER = "opcode='FLAG'\ndtimrang = 1   timeoff=0\n"


def _flag_line_antenna(line: str) -> str | None:
    """Antenna named by one flagging command line, in capitals.

    Args:
        line (str): A line of a .flag or .uvflgfs file.

    Returns:
        str | None: The antenna codename (e.g. 'CM'), or None for comments, settings
            (opcode/dtimrang) and any line that names no antenna.
    """
    stripped = line.strip()
    if not stripped or stripped.startswith('!'):
        return None
    match = _FLAG_ANT_RE.search(stripped)
    return match.group(1).upper() if match else None


def _has_flag_commands(uvflgfs_file: Path) -> bool:
    """Tells whether a .uvflgfs file holds at least one real flagging command.

    Only lines naming an antenna count: comments and the opcode/dtimrang settings that
    uvflg.pl writes at the top of every file are not flagging commands.

    Args:
        uvflgfs_file (Path): The .uvflgfs file to inspect.

    Returns:
        bool: True if any line flags an antenna; False for missing, empty or header-only files.
    """
    if not uvflgfs_file.exists():
        return False
    with open(uvflgfs_file, 'r', errors='replace') as fh:
        return any(_flag_line_antenna(line) is not None for line in fh)


def _is_fallback_uvflgfs(uvflgfs_file: Path) -> bool:
    """True if *uvflgfs_file* was written from the .flag file by supplement_uvflgfs_from_flag."""
    if not uvflgfs_file.exists():
        return False
    with open(uvflgfs_file, 'r', errors='replace') as fh:
        return fh.readline().startswith(_FALLBACK_MARK)


def find_flag_file(directory: Path, expname: str) -> Path | None:
    """Returns the a-priori (vlbeer) flag file of the experiment in *directory*.

    The exact name {exp}.flag is preferred; otherwise the first (sorted) {exp}_*.flag is used,
    e.g. a file carrying a session or pass suffix. The underscore keeps another experiment whose
    name extends this one (e.g. er057b for er057) from matching.

    Args:
        directory (Path): Directory to search (the pipeline temp dir).
        expname (str): Experiment name (any case).

    Returns:
        Path | None: The flag file, or None if there is none.
    """
    exact = directory / f"{expname.lower()}.flag"
    if exact.exists():
        return exact
    candidates = sorted(directory.glob(f"{expname.lower()}_*.flag"))
    if len(candidates) > 1:
        logger.warning(f"Several flag files found ({', '.join(c.name for c in candidates)}); "
                       f"using {candidates[0].name}.")
    return candidates[0] if candidates else None


def supplement_uvflgfs_from_flag(exp, directory: Path) -> dict[str, list[str]]:
    """Fills in the .uvflgfs of observed antennas that have no real one, from the a-priori .flag file.

    The .uvflgfs made from a station's .log (by uvflgall.sh) is the real flagging and is never
    touched. For every observed antenna whose {exp}{ant}.uvflgfs is missing or has no flagging
    command, the lines of the a-priori flag file (vlbeer, see find_flag_file) that flag that
    antenna (ant_name='Xx' or antenna='Xx', any case) are written to {exp}{ant}.uvflgfs
    (overwriting an empty one), after the opcode/dtimrang header. The antenna name in those
    lines is written in capitals, as uvflg.pl does and as the FITS-IDI antenna table has it.

    Args:
        exp (experiment.Experiment): Experiment (uses expname and antennas.observed).
        directory (Path): Directory holding the .uvflgfs and .flag files (the pipeline temp dir).

    Returns:
        dict[str, list[str]]: Antenna codenames (capitals) grouped as 'log' (real .uvflgfs kept),
            'flag' (written from .flag) and 'none' (no flagging available for them).
    """
    expname = exp.expname.lower()
    flag_file = find_flag_file(directory, expname)
    flag_lines = flag_file.read_text(errors='replace').splitlines() if flag_file is not None else []
    lines_by_ant: dict[str, list[str]] = {}
    for line in flag_lines:
        if (ant := _flag_line_antenna(line)) is not None:
            match = _FLAG_ANT_RE.search(line)
            upper_line = line[:match.start(1)] + ant + line[match.end(1):]
            lines_by_ant.setdefault(ant, []).append(upper_line.rstrip() + '\n')

    result: dict[str, list[str]] = {'log': [], 'flag': [], 'none': []}
    for ant in sorted({a.upper() for a in exp.antennas.observed}):
        uvflgfs_file = directory / f"{expname}{ant.lower()}.uvflgfs"
        if _has_flag_commands(uvflgfs_file) and not _is_fallback_uvflgfs(uvflgfs_file):
            result['log'].append(ant)
            continue
        if not (ant_lines := lines_by_ant.get(ant)):
            result['none'].append(ant)
            continue
        with open(uvflgfs_file, 'w') as fh:
            fh.write(f"{_FALLBACK_MARK} {ant} from {flag_file.name}\n")
            fh.write(_UVFLGFS_HEADER)
            fh.writelines(ant_lines)
        result['flag'].append(ant)
        logger.info(f"Created {uvflgfs_file.name} from {flag_file.name} for {ant} ({len(ant_lines)} lines)")

    flag_name = flag_file.name if flag_file is not None else f"{expname}*.flag"
    logger.info(f"uvflgfs sources: from .log: {', '.join(result['log']) or '-'}; "
                f"from {flag_name}: {', '.join(result['flag']) or '-'}; "
                f"no flagging: {', '.join(result['none']) or '-'}")
    if result['none']:
        why = f"no lines for them in {flag_name}" if flag_file is not None else f"{flag_name} not found"
        rprint(f"[yellow]No flagging commands for {', '.join(result['none'])} ({why}).[/yellow]")
    return result


def concatenate_uvflgfs(directory: Path, output: Path) -> list[Path]:
    """Writes all the {directory}/*.uvflgfs files, in sorted order, into *output*.

    Each file ends with a newline in the output even if its last line has none, so two files
    never merge into one line.

    Args:
        directory (Path): Directory holding the .uvflgfs files.
        output (Path): The combined .uvflg file to write (overwritten).

    Returns:
        list[Path]: The .uvflgfs files written, in order. Empty (and *output* not written) if none.
    """
    uvflgfs_files = sorted(directory.glob("*.uvflgfs"))
    if not uvflgfs_files:
        return []
    with open(output, 'w') as out:
        for uvflgfs_file in uvflgfs_files:
            text = uvflgfs_file.read_text(errors='replace')
            out.write(text if text.endswith('\n') or not text else text + '\n')
    logger.info(f"Wrote {output.name} from {len(uvflgfs_files)} .uvflgfs files: "
                f"{', '.join(f.name for f in uvflgfs_files)}")
    return uvflgfs_files


def _warn_unflagged_antennas(exp, uvflg_file: Path) -> list[str]:
    """Warns about observed antennas with no flagging line in an existing .uvflg.

    The existing file is kept (it may have been edited by hand), but one made before a station's
    flagging became available (e.g. by an older version without the .flag fallback) would
    silently leave that station unflagged.

    Args:
        exp: Experiment object (uses antennas.observed).
        uvflg_file: The existing combined .uvflg file.

    Returns:
        The observed antennas (capitals) without any flagging line in it.
    """
    with open(uvflg_file, 'r', errors='replace') as fh:
        flagged = {ant for line in fh if (ant := _flag_line_antenna(line)) is not None}
    missing = sorted({a.upper() for a in exp.antennas.observed} - flagged)
    if missing:
        logger.warning(f"{uvflg_file.name} has no flagging for {', '.join(missing)}. Delete it (and the "
                       f"*.uvflg in pipeline/in) and re-run the antab step to regenerate it.")
    return missing


def create_uvflg(exp) -> bool:
    """Produces the combined uvflg file containing the full flagging from all telescopes.

    The per-antenna .uvflgfs come from the station .log files (uvflgall.sh); antennas without
    a real one fall back to their lines in the a-priori flag file (see supplement_uvflgfs_from_flag).
    Without any .log file it still proceeds if a flag file is available, and fails otherwise.
    Every failure (no input, uvflgall.sh failing, no .uvflgfs produced) is reported and returns
    False; it never raises.

    Args:
        exp (experiment.Experiment): Experiment (uses expname, dirs.pipe_temp, antennas, correlator_passes).

    Returns:
        bool: True if the {exp}.uvflg file was created (or already existed).
    """
    if existing := sorted(Path(exp.dirs.pipe_temp).glob("*.uvflg")):
        logger.info("uvflg files already created. Skipping.")
        _warn_unflagged_antennas(exp, existing[0])
        return True

    expname = exp.expname.lower()
    # Absolute: the Dirs are stored relative to the experiment folder and this function chdirs
    # into pipe_temp, after which the relative path would point to pipe_temp/pipe_temp.
    pipe_temp = Path(exp.dirs.pipe_temp).resolve()
    has_logs = len(glob.glob(str(pipe_temp / "*.log"))) > 0
    flag_file = find_flag_file(pipe_temp, expname)
    if not has_logs and flag_file is None:
        logger.error(f"Neither station .log files nor a {expname}*.flag file found in {pipe_temp}.")
        rprint(f"[bold red]ERROR:[/bold red] [red]Neither station .log files nor a {expname}*.flag file "
               f"found in {pipe_temp}.[/red]")
        return False

    original_cwd = os.getcwd()
    os.chdir(pipe_temp)
    try:
        if has_logs:
            try:
                utils.shell_command(str(tools.script_path("uvflgall.sh")))
            except ValueError as exc:
                logger.error(f"uvflgall.sh failed in {pipe_temp}: {exc}")
                rprint(f"[bold red]ERROR:[/bold red] [red]uvflgall.sh failed: {exc}[/red]")
                return False
        else:
            logger.warning(f"No log files found: building the flagging only from {flag_file.name}.")
            rprint(f"[yellow]No log files found: using only the a-priori {flag_file.name} for the flagging.[/yellow]")

        supplement_uvflgfs_from_flag(exp, pipe_temp)
        uvflg_file = pipe_temp / f"{expname}.uvflg"
        if not concatenate_uvflgfs(pipe_temp, uvflg_file):
            logger.error(f"No .uvflgfs file was produced in {pipe_temp}; {uvflg_file.name} not created.")
            rprint(f"[bold red]ERROR:[/bold red] [red]No .uvflgfs files were produced, so {uvflg_file.name} "
                   "could not be created.[/red]")
            return False

        if len(pipepass := [apass.pipeline for apass in exp.correlator_passes if apass.pipeline]) > 1:
            for p in range(1, len(pipepass) + 1):
                shutil.copy(uvflg_file, pipe_temp / f"{expname}_{p}.uvflg")
    finally:
        os.chdir(original_cwd)
    return True


def _select_bandpass_sources(sources) -> list[str]:
    """Return the best available source list for bandpass calibration.

    The EVN pipeline normally derives the bandpass from fringe-finder sources.
    When none exist, fall back to phase calibrators, then to the target sources
    themselves. This keeps NME-style observations (targets only) able to run
    the pipeline with the sources that are actually present.
    """
    for source_list in (sources.fringefinder, sources.calibrator, sources.target, sources.names):
        if source_list:
            return list(source_list)
    return []


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

    pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
    userno = _aips_userno(exp.supsci)
    original_cwd = os.getcwd()
    os.chdir(exp.dirs.pipe_in)
    try:
        if len(pipepasses) > 1:
            # Fan out the unnumbered .antab/.uvflg into per-pass copies, but never over a numbered
            # file that already exists (written per pass by antab_editor, or edited by hand).
            for ext in ('antab', 'uvflg'):
                if Path(f"{exp.expname.lower()}.{ext}").exists():
                    for p in range(1, len(pipepasses) + 1):
                        if not Path(f"{exp.expname.lower()}_{p}.{ext}").exists():
                            shutil.copy(f"{exp.expname.lower()}.{ext}", f"{exp.expname.lower()}_{p}.{ext}")

        # Read the template once and format it per pass (also keeps it mockable in tests via
        # importlib.resources).
        template_path = resources.files("evn_postprocess.templates").joinpath("pipeline.inp.txt.template")
        template_text = template_path.read_text()
        for i, apass in enumerate(pipepasses, 1):
            inp_filename = Path(f"{exp.expname.lower()}_{i}.inp.txt" if len(pipepasses) > 1
                                else f"{exp.expname.lower()}.inp.txt")
            if inp_filename.exists():
                continue

            with open(inp_filename, 'w') as f:
                f.write(_fill_pipeline_template(exp, apass, i, len(pipepasses), userno, template_text))
            logger.debug(f"Created pipeline input file: {inp_filename}")
    finally:
        os.chdir(original_cwd)
    return True


def _aips_userno(supsci: str) -> str:
    """AIPS user number of the support scientist (from aips_userno.py), or '100' if unavailable.

    Args:
        supsci: Support scientist's user name.

    Returns:
        The user number as a string; '100' (with a warning) if the helper is missing or prints nothing.
    """
    try:
        userno = subprocess.run(['aips_userno.py', supsci.lower()], capture_output=True, text=True).stdout.strip()
    except OSError as exc:
        logger.warning(f"Could not run aips_userno.py ({exc}); using AIPS user number 100.")
        return '100'
    return userno or '100'


def _fill_pipeline_template(exp, apass, i: int, n_passes: int, userno: str, template_text: str) -> str:
    """Returns the pipeline input file text for one correlator pass.

    Args:
        exp: Experiment object.
        apass: The correlator pass.
        i: 1-based index of the pass among the pipelined passes.
        n_passes: Number of pipelined passes.
        userno: AIPS user number.
        template_text: Content of pipeline.inp.txt.template.

    Returns:
        The template with every placeholder replaced.
    """
    # Order-preserving dedupe (targets, then fringe finders, then calibrators; first
    # occurrence wins). A set would reorder the names on every run, making the generated
    # input file differ between otherwise identical runs.
    all_sources: list[str] = []
    for src_name in list(apass.sources.target) + list(apass.sources.fringefinder) + list(apass.sources.calibrator):
        if src_name not in all_sources:
            all_sources.append(src_name)

    replacements = {
        '{expname}': exp.expname.lower() if n_passes == 1 else f"{exp.expname.lower()}_{i}",
        '{userno}': userno,
        # The full prioritised list: EVN.py uses refant[0] and passes the rest to FRING as
        # SEARCH antennas, so scans without the first one still get fringe solutions.
        '{refant}': ', '.join(exp.refant),
        '{plotref}': exp.refant[0],
        '{bpass}': ', '.join(_select_bandpass_sources(apass.sources)),
        '{dophaseref}': '' if apass.sources.calibrator else '#',
        '{target}': ', '.join(apass.sources.target),
        '{phaseref}': ', '.join([apass.sources.calibrator_for_target(tgt) for tgt in apass.sources.target])
                      if apass.sources.calibrator else '',
        '{dosolint}': '#' if apass.sources.calibrator else '',
        '{solint}': '2',
        # doprimarybeam/setup_station are only relevant for multi-phase-center
        # (wide-field) experiments. For a single-pass experiment they are commented
        # out via the {do_primarybeam} prefix so they never take effect.
        '{do_primarybeam}': '' if exp.multi_phase_center else '#',
        '{doprimarybeam}': '1' if exp.multi_phase_center else '-1',
        '{setup_station}': exp.refant[0] if len(exp.refant) > 0 else '',
        '{do_all_sources}': '' if exp.multi_phase_center else '#',
        '{all_sources}': ', '.join(all_sources) if exp.multi_phase_center else '',
        }
    content = template_text
    for placeholder, value in replacements.items():
        logger.debug(f"Pipeline input file - replaced {placeholder} with {value}")
        content = content.replace(placeholder, value)
    return content


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
        if len(pipepasses) == 1:
            utils.shell_command("EVN.py", [f"{exp.expname.lower()}.inp.txt"], stdout=None)
            return True

        # Each pass is pipelined in its own process: an EVN.py run drags a whole AIPS/
        # ParselTongue session behind it, and a separate interpreter per pass keeps whatever
        # one of them does to its process state (environment, AIPS user, signal handlers,
        # working directory) from reaching the others. The workers inherit the working
        # directory os.chdir just set to pipe_in, which every pass input file name is
        # relative to. The pool is bounded like every other per-pass pool
        # (utils.pass_workers), with the I/O ceiling: one heavy subprocess per pass
        # saturates the disk long before the cores.
        with ProcessPoolExecutor(utils.pass_workers(len(pipepasses),
                                                    utils.MAX_PASS_IO_WORKERS)) as executor:
            futures = [executor.submit(utils.shell_command, "EVN.py",
                                       [f"{exp.expname.lower()}_{i}.inp.txt"], stdout=None)
                       for i in range(1, len(pipepasses) + 1)]
            for i, future in enumerate(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Pipeline pass {i + 1} failed: {e}")
                    traceback.print_exc()
                    return False
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

    Each page shows the sources of its own correlator pass, and only those: a pass
    (in particular each phase centre of a multi-phase-centre experiment) contains just
    the sources that have visibilities in its MS. The experiment-wide source list is only
    used as a fallback for a pass with no sources recorded.
    """
    pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
    exp_sources = [s.name for s in exp.sources]

    def _sources_of(apass, page_name: str) -> list[str]:
        """Returns the source names to show in the feedback page of one correlator pass.

        Args:
            apass (experiment.CorrelatorPass | None): The pass the page belongs to, or None
                if no correlator pass is associated to it.
            page_name (str): Name of the feedback page, only used for logging.

        Returns:
            list[str]: The source names of that pass, or the experiment-wide ones if the
            pass has no sources recorded.
        """
        if apass is None or len(apass.sources) == 0:
            logger.warning(f"No sources recorded for the pass of {page_name}; using the experiment-wide "
                           f"source list ({', '.join(exp_sources)}) in its feedback page.")
            return exp_sources

        return apass.sources.names

    # Network Monitoring Experiments use the NME-formatted feedback page
    # (see experiment.is_nme for the single definition of what an NME is).
    is_nme = experiment.is_nme(exp.expname)

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
        for p, apass in enumerate(pipepasses, 1):
            page_name = f"{exp.expname.lower()}_{p}"
            feedback.generate_feedback_page(page_name, sources=_sources_of(apass, page_name),
                                            nme=is_nme, contact=exp.supsci, directory=exp.dirs.pipe_out)
    else:
        page_name = exp.expname.lower()
        feedback.generate_feedback_page(page_name,
                                        sources=_sources_of(pipepasses[0] if pipepasses else None, page_name),
                                        nme=is_nme, contact=exp.supsci, directory=exp.dirs.pipe_out)
    return True


def _write_pipeline_readme(exp, folder) -> None:
    """Write a README.txt describing the source(s) of each correlator pass into *folder*.

    The README lists the MS-file suffix for each pass and the source names recorded in
    that pass, so the archived pipeline products can be traced back to their data.
    """
    header = f"Correlator pass source summary for {exp.expname} ({exp.obsdate.strftime('%Y-%m-%d')})\n\n"
    lines: list[str] = []
    passes = exp.correlator_passes
    for a_pass in passes[:5]:
        stem = a_pass.msfile.stem
        parts = stem.split('_')
        suffix = '_'.join(parts[-2:]) if len(parts) >= 2 else stem
        source_names = list(a_pass.sources.names)
        sources = ', '.join(source_names) if source_names else '(unknown)'
        lines.append(f"- {suffix} : {sources}")
    if len(passes) > 5:
        lines.append(f"... (+{len(passes) - 5} more)")
    Path(folder / "README.txt").write_text(header + '\n'.join(lines) + '\n', encoding='utf-8')


def archive(exp) -> bool:
    """Archives the EVN Pipeline results.

    Runs ``archive.pl -pipe`` inside ``pipeline/in`` and ``pipeline/out`` without
    changing the Python working directory, so the command is logged to the
    experiment root ``logs/commands.sh`` rather than the per-folder log.
    """
    archive_arg = f"{exp.expname.upper()}_{exp.obsdate.strftime('%y%m%d')}"
    for folder in (exp.dirs.pipe_in, exp.dirs.pipe_out):
        _write_pipeline_readme(exp, folder)
        utils.shell_command(
            "cd",
            f"{shlex.quote(str(folder))} && archive.pl -pipe -e {archive_arg}",
            shell=True, stdout=None,
        )
    return True


# Here there should be a dialog about checking pipeline results, do them manually...

def ampcal(exp) -> bool:
    """Runs the ampcal.sh script to incorporate the gain corrections into the Grafana database.
    """
    original_cwd = os.getcwd()  # must be read before the chdir so the finally can restore it
    try:
        os.chdir(exp.dirs.pipe_out)
        utils.shell_command(str(tools.script_path("ampcal.sh")))
    finally:
        os.chdir(original_cwd)
    return True

