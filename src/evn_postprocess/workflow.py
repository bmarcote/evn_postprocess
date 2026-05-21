"""Workflow functions for post-processing EVN experiments.

These functions are called by the Snakemake workflow and wrap the functionality
from process.py, pipeline.py, and pre.py modules.
"""
import re
import sys
import json
import glob
import shutil
import traceback
from datetime import datetime as dt
from pathlib import Path
from typing import Callable
from dataclasses import dataclass
from loguru import logger
from rich import print as rprint
from rich.panel import Panel
from rich.console import Console

from astropy import units as u
from astropy import coordinates as coord
from . import experiment
from . import io
from . import process
from . import pipeline
from . import lisfiles
from . import dialog
from . import utils
from . import comms as _comms

_RICH_TAG_RE = re.compile(r'\[/?[\w\s#.,;:!?=-]+\]')
_stdout_console = Console(highlight=False)
_stderr_console = Console(stderr=True, highlight=False)

# Module-level batch-mode flag. When True the runner refuses to call interactive
# dialogs and signals "needs_review" by writing a marker file instead of
# printing a Rich panel and waiting for the operator. Set via :func:`set_batch_mode`
# from the CLI entry point.
_BATCH_MODE = False
REVIEW_FLAG_FILENAME = "REVIEW_REQUIRED"

# Module-level notifier for sending messages at key interaction points.
# Set via :func:`set_notifier` from the CLI entry point.
_NOTIFIER: _comms.Notifier | None = None


def set_batch_mode(enabled: bool) -> None:
    """Toggles the package-wide batch-mode flag.

    See module docstring for the contract. Kept as a function (rather than a
    ``Policy.batch`` lookup) because some helpers (notably :func:`msops`) are
    invoked through ``run_isolated_task`` without a Policy attached.
    """
    global _BATCH_MODE
    _BATCH_MODE = bool(enabled)


def is_batch_mode() -> bool:
    """Returns the current batch-mode flag."""
    return _BATCH_MODE


def set_notifier(notifier: _comms.Notifier) -> None:
    """Set the module-level notifier for comms notifications.

    Args:
        notifier: A concrete Notifier instance (NoneNotifier, EmailNotifier, or MattermostNotifier).
    """
    global _NOTIFIER
    _NOTIFIER = notifier


def get_notifier() -> _comms.Notifier | None:
    """Returns the current module-level notifier, or None."""
    return _NOTIFIER


def _review_flag_path(exp: experiment.Experiment) -> Path:
    """Returns the path of the ``REVIEW_REQUIRED`` marker for *exp*.

    The marker lives at the root of the experiment work directory so an
    operator can spot it without descending into ``logs/`` or ``pipeline/``.
    """
    return Path(REVIEW_FLAG_FILENAME)


def _write_review_flag(exp: experiment.Experiment, step: str, reason: str) -> None:
    """Writes the ``REVIEW_REQUIRED`` marker file with a human-readable reason.

    Called from places that previously printed a Rich panel to stdout and
    blocked on ``input``: now we leave a small text file on disk so a queue
    system can detect the pause condition without parsing log output.

    Args:
        exp: Experiment object, used purely for logging context.
        step: The step name that triggered the pause / review.
        reason: Free-form explanation written into the marker for the operator.
    """
    flag = _review_flag_path(exp)
    try:
        flag.write_text(
            f"step: {step}\nexperiment: {exp.expname}\nreason: {reason}\n",
            encoding="utf-8",
        )
    except OSError as e:
        logger.warning(f"Could not write {flag}: {e}")
    logger.info(f"Wrote review marker {flag} for step '{step}'.")


def _clear_review_flag(exp: experiment.Experiment) -> None:
    """Removes the ``REVIEW_REQUIRED`` marker if present (idempotent)."""
    _review_flag_path(exp).unlink(missing_ok=True)


def _signal_pause(exp: experiment.Experiment, step: str) -> None:
    """Signals a "stop and review" condition after a successful step.

    In interactive mode this prints the historical Rich panel and the desktop
    notification. In batch mode it writes a marker file (so the scheduler can
    detect the pause without parsing logs) and stays silent.
    """
    piletter = f"{exp.expname.lower()}.piletter"
    pause_reason = (f"Step '{step}' finished successfully. Review {piletter} and the pipeline "
                     f"output, then run `postprocess run` or `postprocess review ok` to continue.")

    if _BATCH_MODE:
        _write_review_flag(exp, step, pause_reason)
        if _NOTIFIER is not None:
            _comms.notify_step_pause(exp, step, pause_reason, _NOTIFIER)
        return

    # Send comms notification (email / mattermost) if configured
    if _NOTIFIER is not None:
        _comms.notify_step_pause(exp, step, pause_reason, _NOTIFIER)

    body = (f"[bold]Please do the following before continuing:[/bold]\n\n"
            f"  1. Check the pipeline output plots and logs.\n"
            f"  2. Review the PI letter ([bold cyan]{piletter}[/bold cyan]).\n"
            f"     Non-observing antennas and PolConvert remarks have been\n"
            f"     filled in automatically \u2014 verify and edit if needed.\n\n"
            f"[bold]When ready, run one of:[/bold]\n\n"
            f"  [bold green]postprocess run[/bold green]           \u2014 finalize and archive everything\n"
            f"  [bold green]postprocess run {step}[/bold green]  \u2014 re-run this step's diagnostics\n")
    Console().print(Panel(body, title=f"[bold yellow]Paused after '{step}' \u2014 review needed[/bold yellow]",
                          border_style="yellow", padding=(1, 2)))
    utils.notify(f"{exp.expname} post-processing", f"Paused after '{step}' \u2014 review pipeline results")


@dataclass
class Task(object):
    """Executes the command (which must be a Python function), that has the associated doc
    string for help.
    """
    name: str
    command: str
    doc: str
    done: bool = False

    def to_dict(self) -> dict:
        return {'name': self.name, 'command': self.command, 'doc': self.doc, 'done': self.done}

    @classmethod
    def from_dict(cls, data: dict) -> 'Task':
        return cls(name=data['name'], command=data['command'], doc=data['doc'], done=data.get('done', False))


_WORKFLOW_STEPS = [Task('initialize', 'initialize_experiment',
                        "Creates the directory structure to post-process the experiment. "
                        "Checks that the necessary servers are configured. Retrieves the observing date and "
                        "e-EVN run (if applicable) from the MASTER_PROJECT.LIS file. Retrieves the observing "
                        "files (.key, .sum) from vlbeer, and reads the .jexp files related to the project. "
                        "Verifies if the post-processing has already run before and recovers it."),
                   Task('lisfiles', 'retrieve_lisfiles', "Creates the .lis files in ccs and retrieves them. "
                        "Processes the files to this experiment."),
                   Task('checklis', 'check_lisfiles', "Verifies that the .lis files seem to be fine, "
                        "and sets up the correlator passes to be processed from them."),
                   Task('j2ms2', 'create_msfile', "Creates MS files from .lis files using j2ms2. "
                        "It also retrieves some stats from the visibilities, and creates the notes.md file."),
                   Task('standardplots', 'create_standardplots', "Creates the standard plots from the "
                        "required MS file."),
                   Task('msops', 'msops', "Applies MS operations including weight flagging, polswap, "
                        "and 1-bit scaling."),
                   Task('polconvert', 'polconvert', 'Runs polConvert on all available MS files'),
                   Task('standardplots2', 'msops_post', "Re-runs the standard plots after all msops "
                        "have been performed"),
                   Task('antab', 'antfiles', "Retrieves the .antabfs and .log files from vlbeer. Creates "
                        "the .antab (requires graphical interaction via antab_editor), and the .uvflg file."),
                   Task('pipeline', 'run_pipeline', "Prepares the input file for the EVN Pypeline and runs it."),
                   Task('postpipe', 'pipeline_diagnostics', 'Runs diagnostics on the pipeline outputs.'),
                   Task('prearchive', 'pre_archive', "Prepares the experiment for archiving. Attaches the Tsys "
                        "information to the FITS-IDI files."),
                   Task('archive', 'archive', "Sets the credentials, protechs the files, and archives the "
                        "experiment. In case of an NME, it will prepare the .tex file for the NME feedback.")]


def create_folder_structure() -> experiment.Dirs:
    """Creates the folder structure required for post-processing.

    Returns:
        Iterable[Path]: List of created folders.
    """
    folders = {k: Path(v) for k, v in {'logs': "logs", # 'data': "data", 'results': "results",
                                       'plots': "plots",
                                       'pipeline': "pipeline", 'pipe_in': "pipeline/in", 'pipe_out': "pipeline/out",
                                       'pipe_temp': "antenna_files"}.items()}
    for folder in folders.values():
        if not folder.exists():
            Path(folder).mkdir(parents=True, exist_ok=True)
            logger.info(f"Created folder {folder}")
        else:
            logger.debug(f"Folder {folder} already exists. Skipped creation.")

    return experiment.Dirs(**folders)


def initialize_experiment(expname: str, supsci: str) -> experiment.Experiment:
    """Initializes an experiment object with all the relevant metadata, obtained from MASTER_PROJECTS.LIS.

    Args:
        expname (str): Experiment name.
        supsci (str): Support scientist name assigned to this experiment.

    Returns:
        bool: True if experiment was initialized successfully.
    """
    logger.debug(f"Initializing experiment {expname}")
    try:
        servers = experiment.retrieve_servers()
    except FileNotFoundError:
        logger.error("Missing configuration file computers.toml")
        rprint("[red]Expected to be found at ~jops/.config/evn/computers.toml, "
               "or in your local .config directory.[/red]")
        sys.exit(1)

    #if experiment.Experiment.exists(expname):
    #    logger.debug(f"Recovering previously-stored experiment {expname} from file")
    #    return experiment.Experiment.load(expname)

    try:
        obsdate, eEVNname = io.parse_masterprojects(expname, servers['master_projects'])
    except ValueError:
        logger.error(f"[bold red]The assumed experiment code {expname} "
                     "is not recognized.[/bold red]")
        rprint("[red]Run the program from the experiment folder in /data/exp or use --expname[/red]")
        rprint("[red]Or at least it was not found in MASTER_PROJECTS.LIS[/red]")
        sys.exit(1)

    exp = experiment.Experiment(expname, dt.strptime(obsdate, "%y%m%d").date(), supsci,
                                create_folder_structure(), eEVNname)
    try:
        io.get_init_files(exp, servers)
    except ValueError:
        logger.error("Could not retrieve init files from this experiment (vox/vix, piletter, or expsum)")
        sys.exit(1)

    io.get_vlbeer_sched_files(exp.expname if exp.eEVNname is None else exp.eEVNname,
                              exp.obsdate, servers['vlbeer'])
    exp.get_info_from_vex()
    jexp_info = io.get_jexp_info(exp.expname, servers['jexp'])
    assert jexp_info['piname'] is not None, "piname is None"
    assert jexp_info['pimail'] is not None, "pimail is None"
    exp.pi.append(experiment.PI(jexp_info['piname'], jexp_info['pimail']))
    if jexp_info['coname'] is not None:
        assert jexp_info['coimail'] is not None, "coimail is None"
        exp.pi.append(experiment.PI(jexp_info['coname'], jexp_info['coimail']))

    assert jexp_info['schedsrc'] is not None, "No source information supplied in the jexp file"
    for src in jexp_info['schedsrc'].split(','):
        src_name, src_type_str, src_protected = src.strip().replace('(', '').replace(')', '').replace('|', ' ').split()
        match src_type_str.strip():
            case 'T':
                src_type = experiment.SourceType.target
            case 'R':
                src_type = experiment.SourceType.calibrator
            case 'C':
                src_type = experiment.SourceType.fringefinder
            case 'F':
                src_type = experiment.SourceType.fringefinder
            case _:
                src_type = experiment.SourceType.other

        if src_name not in exp.sources.names:
            # Use placeholder coordinates (0,0) - will be updated when MS data is processed
            placeholder_coords = coord.SkyCoord(ra=0*u.deg, dec=0*u.deg, frame='icrs')
            exp.sources.append(experiment.Source(name=src_name, coordinates=placeholder_coords, 
                                               type=src_type, protected=False))
        else:
            exp.sources[src_name].type = src_type
            exp.sources[src_name].protected = src_protected.strip() == 'X'

    # TODO: implement this one!
    # io.get_station_feedback_info(exp.expname, servers['station_feedback'])

    exp.store()
    return exp


def retrieve_lisfiles(exp: experiment.Experiment) -> bool:
    """Retrieves and processes .lis files from ccs.

    Args:
        exp (experiment.Experiment): Experiment object.
        server (experiment.Server): Server object with ccs connection information.

    Returns:
        bool: True if lis files were retrieved and processed successfully.
    """
    try:
        if len(glob.glob(f"{exp.expname.lower()}*.lis")) > 0:
            logger.debug(".lis files already exist. Skipping retrieval.")
            return True

        # Check each step individually for better error reporting
        if not lisfiles.create_lis_files(exp):
            logger.error("Failed to create .lis files")
            return False
            
        if not lisfiles.get_lis_files(exp):
            logger.error("Failed to retrieve .lis files")
            return False
            
        if not lisfiles.get_passes_from_lisfiles(exp):
            logger.error("Failed to extract passes from .lis files")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Unexpected error retrieving .lis files: {e}")
        traceback.print_exc()
        return False


def check_lisfiles(exp: experiment.Experiment) -> bool:
    """Checks and sets up correlator passes from .lis files.

    Args:
        exp (experiment.Experiment): Experiment object with correlator passes.

    Returns:
        bool: True if all lis files are valid.
    """
    try:
        if not exp.correlator_passes:
            if not lisfiles.get_passes_from_lisfiles(exp):
                logger.error("Failed to extract passes from .lis files")
                return False
            
        if all(p.msfile.exists() for p in exp.correlator_passes):
            logger.debug("MS files already exist. Skipping checklis.")
            return True

        if not lisfiles.check_lisfiles(exp):
            # TODO: In case of e-EVN runs, it needs to do it!
            logger.error("Issues found in .lis files. Please check the files.")
            return False

        return True
    except Exception as e:
        logger.error(f"Unexpected error checking .lis files: {e}")
        traceback.print_exc()
        return False


def create_msfile(exp: experiment.Experiment) -> bool:
    """Creates MS files from .lis files using j2ms2.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if MS files were created successfully.
    """
    try:
        # Skip if metadata already loaded (check before replacing correlator_passes)
        if exp.correlator_passes and exp.correlator_passes[0].freqsetup is not None:
            logger.debug("MS metadata already loaded. Skipping MS creation and extraction.")
            return True

        # Re-doing again in case the lis files were updated externally
        lisfiles.get_passes_from_lisfiles(exp)
        if not all(p.msfile.exists() for p in exp.correlator_passes):
            if not process.getdata(exp):
                logger.error("Failed to get data for MS creation")
                return False

            if not process.j2ms2(exp):
                logger.error("Failed to run j2ms2")
                return False 

            process.update_ms_expname(exp)
        else:
            logger.debug("MS files already exist. Skipping creation.")

        if not process.get_metadata_from_ms(exp):
            return False

        process.compute_lag_snr(exp)
        return True
    except Exception as e:
        logger.error(f"Unexpected error creating MS files: {e}")
        traceback.print_exc()
        return False


def create_standardplots(exp: experiment.Experiment, do_weights: bool = True) -> bool:
    """Creates standardplots from MS files.

    Validates that the reference antenna and fringe-finder sources are set
    before attempting to create plots.

    Args:
        exp (experiment.Experiment): Experiment object.
        do_weights (bool): Whether to include weight plots. Default True.

    Returns:
        bool: True if standardplots were created successfully.
    """
    ps_files = glob.glob("*.ps")
    if ps_files:
        pipelinable_ms = [p.msfile for p in exp.correlator_passes if p.pipeline and p.msfile.exists()]
        if pipelinable_ms:
            oldest_plot = min(Path(f).stat().st_mtime for f in ps_files)
            newest_ms = max((m / 'table.dat').stat().st_mtime if (m / 'table.dat').exists()
                            else m.stat().st_mtime for m in pipelinable_ms)
            if oldest_plot > newest_ms:
                logger.info("Standardplots are up-to-date (MS not modified since last plot generation). Skipping.")
                return True
            logger.info("MS modified since last plot generation. Regenerating standardplots.")
        else:
            logger.debug("Standardplots already run. Skipping.")
            return True

    if not exp.refant:
        logger.error("No reference antenna set for standardplots.")
        return False

    pipelinable = [p for p in exp.correlator_passes if p.pipeline]
    if not pipelinable:
        logger.error("No pipelinable correlator passes found.")
        return False

    has_fringefinder = any((p.sources and p.sources.fringefinder) for p in pipelinable) or bool(exp.sources.fringefinder)
    if not has_fringefinder:
        logger.error("No fringe-finder sources found in any correlator pass or experiment.")
        return False

    # Show scan overview before opening standardplots
    # gui = dialog.Terminal()
    # if not gui.show_scan_overview(exp):
    #     logger.info("User cancelled after scan overview.")
    #     return False
    return process.standardplots(exp, do_weights=do_weights)


def msops(exp: experiment.Experiment) -> bool:
    """Applies MS operations including weight flagging, polswap, and 1-bit scaling.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if all MS operations completed successfully.
    """
    if all(len(glob.glob(f"{p.fitsidifile}*")) > 0 for p in exp.correlator_passes):
        logger.debug("FITS IDI files already exist. Skipping creation.")
        return True

    # In batch mode the standardplot dashboard would block forever waiting for
    # the operator to close it, so we skip it. The plots are still on disk and
    # can be reviewed asynchronously via `postprocess info --serve`.
    if not _BATCH_MODE:
        process.open_standardplot_files(exp)

    # --- Comms: send dashboard notification and optionally get interactive feedback ---
    msops_feedback: dict | None = None
    if _NOTIFIER is not None:
        msops_feedback = _comms.notify_dashboard_review(exp, _NOTIFIER)

    if msops_feedback is not None:
        # Interactive Mattermost feedback received — apply directly, skip dialog
        _comms.apply_msops_feedback(exp, msops_feedback)
    else:
        gui = dialog.make_dialog(batch=_BATCH_MODE)
        try:
            if not gui.askMSoperations(exp):
                return False
        except dialog.BatchInteractionError as exc:
            logger.error(f"Cannot run msops in batch mode: {exc}")
            _write_review_flag(exp, "msops", str(exc))
            return False

    exp.store()
    # Use boolean `and` so we short-circuit and don't keep running heavy MS
    # operations after one of them has already reported failure. The previous
    # code used bitwise `&` which always evaluates every operand.
    for op in (process.flag_weights, process.ysfocus, process.polswap, process.onebit,
               lambda e: process.print_exp(e, False), process.tconvert):
        if not op(exp):
            return False
    return True


def polconvert(exp: experiment.Experiment) -> bool:
    """Runs PolConvert automatically with iterative parameter tuning.

    Calls process.polconvert() which finds all fringe-finder scans and iterates over
    different scans, parameter combinations (solve_weight, time_avg, time_range),
    checks quality, and applies the best solution found.
    Then renames output files via post_polconvert/post_post_polconvert.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if PolConvert completed successfully, False on failure.
    """
    if not exp.antennas.polconvert:
        logger.info("No antennas require PolConvert. Skipping.")
        return True

    if not process.polconvert(exp):
        logger.error("PolConvert could not reach a good solution. Try running it manually.")
        return False

    if not process.post_polconvert(exp):
        return False

    return process.post_post_polconvert(exp)


def msops_post(exp: experiment.Experiment) -> bool:
    """Applies MS operations including weight flagging, polswap, and 1-bit scaling.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if all MS operations completed successfully.
    """
    return create_standardplots(exp, do_weights=False)


def antfiles(exp: experiment.Experiment) -> bool:
    """Retrieves antenna files from vlbeer for pipeline processing.

    Args:
        expobj_file: Path to experiment JSON file
    """
    if len(list(exp.dirs.pipe_in.glob("*.antab"))) > 0:
        logger.info("Antenna ANTAB files already exist. Skipping.")
        return True

    if len(list(exp.dirs.pipe_temp.glob("*.antab"))) > 0:
        for afile in exp.dirs.pipe_temp.glob("*.antab"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name)

        logger.info("Antenna ANTAB files already created. Copied to the pipeline input directory.")
        return True

    if (exp.eEVNname is None) or (exp.expname == exp.eEVNname):
        pipeline.get_files_from_vlbeer(exp, experiment.retrieve_servers()['vlbeer'])
        if any(s.lower() in ('br', 'kp', 'la', 'yy', 'mk') for s in exp.antennas.names):
            pipeline.get_vlba_antab(exp)

        if not pipeline.create_uvflg(exp):
            logger.error("uvflg creation needs manual intervention.")
            rprint("[bold red]STOPPED PROCESS:[/bold red] [red]uvflg creation needs manual intervention.[/red]")
            return False

        if not pipeline.run_antab_editor(exp):  # TODO: use the correct codes if eEVN or line
            rprint("[bold yellow]STOPPED PROCESS:[/bold yellow] [yellow]antab_editor needs manual intervention.[/yellow]")
            return False

        for afile in exp.dirs.pipe_temp.glob("*.antab"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name)

        for afile in exp.dirs.pipe_temp.glob("*.uvflg"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name)
    else:
        eEVNpath = Path(str(experiment.retrieve_servers()['eee'].path).format(expname=exp.eEVNname)) \
                    / "pipeline" / "in"
        # Path.glob() returns a generator, which is always truthy. Materialise to a
        # list so the emptiness check actually works.
        antabfiles = list(eEVNpath.glob("*.antab"))
        if not antabfiles:
            logger.error(f"Create the antab/uvflg files from {exp.eEVNname} before continue here.")
            return False

        for afile in antabfiles:
            shutil.copy(afile, exp.dirs.pipe_in / afile.name.replace(exp.eEVNname.lower(), exp.expname.lower()))

        for afile in eEVNpath.glob("*.uvflg"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name.replace(exp.eEVNname.lower(), exp.expname.lower()))

    return True


def run_pipeline(exp: experiment.Experiment) -> bool:
    """Prepares input files for EVN Pipeline.

    Args:
        expobj_file: Path to experiment JSON file
    """
    if not pipeline.create_input_file(exp):
        return False
    return pipeline.run_pipeline(exp)


def pipeline_diagnostics(exp: experiment.Experiment) -> bool:
    """Creates diagnostic files after pipeline completion and updates the PI letter.

    Runs comment_tasav, feedback, and then auto-fills the PI letter with:
    - "Could not observe" for non-participating antennas
    - PolConvert remarks (if applicable)
    - Bandwidth limitation notes
    - Opacity correction notes

    Args:
        exp: Experiment object.
    """
    if not pipeline.comment_tasav_files(exp):
        return False
    if not pipeline.pipeline_feedback(exp):
        return False
    return process.update_piletter(exp)


def pre_archive(exp: experiment.Experiment) -> bool:
    """Appends Tsys/GC information to FITS-IDI files.

    Args:
        expobj_file: Path to experiment JSON file
    """
    return process.append_antab(exp)


def archive(exp: experiment.Experiment) -> bool:
    """Archives experiment files to the EVN archive.

    Args:
        expobj_file: Path to experiment JSON file
    """
    for op in (process.set_credentials,
               process.protect_experiment_files,
               lambda e: process.print_exp(e, display_in_terminal=False),
               process.archive,
               pipeline.archive,
               process.send_letters,
               process.antenna_feedback,
               process.nme_report):
        if not op(exp):
            return False
    return True


@dataclass
class ExecCommand:
    """A single executable command exposed via 'postprocess exec'."""
    func: Callable
    doc: str


def _build_exec_commands() -> dict[str, ExecCommand]:
    """Build the exec command registry. Called once at import time.

    Returns:
        dict mapping command name to ExecCommand.
    """
    return {
        # -- directory / setup --
        'makelis':       ExecCommand(lisfiles.create_lis_files, "Create the .lis files in ccs."),
        'getlis':        ExecCommand(lisfiles.get_lis_files, "Copy the .lis files from ccs to eee."),
        'modlis':        ExecCommand(lisfiles.get_passes_from_lisfiles,
                                     "Read correlator passes from the .lis files and update the header."),
        'checklis':      ExecCommand(lisfiles.check_lisfiles, "Run checklis on all .lis files."),
        # -- MS creation --
        'getdata':       ExecCommand(process.getdata, "Run getdata.pl."),
        'j2ms2':         ExecCommand(process.j2ms2, "Run j2ms2 with the current params."),
        'expname':       ExecCommand(process.update_ms_expname,
                                     "Run expname.py (for e-EVN experiments)."),
        'metadata':      ExecCommand(process.get_metadata_from_ms,
                                     "Retrieve observational metadata from the MS."),
        'lagsnr':        ExecCommand(process.compute_lag_snr,
                                     "Compute lag-space SNR per scan/antenna/polarization."),
        # -- plots --
        'standardplots': ExecCommand(lambda exp: process.standardplots(exp, do_weights=True),
                                     "Run standardplots."),
        'gv':            ExecCommand(process.open_standardplot_files,
                                     "Open the standardplot files with gv."),
        # -- MS operations --
        'ysfocus':       ExecCommand(process.ysfocus, "Run ysfocus.py."),
        'polswap':       ExecCommand(process.polswap, "Run polswap.py."),
        'flag_weights':  ExecCommand(process.flag_weights, "Run flag_weights.py."),
        'onebit':        ExecCommand(process.onebit, "Run onebit.py."),
        'tconvert':      ExecCommand(process.tconvert, "Run tConvert."),
        'polconvert':    ExecCommand(process.polconvert,
                                     "Run PolConvert (or prepare files to run it manually)."),
        'postpolconvert': ExecCommand(process.post_post_polconvert,
                                      "Run all required steps after PolConvert."),
        # -- credentials / archiving --
        'auth':          ExecCommand(process.set_credentials,
                                     "Set / recover the credentials (auth file) for this experiment."),
        'protect':       ExecCommand(process.protect_experiment_files,
                                     "Protect experiment files."),
        'archive-fits':  ExecCommand(process.archive,
                                     "Archive standard plots and FITS-IDI files."),
        'archive-pilet': ExecCommand(process.send_letters, "Archive the PI letter."),
        'append':        ExecCommand(process.append_antab,
                                     "Append the Tsys and GC to the FITS-IDI files."),
        'issues':        ExecCommand(process.antenna_feedback,
                                     "Report observed problems (station feedback / Grafana / RedMine)."),
        'nme':           ExecCommand(process.nme_report,
                                     "Check if an NME Report is needed."),
        # -- pipeline --
        'antab':         ExecCommand(pipeline.run_antab_editor,
                                     "Prepare .antab and run antab_editor.py."),
        'uvflg':         ExecCommand(pipeline.create_uvflg, "Create .uvflg from all log files."),
        'vlbeer':        ExecCommand(lambda exp: pipeline.get_files_from_vlbeer(
                                         exp, experiment.retrieve_servers()['vlbeer']),
                                     "Retrieve the antabfs, log, and flag files from vlbeer."),
        'pyinput':       ExecCommand(pipeline.create_input_file,
                                     "Create the input file for the EVN pipeline."),
        'pipe':          ExecCommand(pipeline.run_pipeline, "Run the EVN Pipeline."),
        'comment_tasav': ExecCommand(pipeline.comment_tasav_files,
                                     "Create the .comment and .tasav files."),
        'feedback':      ExecCommand(pipeline.pipeline_feedback,
                                     "Run the Pipeline Feedback script."),
        'piletter':      ExecCommand(process.update_piletter,
                                     "Auto-fill the PI letter (non-observing antennas, PolConvert, etc.)."),
    }


_EXEC_COMMANDS: dict[str, ExecCommand] = _build_exec_commands()


def list_tasks(expname: str, print_docs: bool = False):
    """Lists all workflow steps and their status.

    Args:
        expname: Experiment name.
        print_docs: Also print the documentation for each step.
    """
    rprint(f"\n\n[bold]Post-processing of {expname}:[/bold]")
    exp = experiment.Experiment.load(expname)
    if exp:
        steps = [Task.from_dict(s) for s in exp.steps]
    else:
        steps = _WORKFLOW_STEPS

    for s in steps:
        rprint(f"{'🟢' if s.done else '🔴'}"
               f" [bold {'green' if s.done else 'red'}]{s.name}[/bold {'green' if s.done else 'red'}]\n" +
               (f"   [dim]{s.doc}[/dim]" if print_docs else ""))


def build_exec_help() -> str:
    """Build a rich-formatted help string for the exec subparser description.

    Returns:
        str with the full exec help text.
    """
    lines = ["[bold]Runs a single command from the experiment post-processing.[/bold]\n",
             "The following commands are available:\n"]
    for name, cmd in _EXEC_COMMANDS.items():
        lines.append(f"  - [bold green]{name}[/bold green] : {cmd.doc}")
    return '\n'.join(lines)


def list_exec_commands():
    """Print all available exec commands with their descriptions."""
    rprint("\n[bold]Available exec commands:[/bold]\n")
    for name, cmd in _EXEC_COMMANDS.items():
        rprint(f"  [bold green]{name:<18}[/bold green] {cmd.doc}")


def run_isolated_task(task_name: str, expname: str | None = None):
    """Run a single exec command independently.

    The experiment must have been initialized previously so that the stored
    JSON file exists and can be loaded.

    Args:
        task_name: Name of the exec command to run.
        expname: Experiment name (case-insensitive).
    """
    try:
        exp = experiment.Experiment.load(expname)
    except FileNotFoundError:
        rprint(f"[bold red]Could not find the stored information for "
               f"{expname if expname is not None else Path().name}"
               "[/bold red].\n[red]Maybe the experiment was never initialized?[/red]")
        sys.exit(1)
    except (json.JSONDecodeError, KeyError) as e:
        rprint(f"[bold red]Error loading experiment data: {e}[/bold red]")
        rprint("[red]The experiment file may be corrupted. "
               "Consider reinitializing the experiment.[/red]")
        sys.exit(1)

    if task_name not in _EXEC_COMMANDS:
        rprint(f"[red]Command '{task_name}' not found.[/red]")
        list_exec_commands()
        sys.exit(1)

    cmd = _EXEC_COMMANDS[task_name]
    try:
        result = cmd.func(exp)
        logger.info(f"Running {task_name} -> {'OK' if result else 'FAILED'}")
        exp.store()
        return result
    except Exception as e:
        logger.error(f"Error running command '{task_name}': {e}")
        traceback.print_exc()
        sys.exit(1)


def validate_steps(from_step: str, to_step: str | None = None) -> tuple[bool, str]:
    """Validates that the given step names exist and are in the correct order.

    Args:
        from_step: Name of the starting step.
        to_step: Name of the ending step (optional).

    Returns:
        Tuple of (is_valid, error_message). If valid, error_message is empty.
    """
    if not _WORKFLOW_STEPS:
        return False, "No workflow steps defined"
    
    step_names = [s.name for s in _WORKFLOW_STEPS]

    if not from_step:
        return False, "Starting step cannot be empty"
    
    if from_step not in step_names:
        return False, f"Step '{from_step}' not found. Available steps: {', '.join(step_names)}"

    if to_step is not None:
        if to_step not in step_names:
            return False, f"Step '{to_step}' not found. Available steps: {', '.join(step_names)}"

        from_idx = step_names.index(from_step)
        to_idx = step_names.index(to_step)
        if from_idx > to_idx:
            return False, f"Step '{from_step}' comes after '{to_step}'. Order should be reversed."

    return True, ""


def _rich_terminal_sink(message):
    """Loguru sink that renders Rich markup via rich.Console.print().

    Errors (level >= 40) go to stderr with a level prefix; everything else
    goes to stdout as-is so that Rich tags like ``[bold]`` render correctly.
    """
    text = message.record["message"]
    if message.record["level"].no >= 40:
        _stderr_console.print(f"{message.record['level'].name}: {text}")
    else:
        _stdout_console.print(text)


def _file_format(record):
    """Loguru format function that strips Rich markup for clean log files.

    Stores the stripped message in ``record["extra"]["clean_msg"]`` and returns
    the format string used by loguru.
    """
    record["extra"]["clean_msg"] = _RICH_TAG_RE.sub('', record["message"])
    return "{level: <8} | {extra[clean_msg]}\n{exception}"


def _file_format_debug(record):
    """Same as ``_file_format`` but with timestamps and code location for debug builds."""
    record["extra"]["clean_msg"] = _RICH_TAG_RE.sub('', record["message"])
    return "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {module}:{function}:{line} | {extra[clean_msg]}\n{exception}"


def _setup_loguru(exp: experiment.Experiment, debug: bool = False):
    """Configure loguru sinks: Rich-aware terminal and markup-stripped file.

    Each sink is added in its own try/except so that failure to attach the file
    sink (e.g. the experiment dir lives on a read-only mount) does not also
    prevent the terminal sink from being registered. The previous one-try
    version silently disabled all logging on file-permission errors.

    Args:
        exp: Experiment object (provides dirs.logs).
        debug: If True, log DEBUG level with full context; otherwise INFO only.
    """
    level = "DEBUG" if debug else "INFO"
    logger.remove()  # Remove default stderr handler to avoid duplicate messages.

    log_path = exp.dirs.logs / "post_processing.log" if exp.dirs and exp.dirs.logs \
        else Path("post_processing.log")
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(log_path, colorize=False, level=level, backtrace=True, diagnose=True,
                   format=_file_format_debug if debug else _file_format)
    except (OSError, PermissionError) as e:
        rprint(f"[yellow]Warning: Could not create log file at {log_path}: {e}[/yellow]")

    try:
        logger.add(_rich_terminal_sink, level=level, colorize=False, backtrace=True, diagnose=True)
    except Exception as e:  # noqa: BLE001 - terminal sink failures must not crash the pipeline
        rprint(f"[yellow]Warning: Could not add terminal log sink: {e}[/yellow]")


def run_workflow(exp: experiment.Experiment, archive: bool = True, debug: bool = False,
                 from_step: str | None = None, to_step: str | None = None):
    """Run the workflow for the given experiment.

    When from_step is None the workflow resumes: steps already marked done are skipped and
    execution begins at the first pending step.  When from_step is given the workflow re-runs
    from that step, resetting the done flag for it and all subsequent steps.

    Args:
        exp: The experiment object.
        archive: Whether to include the archive step.
        debug: Whether to enable debug logging.
        from_step: Re-run from this step name (optional).
        to_step: Stop after this step name (optional, only used with from_step).

    Returns:
        bool: True if workflow completed successfully (or paused at postpipe), False otherwise.
    """
    log_file = (exp.dirs.logs / "post_processing.log") if exp.dirs and exp.dirs.logs \
        else Path("post_processing.log")
    if not log_file.exists():
        log_file.parent.mkdir(parents=True, exist_ok=True)
        exp.write_log_file(log_file)

    _setup_loguru(exp, debug)
    # Clear any stale review marker from a previous run; the workflow may write
    # a fresh one later if it pauses again.
    _clear_review_flag(exp)
    if not archive:
        logger.debug("The data will not be stored in the EVN archive.")

    all_steps = [s for s in _WORKFLOW_STEPS if (archive or s.name != 'archive') and s.name != 'initialize']
    step_names = [s.name for s in all_steps]

    # Deserialize stored steps from JSON dicts into Task objects if needed
    stored_steps = [Task.from_dict(s) if isinstance(s, dict) else s for s in (exp.steps or [])]

    if from_step is not None:
        # Explicit restart: preserve done state for steps before from_step, reset from it onwards
        stored_done = {s.name: s.done for s in stored_steps}
        from_idx = step_names.index(from_step)
        for i, s in enumerate(all_steps):
            s.done = stored_done.get(s.name, False) if i < from_idx else False
        exp.steps = all_steps
        exp.store()

        to_idx = step_names.index(to_step) + 1 if to_step is not None else len(all_steps)
        steps_to_run = all_steps[from_idx:to_idx]
    else:
        # Resume: restore stored done state and only queue steps that are not yet done
        if stored_steps:
            stored_done = {s.name: s.done for s in stored_steps}
            for s in all_steps:
                s.done = stored_done.get(s.name, False)
        exp.steps = all_steps
        exp.store()
        steps_to_run = [s for s in all_steps if not s.done]

    if not steps_to_run:
        rprint("[yellow]No pending steps — the post-processing is already complete.[/yellow]")
        return True

    logger.debug(f"Running steps: {', '.join(s.name for s in steps_to_run)}")
    rprint(f"[green]Running steps: {', '.join(s.name for s in steps_to_run)}[/green]")

    for step in steps_to_run:
        rprint(f"[bold]-- {step.name}[/bold]")
        try:
            if step.command not in globals():
                logger.error(f"Command '{step.command}' not found for step '{step.name}'")
                utils.notify(f"{exp.expname} post-processing", f"Step {step.name} failed (command not found)")
                return False

            if not globals()[step.command](exp):
                logger.error(f"Step {step.name} failed.")
                utils.notify(f"{exp.expname} post-processing", f"Step {step.name} failed")
                return False

            step.done = True
            exp.store()
            logger.info(f"Step {step.name} completed successfully")
        except Exception as e:
            logger.error(f"Unexpected error in step {step.name}: {e}")
            traceback.print_exc()
            utils.notify(f"{exp.expname} post-processing", f"Crashed at step {step.name}: {e}")
            return False

        # After every step in the policy's pause_after list, pause for human review.
        # Default behaviour preserves the historical pause after postpipe.
        pause_after = (exp.policy.pause_after if getattr(exp, 'policy', None) is not None
                       else ['postpipe'])
        if step.name in pause_after:
            remaining = steps_to_run[steps_to_run.index(step) + 1:]
            if remaining:
                _signal_pause(exp, step.name)
                return True

    logger.info(f"The processing of {exp.expname} seems to have finalized properly.")
    utils.notify(f"{exp.expname} post-processing", "Completed successfully")
    return True