"""Workflow functions for post-processing EVN experiments.

Step functions of the post-processing workflow, executed in order by
:func:`run_workflow` (see ``_WORKFLOW_STEPS``). They wrap the functionality
from the inputs, process, pipeline, and lisfiles modules.
"""
import re
import sys
import json
import glob
import shutil
import traceback
from pathlib import Path
from typing import Callable
from dataclasses import dataclass
from loguru import logger
from rich import print as rprint
from rich.panel import Panel
from rich.console import Console
from . import experiment
from . import experiment_state
from . import inputs
from . import distribution
from . import eevn
from . import pipelines
from . import process
from . import retrieval
from . import review
from . import pipeline
from . import lisfiles
from . import dialog
from . import utils
from . import comms as _comms
from . import mode as _mode
from . import reporting
from .mode import Mode


def _jive_backend():
    """Lazily imports the JIVE retrieval backend module (holds the ccs/vlbeer transport).

    Kept lazy so a non-jive run never imports the JIVE server machinery; used by the
    granular `postprocess exec makelis/getlis` power-user commands.
    """
    from .retrieval import jive
    return jive


def _backends(exp: experiment.Experiment) -> _mode.Backends:
    """Returns the retrieval/pipeline/distribution backend names for *exp*'s mode.

    The mode is normally resolved and persisted by the CLI at initialization; if it is
    somehow absent (a step reached directly on a pre-Phase-2 checkpoint), it is
    re-detected from the OS so selection never fails for lack of a stored value.
    """
    m = exp.mode if getattr(exp, 'mode', None) is not None else _mode.detect()
    return _mode.backends_for(m)

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


def _notify_step_failure(exp: experiment.Experiment, step: str, reason: str) -> None:
    """Announces a hard step failure: terminal + configured comms, resumable state kept.

    A failure is deliberately distinct from the clean review-pause (which writes a marker
    and exits 0): the step is NOT marked done, so re-running `postprocess run` resumes from
    it, and the caller returns False so the process exits non-zero (PRD stories 35-36).
    """
    logger.error(f"Step {step} failed: {reason}.")
    reporting.announce(f"Step '{step}' failed: {reason}. "
                       f"Fix the cause and re-run `postprocess run` to resume from here.",
                       style='bold red')
    utils.notify(f"{exp.expname} post-processing", f"FAILED at step {step}: {reason}")
    if _NOTIFIER is not None:
        _comms.notify_operator(exp, f"post-processing FAILED at step {step}",
                               f"{reason}. The run stopped; re-run `postprocess run` to resume "
                               f"from '{step}' once fixed.", _NOTIFIER)


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


_WORKFLOW_STEPS = [Task('init', 'initialize_experiment',
                        "Creates the directory structure to post-process the experiment. "
                        "Locates (or retrieves) the .vex file and derives all metadata from it: "
                        "observing date, e-EVN run (from exper_description), stations, sources, and "
                        "scans. Applies the experiment toml (source types, PI, support scientist) "
                        "when present. Verifies if the post-processing has already run before and "
                        "recovers it."),
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
                   Task('tconvert', 'tconvert', "Creates the FITS-IDI files from the MS by running "
                        "tConvert on every correlator pass."),
                   Task('polconvert', 'polconvert', "Runs PolConvert on the FITS-IDI files for the "
                        "antennas that observed with linear polarization (only if any of them need it)."),
                   Task('post_polconvert', 'post_polconvert', "Post-processes PolConvert: renames the "
                        "new *.PCONVERT files to the standard FITS-IDI names (backing up the originals) "
                        "and re-runs the standard plots on the converted data."),
                   Task('standardplots2', 'msops_post', "Re-runs the standard plots after all msops "
                        "have been performed"),
                   Task('antab', 'antfiles', "Retrieves the .antabfs and .log files from vlbeer. Creates "
                        "the .antab (requires graphical interaction via antab_editor), and the .uvflg file."),
                   Task('pipeinputs', 'create_pipeline_inputs', "Prepares a draft input file for the EVN "
                        "Pipeline and gathers the required antab/uvflg files into pipeline/in."),
                   Task('pipeline', 'run_pipeline', "Runs the EVN Pipeline for all correlated passes."),
                   Task('postpipe', 'pipeline_diagnostics', 'Runs diagnostics on the pipeline outputs.'),
                   Task('prearchive', 'pre_archive', "Prepares the experiment for archiving. Attaches the Tsys "
                        "information to the FITS-IDI files."),
                   Task('distribute', 'archive', "Delivers the experiment through the mode's "
                        "distribution backend (supsci: credentials, PI letter, archive; regular: "
                        "verify the FITS-IDI files are in order). Deprecated alias: 'archive'.")]

# Deprecated step-name aliases: {old: current}. 'archive' -> 'distribute' (renamed in Phase 2).
_STEP_ALIASES = {'archive': 'distribute'}


def _resolve_step_alias(name: str | None) -> str | None:
    """Maps a deprecated step name to its current name (warns once), passing others through."""
    if name in _STEP_ALIASES:
        current = _STEP_ALIASES[name]
        logger.warning(f"Step name '{name}' is deprecated; use '{current}'.")
        return current
    return name


def create_folder_structure() -> experiment.Dirs:
    """Creates the folder structure required for post-processing.

    Thin alias of :func:`inputs.create_folder_structure` (the canonical
    implementation), kept for existing callers.
    """
    return inputs.create_folder_structure()



def initialize_experiment(expname: str, supsci: str, mode: Mode) -> experiment.Experiment:
    """Initializes an experiment object with all the metadata derived from the .vex file.

    Vex-only bootstrap (no MASTER_PROJECTS.LIS, .jexp, or .expsum): the observing date
    comes from the vex $EXPER block, the e-EVN membership from exper_description, and
    stations/sources/scans from the vex sections. Source types, PI contacts, and the
    support scientist may be complemented by the experiment toml ({expname}.toml).

    The *mode* (resolved and persisted by the CLI) picks the retrieval backend: ``supsci``
    fetches the vex/lis from the correlator server, ``regular`` expects them already local,
    ``sweeps`` is not implemented yet.

    Args:
        expname (str): Experiment name.
        supsci (str): Support scientist name assigned to this experiment.
        mode (Mode): The resolved operating mode.

    Returns:
        experiment.Experiment: The initialized experiment.
    """
    logger.debug(f"Initializing experiment {expname} in mode '{mode.value}'")
    dirs = create_folder_structure()

    retrieval_backend = _mode.backends_for(mode).retrieval
    try:
        retriever = retrieval.get_retriever(retrieval_backend)
        inputset = retriever.fetch(Path('.'), expname)
    except retrieval.RetrievalError as e:
        logger.error(f"Retrieval ({retrieval_backend}) failed for {expname}: {e}")
        rprint(f"[red]{e}[/red]")
        sys.exit(1)

    # In sweeps mode the config must be fully prepared: no heuristic classification runs,
    # and any observed source still untyped is a hard, field-named error.
    classify = mode != Mode.sweeps
    try:
        exp = inputs.load_experiment(inputset.vexfile, lisfiles=inputset.lisfiles or None,
                                     supsci=supsci, dirs=dirs, expname=expname, classify=classify)
        if mode == Mode.sweeps:
            inputs.ensure_sources_typed(exp)
    except inputs.InputsError as e:
        logger.error(f"Could not initialize {expname} from {inputset.vexfile}: {e}")
        rprint(f"[red]{e}[/red]")
        sys.exit(1)
    exp.mode = mode

    # The best-effort .key/.sum schedule fetch is a supsci-only, server-touching nicety;
    # it lives in the JIVE retrieval backend, not here, so the engine stays server-agnostic.
    retriever.fetch_schedule_files(exp)

    exp.store()
    return exp


def retrieve_lisfiles(exp: experiment.Experiment) -> bool:
    """Retrieves and processes .lis files from ccs.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if lis files were retrieved and processed successfully.
    """
    try:
        if len(lisfiles._pass_lisfiles(f"{exp.expname.lower()}*.lis")) == 0:
            # No local .lis files: the mode's retrieval backend obtains them
            # (jive: create on ccs + copy; none: explicit error, nothing creates them).
            backend = _backends(exp).retrieval
            try:
                retrieval.get_retriever(backend).fetch_lisfiles(exp)
            except retrieval.RetrievalError as e:
                logger.error(f"Could not obtain the .lis files ({backend}): {e}")
                rprint(f"[red]{e}[/red]")
                return False
        else:
            logger.debug(".lis files already exist. Skipping retrieval.")
            if exp.correlator_passes:
                return True  # passes already set up (at initialize or a previous run)

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
        # Skip if metadata already loaded AND MS files exist on disk
        if exp.correlator_passes and exp.correlator_passes[0].freqsetup is not None \
                and all(p.msfile.exists() for p in exp.correlator_passes):
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

        if not exp.no_lag:
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
    if len(glob.glob("*.ps")) > 0:
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
    # If the FITS-IDI files already exist, the 'tconvert' step (which runs right after msops)
    # has already produced them, which means these in-place MS operations were applied too;
    # skip to avoid re-applying them. tConvert itself is now the separate 'tconvert' step.
    if all(len(glob.glob(f"{p.fitsidifile}*")) > 0 for p in exp.correlator_passes):
        logger.debug("FITS IDI files already exist. MS operations were already applied. Skipping.")
        return True

    if _toml_msops_available(exp):
        # The experiment toml defines every MS-ops decision (a completed [postprocess]
        # section from a previous run, or hand-written): apply silently, no dialog, no
        # dashboard notification. This is the "silent re-run" contract of the PRD.
        _apply_toml_msops(exp)
    elif _auto_msops_available(exp):
        # The lag-MS polarization diagnostics gave a confident answer, so apply the MS
        # operations automatically (no dashboard/dialog needed). See _apply_auto_msops.
        _apply_auto_msops(exp)
    else:
        # Warn about unexpectedly low weights *before* opening the dashboard / sending the
        # review notification, so the operator knows to check the weight plots while reviewing.
        low_weight_antennas = exp.antennas.low_weights
        if low_weight_antennas:
            logger.warning(f"[yellow]Antennas with unexpectedly low weights: "
                           f"{', '.join(low_weight_antennas)}. Check the weight plots in the dashboard.[/yellow]")

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
    ok = process.flag_weights(exp) & process.ysfocus(exp) & process.polswap(exp) & process.onebit(exp) \
        & process.print_exp(exp, False)
    if ok:
        # Whatever path decided the parameters (toml, auto, dialog, Mattermost reply),
        # persist them into the experiment toml so the next run is silent (PRD story 22).
        _record_msops_in_toml(exp)
    return ok


def tconvert(exp: experiment.Experiment) -> bool:
    """Creates the FITS-IDI files from the MS files by running tConvert on every correlator pass.

    Split out from :func:`msops` into its own workflow step so it can be run (and re-run)
    on its own via ``postprocess run tconvert``. This is convenient because tConvert
    currently runs on eee as a temporary workaround (see :func:`process.tconvert`) and may
    need re-triggering independently of the MS operations. The step is idempotent: passes
    whose FITS-IDI files already exist are skipped.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if all correlator passes were converted successfully.
    """
    return process.tconvert(exp)


def _ask_review_confirmation() -> str | None:
    """Asks the operator to approve the review, re-run from a step, or quit.

    Returns:
        None to approve (continue with the final steps), a validated step name to
        re-run from it, or 'quit' to stop the run here.
    """
    step_names = [s.name for s in _WORKFLOW_STEPS]
    while True:
        try:
            answer = input("Review answer [Enter=finalize / STEP=re-run from STEP / quit]: ").strip()
        except EOFError:  # no interactive stdin after all: behave like quit
            logger.warning("No interactive stdin available for the review confirmation; "
                           "stopping here (resume with `postprocess run`).")
            return 'quit'
        if answer == '':
            return None
        if answer.lower() in ('q', 'quit', 'exit'):
            return 'quit'
        if answer in step_names:
            return answer
        rprint(f"[yellow]'{answer}' is not a step name. Available steps: "
               f"{', '.join(step_names)}[/yellow]")


def _pipeline_backend(exp: experiment.Experiment):
    """Returns the pipeline backend for the experiment's mode (currently always 'aips').

    An unknown/unimplemented backend raises pipelines.PipelineError at selection time
    (main already validates at startup; this re-check covers `postprocess exec` and a
    checkpoint reached with no stored mode). Callers turn it into a clean step failure.
    """
    return pipelines.get_pipeline(_backends(exp).pipeline)


def _run_pipeline_stage(exp: experiment.Experiment, stage: str) -> bool:
    """Runs one PipelineBackend stage ('prepare'|'run'|'collect') with clean errors.

    Returns False (with an explicit log, no traceback) on PipelineError, so a wrong
    backend selection or a backend failure reads as a step failure, not a crash.
    """
    try:
        backend = _pipeline_backend(exp)
        return getattr(backend, stage)(exp)
    except pipelines.PipelineError as e:
        logger.error(f"Pipeline backend error at '{stage}': {e}")
        rprint(f"[red]{e}[/red]")
        return False


class StepPaused(Exception):
    """Raised by a step to signal a clean wait state (NOT a failure).

    Used by the e-EVN synchronisation barriers: the step cannot proceed yet, a
    REVIEW_REQUIRED marker explains what it waits for, and the run must stop with a
    "paused" (not "failed") log and notification, exiting cleanly for the scheduler.
    """


def _exp_toml(exp: experiment.Experiment) -> experiment_state.ExperimentToml:
    """Returns the experiment toml attached to *exp* (thin alias of attached_toml).

    Steps reached via `postprocess exec` load the experiment straight from the JSON
    checkpoint, where the toml is not attached; the lazy load keeps them consistent.
    """
    return experiment_state.attached_toml(exp)


def _fresh_exp_toml(exp: experiment.Experiment) -> experiment_state.ExperimentToml:
    """Reloads the experiment toml from disk (thin alias of attached_toml(fresh=True)).

    MUST be used before every write from the workflow process: see
    experiment_state.attached_toml for the lost-update rationale.
    """
    return experiment_state.attached_toml(exp, fresh=True)


def _toml_msops_available(exp: experiment.Experiment) -> bool:
    """Whether the experiment toml defines every MS-operations decision.

    Requires [postprocess] to explicitly define the weight threshold and the three
    antenna lists (an explicit empty list means "no antenna needs it" and counts as
    defined; an absent key does not). refant is optional: the workflow auto-picks one.
    """
    post = _exp_toml(exp).postprocess
    return (post.weight_threshold is not None and post.polswap is not None
            and post.polconvert is not None and post.onebit is not None)


def _apply_toml_msops(exp: experiment.Experiment) -> None:
    """Applies the MS-operations parameters defined in the experiment toml onto *exp*.

    Mirrors dialog.PolicyDriven.askMSoperations, sourcing the values from the toml
    [postprocess] section instead of the policy (the toml wins per the precedence rule).
    Antennas named in the toml but not part of the observation log a warning.
    """
    post = _exp_toml(exp).postprocess
    for a_pass in exp.correlator_passes:
        existing = a_pass.flagged_weights
        if existing and existing.threshold == post.weight_threshold and existing.percentage >= 0:
            continue
        a_pass.flagged_weights = experiment.FlagWeight(post.weight_threshold, -1)
    for key in ('polswap', 'polconvert', 'onebit'):
        for antenna in getattr(post, key) or []:
            if antenna in exp.antennas:
                setattr(exp.antennas[antenna], key, True)
            else:
                logger.warning(f"Antenna {antenna} ({key} in the experiment toml) is not "
                               "part of this observation; ignored.")
    if post.refant and not exp.refant:
        exp.refant = list(post.refant)
    logger.info(f"MS operations resolved from the experiment toml: weight "
                f"threshold={post.weight_threshold}, polswap={post.polswap or 'none'}, "
                f"polconvert={post.polconvert or 'none'}, onebit={post.onebit or 'none'}. "
                "No interaction needed.")


def _record_msops_in_toml(exp: experiment.Experiment) -> None:
    """Persists the applied MS-operations parameters into the experiment toml.

    Called after the MS operations completed, whatever decided the values (toml,
    lag-MS auto-diagnostics, terminal dialog, or Mattermost feedback), so a re-run
    resolves silently from the toml. Never blocks the workflow on failure.
    """
    try:
        threshold, percentage = None, None
        for a_pass in exp.correlator_passes:
            if a_pass.flagged_weights is not None:
                threshold = a_pass.flagged_weights.threshold
                if a_pass.flagged_weights.percentage >= 0:
                    percentage = a_pass.flagged_weights.percentage
                break
        exp_toml = _fresh_exp_toml(exp)  # never save a stale document (lost-update guard)
        exp_toml.record_parameters(weight_threshold=threshold, flagged_percent=percentage,
                                   polswap=list(exp.antennas.polswap),
                                   polconvert=list(exp.antennas.polconvert),
                                   onebit=list(exp.antennas.onebit),
                                   refant=list(exp.refant) if exp.refant else None)
        exp_toml.save()
        logger.debug(f"MS-operations parameters recorded in {exp_toml.path}.")
    except (experiment_state.ExperimentTomlError, OSError) as e:
        logger.warning(f"Could not record the MS-operations parameters in the experiment "
                       f"toml (continuing): {e}")


def _auto_msops_available(exp: experiment.Experiment) -> bool:
    """Whether the MS operations can be decided automatically from the lag-MS diagnostics.

    Requires that the lag-MS polarization analysis ran and classified at least one antenna
    with a detected fringe. It is disabled when the vex shows 1-bit data, because the
    affected stations cannot be inferred from the lag analysis and must be entered manually
    (otherwise process.onebit would fail).
    """
    pd = getattr(exp, 'pol_diagnostics', None) or {}
    if not pd.get('analyzed'):
        return False
    determined = any(a.get('decision') in ('normal', 'polswap', 'polconvert')
                     for a in pd.get('antennas', {}).values())
    if not determined:
        return False
    if utils.station_1bit_in_vix(exp.vixfile):
        logger.info("1-bit data present in the vex: msops needs manual review to set the "
                    "1-bit stations; skipping automatic MS operations.")
        return False
    return True


def _auto_weight_threshold(exp: experiment.Experiment) -> float:
    """Pick the weight-flag threshold automatically (matching the dialog's default of 0.9).

    Logs a warning listing antennas whose weights look unexpectedly low (>5% outside the
    first/last histogram bin, or nothing in the last bin) so the operator can double-check.
    """
    low_weight_antennas = exp.antennas.low_weights
    if low_weight_antennas:
        logger.warning(f"Antennas with unexpectedly low weights: {', '.join(low_weight_antennas)}. "
                       "Using the default flag threshold 0.9; review the weight plots if needed.")
    return 0.9


def _apply_auto_msops(exp: experiment.Experiment) -> None:
    """Apply the automatically-derived MS operations onto the experiment.

    Sets the weight-flag threshold on every correlator pass and toggles the polswap /
    polconvert antenna flags found by the lag-MS diagnostics (see process.compute_lag_snr).
    Mirrors what Terminal.askMSoperations / comms.apply_msops_feedback do, but without
    any user interaction.
    """
    pd = exp.pol_diagnostics
    threshold = _auto_weight_threshold(exp)
    for a_pass in exp.correlator_passes:
        existing = a_pass.flagged_weights
        if existing and existing.threshold == threshold and existing.percentage >= 0:
            continue
        a_pass.flagged_weights = experiment.FlagWeight(threshold, -1)

    for ant in pd.get('polswap', []):
        if ant in exp.antennas:
            exp.antennas[ant].polswap = True
    for ant in pd.get('polconvert', []):
        if ant in exp.antennas:
            exp.antennas[ant].polconvert = True

    logger.info(f"Automatic MS operations applied: weight threshold={threshold}, "
                f"polswap={pd.get('polswap') or 'none'}, polconvert={pd.get('polconvert') or 'none'}.")


def polconvert(exp: experiment.Experiment) -> bool:
    """Runs PolConvert (auto-selecting scan and reference antenna) on the FITS-IDI files.

    Calls process.polconvert(), which converts the linear-polarization antennas locally,
    iterating over the best fringe-finder scans and reference antennas until the solution
    quality passes. The post-processing (renaming the *.PCONVERT files and re-plotting the
    converted data) is handled by the separate 'post_polconvert' step.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if PolConvert produced the converted files (or no antenna needed it);
        False if no good solution could be reached (inspect polconvert_logs or run it manually).
    """
    if not exp.antennas.polconvert:
        logger.info("No antennas require PolConvert. Skipping.")
        return True

    if not process.polconvert(exp):
        logger.warning("PolConvert did not reach a good solution automatically — inspect "
                       "polconvert_logs, adjust polconvert_inputs.toml, and re-run this step.")
        return False

    return True


def post_polconvert(exp: experiment.Experiment) -> bool:
    """Post-processes the PolConvert output.

    Split out from the 'polconvert' step so it can be run (and re-run) on its own once
    PolConvert has produced the *.PCONVERT FITS-IDI files. It converts/plots the output
    (process.post_polconvert) and then backs up the original FITS-IDI files and renames the
    *.PCONVERT files to the standard names (process.post_post_polconvert). It is a no-op when
    no antenna required PolConvert.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if the post-processing completed (or was not needed), False on error.
    """
    if not exp.antennas.polconvert:
        logger.info("No antennas require PolConvert. Skipping post-PolConvert.")
        # The FITS-IDI files are final at this point: publish the completion marker
        # that the e-EVN antab barrier checks in the sibling directories.
        eevn.mark_fitsidi_ready(exp)
        return True

    if not process.post_polconvert(exp):
        return False

    ok = process.post_post_polconvert(exp)
    if ok:
        eevn.mark_fitsidi_ready(exp)
    return ok


def msops_post(exp: experiment.Experiment) -> bool:
    """Re-runs the standard plots after the MS operations (polswap, flagging, 1-bit) have run.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if the standard plots were re-created successfully.
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
        # e-EVN barrier (a): a single antab_editor session covers the whole run, so
        # the run leader must wait until every sibling produced its FITS-IDI files
        # (explicit completion markers). Pause cleanly and resume on re-invocation.
        if exp.eEVNname is not None:
            missing = eevn.fitsidi_missing(exp)
            if missing:
                reason = (f"Waiting for the FITS-IDI completion of the other e-EVN "
                          f"experiments: {', '.join(missing)} (markers in ../EXPn). "
                          f"Re-run `postprocess run` once they are processed.")
                _write_review_flag(exp, 'antab', reason)
                if _NOTIFIER is not None:
                    _comms.notify_step_pause(exp, 'antab', reason, _NOTIFIER)
                raise StepPaused(reason)

        # Station .log/.antabfs files come from the mode's retrieval backend
        # (jive: vlbeer download; none: validate they are already local).
        try:
            backend = _backends(exp).retrieval
            retrieval.get_retriever(backend).fetch_station_files(exp)
        except retrieval.RetrievalError as e:
            logger.error(f"Could not obtain the station files ({backend}): {e}")
            rprint(f"[red]{e}[/red]")
            return False
        if any(s.lower() in ('br', 'kp', 'la', 'yy', 'mk') for s in exp.antennas.names):
            pipeline.get_vlba_antab(exp)

        if not pipeline.create_uvflg(exp):
            logger.error("uvflg creation needs manual intervention.")
            rprint("[bold red]STOPPED PROCESS:[/bold red] [red]uvflg creation needs manual intervention.[/red]")
            return False

        # Show the operator what to fix (stations that did not observe, missed time
        # ranges, reduced bandwidths) right before the manual antab_editor session,
        # in the terminal and via the notifier (PRD stories 10-11).
        review.announce_antab_summary(exp, _NOTIFIER)

        if not pipeline.run_antab_editor(exp):  # TODO: use the correct codes if eEVN or line
            rprint("[bold yellow]STOPPED PROCESS:[/bold yellow] [yellow]antab_editor needs manual intervention.[/yellow]")
            return False

        for afile in exp.dirs.pipe_temp.glob("*.antab"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name)

        for afile in exp.dirs.pipe_temp.glob("*.uvflg"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name)
    else:
        # e-EVN barrier (b): EXPn (n>1) takes the final antab/uvflg files from the run
        # leader in ../EXP1 (sibling-directory convention). When they are not there
        # yet, pause cleanly and resume on the next invocation.
        if not eevn.final_antab_available(exp):
            reason = (f"Waiting for the final .antab files of the e-EVN run leader "
                      f"{exp.eEVNname} (expected in ../{exp.eEVNname.upper()}/pipeline/in/). "
                      f"Re-run `postprocess run` once they exist.")
            _write_review_flag(exp, 'antab', reason)
            if _NOTIFIER is not None:
                _comms.notify_step_pause(exp, 'antab', reason, _NOTIFIER)
            raise StepPaused(reason)

        eEVNpath = eevn.leader_antab_dir(exp)
        for afile in sorted(eEVNpath.glob("*.antab")):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name.replace(exp.eEVNname.lower(), exp.expname.lower()))

        for afile in sorted(eEVNpath.glob("*.uvflg")):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name.replace(exp.eEVNname.lower(), exp.expname.lower()))

    return True


def create_pipeline_inputs(exp: experiment.Experiment) -> bool:
    """Prepares the EVN Pipeline input file(s) and gathers the required antab/uvflg files.

    Split out from 'run_pipeline' into its own step so the draft input file can be reviewed
    (and edited) before the pipeline is actually run.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if the input file(s) were created successfully.
    """
    return _run_pipeline_stage(exp, 'prepare')


def run_pipeline(exp: experiment.Experiment) -> bool:
    """Runs the EVN Pipeline.

    The input file(s) are prepared by the separate 'pipeinputs' step.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if the pipeline ran successfully.
    """
    return _run_pipeline_stage(exp, 'run')


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
    result = _run_pipeline_stage(exp, 'collect')
    if result:
        result &= process.update_piletter(exp)

    # After feedback, re-open the web dashboard so the user can review the pipeline
    # feedback page (shown as a new "Pipeline" tab, on top of the standard plots) in
    # their browser via the SSH tunnel the server prints. In batch mode the dashboard
    # would block forever, so we skip it (the page is on disk for async review).
    if result and not _BATCH_MODE:
        process.open_pipeline_dashboard(exp)

    return result


def pre_archive(exp: experiment.Experiment) -> bool:
    """Appends Tsys/GC information to FITS-IDI files and records the final toml state.

    On success the derived experiment information (final antab/polconvert-input file
    links, flagged-data percentage) is persisted into the experiment toml, completing
    the [postprocess] record that the PI letter and the future feedback upload consume
    (PRD stories 21-22, 34).
    """
    ok = process.append_antab(exp)
    if ok:
        _record_final_in_toml(exp)
    return ok


def _record_final_in_toml(exp: experiment.Experiment) -> None:
    """Records the finalisation products into the experiment toml (never blocks).

    Covers: links to the final .antab files (pipeline/in) and the polconvert input
    files (working directory), plus the flagged-data percentage now that flag_weights
    has run. Gain corrections are added here once a structured source for them exists
    (currently only free text in the ANTAB comments).
    """
    try:
        antab_files = sorted(str(p) for p in exp.dirs.pipe_in.glob('*.antab')) \
            if exp.dirs.pipe_in.exists() else []
        polconvert_inputs = sorted(str(p) for p in Path('.').glob('polconvert*')
                                   if p.is_file() and not p.name.endswith(('.log', '.ms')))
        flagged = None
        for a_pass in exp.correlator_passes:
            if a_pass.flagged_weights is not None and a_pass.flagged_weights.percentage >= 0:
                flagged = a_pass.flagged_weights.percentage
                break
        exp_toml = _fresh_exp_toml(exp)  # never save a stale document (lost-update guard)
        exp_toml.record_parameters(antab_files=antab_files or None,
                                   polconvert_input_files=polconvert_inputs or None,
                                   flagged_percent=flagged)
        exp_toml.save()
        logger.debug(f"Finalisation products recorded in {exp_toml.path}.")
    except (experiment_state.ExperimentTomlError, OSError) as e:
        logger.warning(f"Could not record the finalisation products in the experiment "
                       f"toml (continuing): {e}")


def archive(exp: experiment.Experiment) -> bool:
    """Delivers the experiment through the mode's distribution backend.

    Backend selection follows the mode: ``supsci`` -> ``jive`` (the historical EVN-archive
    delivery), ``regular`` -> ``none`` (completes without archiving anything), ``sweeps`` ->
    ``sweeps`` (not implemented yet).
    """
    try:
        backend = _backends(exp).distribution
        return distribution.get_distributor(backend).deliver(exp)
    except distribution.DistributionError as e:
        logger.error(f"Distribution failed: {e}")
        rprint(f"[red]{e}[/red]")
        return False


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
        # makelis/getlis are supsci-only ccs operations; their ssh bodies live in the JIVE
        # retrieval backend (lazy import keeps jive machinery out of non-jive runs).
        'makelis':       ExecCommand(lambda exp: _jive_backend().create_lis_files(exp),
                                     "Create the .lis files in ccs (JIVE retrieval backend)."),
        'getlis':        ExecCommand(lambda exp: _jive_backend().get_lis_files(exp),
                                     "Copy the .lis files from ccs to eee (JIVE retrieval backend)."),
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
        'vlbeer':        ExecCommand(lambda exp: retrieval.get_retriever(
                                         _backends(exp).retrieval
                                     ).fetch_station_files(exp),
                                     "Obtain the station antabfs/log/flag files (through the "
                                     "mode's retrieval backend)."),
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


def run_isolated_task(task_name: str, expname: str | None = None,
                      tconvert_in_eee: bool = True):
    """Run a single exec command independently.

    The experiment must have been initialized previously so that the stored
    JSON file exists and can be loaded.

    Args:
        task_name: Name of the exec command to run.
        expname: Experiment name (case-insensitive).
        tconvert_in_eee: Whether the tconvert step runs on eee (workaround for the
            broken local tConvert). Forwarded to the loaded experiment.
    """
    try:
        exp = experiment.Experiment.load(expname)
        exp.tconvert_in_eee = tconvert_in_eee
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
    # Accept deprecated aliases (e.g. 'archive' -> 'distribute').
    from_step = _resolve_step_alias(from_step)
    to_step = _resolve_step_alias(to_step)

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


def _validate_outputs(exp: experiment.Experiment, all_steps: list[Task]) -> None:
    """Check that expected output files exist and are up-to-date for steps marked done.

    For each step marked done, verifies:
      1. The expected output files exist on disk.
      2. The output files are newer than the input files they depend on.

    If either check fails, resets that step and all subsequent steps so the
    workflow re-runs them.  Stale output files (older than inputs) are removed.

    Validated steps (input -> output):
      - lisfiles:       (none) -> *.lis
      - j2ms2:          *.lis  -> *.ms
      - standardplots:  *.ms   -> *.ps
      - tconvert:        *.ms   -> *IDI*
      - polconvert:      *IDI*  -> *IDI*.PCONVERT  (only if polconvert antennas exist,
                         and only until post_polconvert renames them away)
      - post_polconvert: *IDI*.PCONVERT -> idi_ori/ backup  (only if polconvert antennas exist)
      - antab:           (none) -> pipeline/in/*.antab
      - pipeinputs:      pipeline/in/*.antab -> pipeline/in/*.inp.txt
      - pipeline:        pipeline/in/* + *IDI* -> pipeline/out/*
    """
    step_names = [s.name for s in all_steps]

    def _reset_from(step_name: str, reason: str) -> None:
        """Reset step_name and all subsequent steps, log the reason."""
        idx = step_names.index(step_name)
        reset_names = []
        for s in all_steps[idx:]:
            if s.done:
                s.done = False
                reset_names.append(s.name)
        if reset_names:
            logger.info(f"{reason} — resetting steps: {', '.join(reset_names)}")

    def _newest_mtime(paths: list[Path]) -> float:
        """Return the newest modification time among existing paths (0.0 if none exist)."""
        mtimes = [p.stat().st_mtime for p in paths if p.exists()]
        return max(mtimes) if mtimes else 0.0

    def _oldest_mtime(paths: list[Path]) -> float:
        """Return the oldest modification time among existing paths (inf if none exist)."""
        mtimes = [p.stat().st_mtime for p in paths if p.exists()]
        return min(mtimes) if mtimes else float('inf')

    def _remove_stale(paths: list[Path]) -> None:
        """Remove files/directories that are stale (will be recreated)."""
        for p in paths:
            if p.exists():
                if p.is_dir():
                    shutil.rmtree(p)
                else:
                    p.unlink()

    # --- lisfiles: outputs are *.lis ---
    if 'lisfiles' in step_names and all_steps[step_names.index('lisfiles')].done:
        lis_files = lisfiles._pass_lisfiles(f"{exp.expname.lower()}*.lis")
        if not lis_files:
            _reset_from('lisfiles', "lis file(s) missing on disk")
            return  # everything downstream depends on this

    # --- j2ms2: inputs=*.lis, outputs=*.ms ---
    if 'j2ms2' in step_names and all_steps[step_names.index('j2ms2')].done:
        if not exp.correlator_passes:
            _reset_from('j2ms2', "No correlator passes defined")
            return

        ms_files = [p.msfile for p in exp.correlator_passes]
        lis_files = [p.lisfile for p in exp.correlator_passes]
        existing_ms = [f for f in ms_files if f.exists()]

        if len(existing_ms) < len(ms_files):
            _reset_from('j2ms2', "MS file(s) missing on disk")
            return

        # Timestamp check: MS must be newer than lis files
        if _oldest_mtime(ms_files) < _newest_mtime(lis_files):
            _remove_stale(ms_files)
            _reset_from('j2ms2', "MS file(s) older than lis input(s)")
            return

    # --- standardplots: inputs=*.ms, outputs=*.ps ---
    if 'standardplots' in step_names and all_steps[step_names.index('standardplots')].done:
        ps_files = list(Path('.').glob("*.ps"))
        ms_files = [p.msfile for p in exp.correlator_passes] if exp.correlator_passes else []

        if not ps_files:
            _reset_from('standardplots', "Standard plot file(s) missing on disk")
        elif ms_files and _oldest_mtime(ps_files) < _newest_mtime(ms_files):
            _remove_stale(ps_files)
            _reset_from('standardplots', "Standard plot(s) older than MS file(s)")

    # --- tconvert: inputs=*.ms, outputs=*IDI* ---
    # (msops itself modifies the MS files in place and has no distinct output file to check;
    # the FITS-IDI files are produced by the separate tconvert step.)
    if 'tconvert' in step_names and all_steps[step_names.index('tconvert')].done:
        idi_files = []
        for p in (exp.correlator_passes or []):
            idi_files.extend(Path('.').glob(f"{p.fitsidifile}*"))
        ms_files = [p.msfile for p in exp.correlator_passes] if exp.correlator_passes else []

        if not idi_files:
            _reset_from('tconvert', "FITS-IDI file(s) missing on disk")
        elif ms_files and _oldest_mtime(idi_files) < _newest_mtime(ms_files):
            _remove_stale(idi_files)
            _reset_from('tconvert', "FITS-IDI file(s) older than MS file(s)")

    # --- polconvert: inputs=*IDI*, outputs=*IDI*.PCONVERT (only if needed) ---
    # Once post_polconvert has run it renames the *.PCONVERT files to the plain IDI names
    # (backing the originals up into idi_ori/), so the *.PCONVERT files no longer exist. In
    # that case the idi_ori/ backup checked under post_polconvert is the evidence that
    # PolConvert ran, so we only check for *.PCONVERT here while post_polconvert is pending.
    post_pc_done = ('post_polconvert' in step_names
                    and all_steps[step_names.index('post_polconvert')].done)
    if 'polconvert' in step_names and all_steps[step_names.index('polconvert')].done:
        if exp.antennas.polconvert and not post_pc_done:
            pconv_files = list(Path('.').glob("*IDI*.PCONVERT"))
            idi_files = []
            for p in (exp.correlator_passes or []):
                idi_files.extend(Path('.').glob(f"{p.fitsidifile}*"))
            idi_inputs = [f for f in idi_files if '.PCONVERT' not in f.name]

            if not pconv_files:
                _reset_from('polconvert', "PolConvert output file(s) missing on disk")
            elif idi_inputs and _oldest_mtime(pconv_files) < _newest_mtime(idi_inputs):
                _remove_stale(pconv_files)
                _reset_from('polconvert', "PolConvert output(s) older than IDI input(s)")

    # --- post_polconvert: outputs=idi_ori/ backup (only if needed) ---
    if 'post_polconvert' in step_names and all_steps[step_names.index('post_polconvert')].done:
        if exp.antennas.polconvert and not (Path('.') / 'idi_ori').is_dir():
            _reset_from('post_polconvert', "PolConvert backup (idi_ori/) missing on disk")

    # --- standardplots2: inputs=*.ms (post-ops), outputs=*.ps (re-created) ---
    # Note: standardplots2 re-runs plots; its outputs overlap with standardplots.
    # We skip timestamp validation here since msops_post just calls create_standardplots
    # which already has internal existence checks.

    # --- antab: outputs=pipeline/in/*.antab ---
    if 'antab' in step_names and all_steps[step_names.index('antab')].done:
        antab_files = list(exp.dirs.pipe_in.glob("*.antab")) if exp.dirs.pipe_in.exists() else []
        if not antab_files:
            _reset_from('antab', "ANTAB file(s) missing in pipeline/in")

    # --- pipeinputs: outputs=pipeline/in/*.inp.txt ---
    if 'pipeinputs' in step_names and all_steps[step_names.index('pipeinputs')].done:
        inp_files = list(exp.dirs.pipe_in.glob("*.inp.txt")) if exp.dirs.pipe_in.exists() else []
        if not inp_files:
            _reset_from('pipeinputs', "Pipeline input file(s) missing in pipeline/in")

    # --- pipeline: inputs=pipeline/in/*, outputs=pipeline/out/* ---
    if 'pipeline' in step_names and all_steps[step_names.index('pipeline')].done:
        pipe_out = list(exp.dirs.pipe_out.glob("*")) if exp.dirs.pipe_out.exists() else []
        pipe_in = list(exp.dirs.pipe_in.glob("*")) if exp.dirs.pipe_in.exists() else []

        if not pipe_out:
            _reset_from('pipeline', "Pipeline output file(s) missing")
        elif pipe_in and _oldest_mtime(pipe_out) < _newest_mtime(pipe_in):
            _remove_stale(pipe_out)
            _reset_from('pipeline', "Pipeline output(s) older than input(s)")


def _log_file_path(exp: experiment.Experiment) -> Path:
    """Returns the loguru debug-log path: ``logs/logging_messages.log`` in the working
    directory (see evn_postprocess.reporting; the replayable command log is a separate
    file, ``logs/commands.sh``)."""
    return reporting.debug_log_path()


def _setup_loguru(exp: experiment.Experiment, debug: bool = False):
    """Configure the loguru file sink for the debug log (logs/logging_messages.log).

    Args:
        exp: Experiment object (provides dirs.logs).
        debug: If True, log DEBUG level with full context; otherwise INFO only.
    """
    level = "DEBUG" if debug else "INFO"

    def _file_format(record):
        """Plain-text file format: strip the Rich markup tags from the message.

        When ``format`` is a callable, loguru does not auto-append the line ending or the
        exception, so the returned template must include ``\\n{exception}`` explicitly.
        """
        record["extra"]["clean"] = _RICH_TAG_RE.sub("", record["message"])
        if debug:
            return ("{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
                    "{module}:{function}:{line} | {extra[clean]}\n{exception}")
        return "{level: <8} | {extra[clean]}\n{exception}"

    def _console_sink(message):
        """Render each record through Rich so the Rich-style markup in the message
        (e.g. ``[bold]> command[/bold]``) shows up as bold/colour in the terminal.

        loguru's own markup uses ``<bold>`` tags, so its ``colorize`` never rendered
        the ``[bold]`` markup the rest of the codebase emits; it leaked through as
        plain text. Routing through a Rich ``Console`` fixes that.
        """
        record = message.record
        is_error = record["level"].no >= 40
        console = _stderr_console if is_error else _stdout_console
        text = record["message"]
        if is_error:
            text = f"[bold red]{record['level'].name}:[/bold red] {text}"
        try:
            console.print(text)
        except Exception:
            # A stray '[' in interpolated data must never abort logging: fall back to plain.
            console.print(text, markup=False)
        if record["exception"] is not None:
            from rich.traceback import Traceback
            exc = record["exception"]
            console.print(Traceback.from_exception(exc.type, exc.value, exc.traceback))

    try:
        logger.remove()  # Remove default stderr handler to avoid duplicate messages
        logger.add(_log_file_path(exp), colorize=False, level=level, backtrace=True,
                   diagnose=True, format=_file_format)
        logger.add(_console_sink, colorize=False, level=level, backtrace=True, diagnose=True,
                   format="{message}")
    except (OSError, PermissionError) as e:
        rprint(f"[yellow]Warning: Could not create debug log file: {e}[/yellow]")


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
    if not (f := _log_file_path(exp)).exists():
        exp.write_log_file(f)

    _setup_loguru(exp, debug)
    if not archive:
        logger.debug("The data will not be stored in the EVN archive.")

    from_step = _resolve_step_alias(from_step)
    to_step = _resolve_step_alias(to_step)
    all_steps = [s for s in _WORKFLOW_STEPS if (archive or s.name != 'distribute') and s.name != 'init']
    # skip_steps from the prepared config (used in sweeps mode): bypass the named steps.
    skip = set(getattr(_exp_toml(exp), 'skip_steps', []) or [])
    if skip:
        skipped = [s.name for s in all_steps if s.name in skip]
        if skipped:
            logger.info(f"Skipping steps from the config skip_steps: {', '.join(skipped)}.")
        all_steps = [s for s in all_steps if s.name not in skip]
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
        _validate_outputs(exp, all_steps)
        exp.steps = all_steps
        exp.store()
        steps_to_run = [s for s in all_steps if not s.done]

    if not steps_to_run:
        rprint("[yellow]No pending steps — the post-processing is already complete.[/yellow]")
        return True

    logger.debug(f"Running steps: {', '.join(s.name for s in steps_to_run)}")
    rprint(f"[green]Running steps: {', '.join(s.name for s in steps_to_run)}[/green]")

    for step in steps_to_run:
        # Three channels (see evn_postprocess.reporting): a concise terminal line for the
        # operator, verbose detail to the loguru debug file, and the exact commands this
        # step runs appended (headed by its name) to logs/commands.sh.
        reporting.set_current_step(step.name)
        reporting.announce(f"-- {step.name}")
        try:
            if step.command not in globals():
                _notify_step_failure(exp, step.name, "internal error: command not found")
                return False

            if not globals()[step.command](exp):
                _notify_step_failure(exp, step.name, "the step reported a failure")
                return False

            step.done = True
            exp.store()
            logger.info(f"Step {step.name} completed successfully")
        except StepPaused as pause:
            # A clean wait state (e.g. an e-EVN barrier), NOT a failure: log and
            # notify as "paused" so failure notifications stay trustworthy, and exit
            # cleanly (return True -> exit code 0) for the scheduler. The step stays
            # pending and re-runs on resume.
            logger.info(f"Step {step.name} paused: {pause}")
            utils.notify(f"{exp.expname} post-processing", f"Paused at {step.name}: {pause}")
            return True
        except Exception as e:
            traceback.print_exc()
            _notify_step_failure(exp, step.name, f"unexpected error: {e}")
            return False

        # After postpipe, ask the user to review the dashboard before archiving
        # (PRD stories 13, 20, 21): announce in the terminal AND via the notifier with
        # the exact command to open, then confirm interactively — approving continues
        # with the final steps in this same run; naming a step re-runs from it.
        if step.name == 'postpipe':
            remaining = steps_to_run[steps_to_run.index(step) + 1:]
            if remaining:
                piletter = f"{exp.expname.lower()}.piletter"
                open_cmd = f"postprocess -e {exp.expname} info --serve"
                body = (f"[bold]Please review before continuing:[/bold]\n\n"
                        f"  1. Open the dashboard: [bold green]{open_cmd}[/bold green]\n"
                        f"     (from [dim]{Path.cwd()}[/dim]; check the plots, the Pipeline\n"
                        f"     tab, and fill in the [bold]Comments[/bold] tab per station).\n"
                        f"  2. Review the PI letter ([bold cyan]{piletter}[/bold cyan]).\n"
                        f"     Non-observing antennas and PolConvert remarks have been\n"
                        f"     filled in automatically — verify and edit if needed.\n\n"
                        f"[bold]Then answer below:[/bold] press Enter to finalize and archive,\n"
                        f"type a step name (e.g. [bold green]pipeline[/bold green]) to re-run from it, "
                        f"or type [bold green]quit[/bold green] to stop here.")
                Console().print(Panel(body, title="[bold yellow]Pipeline Results Ready for Review[/bold yellow]",
                                      border_style="yellow", padding=(1, 2)))
                utils.notify(f"{exp.expname} post-processing", "Paused — review pipeline results before continuing")
                if _NOTIFIER is not None:
                    _comms.notify_step_pause(exp, 'postpipe',
                                             f"Review the dashboard: run `{open_cmd}` in {Path.cwd()} "
                                             f"(plots + Pipeline tab + Comments tab), and check the "
                                             f"PI letter ({piletter}). Then answer in the terminal "
                                             f"(or `postprocess run` to finalize).", _NOTIFIER)
                if _BATCH_MODE:
                    _write_review_flag(exp, 'postpipe',
                                       f"Review the dashboard ({open_cmd}) and the PI letter, then "
                                       f"resume with `postprocess run` (or `postprocess run STEP` "
                                       f"to re-run from STEP).")
                    return True
                answer = _ask_review_confirmation()
                if answer == 'quit':
                    return True
                if answer is not None:
                    logger.info(f"Operator requested a re-run from step '{answer}'.")
                    return run_workflow(exp, archive=archive, debug=debug,
                                        from_step=answer, to_step=to_step)
                _clear_review_flag(exp)
                # approved: fall through and continue with the remaining steps

    logger.info(f"The processing of {exp.expname} seems to have finalized properly.")
    utils.notify(f"{exp.expname} post-processing", "Completed successfully")
    return True
