#! /usr/bin/env python3
"""Post-Processing of EVN experiments.
"""
import os
import sys
import json
import argparse
from pathlib import Path
from importlib.metadata import version
from loguru import logger
from rich import print as rprint
from rich.console import Console
from rich_argparse import RawTextRichHelpFormatter
from . import comms
from . import distribution
from . import experiment
from . import experiment_state
from . import inputs
from . import lisfiles
from . import mode as _mode
from . import pipelines
from . import retrieval
from . import workflow
from .plotting import serve_dashboard
from .policy import Policy


__version__: str = version(distribution_name='evn_postprocess')
__prog__: str = 'postprocess'
usage: str = "%(prog)s  [-h] [options] [commands]\n"
description: str = """[bold]Post-processing of EVN experiments.[/bold]\n

Runs the full post-processing for a correlated EVN experiment. It takes the output from the SFXC
correlator and finishes when the data are ready for distribution, following the steps described
in the EVN Post-Processing Guide, in a semi-automatic way.

[dim]The program would retrieve the experiment code from the current working directory,
and the associated Support Scientist from the current user. Otherwise they need to be
specified manually.

The user can also specify to run only some of the steps or to start the process from a given step
(for those cases when the process has partially run previously). If the post-processing already run
in the past, it will automatically continue from the last successful step that run.

[italic]If the post-processing partially run before this execution, it will continue from the last
successful step.[/italic][/dim]
"""

# Every field ``postprocess edit`` accepts: name -> the one-line description shown in its
# help. The source-type fields additionally map to the SourceType they set, below. Keeping
# the CLI choices, the help text and the behaviour in one table stops them from drifting.
EDITABLE_SOURCE_TYPES: dict[str, 'experiment.SourceType'] = {
    'target': experiment.SourceType.target,
    'phasecal': experiment.SourceType.calibrator,
    'fringefinder': experiment.SourceType.fringefinder}

EDITABLE_FIELDS: dict[str, str] = {
    'refant': "reference antenna(s) to use, in order of preference (space-separated codes).",
    'target': "set the source type to target (also for phase-referenced check sources).",
    'phasecal': "set the source type to phase calibrator.",
    'fringefinder': "set the source type to fringe-finder."}

help_edit = "[bold]Edit some of the parameters of the experiment[/bold].\n\n" \
            "Values assigned before the corresponding step reads them may be overwritten.\n" \
            "Called with no value, the field lists the options available for it.\n\n" \
            "The following fields can be edited:\n" + \
            '\n'.join(f"  - [bold green]{name}[/bold green] : {doc}"
                      for name, doc in EDITABLE_FIELDS.items()) + "\n"


help_info = """[bold]Shows the info related to the given experiment
(all what postprocess knows until the present moment).[/bold]

Requires an experiment whose post-processing has already been started: it only reports
what is stored, it never retrieves files or initializes anything (that is 'postprocess run').

(The 'notes.md' file with the same summary is written by the workflow itself, at the
msops step, not by this command.)

With [bold green]--serve[/bold green] the information is shown in a web dashboard (served on a local port)
instead of the terminal. Instructions on how to open it (SSH tunnel command) are printed.
"""

help_dashboard = """[bold]Opens the web dashboard for the experiment.[/bold]

Serves a local web dashboard with the experiment metadata, the scan overview, the
standard plots, the review-comments editor and, once the EVN Pipeline has run, its
feedback page. The SSH tunnel command needed to open it from your local browser is
printed, and the server runs until you press Ctrl+C.

[dim]This is the same dashboard reachable through 'postprocess info --serve'. Like it,
it requires an experiment whose post-processing has already been started.[/dim]
"""

help_list = "[bold]Shows every step of the post-processing and which ones have already run " \
            "in this experiment.[/bold]\n\n[dim]The same information is shown in the " \
            "'Progress' tab of the web dashboard.[/dim]"

def _apply_refant(exp: experiment.Experiment, refant_args: list[str]):
    """Validates and applies reference antenna override to the experiment.

    Args:
        exp: Experiment object.
        refant_args: List of antenna codes from CLI.
    """
    if (invalid := [a for a in refant_args if a not in exp.antennas.names]):
        _fail(f"Unknown antenna(s): {', '.join(invalid)}.",
              f"Available antennas: {', '.join(exp.antennas.names)}")

    exp.refant = list(refant_args)
    logger.info(f"Reference antenna(s) set to: {', '.join(exp.refant)}.")


def _handle_edit(exp: experiment.Experiment, field: str, values: list[str]):
    """Handles the ``postprocess edit`` subcommand.

    With no *values* the available options for the field are listed; otherwise the values
    are validated and applied (see EDITABLE_FIELDS for the accepted fields).

    Args:
        exp: Experiment object.
        field: One of the keys of EDITABLE_FIELDS.
        values: Values provided by the operator (may be empty, to list the options).
    """
    if field == 'refant':
        if not values:
            rprint("[bold]Available antennas:[/bold] " +
                   ', '.join(f"[{'green' if ant.observed else 'red'}]{ant.name}"
                             f"[/{'green' if ant.observed else 'red'}]" for ant in exp.antennas))
            if exp.refant:
                rprint(f"\n[dim]Current refant: {', '.join(exp.refant)}[/dim]")
            return

        _apply_refant(exp, values)
        return

    src_type = EDITABLE_SOURCE_TYPES[field]  # argparse already restricted the choices
    if not values:
        rprint("[bold]Available sources[/bold]:")
        for src in exp.sources:
            rprint(f"  {src.name}  [dim]({src.type.name})[/dim]")
        return

    if (unknown := [name for name in values if name not in exp.sources.names]):
        _fail(f"Unknown source{'s' if len(unknown) > 1 else ''}: {', '.join(unknown)}.",
              f"Available sources: {', '.join(exp.sources.names)}")

    for src_name in values:
        exp.sources[src_name].type = src_type

    # The per-pass source lists are separate objects: propagate the change to them too.
    for a_pass in exp.correlator_passes:
        for src_name in values:
            if a_pass.sources and src_name in a_pass.sources.names:
                a_pass.sources[src_name].type = src_type


def _build_parser() -> argparse.ArgumentParser:
    """Builds the full ``postprocess`` command-line parser.

    The per-step and per-command help texts are generated from the workflow tables
    (:func:`workflow.build_run_help` / :func:`workflow.build_exec_help`) so they can never
    name a step or command that does not exist.
    """
    parser = argparse.ArgumentParser(description=description, prog=__prog__, usage=usage,
                                     formatter_class=RawTextRichHelpFormatter)
    parser.add_argument('-e', '--expname', type=str, default=None,
                        help='Name of the EVN experiment (case-insensitive).\n'
                             '[dim]By default recovered from the current working directory.[/dim]')
    parser.add_argument('-jss', '--supsci', type=str, default=None,
                        help='Surname of the EVN Support Scientist.\n'
                             '[dim]By default recovered assuming the user that is running this '
                             'program.[/dim]')
    parser.add_argument('-d', '--dir', type=str, default=None,
                        help='Directory to run the post-processing. By default in CWD.')
    parser.add_argument('-a', '--no-archive', dest='archive', action='store_false', default=True,
                        help='Skip the delivery of the files to the EVN archive '
                             '(assuming you are a support scientist).')
    parser.add_argument('--no-lag', action='store_true', default=False,
                        help='Do not create the auxiliary lag-space MS nor compute the per-scan '
                             'antenna signal-to-noise from it. The scan overview then only reports '
                             'whether each antenna has data in a scan, without the SNR comparison.')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Debug mode: shows a more verbose output')
    parser.add_argument('--refant', type=str, nargs='+', default=None,
                        help='Reference antenna(s) to use (space-separated two-letter codes).\n'
                             'Overrides the auto-selected reference antenna after loading the '
                             'experiment.')
    parser.add_argument('--mode', type=str, default=None, choices=[m.value for m in _mode.Mode],
                        help='Operating mode. Auto-detected from the OS user/group when omitted '
                             '("jops" or the "supsci" group -> supsci; the "sweeps" group -> '
                             'sweeps; otherwise regular).\n'
                             '- "supsci": JIVE support scientist (retrieve from the correlator, '
                             'ANTAB from vlbeer, archive/deliver).\n'
                             '- "regular": all inputs already local, no archiving.\n'
                             '- "sweeps": the automated SWEEPS system (not implemented yet).\n'
                             '[dim]Overrides the mode stored on the experiment.[/dim]')
    parser.add_argument('--config', type=str, default=None, metavar='FILE',
                        help='Path to the experiment TOML used as config. '
                             'Optional: defaults to the conventional {expname}.toml '
                             'in the directory.')
    parser.add_argument('--policy', type=str, default=None, metavar='FILE',
                        help='Path to a policy.toml file with the unattended decisions '
                             '(weight threshold, polswap/polconvert/onebit antennas, refant, '
                             'pause_after, skip_archive). See evn_postprocess.policy.')
    parser.add_argument('--batch', action='store_true', default=False,
                        help='Run unattended: never invoke interactive dialogs or open the '
                             'standardplots dashboard. The runner stops with exit code 0 and '
                             'writes a REVIEW_REQUIRED marker file when human input is needed. '
                             '[bold]Implies --policy if any decision is required[/bold].')
    parser.add_argument('--comms', type=str, default=None, metavar='FILE',
                        help='Path to a comms.toml file with the communication settings '
                             '(mode, username, email/mattermost config). If not provided, '
                             'auto-searches ./comms.toml and ~/.config/evn/comms.toml.')
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}')

    subparsers = parser.add_subparsers(dest='subpar',
                                       help='[bold]If no command is provided, the full '
                                            'postprocessing will run from the last successful '
                                            'step.[/bold]')
    parser_info = subparsers.add_parser('info', help='Shows the metadata associated to the '
                                        'experiment', description=help_info,
                                        formatter_class=parser.formatter_class)
    parser_info.add_argument('--serve', action='store_true', default=False,
                             help='Open the web dashboard with the experiment info and plots '
                                  'instead of printing to the terminal. Prints the SSH tunnel '
                                  'command needed to open it from your local browser.')
    subparsers.add_parser('dashboard',
                          help='Open the web dashboard with the experiment info and plots.',
                          description=help_dashboard,
                          formatter_class=parser.formatter_class).set_defaults(serve=True)
    for command in ('list', 'last'):  # 'last' is a long-standing alias of 'list'
        subparsers.add_parser(command, help='Shows the different steps to be run and which ones '
                              'have been run.', description=help_list,
                              formatter_class=parser.formatter_class)
    parser_run = subparsers.add_parser('run', help='Runs the post-processing from a given step.',
                                       description=workflow.build_run_help(),
                                       formatter_class=parser.formatter_class)
    parser_run.add_argument('steps', type=str, nargs='*', default=[],
                            help='Optional step range: [STEP1 [STEP2]]. '
                                 'Runs from STEP1 to end, or from STEP1 to STEP2 (inclusive).')
    parser_exec = subparsers.add_parser('exec', help='Runs a single command from the '
                                        'post-processing workflow.',
                                        description=workflow.build_exec_help(),
                                        formatter_class=parser.formatter_class)
    parser_exec.add_argument('task_name', type=str, nargs='?', default=None,
                             help='Name of the command to run. If not provided, lists all '
                                  'available commands.')
    parser_edit = subparsers.add_parser('edit', help='Edit experiment metadata.',
                                        description=help_edit,
                                        formatter_class=parser.formatter_class)
    parser_edit.add_argument('field', type=str, choices=list(EDITABLE_FIELDS),
                             help='Metadata field to edit.')
    parser_edit.add_argument('values', type=str, nargs='*', default=[],
                             help='Value(s) to set. If omitted, lists available options.')
    return parser


def _fail(message: str, *hints: str) -> None:
    """Prints an error (plus optional hints) and exits with a non-zero status."""
    rprint(f"[bold red]{message}[/bold red]")
    for hint in hints:
        rprint(f"[dim]{hint}[/dim]")
    sys.exit(1)


def _setup_initial_logging(debug: bool) -> None:
    """Routes loguru through Rich until the workflow installs its own file+console sinks.

    Everything before ``run_workflow`` (loading the experiment, the CLI subcommands) logs
    through this sink, so a message is never lost between the CLI and the workflow.
    """
    out, err = Console(stderr=False, highlight=False), Console(stderr=True, highlight=False)

    def sink(message):
        record = message.record
        if record["level"].no >= 40:  # WARNING is 30, ERROR is 40
            err.print(f"[bold red]{record['level'].name}[/bold red]: {record['message']}")
        else:
            out.print(record["message"])

    logger.remove()
    logger.add(sink, level="DEBUG" if debug else "INFO", colorize=False)


def _enter_workdir(directory: Path) -> None:
    """Creates (if needed) and moves into the experiment working directory."""
    try:
        directory.mkdir(exist_ok=True)
        os.chdir(directory)
    except (OSError, PermissionError) as e:
        _fail(f"Could not create or access the directory {directory}: {e}")


def _stored_experiment(expname: str) -> experiment.Experiment:
    """Loads the experiment checkpoint, failing when the post-processing never started.

    Used by the commands that report on (or edit) an experiment: they must never create
    one. Only `postprocess run` initializes an experiment, because that is the command
    that retrieves the vex file and builds the directory structure.
    """
    try:
        return experiment.Experiment.load(expname)
    except (FileNotFoundError, ValueError, RuntimeError, json.JSONDecodeError) as e:
        _fail(f"No post-processing data for {expname}: {e}",
              "Only `postprocess run` starts the post-processing of an experiment.")


def _resolve_support_scientist(exp: experiment.Experiment, cli_supsci: str | None) -> None:
    """Makes sure ``exp.supsci`` names a person, not the shared account.

    ``exp.supsci`` is used well beyond the notifications: it picks the AIPS user number
    for the pipeline input file, signs the pipeline feedback page, and addresses the
    review messages. Under the shared 'jops' login it would be nobody, so the assignment
    is read from the ``support`` field of the experiment's .jex file and kept in the
    checkpoint (so the lookup happens once). ``-jss`` always wins: it names a person
    explicitly.

    Args:
        exp: Experiment object, updated in place.
        cli_supsci: The ``-jss`` value, or None.
    """
    if cli_supsci and cli_supsci != exp.supsci:
        logger.info(f"Support scientist set to {cli_supsci} (from -jss).")
        exp.supsci = cli_supsci
    if exp.supsci.lower() != _mode.SUPSCI_USER:
        return

    supsci = retrieval.get_retriever(
        _mode.backends_for(exp.mode).retrieval).fetch_support_scientist(exp)
    if supsci:
        logger.info(f"Support scientist of {exp.expname}, from its .jex file: {supsci}.")
        exp.supsci = supsci
    else:
        logger.warning(f"Could not determine who the support scientist of {exp.expname} is; "
                       f"keeping '{exp.supsci}'. Name them with -jss if needed.")


def _load_experiment(expname: str, args) -> experiment.Experiment:
    """Recovers the stored experiment, or initializes a new one when there is none.

    On recovery the mode is re-resolved (``--mode`` wins, then the stored mode, then
    auto-detection), the folder structure is re-created in case folders were removed by
    hand, and the correlator passes are reloaded when the operator changed the .lis files.
    Either way the support scientist is resolved to a person before the experiment is
    stored (see :func:`_resolve_support_scientist`).

    Args:
        expname: Experiment name (upper case).
        args: The parsed CLI arguments.

    Returns:
        The experiment, already stored on disk.
    """
    if Path(f"{expname.lower()}.json").exists():
        logger.info(f"Recovering the previously-stored information for {expname}.")
        exp = _stored_experiment(expname)
        exp.mode = _mode.resolve(cli_mode=args.mode, stored_mode=exp.mode)
        inputs.create_folder_structure()  # in case the operator removed some of them
        # The operator may have changed the .lis files (the auxiliary {expname}-lag.lis is
        # not a correlator pass and is excluded from the count).
        if len(exp.correlator_passes) != len(lisfiles._pass_lisfiles(f"{expname.lower()}*.lis")):
            logger.warning("The set of .lis files changed: reloading the correlator passes.")
            if not lisfiles.get_passes_from_lisfiles(exp):
                _fail("Could not reload the correlator passes from the .lis files.")
    else:
        supsci = args.supsci if args.supsci else experiment.retrieve_username()
        if supsci == 'unknown':
            _fail("Could not determine the support scientist from the current user.",
                  "Name it explicitly with -jss/--supsci.")
        # A fresh experiment: resolve the mode from --mode or auto-detection, and persist
        # it so every later invocation reuses it (no re-detection).
        resolved = _mode.resolve(cli_mode=args.mode, stored_mode=None)
        logger.info(f"Initializing {expname} (operating mode: {resolved.value}).")
        try:
            exp = workflow.initialize_experiment(expname, supsci, resolved)
        except (ValueError, FileNotFoundError, RuntimeError) as e:
            _fail(f"Could not initialize {expname}: {e}")

    _resolve_support_scientist(exp, args.supsci)
    exp.store()
    return exp


def _apply_cli_options(exp: experiment.Experiment, args) -> None:
    """Applies every CLI option that configures the experiment, and validates the backends.

    Covers the experiment toml, the backend selection (failing fast on an unimplemented
    one), ``--refant``, ``--no-lag``, ``--policy``, ``--batch`` and the comms notifier.
    """
    # The experiment toml is the prepared config. Runtime-only: never serialized into the
    # JSON checkpoint (see experiment_state). --config names an explicit toml (sweeps);
    # otherwise the conventional {expname}.toml is used.
    try:
        if args.config:
            exp.exp_toml = experiment_state.load_toml(Path(args.config))
        else:
            experiment_state.attached_toml(exp, fresh=True)
    except experiment_state.ExperimentTomlError as e:
        _fail(f"Error in the experiment toml file: {e}")

    # Fail fast on an invalid/unimplemented backend for the resolved mode: a bad mode must
    # not surface only hours later at the pipeline step.
    try:
        backends = _mode.backends_for(exp.mode)
        retrieval.get_retriever(backends.retrieval)  # the sweeps stubs raise here
        pipelines.get_pipeline(backends.pipeline)
        distribution.get_distributor(backends.distribution)
    except (retrieval.RetrievalError, pipelines.PipelineError,
            distribution.DistributionError) as e:
        _fail(str(e))

    if args.refant:
        _apply_refant(exp, args.refant)
        exp.store()

    # --no-lag is sticky: once opted out it stays opted out across re-runs, so not passing
    # the flag again does not silently re-enable the lag MS.
    if args.no_lag and not exp.no_lag:
        exp.no_lag = True
        exp.store()

    # The policy is attached to the experiment so the helpers that need it (dialog.
    # PolicyDriven, workflow._pause_steps) can read it without threading it through
    # every call.
    if args.policy:
        try:
            exp.policy = Policy.load(args.policy)
        except FileNotFoundError:
            _fail(f"Policy file not found: {args.policy}")
        except Exception as e:  # tomllib.TOMLDecodeError, etc.
            _fail(f"Could not parse the policy file {args.policy}: {e}")
        exp.store()

    if args.batch:
        workflow.set_batch_mode(True)
        if exp.policy is None:  # so PolicyDriven always has fields to read
            exp.policy = Policy(batch=True)


def _print_info(exp: experiment.Experiment) -> None:
    """Prints everything the program knows about the experiment to the terminal."""
    exp.print_blessed(outputfile=None)
    if exp.mode is not None:
        rprint(f"[bold]Operating mode[/bold]: {exp.mode.value}")

    # The values sourced from the experiment toml are marked with their origin, so they
    # are distinguishable from the vex/lis metadata above.
    if exp.exp_toml is not None:
        toml_lines = experiment_state.summary_lines(exp.exp_toml)
        if toml_lines:
            rprint(f"\n[bold]From the experiment file {exp.exp_toml.path.name}:[/bold]")
            for line in toml_lines:
                print(f"  {line}")  # plain print: the lines may contain [brackets]


def _configure_comms(exp: experiment.Experiment, args) -> None:
    """Sets the workflow notifier, resolving who to notify about this experiment.

    The recipient is the support scientist of the experiment (``exp.supsci``, already
    resolved to a person by :func:`_resolve_support_scientist`), looked up in the
    ``[[people]]`` directory of comms.toml. Notifications stay off, with a warning, when
    nobody can be resolved.
    """
    config = comms.CommsConfig.load(args.comms)
    if config.mode == "none":
        return

    config.username = comms.recipient_for(config, exp.supsci)
    if not config.username:
        logger.warning(f"Notifications ({config.mode}) are configured but nobody could be "
                       "resolved to notify; they stay off for this run.")
        return

    logger.info(f"Notifications ({config.mode}) for {exp.supsci} will go to {config.username}.")
    workflow.set_notifier(comms.make_notifier(config))


def _run(exp: experiment.Experiment, args) -> None:
    """Runs the workflow, exiting non-zero when a step failed.

    A step failure returns False -> exit code 1. That is distinct from a clean review
    pause or e-EVN barrier, which returns True -> exit code 0; the failed step stays the
    resume point for the next ``postprocess run``.
    """
    from_step, to_step = None, None
    if args.subpar == 'run' and args.steps:
        if len(args.steps) > 2:
            _fail("'run' accepts at most two step names (from [to]).")
        from_step = args.steps[0]
        to_step = args.steps[1] if len(args.steps) == 2 else None
        valid, error_msg = workflow.validate_steps(from_step, to_step)
        if not valid:
            _fail(error_msg)

    _configure_comms(exp, args)
    if not workflow.run_workflow(exp, archive=args.archive, debug=args.debug,
                                 from_step=from_step, to_step=to_step):
        sys.exit(1)


def main() -> None:
    args = _build_parser().parse_args()
    _setup_initial_logging(args.debug)

    # An explicit --config must exist if given (the sweeps prepared config; optional).
    if args.config is not None and not Path(args.config).is_file():
        _fail(f"--config file not found: {args.config}")

    # Move into the working directory first: the experiment name defaults to its name.
    _enter_workdir(Path(args.dir) if args.dir else Path('.'))
    try:
        expname = args.expname.upper() if args.expname else experiment.retrieve_expname()
    except (ValueError, FileNotFoundError) as e:
        _fail(f"Could not determine the experiment name: {e}",
              "Name it with -e/--expname, or run from the experiment directory.")

    if args.subpar in ('list', 'last'):
        workflow.list_tasks(expname, print_docs=True)
        return

    if args.subpar == 'edit':
        exp = _stored_experiment(expname)
        _handle_edit(exp, args.field, args.values)
        exp.store()
        return

    if args.subpar == 'exec':
        if args.task_name is None:
            workflow.list_exec_commands()
            sys.exit(1)
        try:
            workflow.run_isolated_task(args.task_name, expname)
        except (FileNotFoundError, KeyError, AttributeError) as e:
            _fail(f"Could not run the task '{args.task_name}': {e}")
        return

    # 'run' (or no command) is the only entry point that may initialize an experiment;
    # 'info' and 'dashboard' merely report on one that has already been started.
    exp = _load_experiment(expname, args) if args.subpar in (None, 'run') \
        else _stored_experiment(expname)
    _apply_cli_options(exp, args)

    if args.subpar in (None, 'run'):
        _run(exp, args)
    elif args.serve:  # 'info --serve' and 'dashboard' are the same thing
        serve_dashboard(exp, exp.dirs.plots, pipeline_dir=exp.dirs.pipe_out)
    else:  # 'info' without --serve
        _print_info(exp)


if __name__ == '__main__':
    main()
