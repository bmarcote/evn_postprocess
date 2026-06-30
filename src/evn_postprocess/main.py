#! /usr/bin/env python3
"""Post-Processing of EVN experiments.
"""
import os
import sys
import json
import glob
import argparse
from loguru import logger
from rich import print as rprint
from rich.console import Console
from rich_argparse import RawTextRichHelpFormatter
from pathlib import Path
from importlib.metadata import version
from . import workflow
from . import experiment
from . import lisfiles


__version__ = version('evn_postprocess')
__prog__ = 'postprocess'
usage = "%(prog)s  [-h] [options] [commands]\n"
description = """[bold]Post-processing of EVN experiments.[/bold]\n

This program runs the full post-processing for a correlated EVN experiment until distribution,
following the steps described in the EVN Post-Processing Guide, in a semi-automatic way.

[dim]The program would retrieve the experiment code from the current working directory,
and the associated Support Scientist from the parent directory. Otherwise they need to be
specified manually.

The user can also specify to run only some of the steps or to start the process from a given step
(for those cases when the process has partially run previously). If the post-processing already run
in the past, it will automatically continue from the last successful step that run.

[italic]If the post-processing partially run before this execution,
it will continue from the last successful step.[/italic][/dim]
"""

help_calsources = 'Calibrator sources to use in standardplots (comma-separated, no spaces). ' \
                  'If not provided, it will pick the fringefinders found in the .expsum file.'

help_run = """[bold]Runs the post-process from a given step[/bold].

        Three different approaches can be used:

        [italic]postprocess run[/italic] (no param)
                            Runs the entire post-process (or from the last run step).
        [italic]postprocess run STEP1[/italic]
                            Runs from STEP1 until the end (or until manual interaction is required).
        [italic]postprocess run STEP1 STEP2[/italic]
                            Runs from STEP1 until STEP2 (both included).


        The available steps are:
            - [bold green]start[/bold green] : Sets up the experiment, creates the required folders
                                               in @eee and @pipe, and copy the already-existing
                                               files (.expsum, .vix, etc).
            - [bold green]lisfile[/bold green] : Produces a .lis file(s) in @ccs
                                                 and copies them to @eee.
            - [bold green]checklis[/bold green] : Checks the existing .lis files.
            - [bold green]ms[/bold green] : Gets the data for all available .lis files and
                                            runs j2ms2 to produce MS files.
            - [bold green]plots[/bold green] : Runs standardplots.
            - [bold green]msops[/bold green] : Runs the full MS operations like ysfocus, polswap,
                                               flag_weights, etc.
            - [bold green]tconvert[/bold green] : Runs tConvert on all available MS files to
                                                  create the FITS-IDI files.
            - [bold green]polconvert[/bold green] : Runs PolConvert on the FITS-IDI files
                                                    (only if some antennas need it).
            - [bold green]post_polconvert[/bold green] : if polConvert did run, then this steps
                                                         renames the new *.PCONVERT files and do
                                                         standardplots on them.
            - [bold green]antab[/bold green] : Retrieves the .antab file to be used in the pipeline.
                                               If it was not generated, Opens antab_editor.py.
                Needs to run again once you have run antab_editor.py manually.
            - [bold green]pipeinputs[/bold green] : Prepares a draft input file for the pipeline
                                                    and recovers all needed files.
            - [bold green]pipeline[/bold green] : Runs the EVN Pipeline for all correlated passes.
            - [bold green]postpipe[/bold green] : Runs all steps to be done after the pipeline:
                                                  creates tasav, comment files, feedback.pl
            - [bold green]prearchive[/bold green] : Appends Tsys/GC and re-archive FITS-IDI and
                                              the PI letter. Asks to conduct the
                                              last post-processing steps.
            - [bold green]archive[/bold green] : Sets the credentials for the experiment,
                                                 create the pipe letter and archive all the data.
"""
help_edit = """[bold]Edit some of the parameters related to the experiment[/bold].

    Note that if you assign the values before they are read from the standard processing tasks,
    they may be overwriten.

    The following parameters are allowed:
        - [bold green]refant[/bold green] : change the reference antenna(s) to the provided one(s)
                                            (comma-separated).
        - [bold green]calsour[/bold green] : change the sources used for standardplots.
            If more than one, they must be comma-separated and with no spaces.
        - [bold green]calibrator[/bold green] : Set the source type to calibrator (phase cal.)
                                                for the given source.
        - [bold green]target[/bold green] : Set the source type to target for the given source
            (to be used also for phase-referenced check sources).
        - [bold green]fringefinder[/bold green] : Set the source type to fringe-finder
                                                  for the given source.
        - [bold green]polconvert[/bold green] : marks the antennas to be pol converted.
        - [bold green]polswap[/bold green] : marks the antennas to be pol swapped.
        - [bold green]onebit[/bold green] :  marks the antennas to be corrected because
                                             they observed with one bit.
"""


help_info = """[bold]Shows the info related to the given experiment
(all what postprocess knows until the presentmoment).[/bold]

It will also write this information down into a 'notes.md' file is this does not exist.

With [bold green]--serve[/bold green] the information is shown in a web dashboard (served on a local port)
instead of the terminal. Instructions on how to open it (SSH tunnel command) are printed.
"""

help_last = "[bold]Returns the last step that run successfully from post-process " \
            "in this experiment.[/bold]"

help_gui = 'Type of GUI to use for interactions with the user:\n' \
           '- "terminal" (default): it uses the basic prompt in the terminal.\n' \
           '- "tui": uses the Terminal-based User Interface.\n' \
           '- "gui": uses the Graphical User Interface.'


def _apply_refant(exp: experiment.Experiment, refant_args: list[str]):
    """Validates and applies reference antenna override to the experiment.

    Args:
        exp: Experiment object.
        refant_args: List of antenna codes from CLI.
    """
    known = set(exp.antennas.names)
    invalid = [a for a in refant_args if a not in known]
    if invalid:
        rprint(f"[red]Unknown antenna(s): {', '.join(invalid)}[/red]")
        rprint(f"[dim]Available antennas: {', '.join(sorted(known))}[/dim]")
        sys.exit(1)

    exp.refant = list(refant_args)
    rprint(f"[green]Reference antenna(s) set to: {', '.join(exp.refant)}[/green]")


def _handle_edit(exp: experiment.Experiment, field: str, values: list[str]):
    """Handles the 'postprocess edit' subcommand.

    If values is empty, lists available options. Otherwise validates and applies the change.

    Args:
        exp: Experiment object.
        field: One of 'refant', 'target', 'phasecal', 'fringefinder'.
        values: Values provided by the user (may be empty to list options).
    """
    if field == 'refant':
        if not values:
            rprint("[bold]Available antennas:[/bold]")
            for ant in exp.antennas:
                status = "[green]observed[/green]" if ant.observed else "[red]not observed[/red]"
                rprint(f"  {ant.name}  {status}")
            if exp.refant:
                rprint(f"\n[dim]Current refant: {', '.join(exp.refant)}[/dim]")
            return

        _apply_refant(exp, values)
        return

    # Source-type fields: target, phasecal, fringefinder
    type_map = {'target': experiment.SourceType.target, 'phasecal': experiment.SourceType.calibrator,
                'fringefinder': experiment.SourceType.fringefinder}
    src_type = type_map[field]

    if not values:
        rprint(f"[bold]Available sources[/bold] (current {field} sources marked with *):")
        for src in exp.sources:
            marker = " *" if src.type == src_type else ""
            rprint(f"  {src.name}  [dim]({src.type.name})[/dim]{marker}")
        return

    known_sources = set(exp.sources.names)
    for src_name in values:
        if src_name not in known_sources:
            rprint(f"[red]Unknown source '{src_name}'.[/red]")
            rprint(f"[dim]Available sources: {', '.join(sorted(known_sources))}[/dim]")
            sys.exit(1)

        exp.sources[src_name].type = src_type
        rprint(f"[green]Source '{src_name}' set to {field}.[/green]")

    # Propagate type changes to per-pass sources
    for a_pass in exp.correlator_passes:
        if not a_pass.sources:
            continue
        for src_name in values:
            if src_name in a_pass.sources.names:
                a_pass.sources[src_name].type = src_type


def main():
    parser = argparse.ArgumentParser(description=description, prog=__prog__, usage=usage,
                                     formatter_class=RawTextRichHelpFormatter)
                                     # formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-e', '--expname', type=str, default=None, \
                        help='Name of the EVN experiment (case-insensitive).' \
                        '\n[dim]By default recovered from the current working directory.[/dim]')
    parser.add_argument('-jss', '--supsci', type=str, default=None,
                        help='Surname of the EVN Support Scientist.\n' \
                        '[dim]By default recovered assuming the user that is running this program.[/dim]')
    parser.add_argument('-d', '--dir', type=str, default=None,
                        help='Directory to run the post-processing. By default in /data/exp/<expname>.')
    parser.add_argument('-a', '--no-archive', action='store_false', default=True,
                        help='Skip the archive part of the files to the EVN archive.')
    parser.add_argument('--no-lag', action='store_true', default=False,
                        help='Do not create the auxiliary lag-space MS nor compute the per-scan '
                             'antenna signal-to-noise from it. The scan overview then only reports '
                             'whether each antenna has data in a scan, without the SNR comparison.')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Debug mode: shows a more verbose output')
    # parser.add_argument('--j2ms2par', type=str, default=None,
    #                     help='Additional attributes for j2ms2 (like the fo:XXXXX).')
    parser.add_argument('--refant', type=str, nargs='+', default=None,
                        help='Reference antenna(s) to use (space-separated two-letter codes).\n'
                        'Overrides the auto-selected reference antenna after loading the experiment.')
    parser.add_argument('--policy', type=str, default=None, metavar='FILE',
                        help='Path to a policy.toml file with the unattended decisions '
                             '(weight threshold, polswap/polconvert/onebit antennas, refant, '
                             'pause_after, skip_archive). See evn_postprocess.policy for the schema.')
    parser.add_argument('--tConvert-in-eee', action=argparse.BooleanOptionalAction, default=True,
                        help='Temporary workaround for the broken local tConvert and PolConvert: run '
                             'both steps on eee instead. tConvert copies the MS files to '
                             'jops@eee:/data0/temp/, runs there, and copies the FITS-IDI files back; '
                             'polconvert pushes its input file to the experiment directory on eee '
                             '(where the FITS-IDI files already are), runs there, and copies the '
                             '.PCONVERT files back. Enabled by default; use --no-tConvert-in-eee to '
                             'run both locally.')
    parser.add_argument('--batch', action='store_true', default=False,
                        help='Run unattended: never invoke interactive dialogs or open the '
                             'standardplots dashboard. The runner stops with exit code 0 and '
                             'writes a REVIEW_REQUIRED marker file when human input is needed. '
                             'Implies --policy if any decision is required.')
    parser.add_argument('--comms', type=str, default=None, metavar='FILE',
                        help='Path to a comms.toml file with the communication settings '
                             '(mode, username, email/mattermost config). If not provided, '
                             'auto-searches ./comms.toml and ~/.config/evn/comms.toml.')
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    subparsers = parser.add_subparsers(help='[bold]If no command is provided, the full postprocessing will run ' \
                                       'from the last successful step.[/bold]', dest='subpar')
    parser_info = subparsers.add_parser('info', help='Shows the metadata associated to the experiment',
                                        description=help_info,
                                        formatter_class=parser.formatter_class)
    parser_info.add_argument('--serve', action='store_true', default=False,
                             help='Open the web dashboard with the experiment info and plots '
                                  'instead of printing to the terminal. Prints the SSH tunnel '
                                  'command needed to open it from your local browser.')
    _ = subparsers.add_parser('list', help='Shows the different steps to be run and which ones have been run.',
                              description=help_last,
                              formatter_class=parser.formatter_class)
    _ = subparsers.add_parser('last', help='Shows the different steps to be run and which ones have been run.',
                              description=help_last,
                              formatter_class=parser.formatter_class)
    parser_run = subparsers.add_parser('run', help='Runs the post-processing from a given step.',
                                       description=help_run, formatter_class=parser.formatter_class)
    parser_run.add_argument('steps', type=str, nargs='*', default=[],
                            help='Optional step range: [STEP1 [STEP2]]. '
                            'Runs from STEP1 to end, or from STEP1 to STEP2 (inclusive).')
    help_exec = workflow.build_exec_help()
    parser_exec = subparsers.add_parser('exec', help='Runs a single command from the post-processing workflow.',
                                        description=help_exec, formatter_class=parser.formatter_class)
    parser_exec.add_argument('task_name', type=str, nargs='?', default=None,
                             help='Name of the command to run. If not provided, lists all available commands.')
    parser_edit = subparsers.add_parser('edit', help='Edit experiment metadata.',
                                        description=help_edit, formatter_class=parser.formatter_class)
    parser_edit.add_argument('field', type=str, choices=['refant', 'target', 'phasecal', 'fringefinder'],
                             help='Metadata field to edit.')
    parser_edit.add_argument('values', type=str, nargs='*', default=[],
                             help='Value(s) to set. If omitted, lists available options.')
    args = parser.parse_args()

    _con = Console(stderr=False, highlight=False)
    _err_con = Console(stderr=True, highlight=False)
    def _initial_sink(message):
        record = message.record
        if record["level"].no >= 40:
            _err_con.print(f"[bold red]{record['level'].name}[/bold red]: {record['message']}")
        else:
            _con.print(record["message"])

    logger.remove()
    logger.add(_initial_sink, level="DEBUG" if args.debug else "INFO", colorize=False)
    try:
        expname = args.expname.upper() if args.expname else experiment.retrieve_expname()
    except (ValueError, FileNotFoundError) as e:
        rprint("[bold red]Error retrieving experiment name[/bold red]")
        rprint(f"[red]{e}[/red]")
        rprint("[red]Please specify the experiment name with -e/--expname or run from the experiment directory[/red]")
        sys.exit(1)
    
    try:
        cwd = Path(args.dir if args.dir else experiment.retrieve_servers()['eee'].path / expname.upper())
    except (FileNotFoundError, KeyError) as e:
        rprint(f"[red]Error setting up directory: {e}[/red]")
        rprint("[red]Please check the server configuration or specify a directory with -d/--dir[/red]")
        sys.exit(1)

    if (not args.subpar) or (args.subpar in ('info', 'run')):
        try:
            cwd.mkdir(exist_ok=True)
            os.chdir(cwd)
        except (OSError, PermissionError) as e:
            rprint(f"[red]Error creating or accessing directory {cwd}: {e}[/red]")
            sys.exit(1)
            
        if Path(f"{expname.lower()}.json").exists():
            rprint(f"[bold]Recovering previously-stored information for {expname}[/bold]")
            try:
                exp = experiment.Experiment.load(expname)
                # Just to avoid that the user deleted some folders
                workflow.create_folder_structure()
                # User may have changed the lis files... (exclude the auxiliary
                # {expname}-lag.lis, which is not a correlator pass).
                if len(exp.correlator_passes) != len(lisfiles._pass_lisfiles(f"{expname.lower()}*.lis")):
                    rprint("[bold yellow]Reloading .lis files information...[/bold yellow]")
                    if not lisfiles.get_passes_from_lisfiles(exp):
                        rprint("[red]Error: Failed to reload .lis files[/red]")
                        sys.exit(1)
            except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
                rprint(f"[red]Error loading experiment data: {e}[/red]")
                rprint("[red]The experiment file may be corrupted. Consider reinitializing the experiment by removing the json file.[/red]")
                sys.exit(1)
        else:
            try:
                supsci = args.supsci if args.supsci else experiment.retrieve_username()
                if supsci == 'unknown':
                    raise ValueError("Could not determine the username. Please specify it with --supsci.")

                exp = workflow.initialize_experiment(expname, supsci)
                # if args.j2ms2par is not None:
                #     exp.special_params = {'j2ms2': [par.strip() for par in args.j2ms2par.split(',')]}
                exp.store()
            except (ValueError, FileNotFoundError, RuntimeError) as e:
                rprint(f"[red]Error initializing experiment: {e}[/red]")
                sys.exit(1)

        # Apply --refant override if provided
        if args.refant:
            _apply_refant(exp, args.refant)
            exp.store()

        # Apply --no-lag if requested. This is sticky: once opted out, it stays opted out
        # across re-runs (not passing the flag again does not silently re-enable the lag MS).
        if args.no_lag and not exp.no_lag:
            exp.no_lag = True
            exp.store()

        # --policy / --batch wiring. We attach the policy onto the experiment so
        # downstream helpers (e.g. dialog.PolicyDriven, workflow._signal_pause)
        # can read it without threading the value through every call.
        if args.policy:
            from .policy import Policy
            try:
                exp.policy = Policy.load(args.policy)
            except FileNotFoundError:
                rprint(f"[red]Policy file not found: {args.policy}[/red]")
                sys.exit(1)
            except Exception as e:  # tomllib.TOMLDecodeError, etc.
                rprint(f"[red]Could not parse policy file {args.policy}: {e}[/red]")
                sys.exit(1)
            exp.store()
        # tConvert workaround: run the step on eee unless explicitly disabled.
        exp.tconvert_in_eee = args.tConvert_in_eee

        if args.batch:
            workflow.set_batch_mode(True)
            # Make sure exp.policy at least exists so PolicyDriven can read fields.
            if exp.policy is None:
                from .policy import Policy
                exp.policy = Policy(batch=True)

        # --- Comms wiring: load config and set the workflow notifier ---
        from . import comms as _comms
        _comms_config = _comms.CommsConfig.load(args.comms)
        if _comms_config.mode != "none":
            workflow.set_notifier(_comms.make_notifier(_comms_config))

        if not args.subpar or args.subpar == 'run':
            from_step, to_step = None, None
            if args.subpar == 'run' and args.steps:
                if len(args.steps) > 2:
                    rprint("[red]Error: 'run' accepts at most two step names.[/red]")
                    sys.exit(1)
                from_step = args.steps[0]
                to_step = args.steps[1] if len(args.steps) == 2 else None
                valid, error_msg = workflow.validate_steps(from_step, to_step)
                if not valid:
                    rprint(f"[red]Error: {error_msg}[/red]")
                    sys.exit(1)
            workflow.run_workflow(exp, args.no_archive, debug=args.debug, from_step=from_step, to_step=to_step)
        else:  # args.subpar == 'info'
            if args.serve:
                from .plotting import serve_dashboard
                serve_dashboard(exp, exp.dirs.plots)
            else:
                exp.print_blessed(outputfile=None)
    elif args.subpar == 'list' or args.subpar == 'last':
        try:
            workflow.list_tasks(expname, print_docs=True)
        except (FileNotFoundError, KeyError) as e:
            rprint(f"[red]Error listing tasks: {e}[/red]")
            sys.exit(1)
    elif args.subpar == 'edit':
        try:
            cwd.mkdir(exist_ok=True)
            os.chdir(cwd)
        except (OSError, PermissionError) as e:
            rprint(f"[red]Error creating or accessing directory {cwd}: {e}[/red]")
            sys.exit(1)

        try:
            exp = experiment.Experiment.load(expname)
        except FileNotFoundError:
            rprint(f"[red]No stored experiment found for {expname}. Run postprocess first.[/red]")
            sys.exit(1)
        except (json.JSONDecodeError, KeyError) as e:
            rprint(f"[red]Error loading experiment data: {e}[/red]")
            sys.exit(1)

        _handle_edit(exp, args.field, args.values)
        exp.store()

    elif args.subpar == 'exec':
        if args.task_name is None:
            workflow.list_exec_commands()
            sys.exit(1)

        try:
            cwd.mkdir(exist_ok=True)
            os.chdir(cwd)
        except (OSError, PermissionError) as e:
            rprint(f"[red]Error creating or accessing directory {cwd}: {e}[/red]")
            sys.exit(1)

        try:
            # tConvert workaround: run the step on eee unless explicitly disabled.
            workflow.run_isolated_task(args.task_name, expname,
                                       tconvert_in_eee=args.tConvert_in_eee)
        except (FileNotFoundError, KeyError, AttributeError) as e:
            rprint(f"[red]Error running task '{args.task_name}': {e}[/red]")
            sys.exit(1)


if __name__ == '__main__':
    main()