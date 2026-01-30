#! /usr/bin/env python3
"""Post-Processing of EVN experiments.

"""
import os
import json
import glob
import argparse
from rich import print as rprint
from rich_argparse import RawTextRichHelpFormatter
from pathlib import Path
# from inspect import signature  # WHAT?  to know how many parameters has each function
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
            - [bold green]tconvert[/bold green] : Runs tConvert on all available MS files,
                                                  and runs polConvert is required.
            - [bold green]post_polconvert[/bold green] : if polConvert did run, then this steps
                                                         renames the new *.PCONVERT files and do
                                                         standardplots on them.
            - [bold green]archive[/bold green] : Sets the credentials for the experiment,
                                                 create the pipe letter and archive all the data.
            - [bold green]antab[/bold green] : Retrieves the .antab file to be used in the pipeline.
                                               If it was not generated, Opens antab_editor.py.
                Needs to run again once you have run antab_editor.py manually.
            - [bold green]pipeinputs[/bold green] : Prepares a draft input file for the pipeline
                                                    and recovers all needed files.
            - [bold green]pipeline[/bold green] : Runs the EVN Pipeline for all correlated passes.
            - [bold green]postpipe[/bold green] : Runs all steps to be done after the pipeline:
                                                  creates tasav, comment files, feedback.pl
            - [bold green]last[/bold green] : Appends Tsys/GC and re-archive FITS-IDI and
                                              the PI letter. Asks to conduct the
                                              last post-processing steps.
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
"""

help_last = "[bold]Returns the last step that run successfully from post-process " \
            "in this experiment.[/bold]"

help_gui = 'Type of GUI to use for interactions with the user:\n' \
           '- "terminal" (default): it uses the basic prompt in the terminal.\n' \
           '- "tui": uses the Terminal-based User Interface.\n' \
           '- "gui": uses the Graphical User Interface.'


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
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Debug mode: shows a more verbose output')
    parser.add_argument('--j2ms2par', type=str, default=None,
                        help='Additional attributes for j2ms2 (like the fo:XXXXX).')
    parser.add_argument('-s', '--steps', type=str, nargs='+', default=None,
                        help='Run from a specific step (and optionally to another step).\n'
                        'If one step is given, runs from that step to the end.\n'
                        'If two steps are given, runs from the first to the second (inclusive).')
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    subparsers = parser.add_subparsers(help='[bold]If no command is provided, the full ' \
                                       'postprocessing will run ' \
                                       'from the last successful step.[/bold]', dest='subpar')
    _ = subparsers.add_parser('info', help='Shows the metadata associated to the experiment',
                              description=help_info,
                              formatter_class=parser.formatter_class)
    _ = subparsers.add_parser('list', help='Shows the different steps to be run.',
                              description=help_last,
                              formatter_class=parser.formatter_class)
    parser_exec = subparsers.add_parser('exec', help='Runs a single task from the post-processing workflow',
                                        formatter_class=parser.formatter_class)
    parser_exec.add_argument('task_name', type=str, nargs='?', default=None,
                             help="Name of the task to run. If not provided, lists all available tasks.")
    args = parser.parse_args()

    try:
        expname = args.expname.upper() if args.expname else experiment.retrieve_expname()
    except (ValueError, FileNotFoundError) as e:
        rprint(f"[red]Error retrieving experiment name: {e}[/red]")
        rprint("[red]Please specify the experiment name with -e/--expname or run from the experiment directory[/red]")
        return
    
    try:
        if not args.dir:
            servers = experiment.retrieve_servers()
            cwd = Path(eval(f"f'{servers['eee'].path}'", {'expname': expname}))
        else:
            cwd = Path(args.dir)
    except (FileNotFoundError, KeyError) as e:
        rprint(f"[red]Error setting up directory: {e}[/red]")
        rprint("[red]Please check the server configuration or specify a directory with -d/--dir[/red]")
        return

    if (not args.subpar) or (args.subpar == 'info'):
        try:
            cwd.mkdir(exist_ok=True)
            os.chdir(cwd)
        except (OSError, PermissionError) as e:
            rprint(f"[red]Error creating or accessing directory {cwd}: {e}[/red]")
            return
            
        # A previous execution of the post-process has been done
        if Path(f"{expname.lower()}.json").exists():
            rprint(f"[bold]Recovering previously-stored information for {expname}[/bold]")
            try:
                exp = experiment.Experiment.load(expname)
                # Just to avoid that the user deleted some folders
                workflow.create_folder_structure()
                
                # Check if correlator passes are empty but .lis files exist
                if len(exp.correlator_passes) == 0 and len(glob.glob(f"{expname.lower()}*.lis")) > 0:
                    rprint("[bold yellow]No correlator passes found but .lis files exist. Reloading .lis files...[/bold yellow]")
                    if not lisfiles.get_passes_from_lisfiles(exp):
                        rprint("[red]Error: Failed to reload .lis files[/red]")
                        return
                        
            except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
                rprint(f"[red]Error loading experiment data: {e}[/red]")
                rprint("[red]The experiment file may be corrupted. Consider reinitializing the experiment.[/red]")
                return
        else:
            try:
                supsci = args.supsci if args.supsci else experiment.retrieve_username()
                exp = workflow.initialize_experiment(expname, supsci)
                if args.j2ms2par is not None:
                    exp.special_params = {'j2ms2': [par.strip() for par in args.j2ms2par.split(',')]}
                    exp.store()
            except (ValueError, FileNotFoundError, RuntimeError) as e:
                rprint(f"[red]Error initializing experiment: {e}[/red]")
                return

        if not args.subpar:
            from_step, to_step = None, None
            if args.steps:
                if len(args.steps) > 2:
                    rprint("[red]Error: --steps accepts at most two step names.[/red]")
                    return
                from_step = args.steps[0]
                to_step = args.steps[1] if len(args.steps) == 2 else None
                valid, error_msg = workflow.validate_steps(from_step, to_step)
                if not valid:
                    rprint(f"[red]Error: {error_msg}[/red]")
                    return
            workflow.run_workflow(exp, args.no_archive, debug=args.debug, from_step=from_step, to_step=to_step)
        else:  # elif args.subpar == 'info':
            exp.print_blessed(outputfile=None)
    elif args.subpar == 'list':
        try:
            workflow.list_tasks(expname, print_docs=True)
        except (FileNotFoundError, KeyError) as e:
            rprint(f"[red]Error listing tasks: {e}[/red]")
            return
    elif args.subpar == 'exec':
        if args.task_name is None:
            try:
                workflow.list_tasks(expname, print_docs=True)
            except (FileNotFoundError, KeyError) as e:
                rprint(f"[red]Error listing tasks: {e}[/red]")
                return
        else:
            try:
                workflow.run_isolated_task(args.task_name, expname)
            except (FileNotFoundError, KeyError, AttributeError) as e:
                rprint(f"[red]Error running task '{args.task_name}': {e}[/red]")
                return


if __name__ == '__main__':
    main()