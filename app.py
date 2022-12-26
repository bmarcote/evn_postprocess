#! /usr/bin/env python3
"""
"""
import os
import sys
import argparse
import traceback
from typing import Optional
import rich
from rich_argparse import RichHelpFormatter, RawTextRichHelpFormatter
from pathlib import Path
from datetime import datetime as dt
# from inspect import signature  # WHAT?  to know how many parameters has each function
from evn_postprocess.evn_postprocess import experiment
from evn_postprocess.evn_postprocess import scheduler as sch
from evn_postprocess.evn_postprocess import dialog
from evn_postprocess.evn_postprocess import environment as env
from evn_postprocess.evn_postprocess import process_ccs as ccs
from evn_postprocess.evn_postprocess import process_eee as eee
from evn_postprocess.evn_postprocess import process_pipe as pipe

__version__ = '1.0.3'
__prog__ = 'postprocess'
usage = "%(prog)s  [-h] [options] [commands]\n"
description = """[bold]Post-processing of EVN experiments.[/bold]\n

This program runs the full post-processing for a correlated EVN experiment until distribution, following the steps described in the EVN Post-Processing Guide, in a semi-automatic way.

[dim]The program would retrieve the experiment code from the current working directory, and the associated Support Scientist from the parent directory. Otherwise they need to be specified manually.

The user can also specify to run only some of the steps or to start the process from a given step
(for those cases when the process has partially run previously). If the post-processing already run in teh past,
it will automatically continue from the last successful step that run.

[italic]If the post-processing partially run before this execution, it will continue from the last successful step.[/italic][/dim]
"""

help_calsources = 'Calibrator sources to use in standardplots (comma-separated, no spaces). ' \
                  'If not provided, it will pick the fringefinders found in the .expsum file.'

help_run = """[bold]Runs the post-process from a given step[/bold].

        Three different approaches can be used:

        [italic]postprocess run[/italic] (no param)  - Runs the entire post-process (or from the last run step).
        [italic]postprocess run STEP1[/italic]       - Runs from STEP1 until the end (or until manual interaction is required).
        [italic]postprocess run STEP1 STEP2[/italic] - Runs from STEP1 until STEP2 (both included).


        The available steps are:
            - [bold green]setting_up[/bold green] : Sets up the experiment, creates the required folders in @eee and @pipe, and copy the already-existing files (.expsum, .vix, etc).
            - [bold green]lisfile[/bold green] : Produces a .lis file in @ccs and copies them to @eee.
            - [bold green]checklis[/bold green] : Checks the existing .lis files.
            - [bold green]ms[/bold green] : Gets the data for all available .lis files and runs j2ms2 to produce MS files.
            - [bold green]plots[/bold green] : Runs standardplots.
            - [bold green]msops[/bold green] : Runs the full MS operations like ysfocus, polswap, flag_weights, etc.
            - [bold green]tconvert[/bold green] : Runs tConvert on all available MS files, and runs polConvert is required.
            - [bold green]post_polconvert[/bold green] : if polConvert did run, then this steps renames the new *.PCONVERT files and do standardplots on them.
            - [bold green]archive[/bold green] : Sets the credentials for the experiment, create the pipe letter and archive all the data.
            - [bold green]antab[/bold green] : Retrieves the .antab file to be used in the pipeline. If it was not generated, Opens antab_editor.py.
                Needs to run again once you have run antab_editor.py manually.
            - [bold green]pipeinputs[/bold green] : Prepares a draft input file for the pipeline and recovers all needed files.
            - [bold green]pipeline[/bold green] : Runs the EVN Pipeline for all correlated passes.
            - [bold green]postpipe[/bold green] : Runs all steps to be done after the pipeline: creates tasav, comment files, feedback.pl
            - [bold green]last[/bold green] : Appends Tsys/GC and re-archive FITS-IDI and the PI letter. Asks to conduct the last post-processing steps.
"""
help_edit = """[bold]Edit some of the parameters related to the experiment[/bold].

    Note that if you assign the values before they are read from the standard processing tasks,
    they may be overwriten.

    The following parameters are allowed:
        - [bold green]refant[/bold green] : change the reference antenna(s) to the provided one(s) (comma-separated).
        - [bold green]calsour[/bold green] : change the sources used for standardplots.
            If more than one, they must be comma-separated and with no spaces.
        - [bold green]calibrator[/bold green] : Set the source type to calibrator (phase cal.) for the given source.
        - [bold green]target[/bold green] : Set the source type to target for the given source
            (to be used also for phase-referenced check sources).
        - [bold green]fringefinder[/bold green] : Set the source type to fringe-finder for the given source.
        - [bold green]polconvert[/bold green] : marks the antennas to be pol converted.
        - [bold green]polswap[/bold green] : marks the antennas to be pol swapped.
        - [bold green]onebit[/bold green] :  marks the antennas to be corrected because they observed with one bit.
"""


help_info = """[bold]Shows the info related to the given experiment (all what postprocess knows until the presentmoment).[/bold]

    It will also write this information down into a 'notes.md' file is this does not exist.
"""

help_last = """[bold]Returns the last step that run successfully from post-process in this experiment.[/bold]
    """

help_gui = 'Type of GUI to use for interactions with the user:\n' \
           '- "terminal" (default): it uses the basic prompt in the terminal.\n' \
           '- "tui": uses the Terminal-based User Interface.\n' \
           '- "gui": uses the Graphical User Interface.'

class Command(object):
    @property
    def command(self):
        return self._cmd

    @property
    def doc(self):
        return self._doc

    def __init__(self, command, doc):
        """Executes the command (which must be a Python function), that has the associated doc string for help.
        """
        self._cmd = command
        self._doc = doc


edit_params = ('refant', 'calsour', 'onebit', 'polswap', 'polconvert', 'target', 'calibrator', 'fringefinder')
all_steps = {'setting_up': sch.setting_up_environment,
             'lisfile': sch.preparing_lis_files,
             'checklis': sch.first_manual_check,
             'ms': sch.creating_ms,
             'plots': sch.standardplots,
             'msops': sch.ms_operations,
             'tconvert': sch.tconvert,
             'post_polconvert': sch.post_polconvert,
             'archive': sch.archive,
             'antab': sch.antab_editor,
             'pipeinputs': sch.getting_pipeline_files,
             'pipeline': sch.pipeline,   # TODO:  sch.protect_archive_data
             'postpipe': sch.after_pipeline,
             'last': sch.final_steps}

all_commands = {'dirs': Command(env.create_all_dirs, "Creates folders in eee and jop83: " \
                                    "/data0/{supsci}/{EXP}, jop83:$IN/{exp}, $OUT/{exp}, $IN/{supsci}/{exp}"),
                'copyfiles': Command(env.copy_files, "Copies the .vix, .expsum, .piletter, .key/sum  to eee."),
                'auth': Command(eee.set_credentials,
                                "Sets/recovers the credentials (auth file) for this experiment."),
                'vlbeer': Command(pipe.get_files_from_vlbeer,
                                  "Retrieves the antabfs, log, and flag files from vlbeer"),
                'makelis': Command(ccs.create_lis_files, "Create the .lis files in ccs."),
                'getlis': Command(ccs.get_lis_files, "Copies the .lis files from ccs to eee."),
                'modlis': Command(eee.get_passes_from_lisfiles,
                                  "Reads the correlator passes from the lis files and updates the header."),
                'checklis': Command(env.check_lisfiles, "Runs checklis.py in all .lis files."),
                'getdata': Command(eee.getdata, "Runs getdata.pl."),
                'j2ms2': Command(eee.j2ms2,
                                 "Runs j2ms2 with the specified params (modify them with the 'edit' command)."),
                'expname': Command(eee.update_ms_expname, "Runs expname.py (for e-EVN experiments)."),
                'metadata': Command(eee.get_metadata_from_ms,
                                    "Retrieves the observational metadata from the MS."),
                'standardplots': Command(eee.standardplots, "Runs standardplots."),
                'gv': Command(eee.open_standardplot_files, "Opens the standardplots files with gv."),
                'ysfocus': Command(eee.ysfocus, "Runs ysfocus.py"),
                'polswap': Command(eee.polswap, "Runs polswap.py"),
                'flag_weights': Command(eee.flag_weights, "Runs flag_weights.py"),
                'onebit': Command(eee.onebit, "Runs onebit.py"),
                'piletter': Command(eee.update_piletter,
                                    "Updates the PI letter with info on MS and PolConvert."),
                'tconvert': Command(eee.tconvert, "Runs tConvert"),
                'polconvert': Command(eee.polconvert, "Runs PolConvert (or prepares files to run it manually)."),
                'postpolconvert': Command(eee.post_polconvert, "Runs all required steps after run PolConvert."),
                'archive-fits': Command(eee.archive, "Runs archive in eee on the standard plots and " \
                                                     "FITS-IDI files"),
                'archive-pilet': Command(eee.send_letters, "Archives the PI letter."),
                'antab': Command(pipe.run_antab_editor, "Prepares .antab and antab_editor.py."),
                'uvflg': Command(pipe.create_uvflg, "Creates .uvflg from all log files."),
                'pyinput': Command(pipe.create_input_file, "Creates the input file for the EVN pipeline."),
                'pipe': Command(pipe.run_pipeline, "Runs the EVN Pipeline."),
                'comment_tasav': Command(pipe.comment_tasav_files, "Creates the .comment and .tasav file."),
                'feedback': Command(pipe.pipeline_feedback, "Runs the Pipeline Feedback script."),
                'archive-pipe': Command(pipe.archive, "Archives the pipeline results."),
                'append': Command(eee.append_antab, "Appends the Tsys and GC to the FITS-IDI files."),
                'ampcal': Command(pipe.ampcal,
                                  "Runs ampcal.sh script to incorporate the gain corrections into Grafana"),
                'pipelet': Command(eee.create_pipelet, "Creates the piletter_auth containing the credentials."),
                'issues': Command(eee.antenna_feedback, "Tells you where to store the observed problems " \
                                                        "(station feedback, aka to Grafana, and JIVE RedMine."),
                'nme': Command(eee.nme_report, "Tells you if you need to write an NME Report.")}
supsciers = ('agudo', 'bayandina', 'blanchard', 'burns', 'immer', 'marcote', 'minnie', 'murthy', 'nair', 'oh',
         'orosz', 'paragi', 'rmc', 'surcis', 'yang')

breakline = '\n'
help_exec = f"""[bold]Runs a single command of the experiment post-process.[/bold]

This method allows you even more granularity than 'run' as it will only run a single command from the post-processing.

The following commands are allowed:
{breakline.join(['- [bold green]'+c+'[/bold green] : '+all_commands[c].doc for c in all_commands])}

"""



def run(exp: experiment.Experiment, step1: Optional[str] = None, step2: Optional[str] = None,
        j2ms2par: Optional[str] = None):
    """Runs the post-process from a given step.
    """
    if j2ms2par is not None:
        exp.special_params = {'j2ms2': [par.strip() for par in j2ms2par.split(',')]}

    exp.log(f"\n\n\n{'#'*37}\n# Post-processing of {exp.expname} ({exp.obsdate}).\n"
            f"# Running on {dt.today().strftime('%d %b %Y %H:%M')} by {exp.supsci}.\n"
            f"Using evn_postprocess version {__version__}.")
    try:
        step_keys = list(all_steps.keys())
        if (exp.last_step is None) and (step1 is None):
            the_steps = step_keys
        elif (exp.last_step is not None) and (step1 is None):
            the_steps = step_keys[step_keys.index(exp.last_step)+1:]
            exp.log(f"Starting after the last sucessful step from a previous run ({exp.last_step}).", False)
            rich.print("[italic]Starting after the last sucessful step from a previous run " \
                       f"({exp.last_step})[/italic].")
        # step1 is not None
        elif step2 is None:
            assert step1 in step_keys, f"The introduced step1 {step1} is not recognized from the list {step_keys}."
            the_steps = step_keys[step_keys.index(step1):]
            exp.log(f"Starting at the step '{step1}'.")
            print(f"Starting at the step '{step1}'.")
        else:
            assert step2 in step_keys, f"The introduced step2 {step2} is not recognized from the list {step_keys}."
            the_steps = step_keys[step_keys.index(step1):step_keys.index(step2)]
            exp.log(f"Running only the following steps: {', '.join(the_steps)}.", False)
            print(f"Running only the following steps: {', '.join(the_steps)}.")
    except ValueError:
        print("ERROR: more than two steps have been introduced.\n"
              "Only one or two options are expected.")
        traceback.print_exc()
        sys.exit(1)
    except KeyError:
        print("ERROR: the introduced step ({step1}) is not recognized.\n"
              "Run the program with '-h' to see the expected options.")
        traceback.print_exc()
        sys.exit(1)

    try:
        for a_step in the_steps:
            if not all_steps[a_step](exp):
                raise RuntimeError(f"An error was found in {exp.expname} at the step {all_steps[a_step].__name__}")
            exp.last_step = a_step
            exp.store()
    except sch.ManualInteractionRequired:
        print('\n\nStopped for manual interaction (see above). Re-run once you have done your duty.')
        return

    exp.last_step = "Finished."
    exp.store()
    rich.print('\n[italic green]The post-processing has finished properly.[/italic green]')


def edit(exp: experiment.Experiment, param: str, value: str):
    """Edits PARAM with VALUE in the associated experiment.
    """
    assert param in edit_params, f"The provided PARAM {param} is not recognized. " \
                                 f"Only the following values are allow: {', '.join(edit_params)}"
    if param == 'refant':
        exp.refant = value.strip().capitalize()
    elif param == 'calsour':
        exp.sources_stdplot = [cs.strip() for cs in value.split(',')]
        # I leave this generic, as for e-EVN it can be tricky and it may complicate the parsing while still
        # it will work as expected.
        # for src in exp.sources_stdplot:
        #     assert src in exp.sources, f"The introduced source {src} was not observe in the observation."
    elif param == 'onebit':
        exp.special_params = {'onebit': [ant.strip().capitalize() for ant in value.split(',')]}
    elif param == 'polswap':
        for ant in value.split(','):
            exp.antennas[ant].polswap = True
    elif param == 'polconvert':
        for ant in value.split(','):
            exp.antennas[ant].polconvert = True
    elif param == 'target':
        for src in value.split(','):
            for exp_src in exp.sources:
                if exp_src.name == src:
                    exp_src.type = experiment.SourceType.target
    elif param == 'calibrator':
        for src in value.split(','):
            for exp_src in exp.sources:
                if exp_src.name == src:
                    exp_src.type = experiment.SourceType.calibrator
    elif param == 'fringefinder':
        for src in value.split(','):
            for exp_src in exp.sources:
                if exp_src.name == src:
                    exp_src.type = experiment.SourceType.fringefinder

    exp.store()
    rich.print('[italic green]Changes properly stored for experiment.[/italic green]')
    sys.exit(0)


def info(exp: experiment.Experiment):
    """Shows the info related to the experiment.
    """
    exp.print_blessed(outputfile='notes.md')
    sys.exit(0)


def last(exp: experiment.Experiment):
    """Returns the last step that run successfully from post-process in this experiment.
    """
    rich.print("\n\n" + f"[italic]The last step that successfully run for this experiment was " \
               f"[green]{exp.last_step}[/green][/italic].")
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description=description, prog=__prog__, usage=usage,
                                     formatter_class=RawTextRichHelpFormatter)
                                     # formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-e', '--expname', type=str, default=None,
                        help='Name of the EVN experiment (case-insensitive).\n[dim]By default recovered assuming you' \
                             ' run this from /data0/{supsci}/{EXPNAME}.[/dim]')
    parser.add_argument('-jss', '--supsci', type=str, default=None, help='Surname of the EVN Support Scientist.\n' \
                        '[dim]By default recovered assuming you run this from /data0/{supsci}/{EXPNAME}.[/dim]')
    parser.add_argument('--j2ms2par', type=str, default=None,
                        help='Additional attributes for j2ms2 (like the fo:XXXXX).')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {}'.format(__version__))
    subparsers = parser.add_subparsers(help='[bold]If no command is provided, the full postprocessing will run ' \
                                            'from the last successful step.[/bold]', dest='subpar')
    parser_run = subparsers.add_parser('run', help='Runs the post-process from a given step.', description=help_run,
                                       formatter_class=parser.formatter_class)
    parser_run.add_argument('step1', type=str, help='Step to start the post-process.')
    parser_run.add_argument('step2', type=str, default=None, nargs='?', help='Last step to run (included). ' \
                                                                  'By default None so it will run until the end.')
    parser_exec = subparsers.add_parser('exec', help='Executes a single function from post-processing.',
                                        description=help_exec, formatter_class=parser.formatter_class)
    parser_info = subparsers.add_parser('info', help='Shows the metadata associated to the experiment',
                                        description=help_info, formatter_class=parser.formatter_class)
    parser_last = subparsers.add_parser('last',
                                        help='Returns the last step that run successfully from the post-process.',
                                        description=help_last, formatter_class=parser.formatter_class)
    parser_edit = subparsers.add_parser('edit', help='Edits one of the parameters related to the experiment',
                                        description=help_edit, formatter_class=parser.formatter_class)
    parser_edit.add_argument('param', type=str, help='Modifier to apply.')
    parser_edit.add_argument('value', type=str, help='Value to write or change.')
    parser_exec.add_argument('command', type=str, help='Rusn a single command from the experiment post-processing.')

    args = parser.parse_args()

    if args.expname is None:
        args.expname = Path.cwd().name

    try:
        assert env.grep_remote_file('jops@ccs', '/ccs/var/log2vex/MASTER_PROJECTS.LIS', args.expname.upper()) != '', \
            f"The experiment name {args.expname} is not recognized (not present in MASTER_PROJECTS). " \
            "You may need to manually specify with --expname"
    except ValueError as e:
        rich.print(f"[italic red]The assumed eperiment code {args.expname} is not recognized.[/italic red]")
        rich.print('\n' + description)
        sys.exit(0)

    if args.supsci is None:
        args.supsci = Path.cwd().parent.name

    assert args.supsci in supsciers, f"It seems like the JIVE Support Scientist {args.supsci} is not recognized " \
                                f"from my database. You may need to use the --supsci option or ask for support."

    exp = experiment.Experiment(args.expname, args.supsci)
    exp.gui = dialog.Terminal()

    if exp.exists_local_copy():
        print('A local copy from a previous run has been found and restored.')
        exp = exp.load()

    if args.j2ms2par is not None:
        exp.special_params = {'j2ms2': [par.strip() for par in args.j2ms2par.split(',')]}

    if args.subpar is None:
        # Run the whole post-process
        run(exp)
    elif args.subpar == 'run':
        run(exp, args.step1, args.step2)
    elif args.subpar == 'info':
        info(exp)
    elif args.subpar == 'edit':
        edit(exp, args.param, args.value)
    elif args.subpar == 'last':
        last(exp)
    elif args.subpar == 'exec':
        assert args.command in all_commands.keys(), f"The provided command {args.command} is not recognized." \
                f"Accepted commands are: {', '.join(all_commands.keys())}. Run 'postprocess exec -h' for more info."


if __name__ == '__main__':
    main()


