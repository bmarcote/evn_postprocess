#! /usr/bin/env python3
"""
"""
import os
import sys
import argparse
import traceback
import rich
import typer
from typing import Optional
from pathlib import Path
from datetime import datetime as dt
from evn_postprocess.evn_postprocess import experiment
from evn_postprocess.evn_postprocess import scheduler as sch
from evn_postprocess.evn_postprocess import dialog
from evn_postprocess.evn_postprocess import environment as env
from evn_postprocess.evn_postprocess import process_ccs as ccs
from evn_postprocess.evn_postprocess import process_eee as eee
from evn_postprocess.evn_postprocess import process_pipe as pipe


__version__ = '1.0a'
__prog__ = 'postprocess'
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
# all_exec_command = {'dirs': Command(env.create_all_dirs, "Creates the required folders (in eee and jop83): " \
#                                        "eee:/data0/{supsci}/{EXP}, jop83:$IN/{exp}, $OUT/{exp}", "$IN/{supsci}/{exp}"),
#                     'copyfiles': Command(env.copy_files, "Copies the .vix, .expsum, .piletter, .key/sum  to eee."),
#                     '': Command(eee.set_credentials, "Sets/recovers the credentials for this experiment."),
#                     '': Command(pipe.get_files_from_vlbeer, ""),
#                     'createlis': Command(ccs.create_lis_files, "Create the .lis files in ccs."),
#                     'getlis': Command(ccs.get_lis_files, "Copies the .lis files from ccs to eee."),
#                     'modlis': Command(eee._get_passes_from_lisfiles, "Reads the correlator passes from the lis files" \
#                                                                      " and updates the header."),
#                     'checklis': Command(env.check_lisfiles, "Runs checklis.py in all .lis files."),
#                     'getdata': Command(eee.getdata, "Runs getdata.pl."),
#                     'j2ms2': Command(eee.j2ms2, "Runs j2ms2 with the specified params (modify them with the 'edit'" \
#                                                 "command)."),
#                     'expname': Command(eee.update_ms_expname, "Runs expname.py (for e-EVN experiments)."),
#                     'metadata': Command(eee.get_metadata_from_ms, "Retrieves the observational metadata from the MS."),
#                     'standardplots': Command(eee.standardplots, "Runs standardplots."),
#                     'gv': Command(eee.open_standardplot_files, "Opens the standardplots files with gv."),
#                     'ysfocus': Command(eee.ysfocus, "Runs ysfocus.py"),
#                     'polswap': Command(eee.polswap, "Runs polswap.py"),
#                     'flag_weights': Command(eee.flag_weights, "Runs flag_weights.py"),
#                     'onebit': Command(eee.onebit, "Runs onebit.py"),
#                     'piletter': Command(eee.update_piletter, "Updates the PI letter with info on MS and PolConvert."),
#                     'tconvert': Command(eee.tconvert, "Runs tConvert"),
#                     'polconvert': Command(eee.polconvert, "Runs PolConvert (or prepares files to run it manually)."),
#                     'postpolconvert': Command(eee.post_polconvert, "Runs all required steps after run PolConvert."),
#                     'archive': Command(eee.archive, "Runs archive in eee"),
#                     '': Command(pipe.run_antab_editor, ""),
#                     '': Command(pipe.create_uvflg, ""),
#                     '': Command(pipe.create_input_file, ""),
#                     '': Command(pipe.run_pipeline, ""),
#                     '': Command(pipe.comment_tasav_files, ""),
#                     '': Command(pipe.pipeline_feedback, ""),
#                     '': Command(pipe.archive, ""),
#                     '': Command(eee.append_antab, ""),
#                     '': Command(pipe.ampcal, ""),
#                     '': Command(eee.create_pipelet, ""),
#                     '': Command(eee.send_letter, ""),
#                     '': Command(eee.antenna_feedback, ""),
#                     '': Command(eee.nme_report, "")}
# supsciers = ('agudo', 'bayandina', 'blanchard', 'burns', 'immer', 'marcote', 'minnie', 'murthy', 'nair', 'oh',
#              'orosz', 'paragi', 'rmc', 'surcis', 'yang')

exp = None
app = typer.Typer(help="Runs the full post-processing of an EVN experiment.", rich_markup_mode='rich')

class Command(object):
    def __init__(self, command, doc: str):
        """Executes the command (which must be a Python function), that has the associated doc string for help.
        """
        self.command = command
        self.doc = doc


@app.command()
def run(step1: Optional[str] = typer.Argument(None, help='Start the post-processing from this step.'),
        step2: Optional[str] = typer.Argument(None, help='Run the post-processing until this step (included).'),
        j2ms2par: str = typer.Option(None, help='Additional attributes for j2ms2 (like the fo:XXXXXX).' \
                                                                ' Comma separated if multiple.')):
    """[bold]Runs the post-process from a given step[/bold].

    Four different approaches can be used:

    [italic]postprocess run[/italic]  (no param)  - Runs the entire post-process (or from the last run step).
    [italic]postprocess run STEP1[/italic]   - Runs from STEP1 until the end (or until manual interaction is
                                               required).
    [italic]postprocess run STEP1,[/italic]        - Only runs STEP1.
    [italic]postprocess run STEP1,STEP2[/italic]   - Runs from STEP1 until STEP2 (both included).

    The available steps are:
        - [bold green]setting_up[/bold green] : Sets up the experiment, creates the required folders in
            @eee and @pipe, and copy the already-existing files (.expsum, .vix, etc).
        - [bold green]lisfile[/bold green] : Produces a .lis file in @ccs and copies them to @eee.
        - [bold green]checklis[/bold green] : Checks the existing .lis files.
        - [bold green]ms[/bold green] : Gets the data for all available .lis files and runs j2ms2 to produce
            MS files.
        - [bold green]plots[/bold green] : Runs standardplots.
        - [bold green]msops[/bold green] : Runs the full MS operations like ysfocus, polswap, flag_weights, etc.
        - [bold green]tconvert[/bold green] : Runs tConvert on all available MS files, and runs polConvert
            is required.
        - [bold green]post_polconvert[/bold green] : if polConvert did run, then this steps renames the
            new *.PCONVERT files and do standardplots on them.
        - [bold green]archive[/bold green] : Sets the credentials for the experiment,
            create the pipe letter and archive all the data.
        - [bold green]antab[/bold green] : Retrieves the .antab file to be used in the pipeline.
            If it was not generated, Opens antab_editor.py.
            Needs to run again once you have run antab_editor.py manually.
        - [bold green]pipeinputs[/bold green] : Prepares a draft input file for the pipeline and recovers
            all needed files.
        - [bold green]pipeline[/bold green] : Runs the EVN Pipeline for all correlated passes.
        - [bold green]postpipe[/bold green] : Runs all steps to be done after the pipeline:
            creates tasav, comment files, feedback.pl
        - [bold green]last[/bold green] : Appends Tsys/GC and re-archive FITS-IDI and the PI letter.
            Asks to conduct the last post-processing steps.
    """
    if j2ms2par is not None:
        exp.special_params = {'j2ms2': [par.strip() for par in j2ms2par.split(',')]}

    exp.log(f"\n\n\n{'#'*37}\n# Post-processing of {exp.expname} ({exp.obsdate}).\n"
            f"# Running on {dt.today().strftime('%d %b %Y %H:%M')} by {exp.supsci}.\n")
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


@app.command()
def edit(param: str, value: str):
    """[bold]Edit some of the parameters related to the experiment[/bold].

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
    raise typer.Exit()


@app.command()
def info():
    """[bold]Shows the info related to the given experiment (all what postprocess knows until the presentmoment).[/bold]

    It will also write this information down into a 'notes.md' file is this does not exist.
    """
    print(exp)
    exp.print_blessed(outputfile='notes.md')
    raise typer.Exit()


@app.command()
def last():
    """Returns the last step that run successfully from post-process in this experiment.
    """
    rich.print("\n\n" + f"[italic]The last step that successfully run for this experiment was " \
               f"[green]{exp.last_step}[/green][/italic].")
    raise typer.Exit()


def version_callback(value: bool):
    if value:
        print(f"Post-process Version:  {__version__}")
        raise typer.Exit()


# @app.command()
# def exec():
#     pass



# typer.Exit(code=0)
# typer.launch("https://typer.tiangolo.com")

@app.callback(help="""Runs the full post-processing steps on an EVN Experiment.

    It will go through the steps between correlation to distribution in a semi-automatic way.
    Both the experiment name and associated support scientist would be retrieved assuming that this is
    executed from a folder following [italic]/data0/{supsci}/{EXPNAME}[/italic] path.
    Otherwise, they can be manually specifed with the --expname and --supsci options.

    There are some commands that can be used with postprocess:
        - [green]run[/green] : the basic mode. To run the actuall post-process.
            With no extra arguments: it will run everything from the beginning (or last previous step that run).
            With STEP1 argument: it runs from STEP1 until the end.
            With STEP1 STEP2 arguments: it runs from STEP1 until STEP2 (both included).
        - [green]info[/green] : shows the info related to the experiment (all what is known until this moment).
        - [green]edit[/green] : edits some of the parameters associated to the experiment. --help for more info.
        - [green]last[/green] : shows the last successful step that run for this experiment.
""")
def main(version: Optional[bool] = typer.Option(None, "--version", callback=version_callback, is_eager=True),
         expname: str = typer.Option(None, help="Experiment code (case insensitive)."),
         supsci: str = typer.Option(None, help='Associated JIVE Support Scientist (surname; case insensitive).')):
    """Runs the full post-processing steps on an EVN Experiment.

    It will go through the steps between correlation to distribution in a semi-automatic way.
    Both the experiment name and associated support scientist would be retrieved assuming that this is
    executed from a folder following [italic]/data0/{supsci}/{EXPNAME}[/italic] path.
    Otherwise, they can be manually specifed with the --expname and --supsci options.

    There are some commands that can be used with postprocess:
        - [green]run[/green] : the basic mode. To run the actuall post-process.
            With no extra arguments: it will run everything from the beginning (or last previous step that run).
            With STEP1 argument: it runs from STEP1 until the end.
            With STEP1 STEP2 arguments: it runs from STEP1 until STEP2 (both included).
        - [green]info[/green] : shows the info related to the experiment (all what is known until this moment).
        - [green]edit[/green] : edits some of the parameters associated to the experiment. --help for more info.
        - [green]last[/green] : shows the last successful step that run for this experiment.
    """
    if expname is None:
        expname = Path.cwd().name

    assert env.grep_remote_file('jops@ccs', '/ccs/var/log2vex/MASTER_PROJECTS.LIS', expname.upper()) != '', \
        f"The experiment name {expname} is not recognized (not present in MASTER_PROJECTS). " \
        "You may need to manually specify with --expname"

    if supsci is None:
        supsci = Path.cwd().parent.name

    assert supsci in supsciers, f"It seems like the JIVE Support Scientist {supsci} is not recognized " \
                                f"from my database. You may need to use the --supsci option or ask for support."

    global exp
    exp = experiment.Experiment(expname, supsci)
    exp.gui = dialog.Terminal()

    if exp.exists_local_copy():
        print('A local copy from a previous run has been found and restored.')
        exp = exp.load()


if __name__ == '__main__':
    app()


