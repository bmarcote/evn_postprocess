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

exp = None
app = typer.Typer(help="Runs the full post-processing of an EVN experiment.", rich_markup_mode='rich')


@app.command()
def run(step1: Optional[str] = typer.Argument(None, help='Start the post-processing from this step.'),
        step2: Optional[str] = typer.Argument(None, help='Run the post-processing until this step (included).'),
        j2ms2par: str = typer.Option(None, help='Additional attributes for j2ms2 (like the fo:).' \
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
            print(f"[italic]Starting after the last sucessful step from a previous run ({exp.last_step})[/italic].")
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

    print('\nThe post-processing has finished properly.')





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
    print('Changes properly stored for experiment.')
    raise typer.Exit()


@app.command()
def info():
    """[bold]Shows the info related to the given experiment (all what postprocess knows until this moment).[/bold]

    It will also write this information down into a 'notes.md' file is this does not exist.
    """
    exp.print_blessed(outputfile='notes.md')
    raise typer.Exit()


@app.command()
def last():
    """Returns the last step that run successfully from post-process in this experiment.
    """
    print("\n\n" + f"The last step that run for this experiment was {exp.last_step}.")
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

@app.callback()
def main(version: Optional[bool] = typer.Option(None, "--version", callback=version_callback, is_eager=True),
         expname: str = typer.Option(None, help="Experiment code (case insensitive)."),
         supsci: str = typer.Option(None, help='Associated JIVE Support Scientist (surname; case insensitive).')):
    """Runs the full post-processing steps on an EVN Experiment.

    By default it will retrieve the experiment name and associated Support Scientist from the current
    directory where this script runs. Otherwise they can be manually specified.
    """
    if expname is None:
        expname = Path.cwd().name
        print(f"\nAssuming the experiment code is {expname}.")

    if supsci is None:
        supsci = Path.cwd().parent.name
        print(f"Assuming the JIVE Support Scientist is {supsci}.\n")

    exp = experiment.Experiment(expname, supsci)
    exp.gui = dialog.Terminal()

    if exp.exists_local_copy():
        print('Restoring stored information from a previous run.')
        exp = exp.load()


if __name__ == '__main__':
    app()


