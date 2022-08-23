#! /usr/bin/env python3
"""
"""
import os
import sys
import argparse
import traceback
import rich
from pathlib import Path
from datetime import datetime as dt
# from inspect import signature  # WHAT?  to know how many parameters has each function
from evn_postprocess.evn_postprocess import experiment
from evn_postprocess.evn_postprocess import scheduler as sch
from evn_postprocess.evn_postprocess import dialog
from evn_postprocess.evn_postprocess import environment as env


__version__ = 0.8
__prog__ = 'postprocess'
usage = "%(prog)s  [-h] [options]\n"
description = """Post-processing of EVN experiments.
The program runs the full post-processing for a correlated EVN experiment until distribution, following the steps described in the EVN Post-Processing Guide.

The program would retrieve the experiment code from the current working directory, and the associated Support Scientist from the parent directory. Otherwise they need to be specified manually.

The user can also specify to run only some of the steps or to start the process from a given step
(for those cases when the process has partially run previously). If the post-processing already run in teh past,
it will automatically continue from the last successful step that run.

"""

help_calsources = 'Calibrator sources to use in standardplots (comma-separated, no spaces). ' \
                  'If not provided, it will pick the fringefinders found in the .expsum file.'
help_steps = """Specify the step to start the post-processing (if you want to start it mid-way),
check with -h the available steps. If two steps are provided (comma-separated
without spaces), then it will run the steps from the first to the second one.
The available steps are:

    - setting_up : Sets up the experiment, creates the required folders in @eee and @pipe,
                   and copy the already-existing files (.expsum, .vix, etc).
    - lisfile : Produces a .lis file in @ccs and copies them to @eee.
    - checklis : Checks the existing .lis files and asks the user some parameters to continue.
    - ms : Gets the data for all available .lis files and runs j2ms2 to produce MS files.
    - plots : Runs standardplots.
    - msops : Runs the full MS operations like ysfocus, polswap, flag_weights, etc.
    - tconvert : Runs tConvert on all available MS files, and runs polConvert is required.
    - post_polconvert : if polConvert did run, then this steps renames the new *.PCONVERT
                        files and do standardplots on them.
    - archive : Sets the credentials for the experiment,
                create the pipe letter and archive all the data.
    - antab : Retrieves the .antab file to be used in the pipeline.
              If it was not generated, Opens antab_editor.py.
              Needs to run again once you have run antab_editor.py manually.
    - pipeinputs : Prepares a draft input file for the pipeline and recovers all needed files.
    - pipeline : Runs the EVN Pipeline for all correlated passes.
    - postpipe : Runs all steps to be done after the pipeline:
                 creates tasav, comment files, feedback.pl
    - last : Appends Tsys/GC and re-archive FITS-IDI and the PI letter.
             Asks to conduct the last post-processing steps.
"""

help_edit = """You can edit some of the parameters of the experiment.
Note that if you assign the values before they are read from the processing
normal tasks they may be overwriten.
The following parameters are allowed:
    - refant : change the reference antenna(s).
    - calsour : change the sources used for standardplots.
                If more than one, they must be comma-separated and with no spaces.
    - polconvert : marks the antennas to be pol converted.
    - polswap : marks the antennas to be pol swapped.
    - onebit :  marks the antennas to be corrected because they observed with one bit.
"""
help_gui = 'Type of GUI to use for interactions with the user:\n' \
           '- "terminal" (default): it uses the basic prompt in the terminal.\n' \
           '- "tui": uses the Terminal-based User Interface.\n' \
           '- "gui": uses the Graphical User Interface.'


def main():
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
    parser = argparse.ArgumentParser(description=description, prog=__prog__, usage=usage,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-e', '--expname', type=str, default=None,
                        help='Name of the EVN experiment (case-insensitive).')
    parser.add_argument('-jss', '--supsci', type=str, default=None, help='Surname of the EVN Support Scientist.')
    parser.add_argument('--info', default=False, action='store_true',
                        help='Returns the metadata from the experiment (all what is known to this moment to me).')
    parser.add_argument('--last', default=False, action='store_true',
                        help='Returns the last step conducted in a previous run.')
    parser.add_argument('--step', type=str, default=None, help=help_steps)
    parser.add_argument('--edit', type=str, nargs=2, default=None, help=help_edit, metavar=('PARAM', 'VALUE'))
    parser.add_argument('--j2ms2par', type=str, default=None,
                        help='Additional attributes for j2ms2 (like the fo:).')
    # parser.add_argument('--gui', type=str, default=None, help=help_gui)
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {}'.format(__version__))

    args = parser.parse_args()

    if args.expname is None:
        args.expname = Path.cwd().name
        print(f"\nAssuming the experiment code is {args.expname}.")

    if args.supsci is None:
        args.supsci = Path.cwd().parent.name
        print(f"Assuming the Support Scientist is {args.supsci}.\n")

    exp = experiment.Experiment(args.expname, args.supsci)
    exp.log(f"\n\n\n{'#'*10}\n# Post-processing of {exp.expname} ({exp.obsdate}).\n"
            f"# Running on: {dt.today().strftime('%d %b %Y %H:%M')}\n")

    # if args.gui == 'terminal' or args.gui is None:
    #     exp.gui = dialog.Terminal()
    # elif args.gui.lower() == 'tui':
    #     raise NotImplementedError("'tui' option not implemented yet.")
    # elif args.gui.lower() == 'gui':
    #     raise NotImplementedError("'gui' option not implemented yet.")
    # else:
    #     print(f"gui option not recognized. Expecting 'terminal', 'tui', or 'gui'. Obtained {args.gui}")
    #     sys.exit(1)

    if exp.cwd != Path.cwd():
        os.chdir(exp.cwd)
        exp.log(f"# Moved to the experiment folder\ncd{exp.cwd}", timestamp=False)

    # -n  parameter so it can ignore all the previous steps
    if exp.exists_local_copy():
        print('Restoring stored information from a previous run.')
        exp = exp.load()

    if args.last:
        print("\n\n" + f"The last step that run for this experiment was {exp.last_step}.")
        sys.exit(0)

    if args.info:
        exp.print_blessed()
        sys.exit(0)

    if args.edit is not None:
        edit_param = args.edit[0].strip()
        if edit_param == 'refant':
            exp.refant = args.edit[1].strip().capitalize()
        elif edit_param == 'calsour':
            exp.sources_stdplot = [cs.strip() for cs in args.edit[1].split(',')]
        elif edit_param == 'onebit':
            exp.special_params = {'onebit': [ant.strip().capitalize() for ant in args.edit[1].split(',')]}
        elif edit_param == 'polswap':
            for ant in args.edit[1].split(','):
                exp.antennas[ant].polswap = True
        elif edit_param == 'polconvert':
            for ant in args.edit[1].split(','):
                exp.antennas[ant].polconvert = True
        sys.exit(0)

    if args.j2ms2par is not None:
        exp.special_params = {'j2ms2': [par.strip() for par in args.j2ms2par.split(',')]}

    try:
        step_keys = list(all_steps.keys())
        if (exp.last_step is None) and (args.step is None):
            the_steps = step_keys
        elif (exp.last_step is not None) and (args.step is None):
            the_steps = step_keys[step_keys.index(exp.last_step)+1:]
            exp.log(f"Starting after the last sucessful step from a previous run ('{exp.last_step}').", False)
            print(f"Starting after the last sucessful step from a previous run ('{exp.last_step}').")
        else:
            if ',' in args.step:
                if args.step.count(',') > 1:
                    raise ValueError
                args.step = args.step.split(',')
                for a_step in args.step:
                    if a_step not in all_steps:
                        raise KeyError
                the_steps = step_keys[step_keys.index(args.step[0]):step_keys.index(args.step[1])]
                exp.log(f"Running only the following steps: {', '.join(the_steps)}.", False)
                print(f"Running only the following steps: {', '.join(the_steps)}.")
            else:
                if args.step not in all_steps:
                    raise KeyError
                the_steps = step_keys[step_keys.index(args.step):]
                exp.log(f"Starting at the step '{args.step}'.")
                print(f"Starting at the step '{args.step}'.")
    except ValueError:
        print("ERROR: more than two steps have been introduced.\n"
              "Only one or two options are expected.")
        traceback.print_exc()
        sys.exit(1)
    except KeyError:
        print("ERROR: the introduced step ({args.step}) is not recognized.\n"
              "Run the program with '-h' to see the expected options")
        traceback.print_exc()
        sys.exit(1)

    # TODO: This is temporal, until the script works completely
    if not os.path.isfile('processing_manual.log'):
        if (exp.eEVNname is not None) and (exp.expname != exp.eEVNname):
            env.shell_command('create_processing_log.py',
                              ['-o', 'processing_manual.log', '-e', exp.eEVNname, exp.expname], shell=True)
        else:
            env.shell_command('create_processing_log.py', ['-o', 'processing_manual.log', exp.expname], shell=True)
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


if __name__ == '__main__':
    main()


