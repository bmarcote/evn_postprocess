#!/usr/bin/env python3
"""
"""
import os
import sys
import glob
import time
import string
import random
import argparse
from pathlib import Path
import configparser
import logging
import subprocess
from datetime import datetime as dt
# from inspect import signature  # WHAT?  to know how many parameters has each function
from datetime import datetime
from evn_postprocess.evn_postprocess import experiment
from evn_postprocess.evn_postprocess import scheduler as sch
from evn_postprocess.evn_postprocess import dialog
from evn_postprocess.evn_postprocess import process_ccs as ccs
from evn_postprocess.evn_postprocess import process_eee as eee
from evn_postprocess.evn_postprocess import process_pipe as pipe


# Rename the file to __main__.py. Then it can be executed by python -m evn_postprocess

__version__ = 0.0
__prog__ = 'evn_postprocess.py'
usage = "%(prog)s  [-h]  <experiment_name>  <support_scientist>\n"
description = """Post-processing of EVN experiments.
The program runs the full post-process for a correlated EVN experiment, from retrieving the correlated products to run the EVN pipeline following the steps described in the EVN Post-Processing Guide.

The user can also specify to run only some of the steps or to start the process from a given step
(for those cases when the process has partially run previously).

The available steps are: TODO: UPDATE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    - showlog : produces a .lis file in @ccs and copies them to @eee.
    - checklis : checks the existing lis files and asks the user some parameters to continue.
    - j2ms2 : gets the data for all available .lis files and runs j2ms2 to produce MS files.
              Runs scale 1 bit if necessary.
    - standardplots : runs standardplots.
    - MSoperations : runs the full MS operations like ysfocus, polswap, flag_weights, etc.
    - tConvert : runs tConvert on all available MS files, and asks if polConvert is required.
    - archive : sets the credentials for the experiment, create the pipe letter and archive all the data.
    - prepipeline : retrieves all ANTAB, uvflg files, and prepares a draft input file for the pipeline.
    - pipeline : Runs the EVN Pipeline for all correlated passes.
    - postpipeline : runs all steps to be done after the pipeline: creates tasav, comment files, feedback.pl
    - letters : Asks to update the PI letter, and sends it and pipeletter. Also runs parsePIletter.py.

"""

help_calsources = 'Calibrator sources to use in standardplots (comma-separated, no spaces). ' \
                  'If not provided, it will pick the fringefinders found in the .expsum file.'
help_steps = 'Specify the step to start the post-processing (if you want to start it mid-way), ' \
             'check with -h the available steps. If two steps are provided (comma-separated without spaces), ' \
             'then it will run the steps from the first to the second one.'



def main():
    all_steps = {'setting_up': sch.setting_up_environment,
                 'lisfile': sch.preparing_lis_files,
                 'first_check': sch.first_manual_check,
                 'ms': sch.creating_ms,
                 'plots': sch.standardplots,
                 'MSoperations': sch.ms_operations,
                 'tconvert': sch.tconvert,
                 'archive': sch.archive,
                 'vlbeer': sch.getting_pipeline_files,
                 'pipeline': sch.pipeline,
                 'post_pipeline': sch.after_pipeline}
    parser = argparse.ArgumentParser(description=description, prog=__prog__, usage=usage,
                                    formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('expname', type=str, help='Name of the EVN experiment (case-insensitive).')
    parser.add_argument('supsci', type=str, help='Surname of the EVN Support Scientist.')
    parser.add_argument('-r', '--refant', type=str, default=None, help='Reference antenna.')
    parser.add_argument('-s', '--calsources', type=str, default=None, help=help_calsources)
    parser.add_argument('--onebit', type=str, default=None, help='Antennas recording at 1 bit (comma-separated)')
    parser.add_argument('--steps', type=str, default=None, help=help_steps)
    parser.add_argument('--j2ms2par', type=str, default=None, help='Additional attributes for j2ms2 (like the fo:).')
    # parser.add_argument('--gui', type=str, default=None, help='Additional attributes for j2ms2 (like the fo:).')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {}'.format(__version__))

    args = parser.parse_args()

    try:
        step_keys = list(all_steps.keys())
        if args.steps is None:
            the_steps = list(all_steps.keys())
        else:
            if ',' in args.steps:
                if args.steps.count(',') > 1:
                    raise ValueError
                args.steps = args.steps.split(',')
                for a_step in args.steps:
                    if a_step not in all_steps:
                        raise KeyError
                the_steps = step_keys[step_keys.index(args.steps[0]):step_keys.index(args.steps[1])]
            else:
                if args.steps not in all_steps:
                    raise KeyError
                the_steps = step_keys[step_keys.index(args.steps[0]):]
    except ValueError:
        print("ERROR: more than two steps have been introduced.\n" \
              "Only one or two options are expected.")
        sys.exit(1)
    except KeyError:
        print("ERROR: the introduced step ({args.steps}) is not recognized.\n" \
              "Run the program with '-h' to see the expected options")
        sys.exit(1)


    exp = experiment.Experiment(args.expname, args.supsci)

    if args.refant is not None:
        exp.refant = args.refant

    if args.calsources is not None:
        exp.sources_stdplot = [cs.strip() for cs in args.calsources.split(',')]

    if args.onebit is not None:
        exp.special_params = {'onebit': [ant.strip() for ant in args.onebit(',')]}

    if args.j2ms2par is not None:
        exp.special_params = {'j2ms2': [par.strip() for par in args.j2ms2par(',')]}


    exp.log(f"\n\n\n{'#'*10}\n# Post-processing of {exp.expname} (exp.obsdate).\n" \
            f"# Current date: {dt.today().strftime('%d %b %Y %H:%M')}\n")

    if exp.cwd != Path.cwd():
        os.chdir(exp.cwd)
        exp.log(f"# Moved to the experiment folder\ncd{exp.cwd}", timestamp=False)

    # -n  parameter so it can ignore all the previous steps
    if exp.exists_local_copy():
        exp.load()
        if args.steps is not None:
            print("Note that the steps will be ignored as a previous running is present." \
                  "Cancel and use '-n' if you want to overwrite all steps.")
            time.sleep(5)

    # And move to the cwd dir if needed
    # Create processing_log?  log dir.

    # Optional inputs:
    # - fo: for j2ms2.
    # - if refant set, then avoid first_manual_check.
    # - which gui to use

    # - In the case of e-EVN, it should be run until the ANTAB steps in all the other experiments.

    for a_step in the_steps:
        print(f'It would run: {a_step}')
        # if not all_steps[a_step](exp):
        #     raise RuntimeError(f"An error was found in {exp.expname} at the step {a_step.__name__}")

    print('\nThe post-processing has finished properly.')




if __name__ == '__main__':
    main()



