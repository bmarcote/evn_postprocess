#!/usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.

"""
import os
import sys
import glob
import string
import random
import argparse
import configparser
import logging
import subprocess
from inspect import signature
from datetime import datetime
# from src import metadata
# from src import actions
# from src import process_eee as eee
# from src import process_pipe as pipe

# Rename the file to __main__.py. Then it can be executed by python -m evn_postprocess

__version__ = 0.5
__prog__ = 'evn_postprocess.py'
usage = "%(prog)s [-h]  <experiment_name>  <support_scientist>  <refant>"
description = """Post-processing of EVN experiments.
The program runs the full post-process for a correlated EVN experiment, from retrieving the correlated products to run the EVN pipeline following the steps described in the EVN Post-Processing Guide (see the JIVE Wiki: http://www.jive.nl/jivewiki/doku.php?id=evn:supportscientists).

The user can also specify to run only some of the steps or to start the process from a given step
(for those cases when the process has partially run previously).

The available steps are:

    - showlog : produces a .lis file in @ccs and copies them to @eee.
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

help_calsources = 'Calibrator sources to use in standardplots (comma-separated, no spaces). If not provided, the user will be asked at due time'
help_steps = """Run only the specified steps (comma-separated list of steps).
Run with -h to see the available steps. If only one provided, then it runs the program from that step to the end.
If multiple provided, only runs the specified steps.
"""


# From Python 3.6 dicts keep order of keys.
# all_steps = ['showlog', 'j2ms2', 'standardplots', 'MSoperations', 'tConvert', 'archive',
#                     'prepipeline', 'pipeline', 'postpipeline', 'letters']


all_steps = {'eee_folders': [eee.folders],
             'showlog': [eee.ccs],
             'pi_expsum': [actions.get_pi_from_expsum, actions.get_passes_from_lisfiles],
             'j2ms2': [eee.getdata, eee.j2ms2, eee.onebit],
             'MSmetadata': [actions.append_freq_setup_from_ms_to_exp],
             'standardplots': [eee.standardplots],
             'MSoperations': [eee.MSoperations],
             'tConvert': [eee.tConvert, eee.polConvert],
             'archive': [eee.letters, eee.archive],
             'pipe_folders': [None], # [pipe.folders],
             'prepipeline': [None], # pipe.pre_pipeline
             'pipeline': [None], # pipe.pipeline
             'postpipeline': [None], # pipe.post_pipeline
             'letters': [None] # pipe.ampcal, eee.send_letters
             }

# Steps hidden for the user but that they need to be triggered under all circunstances.
wild_steps = ['eee_folders', 'pi_expsum', 'MSmetadata', 'pipe_folders']


if __name__ == '__main__':
    # Input parameters
    parser = argparse.ArgumentParser(description=description, prog=__prog__, usage=usage,
                                    formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('expname', type=str, help='Name of the EVN experiment.')
    parser.add_argument('supsci', type=str, help='Surname of EVN Support Scientist.')
    parser.add_argument('refant', type=str, help='Reference antenna.')
    parser.add_argument('-s', '--calsources', type=str, default=None, help=help_calsources)
    parser.add_argument('--onebit', type=str, default=None,
                         help='Antennas recording at 1 bit (comma-separated)')
    parser.add_argument('--steps', type=str, default=None, help=help_steps)
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {}'.format(__version__))

    args = parser.parse_args()


    # Gets the steps that need to be executed
    if args.steps is None:
        args.steps = all_steps
    else:
        args.steps = actions.parse_steps(args.steps, all_steps, wild_steps=wild_steps)


    # # TODO: Logger. To remove? Better implemantation?
    log_cmd = logging.getLogger('Executed commands')
    log_cmd.setLevel(logging.INFO)
    # log_cmd_file = logging.FileHandler('./processing.log')
    # # log_cmd_stdout = logging.StreamHandler(sys.stdout)
    # log_cmd_file.setFormatter(logging.Formatter('\n\n%(message)s\n'))
    # log_cmd.addHandler(log_cmd_file)
    # # log_cmd.addHandler(log_cmd_stdout)

    log_full = logging.getLogger('Commands full log')
    log_full.setLevel(logging.INFO)
    # log_full_file = logging.FileHandler('./full_log_output.log')
    # log_full.addHandler(log_full_file)


    # It creates the experiment object
    exp = metadata.Experiment(args.expname)

    print(f"Processing experiment {exp.expname}.\n")
    print(f"Observation Date: {exp.obsdatetime.strftime('%d %b %Y')} ({exp.obsdatetime.strftime('%y%m%d')}).")
    print(f"Current Date: {datetime.today().strftime('%d %b %Y')}.\n")

    # TODO: Should make a check that all required computers are accessible!
    # actions.check_systems_up()
    for a_step_name in args.steps:
        for a_step in all_steps[a_step_name]:
            if a_step is not None:
                if len(signature(a_step).parameters) == 1:
                    a_step(exp)
                elif len(signature(a_step).parameters) == 2:
                    a_step(exp, args)
                else:
                    # Should never happend
                    raise ValueError(f"Function {a_step} has unexpected number of arguments")

    print('The post-processing pipeline finished happily.\n\nBye.')
    print('Please continue manually in pipe.')
    # Work done!!



