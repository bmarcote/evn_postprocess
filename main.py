#!/usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.


Usage: post_processing.py  <expname>

Options:
    expname : str   The experiment name to be processed (case insensitive).


Version: 0.0
Date: Sep 2019
Written by Benito Marcote (marcote@jive.eu)
"""


import os
import sys
import argparse
import ConfigParser
import logging
from datetime import datetime
from . import metadata

# Rename the file to __main__.py. Then it can be executed by python -m evn_postprocess

__version__ = 0.1
__prog__ = 'evn_postprocess.py'
description = 'Post-processing of EVN experiments.'
usage = "%(prog)s [-h]  experiment_name  support_scientist"

help_calsources = 'Calibrator sources to use in standarplots (comma-separated, no spaces). If not provided, the user will be asked at due time'

# Input parameters
parser.argparse.ArgumentParser(description=description, prog=__prog__, usage=usage)
parser.add_argument('expname', type=str, help='Name of the EVN experiment.')
parser.add_argument('supsci', type=str, help='Surname of EVN Support Scientist.')
parser.add_argument('refant', type=str, help='Reference antenna.')
parser.add_argument('--sour', 'calsources', type=str, default=None, help=help_calsources)
parser.add_argument('--version', action='version', version='%(prog)s {}'.format(__version__))
args = parser.parse_args()


# If required, move to the required directory (create it if needed).
expdir = '/data0/{}/{}'.format(args.supsci, args.expname.upper())
if not os.path.isdir(expdir):
    os.makedirs(expdir)

os.chdir(expdir)



# Passing the input parameters and read the config file.
config = ConfigParser.ConfigParser()

# If file set from parameters use it, otherwise the one in program's directory.
config.read('./setup.inp')



# Logger
logger = logging.getLogger(__name__)
logger_out = logging.StreamHandler(stream=sys.stdout)
logger_err = logging.FileHandler(filename=config.defaults()['pathlogdir'] + '/error_messages.log', filemode='a')

logcmd = logging.Logger()
logcmd_out = logcmd.StreamHandler(stream=sys.stdout)
logcmd_cmd = logcmd.FileHandler(filename=config.defaults()['pathcommands'], filemode='a')

logger.addHandler(logger_out)
logger.addHandler(logger_err)
logcmd.addHandler(logcmd_out)
logcmd.addHandler(logger_cmd)






# It creates the experiment object
exp = metadata.Experiment(args.expname)

logcmd.info('Processing of EVN experiment {} (observed on {})'.format(exp.expname, exp.obsdatetime.strftime('%d %b %Y')))
logcmd.info('Date: {}\n'.format(datetime.today().strftime('%d %b %Y')))



# Should make a check that all required computers are accessible!
# actions.check_systems_up()

actions.get_lis_vex(exp.expname)

actions.can_continue('Is the lis file OK and can I continue?')

actions.get_data(exp.expname)

actions.j2ms2(exp.expname)



if args.calsources is None:
    args.calsources = actions.ask_user('Please, introduce the sources to be used for standardplots')

# Open produced plots, ask user if wants to continue / repeate plots with different inputs / q:
actions.standardplots(expname, args.refant, args.calsources)


