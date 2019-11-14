#!/usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the eee computer.
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
from datetime import datetime
from epp import metadata
from epp import actions

# Rename the file to __main__.py. Then it can be executed by python -m evn_postprocess

__version__ = 0.3
__prog__ = 'evn_postprocess.py'
description = 'Post-processing of EVN experiments.'
usage = "%(prog)s [-h]  experiment_name  support_scientist  refant"

help_calsources = 'Calibrator sources to use in standarplots (comma-separated, no spaces). If not provided, the user will be asked at due time'


# If required, move to the required directory (create it if needed).
expdir = '/data0/{}/{}'.format(args.supsci, args.expname.upper())
if not os.path.isdir(expdir):
    os.makedirs(expdir)

os.chdir(expdir)
print(f"Moved to {expdir}.")
print("Two log files will be created:")
print("  - processing.log: contains the executed commands and very minimal output.")
print("  - full_log_output.log: contains the full output received from all commands.\n\n")


# Passing the input parameters and read the config file.
config = configparser.ConfigParser()

# If file set from parameters use it, otherwise the one in program's directory.
config.read(os.path.abspath('../' + sys.argv[0][:-8]) + '/setup.inp')

# Create the log directories if they do not exist
# for logdir in (config.defaults()['pathlogdir'], config.defaults()['pathoutput']):
#     if not os.path.isdir(logdir):
#         os.mkdir(logdir)

# Logger
log_cmd = logging.getLogger('Executed commands')
log_cmd.setLevel(logging.INFO)
log_cmd_file = logging.FileHandler('./processing.log')
log_cmd_stdout = logging.StreamHandler(sys.stdout)
log_cmd_file.setFormatter(logging.Formatter('\n\n%(message)s\n'))
log_cmd.addHandler(log_cmd_file)
# log_cmd.addHandler(log_cmd_stdout)

log_full = logging.getLogger('Commands full log')
log_full.setLevel(logging.INFO)
log_full_file = logging.FileHandler('./full_log_output.log')
log_full.addHandler(log_full_file)



# It creates the experiment object
exp = metadata.Experiment(args.expname)

# log_cmd.info('#'*82)
for a_log in (log_cmd, log_full):
    a_log.info('Processing experiment {} observed on {} ({}).'.format(exp.expname,
                                    exp.obsdatetime.strftime('%d %b %Y'), exp.obsdatetime.strftime('%y%m%d')))
    a_log.info('Current Date: {}\n'.format(datetime.today().strftime('%d %b %Y')))


print('Processing experiment {} observed on {} ({}).'.format(exp.expname, exp.obsdatetime.strftime('%d %b %Y'),
                                                    exp.obsdatetime.strftime('%y%m%d')))

# Should make a check that all required computers are accessible!
# actions.check_systems_up()


actions.get_lis_vex(exp.expname, config['computers']['ccs'], config['computers']['piletter'],
                    eEVNname=exp.eEVNname)

    # print("\n\nYou SHOULD check now the lis files and modify them if needed.")
actions.can_continue('Check the lis file(s) and modify them if needed. Can I continue?')

actions.get_data(exp.expname, eEVNname=exp.eEVNname)

actions.j2ms2(exp.expname)

# NOTE: this step must be conducted always.
# Retrieve the information from the MS and appends in in exp (antennas, sources, freqs.)
exp.get_setup_from_ms(glob.glob(f"{exp.expname.lower()}*.ms")[0])

# NOTE: I should probably write the output? standardplots do it.  I need to do it again for line exp.
# At least to know which IF(s) is in the line and which one is from the continuum.


# 1-bit scaling. Only runs if provided.
# If not provided, checks that no 1-bit stations are in the vex file.
# If 1-bit antennas are present somewhere, it asks user to confirm that no correction is required
# or to provide the list of stations.
if args.onebit is not None:
    actions.scale1bit(exp.expname, args.onebit)
else:
    # Checks if there is some station that recorded at 1bit in the vex file (it may or may not
    # affect to this experiment.
    if actions.station_1bit_in_vix(f"{exp.expname}.vix"):
        scale1bit_stations = actions.ask_user("Are you sure scale1bit is not required? Specify the affected stations or 'none' otherwise")
        if scale1bit_stations is not 'none':
            actions.scale1bit(exp.expname, scale1bit_stations)


if args.calsources is None:
    args.calsources = actions.ask_user(f"""Please, introduce the sources to be used for standardplots
as a comma-separated list (the MS contains: {', '.join(exp.sources)})""")

# Open produced plots, ask user if wants to continue / repeate plots with different inputs / q:
while True:
    try:
        run_standardplots = True
        if (len(glob.glob(f"{exp.expname.lower()}*ps")) > 0) or \
           (len(glob.glob(f"{exp.expname.lower()}*ps.gz")) > 0):
            run_standardplots = actions.yes_or_no_question('Plots exist. Run standardplots again?')

        if run_standardplots:
            actions.standardplots(exp.expname, args.refant, args.calsources)
            # Get all plots done and show them in the best order:
            standardplots = []
            for plot_type in ('weight', 'auto', 'cross', 'ampphase'):
                standardplots += glob.glob(f"{exp.expname.lower()}*{plot_type}*.ps")

            for a_plot in standardplots:
                actions.shell_command("gv", a_plot)

            answer = actions.yes_or_no_question('Are the plots OK? No to pick other sources/stations')
            if answer:
                break

            args.calsources = actions.ask_user(f"""Please, introduce the sources to be used for standardplots
    as a comma-separated list (the MS contains: {', '.join(exp.sources)})""")
        else:
            break

    except Exception as e:
        # NOTE: To implement. Check errors...
        print(f"WARNING: Standardplots crashed ({e}). But no implementation yet. Continuing..")
        break


weight_threshold = actions.ask_user("A couple of questions:\nWhich weight flagging threshold should be used?", valtype=float)
swap_pols = actions.yes_or_no_question("Is polswap required?")

if swap_pols:
    swap_pol_ants = actions.ask_user("List the antennas requiring swapping polarizations (comma-separated list)",
                                     accepted_values=[*exp.antennas])
    for a_swap_ant in swap_pol_ants:
        for msfile in glob.glob(f"{exp.expname.lower()}*.ms"):
            actions.shell_command("polswap.py", [msfile, a_swap_ant])


if ('ys' in exp.antennas) or ('YS' in exp.antennas) or ('Ys' in exp.antennas):
    for msfile in glob.glob(f"{exp.expname.lower()}*.ms"):
        actions.shell_command("ysfocus.py", msfile)
else:
    print('\nYebes is not in the array.\n')

# I keep it separately as Ho is not commonly in EVN observations
if ('ho' in exp.antennas) or ('HO' in exp.antennas) or ('Ho' in exp.antennas):
    print('\nHobart is in the array:\n')
    for msfile in glob.glob(f"{exp.expname.lower()}*.ms"):
        actions.shell_command("ysfocus.py", msfile)


# Flag weights
# TODO: Check why the output of flag_weights is not written in the terminal
for msfile in glob.glob(f"{exp.expname.lower()}*.ms"):
    actions.shell_command("flag_weights.py", [msfile, str(weight_threshold)])




actions.can_continue('Is everything ready to run tConvert? You can update the PI letter in the mean time')


for i, msfile in enumerate(glob.glob(f"{exp.expname.lower()}*.ms")):
    actions.shell_command("tConvert", [msfile, f"{exp.expname.lower()}_{i+1}_1.IDI"])


actions.can_continue('If PolConvert is required, do it manually NOW and then continue')

# pol_convert_ants = actions.ask_user("Are there antennas requiring Pol Convert? (provide comma-separated list)",
#                                     accepted_values=['no', *exp.antennas])

# if pol_convert_ants is not 'no':
#     actions.can_continue('Please, run PolConvert manually and let me know if I can continue?')



# Preparations for archive

# If the auth file exists, take the username and password from it. Otherwise create a new one.

# NOTE: This should always run
if len(glob.glob("*_*.auth")) == 1:
    # the file should have the form username_password.auth.
    exp.set_credentials( *glob.glob("*_*.auth").split('.')[0].split('_')  )
    if not os.path.isfile(f"{exp.expname.lower()}.pipelet"):
        actions.shell_command("pipelet.py", [exp.expname.lower(), args.supsci])

elif len(glob.glob("*_*.auth")) > 1:
    answer = actions.ask_user("WARNING: multiple auth files found. Please introduce username and password (space separated)")
    exp.set_credentials( *[a.strip() for a in answer.split(' ')] )
    actions.shell_command("touch", f"{exp.credentials.username}_{exp.credentials.password}.auth")
    actions.shell_command("pipelet.py", [exp.expname.lower(), args.supsci])
else:
    possible_char = string.digits + string.ascii_letters
    exp.set_credentials(username=exp.expname.lower(), password="".join(random.sample(possible_char, 12)))
    actions.shell_command("touch", f"{exp.credentials.username}_{exp.credentials.password}.auth")
    actions.shell_command("pipelet.py", [exp.expname.lower(), args.supsci])


# NOTE: Should I mention in the log and terminal?

# Compress all figures from standardplots
actions.shell_command('gzip', '*ps', shell=True)

actions.archive("-auth", exp, f"-n {exp.credentials.username} -p {exp.credentials.password}")
actions.archive("-stnd", exp, f"{exp.expname.lower()}.piletter *ps.gz")
actions.archive("-fits", exp, "*IDI*")



# Work at eee done!!



if __name__ == '__main__':
    # Input parameters
    parser = argparse.ArgumentParser(description=description, prog=__prog__, usage=usage)
    parser.add_argument('expname', type=str, help='Name of the EVN experiment.')
    parser.add_argument('supsci', type=str, help='Surname of EVN Support Scientist.')
    parser.add_argument('refant', type=str, help='Reference antenna.')
    parser.add_argument('-s', '--calsources', type=str, default=None, help=help_calsources)
    parser.add_argument('--onebit', type=str, default=None, help='Antennas that observed at 1 bit (comma-separated)')
    parser.add_argument('--steps', type=str, default=None, help='All steps to run.')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {}'.format(__version__))
    # Add one line with the steps to run. By default all the script
    args = parser.parse_args()




