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
import metadata
import actions


def ccs(exp):
    """Runs the initial post-processing steps in ccs: showlog, retrieving the lis, vix files from ccs,
    Running checklis, and retrieving the PI letter and .expsum files.
    """
    return actions.get_lis_vex(exp.expname, config['computers']['ccs'], config['computers']['piletter'],
                    eEVNname=exp.eEVNname)


actions.can_continue('Check the lis file(s) and modify them if needed. Can I continue?')


def getdata(exp):
    """Gets the data into eee from all existing .lis files from the given experiment.
    inputs: exp : metadata.Experiment
    """
    return actions.get_data(exp.expname, eEVNname=exp.eEVNname)


def j2ms2(exp):
    """Runs j2ms2 on all existing .lis files from the given experiment.
    inputs: exp : metadata.Experiment
    """
    return actions.j2ms2(exp.expname)


# NOTE: this step must be conducted always.
# Retrieve the information from the MS and appends in in exp (antennas, sources, freqs.)
exp.get_setup_from_ms(glob.glob(f"{exp.expname.lower()}*.ms")[0])


def onebit(exp, args):
    # 1-bit scaling. Only runs if provided.
    # If not provided, checks that no 1-bit stations are in the vex file.
    # If 1-bit antennas are present somewhere, it asks user to confirm that no correction is required
    # or to provide the list of stations.
    if args.onebit is not None:
        return actions.scale1bit(exp.expname, args.onebit)
    else:
        # Checks if there is some station that recorded at 1bit in the vex file (it may or may not
        # affect to this experiment.
        if actions.station_1bit_in_vix(f"{exp.expname}.vix"):
            scale1bit_stations = actions.ask_user("Are you sure scale1bit is not required? Specify the affected stations or 'none' otherwise")
            if scale1bit_stations is not 'none':
                return actions.scale1bit(exp.expname, scale1bit_stations)
    return False


def standardplots(exp, args):
    if args.calsources is None:
        args.calsources = actions.ask_user(f"""Please, introduce the sources to be used for standardplots
    as a comma-separated list (the MS contains: {', '.join(exp.sources)})""")

    # Open produced plots, ask user if wants to continue / repeate plots with different inputs / q:
    while True:
        try:
            run_standardplots = True
            if (len(glob.glob(f"{exp.expname.lower()}*ps")) > 0) or \
               (len(glob .glob(f"{exp.expname.lower()}*ps.gz")) > 0):
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
                    return True

                args.calsources = actions.ask_user(f"""Please, introduce the sources to be used for standardplots
        as a comma-separated list (the MS contains: {', '.join(exp.sources)})""")
                args.refant = actions.ask_user(f"""Please, introduce the antenna to be used for standardplots
        (the MS contains: {', '.join(exp.antennas)})""")
            else:
                return False

        except Exception as e:
            # NOTE: To implement. Check errors...
            print(f"WARNING: Standardplots crashed ({e}). But no implementation yet. Continuing..")
            return False

    return True


weight_threshold = actions.ask_user("A couple of questions:\nWhich weight flagging threshold should be used?",
                                    valtype=float)
swap_pols = actions.yes_or_no_question("Is polswap required?")

if swap_pols:
    pass

def polswap(exp):
    swap_pol_ants = actions.ask_user("List the antennas requiring swapping polarizations (comma-separated list)",
                                     accepted_values=[*exp.antennas])
    for a_swap_ant in swap_pol_ants:
        for msfile in glob.glob(f"{exp.expname.lower()}*.ms"):
            actions.shell_command("polswap.py", [msfile, a_swap_ant])

    return True


def ysfocus(exp):
    # All this stuff is irrelevant as ysfocus.py already checks for it.
    # if ('ys' in exp.antennas) or ('YS' in exp.antennas) or ('Ys' in exp.antennas):
    #     for msfile in glob.glob(f"{exp.expname.lower()}*.ms"):
    #         actions.shell_command("ysfocus.py", msfile)
    # else:
    #     print('\nYebes is not in the array.\n')
    #
    # # I keep it separately as Ho is not commonly in EVN observations
    # if ('ho' in exp.antennas) or ('HO' in exp.antennas) or ('Ho' in exp.antennas):
    #     print('\nHobart is in the array:\n')
    #     for msfile in glob.glob(f"{exp.expname.lower()}*.ms"):
    #         actions.shell_command("ysfocus.py", msfile)
    #
    for msfile in glob.glob(f"{exp.expname.lower()}*.ms"):
        actions.shell_command("ysfocus.py", msfile)

    return True


def flag_weights(exp, threshold):
    for msfile in glob.glob(f"{exp.expname.lower()}*.ms"):
        actions.shell_command("flag_weights.py", [msfile, str(threshold)])

    return True


actions.can_continue('Is everything ready to run tConvert? You can update the PI letter in the mean time')


def tConvert(exp):
    for i, msfile in enumerate(glob.glob(f"{exp.expname.lower()}*.ms")):
        actions.shell_command("tConvert", [msfile, f"{exp.expname.lower()}_{i+1}_1.IDI"])


actions.can_continue('If PolConvert is required, do it manually NOW before continuing')

# pol_convert_ants = actions.ask_user("Are there antennas requiring Pol Convert? (provide comma-separated list)",
#                                     accepted_values=['no', *exp.antennas])

# if pol_convert_ants is not 'no':
#     actions.can_continue('Please, run PolConvert manually and let me know if I can continue?')



# Preparations for archive

# If the auth file exists, take the username and password from it. Otherwise create a new one.

def letters(exp, args):
    # NOTE: This should always run
    if len(glob.glob("*_*.auth")) == 1:
        # the file should have the form username_password.auth.
        exp.set_credentials( *glob.glob("*_*.auth")[0].split('.')[0].split('_')  )
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


def archive(exp):
    # Compress all figures from standardplots
    actions.shell_command("gzip", "*ps", shell=True)

    actions.archive("-auth", exp, f"-n {exp.credentials.username} -p {exp.credentials.password}")
    actions.archive("-stnd", exp, f"{exp.expname.lower()}.piletter *ps.gz")
    actions.archive("-fits", exp, "*IDI*")


print('Everything is archived. Please continue manually in pipe.\n')

# Work at eee done!!


def main(exp, args, steps):
    """Runs the post-processing steps from a given EVN experiment in @eee.
    """
    # If required, move to the required directory (create it if needed).
    expdir = '/data0/{}/{}'.format(args.supsci, args.expname.upper())
    if expdir is not os.getcwd():
        if not os.path.isdir(expdir):
            os.makedirs(expdir)
            print(f"Directory {expdir} has been created.")

        os.chdir(expdir)
        print(f"Moved to {expdir}.")

    # print("Two log files will be created:")
    # print("  - processing.log: contains the executed commands and very minimal output.")
    # print("  - full_log_output.log: contains the full output received from all commands.\n\n")





