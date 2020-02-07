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
from src import metadata
from src import actions


def folders(exp, args):
    """Moves to the folder associated to the given experiment. If it does not exist, it creates it.
    """
    # If required, move to the required directory (create it if needed).
    expdir = '/data0/{}/{}'.format(args.supsci, exp.expname.upper())
    if expdir is not os.getcwd():
        if not os.path.isdir(expdir):
            os.makedirs(expdir)
            print(f"Directory {expdir} has been created.")

        os.chdir(expdir)
        print(f"Moved to {expdir}.\n")
    else:
        print(f"Running at {expdir}.\n")

    # NOTE: this is a temporary command until the pipeline fully works
    if exp.eEVNname is not None:
        actions.shell_command("create_processing_log.py", [exp.expname, "-e", exp.eEVNname,
                                                           "-o", "processing_manual.log"])
    else:
        actions.shell_command("create_processing_log.py", [exp.expname, "-o", "processing_manual.log"])

    # print("Two log files will be created:")
    # print("  - processing.log: contains the executed commands and very minimal output.")
    # print("  - full_log_output.log: contains the full output received from all commands.\n\n")


def ccs(exp):
    """Runs the initial post-processing steps in ccs: showlog, retrieving the lis,
    vix files from ccs, running checklis, and retrieving the PI letter and .expsum files.
    """
    return actions.get_lis_vex(exp.expname, 'jops@ccs', 'jops@jop83', eEVNname=exp.eEVNname)


def getdata(exp):
    """Gets the data into eee from all existing .lis files from the given experiment.
    inputs: exp : metadata.Experiment
    """
    for a_pass in exp.passes:
        actions.shell_command("getdata.pl", ["-proj", exp.eEVNname if exp.eEVNname is not None else exp.expname,
                                            "-lis", a_pass.lisfile])


def j2ms2(exp):
    """Runs j2ms2 on all existing .lis files from the given experiment.
    inputs: exp : metadata.Experiment
    """
    for a_pass in exp.passes:
        with open(a_pass.lisfile) as f:
            outms = [a for a in f.readline().replace('\n','').split(' ') \
                                 if (('.ms' in a) and ('.UVF' not in a))][0]
        if os.path.isdir(outms):
            if actions.yes_or_no_question(f"{outms} exists. Delete and run j2ms2 again?"):
                actions.shell_command("rm", ["-rf", outms])
                actions.shell_command("j2ms2", ["-v", a_pass.lisfile])
        else:
            actions.shell_command("j2ms2", ["-v", a_pass.lisfile])


def onebit(exp, args):
    # 1-bit scaling. Only runs if provided.
    # If not provided, checks that no 1-bit stations are in the vex file.
    # If 1-bit antennas are present somewhere, it asks user to confirm that no correction is required
    # or to provide the list of stations.
    if args.onebit is not None:
        return actions.scale1bit(exp, args.onebit)
    else:
        # Checks if there is some station that recorded at 1bit in the vex file (it may or may not
        # affect to this experiment.
        if actions.station_1bit_in_vix(f"{exp.expname}.vix"):
            scale1bit_stations = actions.ask_user("Are you sure scale1bit is not required?\n" +\
                                            "Specify the affected stations or 'none' otherwise")
            if scale1bit_stations is not 'none':
                return actions.scale1bit(exp, scale1bit_stations)
    return False


def standardplots(exp, args):
    if args.calsources is None:
        args.calsources = actions.ask_user(f"""Please, introduce the sources to be used for standardplots as a comma-separated list.
The MS contains: {', '.join(exp.passes[0].sources)})""")

    # Open produced plots, ask user if wants to continue / repeate plots with different inputs / q:
    while True:
        try:
            run_standardplots = True
            if (len(glob.glob(f"{exp.expname.lower()}*ps")) > 0) or \
               (len(glob .glob(f"{exp.expname.lower()}*ps.gz")) > 0):
                run_standardplots = actions.yes_or_no_question('Plots exist. Run standardplots again?')

            if run_standardplots:
                actions.standardplots(exp, args.refant, args.calsources)
                # Get all plots done and show them in the best order:
                standardplots = []
                # for plot_type in ('weight', 'auto', 'cross', 'ampphase'):
                    # standardplots += glob.glob(f"{exp.expname.lower()}*{plot_type}*.ps")
                standardplots += glob.glob(f"{exp.expname.lower()}*.ps")

                for a_plot in standardplots:
                    actions.shell_command("gv", a_plot)

                answer = actions.yes_or_no_question('\nAre the plots OK? "no" to pick other sources/stations')
                if answer:
                    return True

                args.calsources = actions.ask_user(f"""Please, introduce the sources to be used for standardplots
as a comma-separated list (the MS contains: {', '.join(exp.passes[0].sources)})""")
                args.refant = actions.ask_user(f"""Please, introduce the antenna to be used for standardplots
(the MS contains: {', '.join([e.capitalize() for e in exp.antennas])})""")
            else:
                return False

        except Exception as e:
            # NOTE: To implement. Check errors...
            print(f"WARNING: Standardplots crashed ({e}). But no implementation yet. Continuing..")
            return False

    return True


def polswap(exp):
    swap_pol_ants = actions.ask_user("List the antennas requiring swapping polarizations (comma-separated list)",
                                     accepted_values=[*exp.antennas])
    for a_swap_ant in swap_pol_ants:
        for a_pass in exp.passes:
            actions.shell_command("polswap.py", [a_pass.msfile, a_swap_ant])

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
    for a_pass in exp.passes:
        actions.shell_command("ysfocus.py", a_pass.msfile)

    return True


def flag_weights(exp, threshold):
    # TODO: use map() to parallelize this function. Is it true parallelization?
    for a_pass in exp.passes:
        actions.shell_command("flag_weights.py", [a_pass.msfile, str(threshold)])

    return True


def MSoperations(exp):
    """Runs polswap if requierd, ysfocus, and flag_weights.
    """
    weight_threshold = actions.ask_user("A couple of questions:\n" +\
                       "Which weight flagging threshold should be used?", valtype=float)
    swap_pols = actions.yes_or_no_question("Is polswap required?")
    if swap_pols:
        polswap(exp)

    ysfocus(exp)
    flag_weights(exp, weight_threshold)
    actions.can_continue('Is everything ready to run tConvert? You can update the PI letter in the mean time')


def tConvert(exp):
    """Runs tConvert in all MS files available in the directory
    """
    for i, a_pass in enumerate(exp.passes):
        actions.shell_command("tConvert", [a_pass.msfile, f"{exp.expname.lower()}_{i+1}_1.IDI"])


def polConvert(exp):
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
        answer = actions.ask_user("WARNING: multiple auth files found." +\
                 "Please introduce username and password (space separated)")
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


def send_letters(exp):
    """Remembers you to update the PI letter and send it , and the pipeletter, to the PIs.
    Finally, it runs parsePIletter.
    """
    actions.can_continue("You should now update the PI letter.")
    actions.shell_command("parsePIletter.py", ["-s", exp.obsdatetime.strftime("%b%y"),
                                              f"{exp.expname.lower()}.piletter"])
    actions.archive("-stnd", exp, f"{exp.expname.lower()}.piletter")
    print(f"Send the PI letter to {exp.piname.capitalize()}: {exp.email} (CC jops@jive.eu).")
    print(f"Send the pipe letter to {exp.piname.capitalize()}: {exp.email}.")
    if exp.expname[0] == 'N':
        # This is a NME.
        print('Now it is time to write the NME Report.')


# def archive_piletter(exp):
#     """(Re-)archive the PI letter.
#     """
#     actions.archive("-stnd", exp, f"{exp.expname.lower()}.piletter")

# print('Everything is archived. Please continue manually in pipe.\n')
# Work at eee done!!


