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
from . import metadata
from . import actions


def folders(exp):
    """Moves to the folder associated to the given experiment.
    If it does not exist, it creates it.
    """
    # If required, move to the required directory (create it if needed).
    expdir = '/data0/{}/{}'.format(exp.supsci.lower(), exp.expname.upper())
    if expdir is not os.getcwd():
        if not os.path.isdir(expdir):
            os.makedirs(expdir)
            print(f"Directory {expdir} has been created.")

        os.chdir(expdir)
        print(f"Moved to {expdir}.\n")

    # TODO: this is a temporary command until the pipeline fully works
    if exp.eEVNname is not None:
        actions.shell_command("create_processing_log.py", \
            [exp.expname, "-e", exp.eEVNname, "-o", "processing_manual.log"])
    else:
        actions.shell_command("create_processing_log.py", \
            [exp.expname, "-o", "processing_manual.log"])

    if not os.path.isdir('log'):
        os.makedirs('log')


def get_passes_from_lisfiles(exp):
    """Gets all .lis files in the directory, which imply different correlator passes.
    Append this information to the current experiment (exp object),
    together with the MS file associated for each of them.
    """
    lisfiles = glob.glob(f"{exp.expname.lower()}*.lis")
    thereis_line = True if (len(lisfiles) == 2 and '_line' in ''.join(lisfiles)) else False
    passes = []
    for i,a_lisfile in enumerate(lisfiles):
        with open(a_lisfile, 'r') as lisfile:
            for a_lisline in lisfile.readlines():
                if '.ms' in a_lisline: # The header line
                    # there is only one .ms input there
                    msname = [elem.strip() for elem in a_lisline.split() if '.ms' in elem][0]
                    if thereis_line:
                        if '_line' in a_lisfile:
                            fitsidiname = f"{exp.expname.lower()}_2_1.IDI"
                        else:
                            fitsidiname = f"{exp.expname.lower()}_1_1.IDI"

                        passes.append(metadata.CorrelatorPass(a_lisfile, msname, fitsidiname))
                    else:
                        fitsidiname = f"{exp.expname.lower()}_{i+1}_1.IDI"
                        passes.append(metadata.CorrelatorPass(a_lisfile, msname, fitsidiname,
                                                              False))

                    # Replaces the old *.UVF string in the .lis file with the FITS IDI
                    # file name to generate in this pass.
                    actions.shell_command('sed', ['-i',
                            f"'s/{exp.expname.lower()}.ms.UVF/{fitsidiname}/g'", a_lisfile])

    exp.passes = passes


def getdata(exp):
    """Gets the data into eee from all existing .lis files from the given experiment.
    inputs: exp : metadata.Experiment
    """
    for a_pass in exp.passes:
        actions.shell_command("getdata.pl",
                    ["-proj", exp.eEVNname if exp.eEVNname is not None else exp.expname,
                     "-lis", a_pass.lisfile])


def j2ms2(exp):
    """Runs j2ms2 on all existing .lis files from the given experiment.
    If the MS to produce already exists, then it will be removed.
    inputs: exp : metadata.Experiment
    """
    for a_pass in exp.passes:
        with open(a_pass.lisfile) as f:
            outms = [a for a in f.readline().replace('\n','').split(' ') \
                                 if (('.ms' in a) and ('.UVF' not in a))][0]
        if os.path.isdir(outms):
            # if actions.yes_or_no_question(f"{outms} exists. Delete and run j2ms2 again?"):
            actions.shell_command("rm", ["-rf", outms])
                # actions.shell_command("j2ms2", ["-v", a_pass.lisfile])
        # else:
        actions.shell_command("j2ms2", ["-v", a_pass.lisfile])


def onebit(exp):
    """In case some stations recorded at 1 bit, scales 1-bit data to correct for
    quantization losses in all MS associated with the given experiment name.
    """
    # Sanity check
    ants2correct = set(exp.onebit_antennas).intersection(exp.antennas)
    for a_pass in exp.passes:
        cmd, output = actions.shell_command("scale1bit.py",
                                            [a_pass.msfile, ' '.join(ants2correct)])



def ysfocus(exp):
    for a_pass in exp.passes:
        actions.shell_command("ysfocus.py", a_pass.msfile)


def standardplots(exp):
    """Runs the standardplots on the specified experiment using a reference antenna
    and sources to be picked for the auto- and cross-correlations.
    """
    # TODO: to be fully rewritten
    # To run for all correlator passes that will be pipelined.
    # Then once all of them finish, open the plots and ask user.
    refant = exp.ref_antennas[0] if len(exp.ref_antennas) == 1 \
                              else f"({'|'.join(exp.ref_antennas)})"
    calsources = ','.join([s.name for s in exp.ref_sources])
    counter = 0
    for a_pass in exp.passes:
        if a_pass.pipeline:
            counter += 1
            if counter == 1:
                actions.shell_command("standardplots",
                              ["-weight", a_pass.msfile, refant, calsources])
            else:
                actions.shell_command("standardplots", [a_pass.msfile, refant, calsources])

    # cmd, output = shell_command("standardplots",
    # # Get all plots done and show them in the best order:


def open_standardplot_files(exp):
    """Calls gv to open all plots generated by standardplots.
    """
    standardplots = []
    for plot_type in ('weight', 'auto', 'cross', 'ampphase'):
        standardplots += glob.glob(f"{exp.expname.lower()}*{plot_type}*.ps")
    # standardplots = glob.glob(f"{exp.expname.lower()}*.ps")

    try:
        for a_plot in standardplots:
            actions.shell_command("gv", a_plot)
    except Exception as e:
        print(f"WARNING: Plots could not be opened. Do it manually.\nError: {e}.")


def polswap(exp, antennas):
    """Swaps the polarization of the given antennas for all associated MS files
    to the given experiment.
    """
    for a_pass in exp.passes:
        actions.shell_command("polswap.py", [a_pass.msfile, ','.join(antennas)])



def flag_weights(exp, threshold):
    # TODO: use map() to parallelize this function. Is it true parallelization?
    for a_pass in exp.passes:
        cmd, output = actions.shell_command("flag_weights.py", [a_pass.msfile, str(threshold)])

    #TODO: the return value must be the percentage of flagged data.
    return output


def ms_operations(exp):
    """After standardplots already run, opens the generated plots and asks the user.
    If needed, runs standardplots again with the updated parameters and
    again if required, runs polswap, stores the info to run PolConvert later.
    Runs flag_weights and finally standardplots if data modifed (no weights).
    """
    while True:
        options = dialog.standardplots_dialog(exp)
        if options['choice'] is dialog.Choice.ok:
            break
        elif options['choice'] is dialog.Choice.repeat:
            # Checks if update values are requested.
            if options['ref_ant'] is not None:
                exp.ref_antennas = options['ref_ant']

            if options['cal_sources'] is not None:
                exp.ref_sources = options['cal_sources']

            standardplots(exp, do_weights=False)
            try:
                open_standardplot_files(exp)
            except Exception as e:
                print(f"WARNING: Plots could not be opened. Do it manually.\nError: {e}.")

        elif options['choice'] is dialog.Choice.abort:
            actions.end_program(exp)
        else:
            raise ValueError(f"Unexpected choice {options['choice']}.")

    # Then do flag_weights and polswap if requested
    # (in the later, run standardplots again without weights)
    if options['polswap'] is not None:
        polswap(exp, options['polswap'])

    if options['polconvert'] is not None:
        exp.polconvert_antennas = options['polconvert']

    percent_flagged = flag_weights(exp, options['flagweight'])
    update_piletter(exp, options['flagweight'], percent_flagged)
    standardplots(exp, do_weights=False)


def update_piletter(exp, weightthreshold, flaggeddata):
    """Updates the PI letter by changing two things:
    - Removing the trailing epoch-related character in the experiment name.
    - Adding the weightthreshold that was used and how much data were flagged.
    """
    pass



def tConvert(exp):
    """Runs tConvert in all MS files available in the directory
    """
    for a_pass in exp.passes:
        # TODO: to parallelize
        actions.shell_command("tConvert", [a_pass.msfile, a_pass.fitsidifile])


def polConvert(exp):
    """Checks if PolConvert is required for any antenna.
    In that case, prepares the templates for running it and (potentially in the future?)
    will run it. For now it just requests the user to run it manually.
    """
    if len(exp.polconvert_antennas) == 0:
        print('PolConvert is not required in this experiment.')
        return

    dialog_text = "PolConvert is required.\n"
    dialog_text += f"Please run it manually for {','.join(exp.polconvert_antennas)}."
    dialog_text += "Once you are done (all FITS properly corrected), press Continue."
    dialog.warning_dialog(dialog_text)



# Preparations for archive

# If the auth file exists, take the username and password from it. Otherwise create a new one.

def set_credentials_pipelet(exp):
    """Sets the credentials for the given experiment and creates the .pipelet file.
    In case of an NME or test, it does not set any credential.
    Otherwise, it will take the credentials from a .auth file if already exists,
    or creates such file iwth a new password.
    """
    if (exp.expname.lower[0] is 'N') or (exp.expname.lower[0] is 'F'):
        print(f"{exp.expname} is an NME or test experiment.\nNo authentification will be set.")
    if len(glob.glob("*_*.auth")) == 1:
        # Some credentials are already in place.
        exp.set_credentials( *glob.glob("*_*.auth")[0].split('.')[0].split('_')  )
        if not os.path.isfile(f"{exp.expname.lower()}.pipelet"):
            actions.shell_command("pipelet.py", [exp.expname.lower(), exp.supsci.lower()])

    elif len(glob.glob("*_*.auth")) > 1:
        raise ValueError("More than one .auth file found in the directory.")
    else:
        possible_char = string.digits + string.ascii_letters
        exp.set_credentials(username=exp.expname.lower(),
                            password="".join(random.sample(possible_char, 12)))
        actions.shell_command("touch",
                f"{exp.credentials.username}_{exp.credentials.password}.auth")
        actions.shell_command("pipelet.py", [exp.expname.lower(), exp.supsci.lower()])


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


