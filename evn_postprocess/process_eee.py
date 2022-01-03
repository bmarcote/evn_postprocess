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
from pathlib import Path
import configparser
import logging
import subprocess
from datetime import datetime
from evn_postprocess import dialog
from evn_postprocess import experiment
from evn_postprocess import environment #as env
# from evn_postprocess import process_css as css
# from evn_postprocess import process_eee as eee
# from evn_postprocess import process_pipe as pipe

def create_folders(expname: str, supsci: str):
    """Creates the folder required for the post-processing of the experiment
    - @eee: /data0/{supportsci}/{exp.upper()}

    Inputs
        - expname: str
            Experiment name (case insensitive).
        - supsci: str
            Surname of the assigned support scientist.
    """
    expdir = Path(f"/data0/{supsci.lower()}/{expname.upper()}")
    if not expdir.exists():
        expdir.mkdir(parents=True)
        print(f"Directory '/data0/{supsci.lower()}/{expname}' has been created.")




##################################################### THE OLD CODE

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
        environment.shell_command("create_processing_log.py", \
            [exp.expname, "-e", exp.eEVNname, "-o", "processing_manual.log"])
    else:
        environment.shell_command("create_processing_log.py", \
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
                    # In case the output FITS IDI name has already been set
                    if '.IDI' in a_lisline:
                        fitsidiname = [elem.strip() for elem in a_lisline.split() if '.IDI' in elem][0]
                        to_pipeline = True if ((fitsidiname.split('_')[-2] == '1') or thereis_line) else False
                    else:
                        if thereis_line:
                            if '_line' in a_lisfile:
                                fitsidiname = f"{exp.expname.lower()}_2_1.IDI"
                            else:
                                fitsidiname = f"{exp.expname.lower()}_1_1.IDI"

                            to_pipeline = True
                        else:
                            fitsidiname = f"{exp.expname.lower()}_{i+1}_1.IDI"
                            to_pipeline = True if (i == 0) else False

                    passes.append(experiment.CorrelatorPass(a_lisfile, msname, fitsidiname, to_pipeline))
                    # Replaces the old *.UVF string in the .lis file with the FITS IDI
                    # file name to generate in this pass.
                    if '.UVF' in a_lisline:
                        environment.shell_command('sed', ['-i',
                            f"'s/{msname}.UVF/{fitsidiname}/g'", a_lisfile])

    exp.passes = passes


def getdata(exp):
    """Gets the data into eee from all existing .lis files from the given experiment.
    inputs: exp : experiment.Experiment
    """
    for a_pass in exp.passes:
        cmd,output = environment.shell_command("getdata.pl",
                    ["-proj", exp.eEVNname if exp.eEVNname is not None else exp.expname,
                     "-lis", a_pass.lisfile], stdout=subprocess.STDOUT)
        exp.log(cmd)

    return True


def j2ms2(exp):
    """Runs j2ms2 on all existing .lis files from the given experiment.
    If the MS to produce already exists, then it will be removed.
    inputs: exp : experiment.Experiment
    """
    for a_pass in exp.passes:
        with open(a_pass.lisfile) as f:
            outms = [a for a in f.readline().replace('\n','').split(' ') \
                                 if (('.ms' in a) and ('.UVF' not in a))][0]
        if os.path.isdir(outms):
            # if environment.yes_or_no_question(f"{outms} exists. Delete and run j2ms2 again?"):
            print('Removing the pre-existing MS file {outms}')
            cmd,output = environment.shell_command("rm", ["-rf", outms])
            exp.log(cmd)
        # else:
        if (exp.special_params is not None and 'j2ms2' in exp.special_params):
            cmd,output = environment.shell_command("j2ms2", ["-v", a_pass.lisfile, *exp.special_params['j2ms2']],
                                                   stdout=subprocess.STDOUT)
        else:
            cmd,output = environment.shell_command("j2ms2", ["-v", a_pass.lisfile], stdout=subprocess.STDOUT)

        exp.log(cmd)

    return True


def update_ms_expname(exp):
    """For e-EVN experiments, where the .vex-file experiment name does not match the actual
    experiment name, this one must be updated in the created MS file(s).
    """
    if (exp.eEVNname is not None) and (exp.eEVNname != exp.expname):
        for a_pass in exp.passes:
            environment.shell_command("expname.py", [a_pass.msfile, exp.expname],
                                      stdout=subprocess.STDOUT, stderr=subprocess.STDOUT)
            exp.log(f"expname.py {a_pass.msfile} {exp.expname}")

    return True


def get_metadata_from_ms(exp):
    exp.get_setup_from_ms()
    return True


def standardplots(exp, do_weights=False):
    """Runs the standardplots on the specified experiment using a reference antenna
    and sources to be picked for the auto- and cross-correlations.
    """
    # TODO: to be fully rewritten
    # To run for all correlator passes that will be pipelined.
    # Then once all of them finish, open the plots and ask user.
    calsources = ','.join(exp.calsources)
    counter = 0
    output = None
    try:
        for a_pass in exp.passes:
            if a_pass.pipeline:
                if exp.refant is not None:
                    refant = exp.refant[0] if len(exp.refant) == 1 else f"({'|'.join(exp.refant)})"
                else:
                    for ant in ('Ef', 'O8', 'Ys', 'Mc', 'Gb', 'At', 'Pt'):
                        if (ant in a_pass.antennas) and (a_pass.antennas[ant].observed):
                            refant = ant
                            break
                    raise ValueError("Couldn't find a good reference antenna for standardplots. " \
                                     "Please specify it manually.")
                counter += 1
                if counter == 1 and do_weights:
                    cmd, output = environment.shell_command("standardplots",
                                  ["-weight", a_pass.msfile, refant, calsources],
                                  stdout=subprocess.STDOUT, stderr=subprocess.STDOUT)
                else:
                    cmd, output = environment.shell_command("standardplots", [a_pass.msfile, refant, calsources],
                                      stdout=subprocess.STDOUT, stderr=subprocess.STDOUT)

    except Exception as e:
        print("WARNING: Standardplots reported an error ({e}). Check if plots were created or run it manually.")
        input("After checking this issue in another terminal, press any key.")
        return False
    # cmd, output = shell_command("standardplots",
    # # Get all plots done and show them in the best order:
    exp.log(environment.extract_tail_standardplots_output(output[0]))
    return True



def open_standardplot_files(exp):
    """Calls gv to open all plots generated by standardplots.
    """
    standardplots = []
    for plot_type in ('weight', 'auto', 'cross', 'ampphase'):
        standardplots += glob.glob(f"{exp.expname.lower()}*{plot_type}*.ps")
    # standardplots = glob.glob(f"{exp.expname.lower()}*.ps")

    try:
        for a_plot in standardplots:
            environment.shell_command("gv", a_plot, stdout=subprocess.STDOUT, stderr=subprocess.STDOUT)
            return True
    except Exception as e:
        print(f"WARNING: Plots could not be opened. Do it manually.\nError: {e}.")
        return False


def onebit(exp):
    """In case some stations recorded at 1 bit, scales 1-bit data to correct for
    quantization losses in all MS associated with the given experiment name.
    """
    # Sanity check

    for a_pass in exp.passes:
        if len(a_pass.antennas.onebit) > 0:
            cmd, output = environment.shell_command("scale1bit.py",
                          [a_pass.msfile, ' '.join(a_pass.antennas.onebit)],
                          stdout=subprocess.STDOUT, stderr=subprocess.STDOUT)
        elif environment.station_1bit_in_vix(exp.vix):
            print(f"\n\n{'#'*10}\n#Traces of 1bit station found in {exp.vix} " \
                  f"but no station specified to be corrected.\n\n")
            return False
    return True


def ysfocus(exp):
    for a_pass in exp.passes:
        environment.shell_command("ysfocus.py", a_pass.msfile, stdout=subprocess.STDOUT, stderr=subprocess.STDOUT)
    return True


def polswap(exp):
    """Swaps the polarization of the given antennas for all associated MS files
    to the given experiment.
    """
    for a_pass in exp.passes:
            if len(a_pass.antennas.polswap) > 0:
                environment.shell_command("polswap.py", [a_pass.msfile, ','.join(a_pass.antennas.polswap)],
                                          stdout=subprocess.STDOUT, stderr=subprocess.STDOUT)
    return True


def flag_weights(exp):
    outputs = []
    for a_pass in exp.passes:
        cmd, output = environment.shell_command("flag_weights.py", [a_pass.msfile, a_pass.flagged_weights.threshold],
                                                stdout=subprocess.STDOUT, stderr=subprocess.STDOUT)
        exp.log(cmd+'\n# '.join(output))
        outputs.append(output[0])
        # Find the percentage of flagged data and stores it in exp
        str_end = '% data with non-zero'
        str_start = 'execution).'
        if '% data with non-zero weights' in output:
            a_pass.flagged_weights.percentage = float(output[output.find(str_start)+len(str_start):output.find(str_end)])

    return True


def update_piletter(exp):
    """Updates the PI letter by changing two things:
    - Removing the trailing epoch-related character in the experiment name.
    - Adding the weightthreshold that was used and how much data were flagged.
    """
    weightthreshold = float(exp.passes[0].flagged_weights.threshold)
    flaggeddata = float(exp.passes[0].flagged_weights.percentage)
    with open(f"{exp.expname.lower()}.piletter", 'r') as orifile:
        with open(f"{exp.expname.lower()}.piletter~", 'w') as destfile:
            for a_line in orifile.readlines():
                tmp_line = a_line
                if ('derived from the following EVN project code(s):' in tmp_line) and \
                                                            (exp.expname[-1].isalpha()):
                    tmp_line = tmp_line.replace(exp.expname, exp.expname[:-1])

                if ('***SuppSci:' not in tmp_line) and ('there is one***' not in tmp_line):
                    if '***weight cutoff***' in tmp_line:
                        tmp_line = tmp_line.replace('***weight cutoff***', f"{weightthreshold:.2}")
                    if '***percent flagged***' in tmp_line:
                        tmp_line = tmp_line.replace('***percent flagged***', f"{flaggeddata:.2}")

                    destfile.write(tmp_line)

    os.rename(f"{exp.expname.lower()}.piletter~", f"{exp.expname.lower()}.piletter")
    return True


def tConvert(exp):
    """Runs tConvert in all MS files available in the directory
    """
    for a_pass in exp.passes:
        existing_files = glob.glob(f"{a_pass.fitsidifile}*")
        if len(existing_files) > 0:
            for a_existing_file in existing_files:
                os.remove(a_existing_file)

        environment.shell_command("tConvert", [a_pass.msfile, a_pass.fitsidifile],
                                  stdout=subprocess.STDOUT, stderr=subprocess.STDOUT)
    return True


def polConvert(exp):
    """Checks if PolConvert is required for any antenna.
    In that case, prepares the templates for running it and (potentially in the future?)
    will run it. For now it just requests the user to run it manually.
    """
    for a_pass in exp.passes:
        if len(a_pass.antennas.polconvert) > 0:
            print("PolConvert has not been implemented yet.\nRun it manually.")
            return False
        else:
            exp.log(f"# PolConvert is not required.")
        # dialog_text = "PolConvert is required.\n"
        # dialog_text += f"Please run it manually for {','.join(exp.polconvert_antennas)}."
        # dialog_text += "Once you are done (all FITS properly corrected), press Continue."
        # dialog.warning_dialog(dialog_text)
    return True

# Preparations for archive

# If the auth file exists, take the username and password from it. Otherwise create a new one.

def set_credentials_pipelet(exp):
    """Sets the credentials for the given experiment and creates the .pipelet file.
    In case of an NME or test, it does not set any credential.
    Otherwise, it will take the credentials from a .auth file if already exists,
    or creates such file iwth a new password.
    """
    if (exp.expname.upper()[0] == 'N') or (exp.expname.upper()[0] == 'F'):
        print(f"NOTE: {exp.expname} is an NME or test experiment.\nNo authentification will be set.")
    elif len(glob.glob("*_*.auth")) == 1:
        # Some credentials are already in place.
        exp.set_credentials( *glob.glob("*_*.auth")[0].split('.')[0].split('_')  )
        if not os.path.isfile(f"{exp.expname.lower()}.pipelet"):
            environment.shell_command("pipelet.py", [exp.expname.lower(), exp.supsci.lower()])
            exp.log(f"pipelet.py {exp.expname.lower()} {exp.supsci.lower()}")

    elif len(glob.glob("*_*.auth")) > 1:
        raise ValueError("More than one .auth file found in the directory.")
    else:
        possible_char = string.digits + string.ascii_letters
        exp.set_credentials(username=exp.expname.lower(),
                            password="".join(random.sample(possible_char, 12)))
        environment.shell_command("touch",
                f"{exp.credentials.username}_{exp.credentials.password}.auth")
        environment.shell_command("pipelet.py", [exp.expname.lower(), exp.supsci.lower()])
        exp.log(f"touch {exp.credentials.username}_{exp.credentials.password}.auth")
        exp.log(f"pipelet.py {exp.expname.lower()} {exp.supsci.lower()}")

    return True


def archive(exp):
    dialog.continue_dialog("Please update the PI letter before continue.", f"{exp.expname} -- PI letter")
    # Compress all figures from standardplots
    environment.shell_command("gzip", "*ps", shell=True)
    # TODO: only auth if no NME
    environment.archive("-auth", exp, f"-n {exp.credentials.username} -p {exp.credentials.password}")
    environment.archive("-stnd", exp, f"{exp.expname.lower()}.piletter *ps.gz")
    environment.archive("-fits", exp, "*IDI*")
    return True


def send_letters(exp):
    """Remembers you to update the PI letter and send it , and the pipeletter, to the PIs.
    Finally, it runs parsePIletter.
    """
    dialog.continue_dialog("Please update the PI letter if needed before continue.", f"{exp.expname} -- PI letter")
    environment.archive("-stnd", exp, f"{exp.expname.lower()}.piletter")
    environment.shell_command("parsePIletter.py", ["-s", exp.obsdatetime.strftime("%b%y"),
                                              f"{exp.expname.lower()}.piletter"])
    print(f"Send the PI letter to {exp.piname.capitalize()}: {exp.email} (CC jops@jive.eu).")
    print(f"Send the pipe letter to {exp.piname.capitalize()}: {exp.email}.")
    if exp.expname[0] == 'N':
        # This is a NME.
        print('Now it is time to write the NME Report. Good luck!')
    else:
        print('Experiment done!\nYou may have a coffee/tea now.')


# def archive_piletter(exp):
#     """(Re-)archive the PI letter.
#     """
#     environment.archive("-stnd", exp, f"{exp.expname.lower()}.piletter")

# print('Everything is archived. Please continue manually in pipe.\n')
# Work at eee done!!


