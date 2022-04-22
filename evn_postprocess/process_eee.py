#! /usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the eee computer.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.

"""


import os
import glob
import string
import random
import traceback
from pathlib import Path
import subprocess
from . import dialog
from . import experiment
from . import environment


def create_folders(exp):
    """Creates the folder required for the post-processing of the experiment
    - @eee: /data0/{supportsci}/{exp.upper()}

    Inputs
        - expname: str
            Experiment name (case insensitive).
        - supsci: str
            Surname of the assigned support scientist.
    """
    expdir = Path(f"/data0/{exp.supsci.lower()}/{exp.expname.upper()}")
    if not expdir.exists():
        expdir.mkdir(parents=True)
        exp.log(f"mkdir /data0/{exp.supsci.lower()}/{exp.expname.upper()}")
        print(f"Directory '/data0/{exp.supsci.lower()}/{exp.expname.upper()}' has been created.")

    return True


def get_passes_from_lisfiles(exp):
    """Gets all .lis files in the directory, which imply different correlator passes.
    Append this information to the current experiment (exp object),
    together with the MS file associated for each of them.
    """
    lisfiles = glob.glob(f"{exp.expname.lower()}*.lis")
    thereis_line = True if (len(lisfiles) == 2 and '_line' in ''.join(lisfiles)) else False
    passes = []
    for i, a_lisfile in enumerate(lisfiles):
        with open(a_lisfile, 'r') as lisfile:
            for a_lisline in lisfile.readlines():
                if '.ms' in a_lisline:  # The header line
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
                        environment.shell_command('sed', ['-i', f"'s/{msname}.UVF/{fitsidiname}/g'", a_lisfile],
                                                  shell=True, bufsize=None)

    exp.correlator_passes = passes
    return True


def getdata(exp):
    """Gets the data into eee from all existing .lis files from the given experiment.
    inputs: exp : experiment.Experiment
    """
    for a_pass in exp.correlator_passes:
        cmd, _ = environment.shell_command("getdata.pl",
                                           ["-proj", exp.eEVNname if exp.eEVNname is not None else exp.expname,
                                            "-lis", a_pass.lisfile.name],
                                           shell=True, stdout=None,
                                           stderr=subprocess.STDOUT, bufsize=0)
        exp.log(cmd)

    return True


def j2ms2(exp):
    """Runs j2ms2 on all existing .lis files from the given experiment.
    If the MS to produce already exists, then it will not generate it again.
    inputs: exp : experiment.Experiment
    """
    for i, a_pass in enumerate(exp.correlator_passes):
        with open(a_pass.lisfile) as f:
            outms = [a for a in f.readline().replace('\n', '').split(' ')
                     if (('.ms' in a) and ('.UVF' not in a))][0]
            exp.correlator_passes[i].msfile = outms
        if not os.path.isdir(outms):
            # print('Removing the pre-existing MS file {outms}')
            # cmd,output = environment.shell_command("rm", ["-rf", outms], shell=True)
            # exp.log(cmd)
            if 'j2ms2' in exp.special_params:
                cmd, _ = environment.shell_command("j2ms2", ["-v", a_pass.lisfile.name,
                                                             *exp.special_params['j2ms2']],
                                                   shell=True, stdout=None,
                                                   stderr=subprocess.STDOUT, bufsize=0)
            else:
                cmd, _ = environment.shell_command("j2ms2", ["-v", a_pass.lisfile.name],
                                                   shell=True, stdout=None,
                                                   stderr=subprocess.STDOUT, bufsize=0)

            exp.log(cmd, timestamp=True)

    return True


def update_ms_expname(exp):
    """For e-EVN experiments, where the .vex-file experiment name does not match the actual
    experiment name, this one must be updated in the created MS file(s).
    """
    if (exp.eEVNname is not None) and (exp.eEVNname != exp.expname):
        for a_pass in exp.correlator_passes:
            environment.shell_command("expname.py", [a_pass.msfile.name, exp.expname],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            exp.log(f"expname.py {a_pass.msfile} {exp.expname}")

    return True


def get_metadata_from_ms(exp):
    exp.get_setup_from_ms()
    return True


def standardplots(exp, do_weights=True):
    """Runs the standardplots on the specified experiment using a reference antenna
    and sources to be picked for the auto- and cross-correlations.
    """
    # TODO: to be fully rewritten
    # To run for all correlator passes that will be pipelined.
    # Then once all of them finish, open the plots and ask user.
    calsources = ','.join(exp.sources_stdplot)
    counter = 0
    for a_pass in exp.correlator_passes:
        try:
            if a_pass.pipeline:
                if exp.refant is not None:
                    refant = exp.refant[0] if len(exp.refant) == 1 else f"({'|'.join(exp.refant)})"
                else:
                    for ant in ('Ef', 'O8', 'Ys', 'Mc', 'Gb', 'At', 'Pt'):
                        if (ant in a_pass.antennas) and (a_pass.antennas[ant].observed):
                            refant = ant
                            break
                    raise ValueError("Couldn't find a good reference antenna for standardplots. "
                                     "Please specify it manually.")
                counter += 1
                if (counter == 1) and do_weights:
                    cmd, _ = environment.shell_command("standardplots",
                                                            ["-weight", a_pass.msfile.name, refant, calsources],
                                                            stdout=None, stderr=subprocess.STDOUT)
                else:
                    cmd, _ = environment.shell_command("standardplots",
                                                            [a_pass.msfile.name, refant, calsources],
                                                            stdout=None, stderr=subprocess.STDOUT)

                exp.log(cmd)
                # Runs again jplotter but only to retrieve the summary into the output
                cmd, output = environment.shell_command("echo",
                                                        [f'"ms {a_pass.msfile.name};r"', "|", "jplotter"],
                                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                exp.log(environment.extract_tail_standardplots_output(output))

        except Exception:
            print("WARNING: Standardplots reported an error.")
            # TODO: these tracebacks should be one level above (in app.py)
            traceback.print_exc()
            return False

    return True


def open_standardplot_files(exp):
    """Calls gv to open all plots generated by standardplots.
    """
    standardplots = []
    for plot_type in ('weight', 'auto', 'cross', 'ampphase'):
        standardplots += glob.glob(f"{exp.expname.lower()}*{plot_type}*.ps")
    # standardplots = glob.glob(f"{exp.expname.lower()}*.ps")

    if len(standardplots) == 0:
        raise FileNotFoundError(f"Standardplots for {exp.expname} not found but expected.")

    try:
        for a_plot in standardplots:
            environment.shell_command("gv", a_plot, stdout=None, stderr=subprocess.STDOUT)
    except Exception as e:
        print(f"WARNING: Plots could not be opened. Do it manually.\nError: {e}.")
        return False

    return True


def onebit(exp):
    """In case some stations recorded at 1 bit, scales 1-bit data to correct for
    quantization losses in all MS associated with the given experiment name.
    """
    # Sanity check
    if len(exp.antennas.onebit) > 0:
        for a_pass in exp.correlator_passes:
            environment.shell_command("scale1bit.py",
                                      [a_pass.msfile.name, ' '.join(exp.antennas.onebit)],
                                      shell=True, stdout=None, stderr=subprocess.STDOUT)
    elif environment.station_1bit_in_vix(exp.vix):
        print(f"\n\n{'#'*10}\n#Traces of 1bit station found in {exp.vix} "
              "but no station specified to be corrected.\n\n")
        return False
    return True


def ysfocus(exp):
    for a_pass in exp.correlator_passes:
        environment.shell_command("ysfocus.py", a_pass.msfile.name, stdout=None, shell=True, stderr=subprocess.STDOUT)
    return True


def polswap(exp):
    """Swaps the polarization of the given antennas for all associated MS files
    to the given experiment.
    """
    if len(exp.antennas.polswap) > 0:
        for a_pass in exp.correlator_passes:
            environment.shell_command("polswap.py", [a_pass.msfile.name, ','.join(exp.antennas.polswap)],
                                      shell=True, stdout=None, stderr=subprocess.STDOUT)
    return True


def flag_weights(exp):
    for a_pass in exp.correlator_passes:
        print('\n\nThe following may take a while...')
        cmd, output = environment.shell_command("flag_weights.py",
                                                [a_pass.msfile.name, str(a_pass.flagged_weights.threshold)],
                                                shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        exp.log(cmd+"\n# "+output.split('\r')[-1].replace('\n', '\n# ')+"\n")
        # Find the percentage of flagged data and stores it in exp
        str_end = '% data with non-zero'
        str_start = 'execution).'
        if '% data with non-zero weights' in output:
            a_pass.flagged_weights.percentage = float(output[output.find(str_start) +
                                                      len(str_start):output.find(str_end)])
    return True


def update_piletter(exp):
    """Updates the PI letter by changing two things:
    - Removing the trailing epoch-related character in the experiment name.
    - Adding the weightthreshold that was used and how much data were flagged.
    """
    weightthreshold = float(exp.correlator_passes[0].flagged_weights.threshold)
    flaggeddata = float(exp.correlator_passes[0].flagged_weights.percentage)
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


def tconvert(exp):
    """Runs tConvert in all MS files available in the directory
    """
    for a_pass in exp.correlator_passes:
        existing_files = glob.glob(f"{a_pass.fitsidifile}*")
        if len(existing_files) > 0:
            continue
        environment.shell_command("tConvert", [a_pass.msfile.name, a_pass.fitsidifile],
                                  stdout=None, stderr=subprocess.STDOUT)
    return True


def polconvert(exp):
    """Checks if PolConvert is required for any antenna.
    In that case, prepares the templates for running it and (potentially in the future?)
    will run it. For now it just requests the user to run it manually.
    """
    if len(exp.antennas.polconvert) > 0:
        polconv_inp = Path('./polconvert_inputs.ini')
        if not polconv_inp.exists():
            exp.log("cp ~/polconvert/polconvert_inputs.ini ./polconvert_inputs.ini")
            environment.shell_command('cp', ['/home/jops/polconvert/polconvert_inputs.ini',
                                             './polconvert_inputs.ini'],
                                      shell=True, stdout=None)
            environment.shell_command('sed', ['-i', f"'s/es100_1_1.IDI6/{exp.expname.lower()}_1_1.IDI*/g'",
                                      polconv_inp.name], shell=True, bufsize=None, stdout=None)
            environment.shell_command('sed', ['-i', f"'s/es100_1_1.IDI*/{exp.expname.lower()}_1_1.IDI*/g'",
                                      polconv_inp.name], shell=True, bufsize=None, stdout=None)
            ants = ', '.join(["\"" + ant.upper() + "\"" for ant in exp.antennas.polconvert])
            environment.shell_command('sed', ['-i', "'s/\"T6\"/" + f"{ants}/g'", polconv_inp.name],
                                      shell=True, bufsize=None, stdout=None)

        print("\n\n\033[1m### PolConvert needs to be run manually.\033[0m\n")
        print("You would find the input template in the current directory.")
        print("Edit it manually and then run it with:\n")
        print("> polconvert.py  polconvert_inputs.ini")
        print("\n\n\033[1mOnce PolConvert has run, re-run me.\033[0m\n\n")
        # Keep the following as it will require a manual interaction
        exp.last_step = 'tconvert'
        return None
    else:
        exp.log("# PolConvert is not required.")
        # dialog_text = "PolConvert is required.\n"
        # dialog_text += f"Please run it manually for {','.join(exp.polconvert_antennas)}."
        # dialog_text += "Once you are done (all FITS properly corrected), press Continue."
        # dialog.warning_dialog(dialog_text)
    return True


def post_polconvert(exp):
    """Assumes that PolConvert has run, creating the new (corrected) files *IDI*.PCONVERT.
    This function (if indeed PolConvert had run) would move all converted files to the
    standard name (keeping the original ones in a folder (./unconverted_idi_files/),
    and runs again standardplots to confirm that the conversion has been loaded properly.
    """
    if len(exp.antennas.polconvert) == 0:
        return True

    if len(glob.glob('*IDI*.PCONVERT')) == 0:
        # Files would be expected but then let's assume the user already renamed them
        return True
    else:
        idi_ori = Path(exp.cwd / 'idi_ori/')
        idi_ori.mkdir(exist_ok=True)
        for an_idi in Path(exp.cwd).glob('*IDI*'):
            if '.PCONVERT' not in an_idi.name:
                an_idi.rename(idi_ori / an_idi.name)

        for an_idi in Path(exp.cwd).glob('*IDI*.PCONVERT'):
            an_idi.rename(an_idi.name.replace('.PCONVERT', ''))

        exp.log("mkdir idi_ori")
        exp.log("mv *IDI? *IDI?? *IDI???  idi_ori/")
        exp.log("zmv '(*).PCONVERT' '$1'")
        # Imports the standard message into the PI letter
        with open(f"{exp.expname.lower()}.piletter", 'a') as piletter:
            s = f"the antenna{'s' if len(exp.antennas.polconvert) > 1 else ''} {', '.join(exp.antennas.polconvert)}"
            piletter.write("\n- Note that " + s + " originally observed linear polarizations, " \
                           "which were transformed to circular ones during post-processing using the " \
                           "PolConvert program (Martí-Vidal, et al. 2016, A&A,587, A143). " \
                           "Thanks to this correction, you can automatically recover the absolute EVPA " \
                           f"value when using {', '.join(exp.antennas.polconvert)} as reference station " \
                           "during fringe-fitting.")

    exp.last_step = 'post_polconvert'
    # Create again a MS from these converted files so I can run standardplots over the corrected data
    # TODO: Doing it manually for now
    return None


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
        exp.set_credentials(*glob.glob("*_*.auth")[0].split('.')[0].split('_'))
        if not os.path.isfile(f"{exp.expname.lower()}.pipelet"):
            environment.shell_command("pipelet.py", [exp.expname.lower(), exp.supsci.lower()])
            exp.log(f"pipelet.py {exp.expname.lower()} {exp.supsci.lower()}")

    elif len(glob.glob("*_*.auth")) > 1:
        raise ValueError("More than one .auth file found in the directory.")
    else:
        possible_char = string.digits + string.ascii_letters
        exp.set_credentials(username=exp.expname.lower(),
                            password="".join(random.sample(possible_char, 12)))
        environment.shell_command("touch", f"{exp.credentials.username}_{exp.credentials.password}.auth")
        environment.shell_command("pipelet.py", [exp.expname.lower(), exp.supsci.lower()])
        exp.log(f"touch {exp.credentials.username}_{exp.credentials.password}.auth")
        exp.log(f"pipelet.py {exp.expname.lower()} {exp.supsci.lower()}")

    return True


def archive(exp):
    # Compress all figures from standardplots if they haven't been yet
    if len(glob.glob("*.ps")) > 0:
        environment.shell_command("gzip", "*ps", shell=True)
        exp.log('gzip *ps')

    if (exp.credentials.username is not None) and (exp.credentials.password is not None):
        environment.archive("-auth", exp, f"-n {exp.credentials.username} -p {exp.credentials.password}")
    else:
        assert len(glob.glob("*_*.auth")) == 0, 'No credentials stored but auth file found'

    environment.archive("-stnd", exp, "*ps.gz")
    environment.archive("-fits", exp, "*IDI*")
    return True


def append_antab(exp):
    """Appends the Tsys and GC information from the experiment ANTAB file into the FITS-IDI files.
    It will also re-archive the files.

    If the ANTAB file is already present in the directory, it will assume that the information was already
    appended.
    """
    if len(glob.glob("*.antab")) == 0:
        environment.shell_command("append_antab_idi,py", shell=True)
        exp.log('append_antab_idi.py')
        environment.archive("-fits", exp, "*IDI*")

    return True


def send_letters(exp):
    """Remembers you to update the PI letter and send it , and the pipeletter, to the PIs.
    Finally, it runs parsePIletter.
    """
    # dialog.continue_dialog("Please update the PI letter if needed before continue.", f"{exp.expname} -- PI letter")
    environment.archive("-stnd", exp, f"{exp.expname.lower()}.piletter")
    # environment.shell_command("parsePIletter.py", ["-s", exp.obsdatetime.strftime("%b%y"),
    #                                                f"{exp.expname.lower()}.piletter"])
    #TODO: what if there are co-pis
    print("\033[1mSend the letters to the PI.\033[0m")
    print(f"Send the PI letter to {exp.piname.capitalize()}: {exp.email} (CC jops@jive.eu).")
    print(f"Send the pipe letter to {exp.piname.capitalize()}: {exp.email}.")
    return True


def antenna_feedback(exp):
    print("\n\nNow it is also time to bookkeep the issues that you may have seen in the antennas.\n")
    print(f"Update the (Grafana) database with the technical issues that antennas did not raise at:")
    print(f"http://archive.jive.nl/scripts/getfeed.php?exp={exp.expname.upper()}_{exp.obsdate.strftime('%y%m%d')}\n")
    print("Also go to the JIVE RedMine to write down the relevant issues with particular antennas:")
    print("https://jrm.jive.nl/projects/science-support/news\n\n")


def nme_report(exp):
    if exp.expname[0] == 'N':
        # This is a NME.
        print('Now it is time to write the NME Report. Good luck!')
    else:
        print('Experiment done!\nYou may have a coffee/tea now.')

    return True

# def archive_piletter(exp):
#     """(Re-)archive the PI letter.
#     """
#     environment.archive("-stnd", exp, f"{exp.expname.lower()}.piletter")

# print('Everything is archived. Please continue manually in pipe.\n')
# Work at eee done!!


