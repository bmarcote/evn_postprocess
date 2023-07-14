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
from collections import defaultdict
import subprocess
import numpy as np
from rich import print as rprint
from evn_support import check_antab_idi
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
    thereis_line = True if '_line' in ''.join(lisfiles) else False
    i_lines_done = 0
    passes = []
    if len(lisfiles) > 1:
        lisfiles.sort()

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
                                fitsidiname = f"{exp.expname.lower()}_{2*i_lines_done + 2}_1.IDI"
                            else:
                                fitsidiname = f"{exp.expname.lower()}_{2*i_lines_done + 1}_1.IDI"

                            to_pipeline = True
                            i_lines_done += 1
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
                if exp.eEVNname is None:
                    cmd, _ = environment.shell_command("j2ms2", ["-v", a_pass.lisfile.name,
                                                                 "fo:nosquash_source_table"],
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

def print_exp(exp):
    """Shows in the terminal all metadata related to the given experiment.
    """
    if exp.print_blessed(outputfile='notes.md'):
        return True

    return None


def open_standardplot_files(exp):
    """Calls gv to open all plots generated by standardplots.
    """
    standardplots = []
    for plot_type in ('weight', 'auto', 'cross', 'ampphase'):
        standardplots += glob.glob(f"{exp.expname.lower()}*{plot_type}*.ps")
    # standardplots = glob.glob(f"{exp.expname.lower()}*.ps")

    if len(standardplots) == 0:
        raise FileNotFoundError(f"Standardplots for {exp.expname} not found but expected.")

    if exp.silent_mode:
        rprint("[bold yellow]You did not want me to open the plots. " \
               "You shall do it manually[/bold yellow]")
        print("Take a look at the produced standard plots:")
        print("\n".join(["- {a_plot}" for a_plot in standardplots]))
        print("[green]Execute me again after that to continue the post-process.[/green]")
        return None

    try:
        for a_plot in standardplots:
            environment.shell_command("gv", a_plot, stdout=None, stderr=subprocess.STDOUT)
    except Exception as e:
        print(f"WARNING: Plots could not be opened. Do it manually.\nError: {e}.")
        return None

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
        cmd, output = environment.shell_command("flag_weights.py",
                                                [a_pass.msfile.name, str(a_pass.flagged_weights.threshold)],
                                                shell=True, stdout=None, stderr=subprocess.STDOUT)
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
    polconvert_written = subprocess.call(["grep", "Martí-Vidal,", f"{exp.expname.lower()}.piletter"],
                                         shell=False, stdout=subprocess.PIPE) == 0
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

                    for ant in exp.correlator_passes[0].antennas:
                        if (f"{ant.name.capitalize()}:" in tmp_line) and (not ant.observed):
                            tmp_line = tmp_line.replace(f"{ant.name.capitalize()}:",
                                                        f"{ant.name.capitalize()}: Could not observe.")

                    destfile.write(tmp_line)
                    if ('Further remarks:' in tmp_line) and (not polconvert_written):
                        if len(exp.antennas.polconvert) > 0:
                            destfile.write("\n")
                            if len(exp.antennas.polconvert) > 1:
                                s = f"s {', '.join(exp.antennas.polconvert[:-1])} and {exp.antennas.polconvert[-1]} "
                            else:
                                s = f" {exp.antennas.polconvert[0]} "

                            destfile.write(f"- Note that the antenna{s}originally observed linear polarizations, "
                                           "which were transformed to circular ones during post-processing via the "
                                           "PolConvert program (Martí-Vidal, et al. 2016, A&A,587, A143). Thanks to "
                                           "this correction, you can automatically recover the absolute EVPA value "
                                           "when using the antenna as reference station during fringe-fitting.\n")

                        ants_bw = {}
                        if len(set([cp.freqsetup.n_subbands for cp in exp.correlator_passes])) == 1:
                            for antenna in exp.correlator_passes[0].antennas:
                                if 0 < len(antenna.subbands) < exp.correlator_passes[0].freqsetup.n_subbands:
                                    # In case the antenna observed a consecutive number of subbands
                                    ant_sbs = np.array(antenna.subbands)
                                    ant_sbs[1:] = ant_sbs[1:] - ant_sbs[:-1]
                                    if (ant_sbs[1:] == 1).all():
                                        ants_bw[antenna.name] = \
                                                        [f"{min(antenna.subbands)+1}-{max(antenna.subbands)+1}"]
                                    else:
                                        ants_bw[antenna.name] = [f"{antenna.subbands}"]
                        else:
                            for antenna in exp.correlator_passes[0].antennas:
                                for i,a_pass in enumerate(exp.correlator_passes):
                                    if 0 < len(antenna.subbands) < a_pass.freqsetup.n_subbands:
                                        if antenna.name not in ants_bw:
                                            ant_sbs = np.array(antenna.subbands)
                                            ant_sbs[1:] = ant_sbs[1:] - ant_sbs[:-1]
                                            if (ant_sbs[1:] == 1).all():
                                                ants_bw[antenna.name] = [f"{min(antenna.subbands)+1}-"
                                                                         f"{max(antenna.subbands)+1} "
                                                                         f"(in correlator pass #{i+1})"]
                                            else:
                                                ants_bw[antenna.name] = [f"{antenna.subbands} "
                                                                         f"(in correlator pass #{i+1})"]
                                        else:
                                            ants_bw[antenna.name].append( \
                                                f"{min(antenna.subbands)+1}-{max(antenna.subbands)+1} "
                                                f"(in correlator pass #{i+1})")

                        if len(ants_bw) > 0:
                            ants_bw_r = defaultdict(list)
                            for ant in ants_bw:
                                for sb_range in ants_bw[ant]:
                                    ants_bw_r[sb_range].append(ant)

                            s = "- Note that "
                            for i,ant_r in enumerate(ants_bw_r):
                                if i == 0:
                                    s += f"{', '.join(ants_bw_r[ant_r])} only observed subbands {ant_r}, "
                                elif i== len(ants_bw_r)-1:
                                    s += f"and {', '.join(ants_bw_r[ant_r])} subbands {ant_r}, "
                                else:
                                    s += f"{', '.join(ants_bw_r[ant_r])} subbands {ant_r}, "

                            s += "due to their local bandwidth limitations.\n"
                            destfile.write(s)

                        s = "- Note that the data from the antenna"
                        s_end = " have been corrected for opacity in the Tsys/Gain Curve measurements."
                        if len(exp.antennas.opacity) > 1:
                            s += f"s {', '.join(exp.antennas.opacity[:-1])} and {exp.antennas.opacity[-1]}"
                            destfile.write(s + s_end)
                        elif len(exp.antennas.opacity) == 1:
                            s += f" {exp.antennas.opacity[0]}"
                            destfile.write(s + s_end)

    os.rename(f"{exp.expname.lower()}.piletter~", f"{exp.expname.lower()}.piletter")
    return True

def tconvert(exp):
    """Runs tConvert in all MS files available in the directory
    """
    for a_pass in exp.correlator_passes:
        if len(glob.glob(f"{a_pass.fitsidifile}*")) > 0:
            continue

        environment.shell_command("tConvert", ["-v", a_pass.lisfile.name], stdout=None, stderr=subprocess.STDOUT)

    return True


def polconvert(exp):
    """Checks if PolConvert is required for any antenna.
    In that case, prepares the templates for running it and (potentially in the future?)
    will run it. For now it just requests the user to run it manually.
    """
    if len(exp.antennas.polconvert) > 0:
        polconv_inp = Path('./polconvert_inputs.toml')
        if not polconv_inp.exists():
            exp.log("cp ~/polconvert/polconvert_inputs.toml ./polconvert_inputs.toml")
            environment.shell_command('cp', ['/home/jops/polconvert/polconvert_inputs.toml',
                                      './polconvert_inputs.toml'], shell=True, stdout=None)
            environment.shell_command('sed', ['-i', f"'s/es100_1_1.IDI6/{exp.expname.lower()}_1_1.IDI*/g'",
                                      polconv_inp.name], shell=True, bufsize=None, stdout=None)
            environment.shell_command('sed', ['-i', f"'s/es100_1_1.IDI/{exp.expname.lower()}_1_1.IDI/g'",
                                      polconv_inp.name], shell=True, bufsize=None, stdout=None)
            ants = ', '.join(["\"" + ant.upper() + "\"" for ant in exp.antennas.polconvert])
            environment.shell_command('sed', ['-i', "'s/\"T6\"/" + f"{ants}/g'", polconv_inp.name],
                                      shell=True, bufsize=None, stdout=None)
            ants = ', '.join(["\"" + ant.name.upper() + "\"" for ant in exp.antennas if not ant.observed])
            environment.shell_command('sed', ['-i', "'s/\"EA\"/" + f"{ants}/g'", polconv_inp.name],
                                      shell=True, bufsize=None, stdout=None)

        rprint("\n\n[red bold]PolConvert needs to be run manually[/red bold]\n")
        print("You would find the input template in the current directory.")
        print("Edit it manually and then run it with:\n")
        rprint("[bold]> polconvert.py  polconvert_inputs.toml[/bold]")
        rprint("\n\n[red bold]Once PolConvert has run, re-run me[/red bold]\n\n")
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

    idi_ori = Path(exp.cwd / 'idi_ori/')
    idi_ori.mkdir(exist_ok=True)
    for an_idi in Path(exp.cwd).glob('*IDI*'):
        if '.PCONVERT' not in an_idi.name:
            an_idi.rename(idi_ori / an_idi.name)

    for an_idi in Path(exp.cwd).glob('*IDI*.PCONVERT'):
        an_idi.rename(an_idi.name.replace('.PCONVERT', ''))

    exp.log("mkdir idi_ori")
    exp.log("mv *IDI *IDI? *IDI?? *IDI???  idi_ori/")
    exp.log("zmv '(*).PCONVERT' '$1'")
    # Creates a new MS with the PolConverted-data in order to plot it to check if the conversion run properly
    _ = environment.shell_command("idi2ms.py", ['--delete',
                                            f"{exp.correlator_passes[0].msfile.name.replace('.ms', '-pconv.ms')}",
                                            f"'{exp.expname.lower()}_1_1.IDI*'"])
    if exp.refant is not None:
        refant = exp.refant[0] if len(exp.refant) == 1 else f"({'|'.join(exp.refant)})"
    else:
        for ant in ('EF', 'O8', 'YS', 'MC', 'GB', 'AT', 'PT'):
            if (ant in exp.antennas.names) and (exp.antennas[ant].observed):
                refant = ant
                break
        raise ValueError("Could not find a good reference antenna for standardplots. Please specify it manually")

    _ = environment.shell_command("standardplots",
                                           [f"{exp.correlator_passes[0].msfile.name.replace('.ms', '-pconv.ms')}",
                                            refant, ','.join(exp.sources_stdplot)], stdout=None,
                                            stderr=subprocess.STDOUT)

    for a_plot in glob.glob(f"{exp.expname.lower()}-*-pconv-cross*.ps"):
        environment.shell_command("gv", a_plot, stdout=None, stderr=subprocess.STDOUT)

    exp.last_step = 'post_polconvert'
    # Create again a MS from these converted files so I can run standardplots over the corrected data
    # TODO: Doing it manually for now
    rprint("\n\n[bold green]If PolConvert worked fine, re-run me to continue. " \
           "Otherwise fix it manually before.[/bold green]\n")
    return None

def post_post_polconvert(exp):
    """When PolConvert run properly and the user continued, it checks if the standardplots from the
    converted MS (exp-pconv.ms) exist and then rename those plots to the usual name.
    """
    if len(exp.antennas.polconvert) == 0:
        return True

    stdplot_files = glob.glob('*-pconv*.ps')
    if len(stdplot_files) > 0:
        for stdplot_file in stdplot_files:
            stdplot_file = Path(stdplot_file)
            stdplot_file.rename(str(stdplot_file).replace('-pconv', ''))

    return True

def set_credentials(exp):
    """Sets the credentials for the given experiment.
    In case of an NME or test, it does not set any credential.
    Otherwise, it will take the credentials from a .auth file if already exists,
    or creates such file iwth a new password.
    """
    if (exp.expname.upper()[0] == 'N') or (exp.expname.upper()[0] == 'F'):
        rprint(f"\n[green][bold]NOTE:[/bold] {exp.expname} is an NME or test experiment.\n"
               "No authentification will be set.[/green]")
    elif len(glob.glob("*_*.auth")) == 1:
        # Some credentials are already in place.
        exp.set_credentials(*glob.glob("*_*.auth")[0].split('.')[0].split('_'))

    elif len(glob.glob("*_*.auth")) > 1:
        raise ValueError("More than one .auth file found in the directory.")
    else:
        possible_char = string.digits + string.ascii_letters
        exp.set_credentials(username=exp.expname.lower(),
                            password="".join(random.sample(possible_char, 12)))
        environment.shell_command("touch", f"{exp.credentials.username}_{exp.credentials.password}.auth")
        exp.log(f"touch {exp.credentials.username}_{exp.credentials.password}.auth")

    return True


def create_pipelet(exp):
    """Makes a copy of the PI letter including the credentials to download the experiment.
    If there are no credentials for the experiment (unprotected ones, like the NMEs), then it does
    not create any file.
    If the file exists, it will be overwritten.
    """
    environment.shell_command("pipelet.py", [exp.expname.lower(), exp.supsci.lower()])
    exp.log(f"pipelet.py {exp.expname.lower()} {exp.supsci.lower()}")
    return True


def archive(exp):
    # Compress all figures from standardplots if they haven't been yet
    if len(glob.glob("*.ps")) > 0:
        # This avoids issues as it seems like gzip freezes when overwriting the same files
        if len(glob.glob("*.ps.gz")) > 0:
            environment.shell_command("rm -rf", "*ps.gz", shell=True)

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
    fits2check = glob.glob(f"{exp.expname.lower()}_*_*.IDI1") + glob.glob(f"{exp.expname.lower()}_*_*.IDI")
    assert len(fits2check) > 0, "Could not find FITS-IDI to append Tsys/GC!"

    if (not all([check_antab_idi.check_consistency(a_fits, verbose=False) for a_fits in fits2check])) \
       or (len(glob.glob(f"{exp.expname.lower()}*.antab")) == 0):
        environment.shell_command("append_antab_idi.py", "-r", shell=True, stdout=None)
        exp.log('append_antab_idi.py')
        if not all([check_antab_idi.check_consistency(a_fits) for a_fits in fits2check]):
            # As now everything should be OK. Means that something failed.
            rprint(f"\n\n[red bold]The Tsys/GC could not be imported into the FITS-IDI.[/red bold]")
            return False
    else:
        rprint("[green]ANTAB information already appended into the FITS-IDI files.[/green]")

    environment.archive("-fits", exp, f"{exp.expname.lower()}_*_*.IDI*")
    return True


def send_letters(exp):
    """Remembers you to update the PI letter and send it , and the pipeletter, to the PIs.
    Finally, it runs parsePIletter.
    """
    environment.archive("-stnd", exp, f"{exp.expname.lower()}.piletter")
    print("\n\n\n")
    rprint("[center][bold red] --- Send the PI letter --- [/bold red][/center]")
    pi = "\n"
    if isinstance(exp.piname, list):
        for a_piname,an_email in zip(exp.piname, exp.email):
            pi += f"{a_piname.capitalize()}: {an_email}\n"
    else:
        pi += f"{exp.piname.capitalize()}: {exp.email}\n"

    rprint(f"[green]Send the file [bold]{exp.expname.lower()}.piletter"
           f"{'_auth' if exp.credentials.password is not None else ''}[/bold] to " + pi + \
           "and CCing jops@jive.eu.[/green]")
    return True


def antenna_feedback(exp):
    rprint("\n[center][bold red] --- Also update the database with the observed issues --- [/bold red][/center]")
    rprint("[bold]Now it is also time to bookkeep the issues that you may have seen in the antennas at[/bold]")
    rprint(f"{exp.feedback_page()}\n")
    rprint("[bold]Also go to the JIVE RedMine to write down the relevant issues with particular antennas[/bold]:")
    rprint("https://jrm.jive.nl/projects/science-support/news\n\n")
    return True


def nme_report(exp):
    if exp.expname[0] == 'N':
        # This is a NME.
        rprint("[center][bold red]Now it is time to write the NME Report... Good luck![/bold red][/center]")
    else:
        rprint("[center][bold]Experiment done![/bold][/center]\n")
        print("You may have a coffee/tea after finishing the last tasks!")

    return True
