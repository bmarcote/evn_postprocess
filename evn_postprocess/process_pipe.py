#!/usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the pipe computer.
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
from . import experiment
from . import environment as env


def create_folders(exp):
    """Creates the folder required for the post-processing of the experiment
    - @eee: /data0/{supportsci}/{exp.upper()}

    Inputs
        - expname: str
            Experiment name (case insensitive).
        - supsci: str
            Surname of the assigned support scientist.
    """
    tmpdir = f"/jop83_0/pipe/in/{exp.supsci.lower()}/{exp.expname.lower()}"
    indir = f"/jop83_0/pipe/in/{exp.expname.lower()}"
    outdir = f"/jop83_0/pipe/out/{exp.expname.lower()}"
    for a_dir in (tmpdir, indir, outdir):
        if not env.remote_file_exists('pipe@jop83', a_dir):
            output = env.ssh('pipe@jop83', f"mkdir -p {a_dir}")
            exp.log(f"mkdir -p {a_dir}")


def get_files_from_vlbeer(exp):
    """Retrieves the antabfs, log, and flag files that should be in vlbeer for the given experiment.
    """
    cd = f"cd /jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}"
    scp = lambda ext : "scp evn@vlbeer.ira.inaf.it:vlbi_arch/" \
                       f"{exp.obsdatetime.strftime('%b%y').lower()}/{exp.expname.lower()}\*.{ext} ."
    cmd, output = env.ssh('pipe@jop83', ';'.join([cd, scp('flag')]))
    exp.log(cmd)
    for ext in ('log', 'antabfs'):
        cmd, output = env.ssh('pipe@jop83', ';'.join([cd, scp(ext)]))
        exp.log(cmd)
        cmd, output = env.ssh('pipe@jop83', ';'.join([cd, f"ls {exp.expname.lower()}*{ext}"]))
        the_files = [o for o in output.split('\n') if o != ''] # just to avoid trailing \n
        for a_file in the_files:
            ant = a_file.split('.')[0].replace(exp.expname.lower(), '').capitalize()
            if ext == 'log':
                exp.antennas[ant].logfsfile = True
            elif ext == 'antabfs':
                exp.antennas[ant].antabfsfile = True

    exp.log(f"\n# Log files found for:\n# {', '.join(exp.antennas.logfsfile)}")
    if len(set(exp.antennas.names)-set(exp.antennas.logfsfile)) > 0:
        exp.log(f"# Missing files for: {', '.join(set(exp.antennas.names)-set(exp.antennas.logfsfile))}\n")
    else:
        exp.log("# No missing log files for any station.\n")

    exp.log(f"# Antab files found for:\n# {', '.join(exp.antennas.antabfsfile)}")
    if len(set(exp.antennas.names)-set(exp.antennas.antabfsfile)) > 0:
        exp.log(f"# Missing files for: {', '.join(set(exp.antennas.names)-set(exp.antennas.antabfsfile))}\n")
    else:
        exp.log("# No missing antab files for any station.\n")

    return True


def run_antab_editor(exp):
    """Opens antab_editor.py for the given experiment.
    """
    cd = f"cd /jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}"
    cdinp = f"/jop83_0/pipe/in/{exp.expname.lower()}"
    cdtemp = f"/jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}"
    if env.remote_file_exists('pipe@jop83', f"{cdinp}/{exp.expname.lower()}*.antab"):
        print("Antab file already found in {cdinp}.")
        return True

    if env.remote_file_exists('pipe@jop83',
                    f"{cdtemp}/{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}*.antab"):
        print("Copying Antab file from {cdinp}.")
        cmd, output = env.ssh('pipe@jop83', f"cp {cdtemp}/*.antab {cdinp}/")
        exp.log(cmd)
        if (exp.eEVNname is not None) and (exp.expname != exp.eEVNname):
            # We need to rename to the actual name
            for an_antab in env.ssh('pipe@jop83', f"{cdinp}/*.antab")[1].split('\n'):
                if an_antab != '':
                    env.ssh('pipe@jop83', f"mv {an_antab} " \
                    f"{'/'.join([*an_antab.split('/')[:-1], an_antab.split('/')[-1].replace(exp.eEVNname, exp.expname)])}")
        return True

    if exp.eEVNname is not None:
        print(f"This experiment {exp.expname} is part of the e-EVN run {exp.eEVNname}.\n" \
              "Please run antab_editor.py manually to include all experiment associated to the run " \
              "(using the '-a' option).\n\nThen run the post-processing again.")
        # I fake it to be sucessful in the object to let it to run seemless in a following iteraction
        return None

    if len(exp.correlator_passes) == 2:
        cmd, output = env.ssh('-Y '+'pipe@jop83', ';'.join([cd, 'antab_editor.py -l']))
    else:
        cmd, output = env.ssh('-Y '+'pipe@jop83', ';'.join([cd, 'antab_editor.py']))

    print('\n\n\nRun antab_editor.py manually in pipe.')
    exp.log(cmd)
    return None


def create_uvflg(exp):
    """Produces the combined uvflg file containing the full flagging from all telescopes.
    """
    if (exp.eEVNname is None) or (exp.expname == exp.eEVNname):
        cd = f"cd /jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}"
        if not env.remote_file_exists('pipe@jop83', f"{cd}/{exp.expname.lower()}.uvflg"):
            cmd, output = env.ssh('pipe@jop83', ';'.join([cd, 'uvflgall.csh']))
            print(output)
            output_tail = []
            for outline in output[::-1].split('\n'):
                if 'line ' in outline:
                    break
                output_tail.append(outline)

            exp.log(cmd + '\n# ' + ',\n'.join(output_tail[::-1]).replace('\n', '\n# '))
            cmd, output = env.ssh('pipe@jop83', ';'.join([cd, f"cat *uvflgfs > {exp.expname.lower()}.uvflg"]))
            exp.log(cmd)
    else:
        cd = f"/jop83_0/pipe/in/{exp.supsci}/{exp.eEVNname.lower()}"
        if not env.remote_file_exists('pipe@jop83', f"{cd}/{exp.eEVNname.lower()}.uvflg"):
            print(f"You first need to process the original experiment in this e-EVN run ({exp.eEVNname}).")
            print("Once you have created the .uvflg file for such expeirment I will be able to run by myself.")
            return None

    cdinp = f"/jop83_0/pipe/in/{exp.expname.lower()}"
    cdtemp = f"/jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}"
    if not env.remote_file_exists('pipe@jop83', f"{cdinp}/{exp.expname.lower()}*.uvflg"):
        cmd, output = env.ssh('pipe@jop83', f"cp {cdtemp}/*.uvflg {cdinp}/")
        exp.log(cmd)

    return True


def create_input_file(exp):
    """Copies the template of an input file for the EVN Pipeline and modifies the standard parameters.
    """
    # First copies the final uvflg and antab files to the input directory
    cdinp = f"/jop83_0/pipe/in/{exp.expname.lower()}/"
    cdtemp = f"/jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}/"

    if env.remote_file_exists('pipe@jop83', f"{cdinp}/{exp.expname.lower()}*.inp.txt"):
        return True

    # Parameters to modify inside the input file
    if exp.supsci == 'marcote':
        cmd, output = env.ssh('pipe@jop83', 'give_me_next_userno.sh')
        if (output is None) or (output.replace('\n', '').strip() == ''):
            raise ValueError('Did not get any output from give_me_next_userno.sh in pipe')
        userno = output
    else:
        userno = 'XXXXX'

    bpass = ', '.join([s.name for s in exp.sources if s.type is experiment.SourceType.fringefinder])
    pcal = ', '.join([s.name for s in exp.sources if s.type is experiment.SourceType.calibrator])
    targets = ', '.join([s.name for s in exp.sources if (s.type is experiment.SourceType.target) or \
                                                            (s.type is experiment.SourceType.other)])
    to_change = f"'experiment = n05c3' 'experiment = {exp.expname.lower()}' " \
                f"'userno = 3602' 'userno = {userno}' " \
                f"'refant = Ef, Mc, Nt' 'refant = {', '.join(exp.refant)}' " \
                f"'plotref = Ef' 'plotref = {', '.join(exp.refant)}' " \
                f"'bpass = 3C345, 3C454.3' 'bpass = {bpass}' " \
                f"'phaseref = 3C454.3' '# SOURCES THAT MAY BE INCLUDED: {pcal}\nphaseref = ' " \
                f"'target = J2254+1341' '# SOURCES THAT MAY BE INCLUDED: {targets}\ntarget = ' "

    if len(exp.correlator_passes) > 2:
        to_change += "'#doprimarybeam = 1' 'doprimarybeam = 1'"

    cmd, output = env.ssh('pipe@jop83',
        "cp /jop83_0/pipe/in/template.inp /jop83_0/pipe/in/{0}/{0}.inp.txt".format(exp.expname.lower()))
    exp.log(cmd, False)
    cmd, output = env.ssh('pipe@jop83',
         f"replace {to_change} -- /jop83_0/pipe/in/{exp.expname.lower()}/{exp.expname.lower()}.inp.txt")
    exp.log(cmd, False)
    if len(exp.correlator_passes) > 1:
        cmd, output = env.ssh('pipe@jop83', "mv /jop83_0/pipe/in/{1}/{1}.inp.txt /jop83_0/pipe/in/{1}/{1}_1.inp.txt".format(exp.expname.lower()))
        exp.log(cmd, False)
        for i in range(2, len(exp.correlator_passes)+1):
            cmd, output = env.ssh('pipe@jop83',
                    "cp /jop83_0/pipe/in/{1}/{1}_1.inp.txt /jop83_0/pipe/in/{1}/{1}_{2}.inp.txt".format(exp.expname.lower(), i))
            exp.log(cmd, False)

    return True


def run_pipeline(exp):
    """Runs the EVN Pipeline
    """
    exp.log('# Running the pipeline...', True)
    cd = f"cd /jop83_0/pipe/in/{exp.expname.lower()}"
    # TODO:
    print('\n\n\n\033[1mModify the input file for the pipeline and run it manually\033[0m')
    exp.last_step = 'pipeline'
    return None
    if len(exp.correlator_passes) > 1:
        cmd, output = env.ssh('pipe@jop83', f"{cd};EVN.py {exp.expname.lower()}_1.inp.txt")
    else:
        cmd, output = env.ssh('pipe@jop83', f"{cd};EVN.py {exp.expname.lower()}.inp.txt")

    exp.log(cmd, False)
    exp.log('# Pipeline finished.', True)
    if len(exp.correlator_passes) == 2:
        # TODO: implement line in the normal pipeline
        cmd, output = env.ssh('pipe@jop83', f"{cd};EVN.py {exp.expname.lower()}_2.inp.txt")

    return True

# Authentification for the credentials


def comment_tasav_files(exp):
    """Creates the comment and tasav files after the EVN Pipeline has run.
    """
    cdin = f"/jop83_0/pipe/in/{exp.expname.lower()}"
    cdout = f"/jop83_0/pipe/out/{exp.expname.lower()}"
    if not (env.remote_file_exists('pipe@jop83', f"{cdout}/eb088\*.comment") and \
           env.remote_file_exists('pipe@jop83', f"{cdin}/eb088\*.tasav.txt")):
        cmd, output = env.ssh('pipe@jop83', f"cd {cdout} && comment_tasav_file.py {exp.expname.lower()}")
        exp.log(cmd)

    return True


def pipeline_feedback(exp):
    """Runs the feedback.pl script after the EVN Pipeline has run.
    """
    cd= f"cd /jop83_0/pipe/out/{exp.expname.lower()}"
    cmd, output = env.ssh('pipe@jop83', f"{cd} && feedback.pl -exp '{exp.expname.lower()}' " \
                  f"-jss '{exp.supsci}' -source '{' '.join([s.name for s in exp.sources])}'", stdout=None)
    exp.log(cmd)
    return True


def archive(exp):
    """Archives the EVN Pipeline results.
    """
    for f in ('in', 'out'):
        cd= f"cd /jop83_0/pipe/{f}/{exp.expname.lower()}"
        cmd, output = env.ssh('jops@jop83', f"{cd} && archive -pipe -e {exp.expname.lower()}_{exp.obsdate}", stdout=None)
        exp.log(cmd)

    return True


## Here there should be a dialog about checking pipeline results, do them manually...

def ampcal(exp):
    """Runs the ampcal.sh script to incorporate the gain corrections into the Grafana database.
    """
    cd= f"cd /jop83_0/pipe/out/{exp.expname.lower()}"
    cmd, output = env.ssh('pipe@jop83', f"{cd} && ampcal.sh")
    exp.log(cmd)
    return True





def get_vlba_antab(exp):
    """If the experiment containts VLBA antennas, it retrieves the *cal.vlba file from @ccs.
    """
    pass




