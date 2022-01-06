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
            exp.log(f"mkdir -p {a_dir}", False)


def get_files_from_vlbeer(exp):
    """Retrieves the antabfs, log, and flag files that should be in vlbeer for the given experiment.
    """
    cd = f"cd $IN/{exp.supsci}/{exp.expname.lower()}"
    scp = lambda ext : "scp evn@vlbeer.ira.inaf.it:vlbi_arch/" \
                       f"{exp.obsdatetime.strftime('%b%y').lower()}/{exp.expname.lower()}\*.{ext} ."
    for ext in ('log', 'antabfs', 'flag'):
        cmd, output = env.ssh('pipe@jop83', ';'.join([cd, scp(ext)]))
        exp.log(cmd, False)
        the_files = [o for o in output.split('\n') if o != ''] # just to avoid trailing \n
        for a_file in the_files:
            ant = a_file.split('.')[0].replace(exp.expname.lower(), '').capitalize()
            if ext == 'log':
                exp.antennas[ant].logfsfile = True
            elif ext == 'antabfs':
                exp.antennas[ant].antabfsfile = True

    exp.log(f"# Log files found for:\n# {', '.join(exp.antennas.logfsfile)}", False)
    exp.log(f"# Antab files found for:\n# {', '.join(exp.antennas.filefsfile)}", False)
    exp.log(f"# Missing ANTAB files for:\n# {', '.join(set(exp.antennas.names)-set(exp.antennas.filefsfile))}", False)
    return True


def create_uvflg(exp):
    """Produces the combined uvflg file containing the full flagging from all telescopes.
    """
    cd = f"cd $IN/{exp.supsci}/{exp.expname.lower()}"
    if not env.remote_file_exists('pipe@jop83', f"{cd}/{exp.expname.lower()}.uvflg"):
        cmd, output = env.ssh('pipe@jop83', ';'.join([cd, 'uvflgfs.sh']))
        exp.log(cmd + output.replace('\n', '\n# '), False)
        print(output)
        cmd, output = env.ssh('pipe@jop83', ';'.join([cd, f"cat *uvflgfs {exp.expname.lower()}.uvflg"]))
        exp.log(cmd + output, False)
        print(output)

    return True


def run_antab_editor(exp):
    """Opens antab_editor.py for the given experiment.
    """
    cd = f"cd $IN/{exp.supsci}/{exp.expname.lower()}"
    if exp.eEVNname is not None:
        print(f"This experiment {exp.expname} is part of the e-EVN run {exp.eEVNname}.\n" \
              "Please run antab_editor.py manually to include all experiment associated to the run " \
              "(using the '-a' option).\n\nThen run the post-processing again.")
        # I fake it to be sucessful in the object to let it to run seemless in a following iteraction
        exp.last_step = 'antab_editor'
        return False

    if len(exp.correlator_passes) == 2:
        cmd, output = env.ssh('pipe@jop83', ';'.join([cd, 'antab_editor.py -l']))
    else:
        cmd, output = env.ssh('pipe@jop83', ';'.join([cd, 'antab_editor.py']))

    exp.log(cmd, False)
    return True


def create_input_file(exp):
    """Copies the template of an input file for the EVN Pipeline and modifies the standard parameters.
    """
    # First copies the final uvflg and antab files to the input directory
    cdinp = f"cd /jop83_0/pipe/in/{exp.expname.lower()}/"
    cdtemp = f"cd /jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}/"
    if not env.remote_file_exists('pipe@jop83', f"{cdinp}{exp.expname.lower()}*.uvflg"):
        cmd, output = env.ssh('pipe@jop83', f"cp {cdtemp}{exp.expname.lower()}*.uvflg {cdinp}")
        exp.log(cmd, False)

    if not env.remote_file_exists('pipe@jop83', f"{cdinp}/{exp.expname.lower()}*.antab"):
        cmd, output = env.ssh('pipe@jop83', f"cp {cdtemp}/{exp.expname.lower()}.antab {cdinp}")
        exp.log(cmd, False)

    if env.remote_file_exists('pipe@jop83', f"{cdinp}/{exp.expname.lower()}*.inp.txt"):
        return True

    # Parameters to modify inside the input file
    if exp.supsci == 'marcote':
        cmd, output = env.ssh('pipe@jop83', 'give_me_next_userno.sh')
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

    cmd, output = env.ssh('pipe@jop83', "cp $IN/template.inp $IN/{1}/{1}.inp.txt;".format(exp.expname.lower()) + \
                 f"replace {to_change} -- $IN/{exp.expname.lower()}/{exp.expname.lower()}.inp.txt")
    exp.log(cmd, False)
    if len(exp.correlator_passes) > 1:
        cmd, output = env.ssh('pipe@jop83', "mv $IN/{1}/{1}.inp.txt $IN/{1}/{1}_1.inp.txt".format(exp.expname.lower()))
        exp.log(cmd, False)
        for i in range(2, len(exp.correlator_passes)+1):
            cmd, output = env.ssh('pipe@jop83',
                    "cp $IN/{1}/{1}_1.inp.txt $IN/{1}/{1}_{2}.inp.txt".format(exp.expname.lower(), i))
            exp.log(cmd, False)

    return True


def run_pipeline(exp):
    """Runs the EVN Pipeline
    """
    exp.log('# Running the pipeline...', True)
    cd = f"cd /jop83_0/pipe/in/{exp.expname.lower()}"
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
    cd = f"cd /jop83_0/pipe/out/{exp.expname.lower()}"
    cmd, output = env.ssh('pipe@jop83', f"{cd} && comment_tasav_file.py {exp.expname.lower()}")
    exp.log(cmd)
    return True


def pipeline_feedback(exp):
    """Runs the feedback.pl script after the EVN Pipeline has run.
    """
    cd= f"cd /jop83_0/pipe/out/{exp.expname.lower()}"
    cmd, output = env.ssh('pipe@jop83', f"{cd} && feedback.pl -exp '{exp.expname.lower()}' " \
                  f"-jsss '{exp.supsci}' -source '{' '.join([s.name for s in exp.sources])}'")
    exp.log(cmd)
    return True


def archive(exp):
    """Archives the EVN Pipeline results.
    """
    for f in ('in', 'out'):
        cd= f"cd /jop83_0/pipe/{f}/{exp.expname.lower()}"
        cmd, output = env.ssh('jops@jop83', f"{cd} && archive -pipe -e {exp.expname.lower()}_{exp.obsdate}")
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




