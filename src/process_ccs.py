#!/usr/bin/env python3
"""Script that runs semi-interactive SFXC post-correlation steps at the ccs computer.
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


def parse_masterprojects(exp):
    """Obtains the observing epoch from the MASTER_PROJECTS.LIS located in ccc.
    In case of being an e-EVN experiment, it will add that information to self.eEVN.
    """
    cmd = f"grep {exp.expname} /ccs/var/log2vex/MASTER_PROJECTS.LIS"
    process = subprocess.Popen(["ssh", "jops@ccs", cmd], shell=False, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    output = process.communicate()[0].decode('utf-8')
    if process.returncode != 0:
        raise ValueError(f"Errorcode {process.returncode} when reading MASTER_PROJECTS.LIS."\
                + f"\n{exp.expname} is probably not in the EVN database.")

    if output.count('\n') == 2:
        # It is an e-EVN experiment!
        # One line will have EXP EPOCH.
        # The other one eEXP EPOCH EXP1 EXP2..
        inputs = [i.split() for i in output[:-1].split('\n')]
        for an_input in inputs:
            if an_input[0] == exp.expname:
                obsdate = an_input[1]
            else:
                # The first element is the expname of the e-EVN run
                exp.eEVNname = an_input[0]

        exp.obsdate = obsdate[2:]

    elif output.count('\n') == 1:
        expline = output[:-1].split()
        if len(expline) > 2:
            # This is an e-EVN, this experiment was the first one (so e-EVN is called the same)
            exp.eEVNname = expline[0].strip()
        else:
            exp.eEVNname = None

        exp.obsdate = expline[1].strip()[2:]

    else:
        raise ValueError(f"{exp.expname} not found in (ccs) MASTER_PROJECTS.LIS" \
                         + "or connection problem.")

    if exp.eEVNname is not None:
        return cmd, ' '.join([exp.obsdate, exp.expname, exp.eEVNname])
    else:
        return cmd, ' '.join([exp.obsdate, exp.expname])




def get_vixfile(exp):
    """Copies the .vix file from ccs to the current directory.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    cmd, output = '', ''
    if not os.path.isfile(f"{eEVNname.lower()}.vix"):
        cmd, output = actions.scp(f"jops@ccs:/ccs/expr/{eEVNname}/{eEVNname.lower()}.vix", '.')

    if not os.path.isfile(f"{exp.expname}.vix"):
        os.symlink(f"{eEVNname.lower()}.vix", f"{exp.expname}.vix")
        return [cmd, f"ln -s {eEVNname.lower()}.vix {exp.expname}.vix"], [output, '']

    return cmd, output


def get_expsumfile(exp):
    """Copies the .expsum file from jop83 to the current directory and parses it.
    """
    cmd = ''
    output = ''
    if not os.path.isfile(f"{exp.expname.lower()}.expsum"):
        cmd, output = actions.scp(f"jops@jop83:piletters/{exp.expname.lower()}.expsum", '.')

    return cmd, output


def parse_expsumfile(exp):
    exp.parse_expsum()


def get_piletter(exp):
    """Copies the piletter file from ccs to the current directory (unless it exists).
    """
    if not os.path.isfile(f"{exp.expname.lower()}.piletter"):
        # print(f"WARNING: {exp.expname.lower()}.piletter is being overwritten.")
        cmd, output = actions.scp(f"jops@jop83:piletters/{exp.expname.lower()}.piletter", '.')
        return cmd, output
    return "#", "PI letter already exists in the current directory."


def lis_files_in_ccs(exp):
    """Returns if there are already lis files created in the experiment directory in ccc.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    return actions.remote_file_exists('jops@ccs',
                                      f"/ccs/expr/{eEVNname}/{eEVNname.lower()}*.lis")


def lis_files_in_local(exp):
    """Returns if there are already lis files created in the experiment in the current
    directory.
    """
    return len(glob.glob(f"{exp.expname.lower()}*.lis")) > 0


def create_lis_files(exp):
    """Creates the lis files in ccs.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    if lis_files_in_ccs(exp):
        print(f"WARNING: {eEVNname.lower()}*.lis files in ccs will be overwritten.")

    print("Creating lis file...")
    cmd = f"cd /ccs/expr/{eEVNname};/ccs/bin/make_lis -e {eEVNname}"
    output = actions.ssh('jops@ccs', cmd)
    return 'ssh jops@ccs:'+cmd, output


def get_lis_files(exp):
    """Retrieves all lis files available in ccs for this experiment.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    cmds, outputs = [], []
    if lis_files_in_local(exp):
        print(f"WARNING: {expname.lower()}*.lis files in eee will be overwritten.")

    cmd, output = actions.scp(f"jops@ccs:/ccs/expr/{eEVNname}/{eEVNname.lower()}*.lis", '.')
    cmds.append(cmd)
    outputs.append(output)

    for a_lis in glob.glob("*.lis"):
        actions.split_lis_cont_line(a_lis)

    # In the case of e-EVN runs, a renaming of the lis files may be required:
    if eEVNname != exp.expname:
        for a_lis in glob.glob("*.lis"):
            # Modify the references for eEVNname to expname inside the lis files
            actions.update_lis_file(a_lis, eEVNname, exp.expname)
            cmds.append(f" Expname updated from {eEVNname} to {exp.expname} in {a_lis}.")
            outputs.append('')
            os.rename(a_lis, a_lis.replace(eEVNname.lower(), exp.expname.lower()))
            cmds.append(f"mv {a_lis} {a_lis.replace(eEVNname.lower(), exp.expname.lower())}")
            outputs.append('')

    return cmds, outputs


def get_files(exp):
    """Retrieves all files from ccs (and piletter dir) that are relevant to the
    experiment. For piletter and .lis files checks if they already exist.
    """
    cmd_output = []
    cmd_output.append(get_vixfile(exp))
    cmd_output.append(get_expsumfile(exp))
    exp.existing_piletter = os.path.isfile(f"{exp.expname.lower()}.piletter")
    if not exp.existing_piletter:
        cmd_output.append(get_piletter(exp))

    exp.existing_lisfile = lis_files_in_local(exp)
    if not exp.existing_lisfile:
        if not lis_files_in_ccs(exp):
            cmd_output.append(create_lis_files(exp))

        cmd_output.append(get_lis_files(exp))

    return cmd_output


def check_lisfiles(exp):
    """Check the existing .lis files to spot possible issues.
    """
    cmds, outputs = [], []
    for a_lis in glob.glob("*.lis"):
        cmd, output = actions.shell_command("checklis.py", a_lis)
        cmds.append(cmd)
        outputs.append(output)

    return cmds, outputs
