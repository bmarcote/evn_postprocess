#!/usr/bin/env python3
"""Script that runs semi-interactive SFXC post-correlation steps at the ccs computer.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.
"""

import os
import glob
from . import environment


def lis_files_in_ccs(exp) -> bool:
    """Returns if there are already lis files created in the experiment directory in ccc.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    return environment.remote_file_exists('jops@ccs',
                                          f"/ccs/expr/{eEVNname}/{eEVNname.lower()}*.lis")


def create_lis_files(exp) -> bool:
    """Creates the lis files in ccs.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    if not lis_files_in_ccs(exp):
        print("Creating lis file...")
        cmd = f"cd /ccs/expr/{eEVNname};/ccs/bin/make_lis -e {eEVNname}"
        environment.ssh('jops@ccs', cmd)
        exp.log(f"# In ccs:\ncd /ccs/expr/{eEVNname};/ccs/bin/make_lis -e {eEVNname}", False)

    return True


def get_lis_files(exp) -> bool:
    """Retrieves all lis files available in ccs for this experiment.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    cmds = []
    if len(glob.glob(f"{eEVNname.lower()}*.lis")) == 0:
        cmd, _ = environment.scp(f"jops@ccs:/ccs/expr/{eEVNname}/{eEVNname.lower()}*.lis", '.')
        exp.log(cmd, False)

    for a_lis in glob.glob("*.lis"):
        environment.split_lis_cont_line(a_lis)

    # In the case of e-EVN runs, a renaming of the lis files may be required:
    if eEVNname != exp.expname:
        for a_lis in glob.glob("*.lis"):
            # Modify the references for eEVNname to expname inside the lis files
            # if it has not been done yet
            if exp.expname.lower() not in a_lis:
                environment.update_lis_file(a_lis, eEVNname, exp.expname)
                cmds.append(f" Expname updated from {eEVNname} to {exp.expname} in {a_lis}.")
                exp.log(f" Expname updated from {eEVNname} to {exp.expname} in {a_lis}.")

            os.rename(a_lis, a_lis.replace(eEVNname.lower(), exp.expname.lower()))
            cmds.append(f"mv {a_lis} {a_lis.replace(eEVNname.lower(), exp.expname.lower())}")
            exp.log(f"mv {a_lis} {a_lis.replace(eEVNname.lower(), exp.expname.lower())}")

    return True




