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
            if ext is 'log':
                exp.antennas[ant].logfsfile = True
            elif ext is 'antabfs':
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
        exp.log(cmd + output), False)
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

    bpass = ', '.join([s.name for s in exp.sources if s.type is metadata.SourceType.fringefinder])
    pcal = ', '.join([s.name for s in exp.sources if s.type is metadata.SourceType.calibrator])
    targets = ', '.join([s.name for s in exp.sources if (s.type is metadata.SourceType.target) or \
                                                            (s.type is metadata.SourceType.other)])
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
    if 'pipe' not in exp.connections:
        exp.connections['pipe'] = Pipe(user='pipe')

    out = exp.connections['pipe'].execute_commands([f"cd $OUT/{exp.expname.lower()} && ampcal.sh"])



# One should ask for editing the PI letter again, and then archive it and send the emails.


# def create_folders_ssh(exp):
#     """Moves to the support-scientist-associated folder for the given experiment.
#     If it does not exist, it creates it. It also creates the folders in the $IN and $OUT dirs.
#     """
#     # If required, move to the required directory (create it if needed).
#     tmpdir = '/jop83_0/pipe/in/{}/{}'.format(exp.supsci, exp.expname.lower())
#     indir = '/jop83_0/pipe/in/{}'.format(exp.supsci, exp.expname.lower())
#     outdir = '/jop83_0/pipe/out/{}'.format(exp.supsci, exp.expname.lower())
#     for adir in (tmpdir, indir, outdir):
#         if not env.remote_file_exists(_login, adir):
#             env.ssh(_login, f"mkdir {adir}")
#             os.makedirs(adir)
#             print(f"Directory {adir} has been created.")

##################################################################################################
#     if not os.path.isfile(f"{exp.expname.lower()}.piletter"):
#         cmd, output = env.scp(f"jops@jop83:piletters/{exp.expname.lower()}.piletter", '.')
#     return env.remote_file_exists('jops@ccs',
#                                       f"/ccs/expr/{eEVNname}/{eEVNname.lower()}*.lis")
#     cmd = f"cd /ccs/expr/{eEVNname};/ccs/bin/make_lis -e {eEVNname}"
#     output = env.ssh('jops@ccs', cmd)
# ##################################################################################################

def get_vlba_antab(exp):
    """If the experiment containts VLBA antennas, it retrieves the *cal.vlba file from @ccs.
    """
    pass



def prepare_input_file(exp, args):
    """Retrieves a template for the EVN Pipeline input file and do really minor changes. A final touch
    is required from the user.
    """
    passes2pipeline = [p for p in exp.correlator_passes if p.pipeline]
    if len(passes2pipeline) == 1:
        extensions = ['']
        # inpfiles = [f"./{exp.expname.lower()}.inp.txt"]
    else:
        extensions = [f"_{i+1}" for i in range(len(passes2pipeline))]
        # inpfiles = [f"./{exp.expname.lower()}_{i+1}.inp.txt" for i in range(len(passes2pipeline))]

    for i,ext in enumerate(extensions):
        inpfile = f"{exp.expname.lower()}{ext}.inp.txt"
        env.shell_command("cp", ["/jop83_0/pipe/in/template.inp", inpfile])
        with open(inpfile, 'r') as f:
            filecontent = f.read()

        if args.supsci == 'marcote':
            # only works for the cool and lazy programmer :)
            # TODO: Check that the output is indeed the output from the sh script...
            get_next_userno = env.shell_command("give_me_next_userno.sh")
            filecontent.replace('3602', get_next_userno)
            print(f"{inpfile} to be executed under AIPS userno. {get_next_userno}.")

        filecontent.replace('n05c3', f"{exp.exname.lower()}{ext}")
        with open(inpfile, 'w') as f:
            f.write(filecontent)

    env.can_continue("Update the input file(s) before running the EVN Pipeline")




def pre_pipeline(exp, args):
    """
    """
    move2dir(f"/jop83_0/pipe/in/{args.supsci}/{exp.expname.lower()}")

    get_files_from_vlbeer(exp)
    # TODO: VLBA ANTAB files
    env.shell_command("uvflgall.csh")
    # TODO: run the program (to be written) to get uvflg entries for all telescopes without/empty .uvflg files.
    env.shell_command("antab_check.py")
    env.can_continue('Check the ANTAB files and fix them now. Continue aftwards')
    # Unify all antabfs/uvflgfs files into one and copy them to the $IN/{expname} directory.
    env.shell_command("cat", [f"{exp.expname.lower()}*antabfs", ">", f"{exp.expname.lower()}.antab"],
                          shell=True)
    env.shell_command("cat", [f"{exp.expname.lower()}*uvflgfs", ">", f"{exp.expname.lower()}.uvflg"],
                          shell=True)
    env.shell_command("cp", [f"{exp.expname.lower()}.antab", f"/jop83_0/pipe/in/{exp.expname.lower()}/"],
                          shell=True)
    env.shell_command("cp", [f"{exp.expname.lower()}.uvflg", f"/jop83_0/pipe/in/{exp.expname.lower()}/"],
                          shell=True)

    move2dir(f"/jop83_0/pipe/in/{exp.expname.lower()}")

    prepare_input_file(exp, args)


def pipeline(exp):
    """Runs the EVN Pipeline.
    In case of multiple passes, it runs different processes of the pipeline in parallel.
    """
    move2dir(f"/jop83_0/pipe/in/{exp.expname.lower()}")

    while True:
        outputs = map(lambda inpfile : env.shell_command("EVN.py", inpfile),
                     [glob.glob(f"{exp.expname.lower()}*inp.txt")])
        # TODO: Check the output, if all the instances say OK.
        if False:
            # env.can_continue("The Pipeline has finished. Do you want to continue with the post-pipeline operations?")
            answer = env.yes_or_no_question("Do you want to continue with the post-pipeline operations? No to run the pipeline again (modify files before answering)")
            if answer:
                return True
        else:
            env.can_continue("It seems like a problem happened in the pipeline. Fix it and then I will rerun it.")


def archive(exp):
    """Archive the output of the EVN Pipeline for the given experiment.
    """
    for folder in ('in', 'out'):
        env.ssh('jops@jop83',
            f"cd /jop83_0/pipe/{folder}/{exp.expname.lower()};archive -pipe -e {exp.expname}_{exp.obsdate}")

    print("Pipeline results have been archived.")


def post_pipeline(exp, args):
    """Runs all steps required after the EVN Pipeline has run.
    """
    move2dir(f"/jop83_0/pipe/out/{exp.expname.lower()}/")

    # TODO: Check if files already exist.
    env.shell_command("comment_tasav_file.py", exp.expname.lower())

    passes2pipeline = [p for p in exp.correlator_passes if p.pipeline]
    if len(passes2pipeline) == 1:
        extensions = ['']
        # inpfiles = [f"./{exp.expname.lower()}.inp.txt"]
    else:
        extensions = [f"_{i+1}" for i in range(len(passes2pipeline))]
        # inpfiles = [f"./{exp.expname.lower()}_{i+1}.inp.txt" for i in range(len(passes2pipeline))]

    for ext in extensions:
        # TODO: Add the sour flag to avoid the manual input. Check which format properly works.
        env.shell_command("feedback.pl", ["-exp", f"{exp.expname.lower()}{ext}", "-jss", args.supsci])

    print('You should protect the private sources at http://archive.jive.nl/scripts/pipe/admin.php.')
    env.print_sourcelist_from_expsum(exp)
    archive(exp)
    env.can_continue("\nCheck the Pipeline results at:\n" +\
            f"http://old.jive.nl/archive-info?experiment={exp.expname}_{exp.obsdate}" +\
            "\n\nCan continue?")


def ampcal(exp):
    """Runs ampcal.sh script on the $OUT/EXPNAME directory
    """
    move2dir(f"/jop83_0/pipe/out/{exp.expname.lower()}/")
    env.shell_command("ampcal.sh")




