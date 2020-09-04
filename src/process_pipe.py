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
from . import metadata
from . import actions


# TODO: Make a decorator function to run all functions remotely in pipe (but launched in eee)
# Is it possible to also add a screen instance?

def move2dir(path, warning_no_move=False):
    """If path is not the current directory, it moves to it and notifies it in the stdout.
    If warning_no_move is True, then it would also write a message that path is already the current
    directory
    """
    if path is not os.getcwd():
        os.chdir(path)
        print(f"Moved to {path}.")
    elif warning_no_move:
        print(f"Already running at {path}.")

    return True


def folders(exp, args):
    """Moves to the support-scientist-associated folder for the given experiment.
    If it does not exist, it creates it. It also creates the folders in the $IN and $OUT dirs.
    """
    # If required, move to the required directory (create it if needed).
    expdir = '/jop83_0/pipe/in/{}/{}'.format(args.supsci, exp.expname.lower())
    indir = '/jop83_0/pipe/in/{}'.format(args.supsci, exp.expname.lower())
    outdir = '/jop83_0/pipe/out/{}'.format(args.supsci, exp.expname.lower())
    for adir in (expdir, indir, outdir):
        if not os.path.isdir(adir):
            os.makedirs(adir)
            print(f"Directory {adir} has been created.")

    move2dir(expdir, True)


def get_files_from_vlbeer(exp):
    """Retrieves the antabfs and log files that should be in vlbeer for the given experiment.
    """
    for ext in ('log', 'antabfs'):
        actions.scp(f"evn@vlbeer.ira.inaf.it:vlbi_arch/{exp.obsdatetime.strftime('%b%y').lower()}/{exp.expname.lower()}*.{ext}", ".")

    # TODO: check if there are ANTAB files in the previous/following month...
    print(actions.shell_command("ls", ["-l", "*antab*", "*log"], shell=True)[1])


def get_vlba_antab(exp):
    """If the experiment containts VLBA antennas, it retrieves the *cal.vlba file from @ccs.
    """
    pass



def prepare_input_file(exp, args):
    """Retrieves a template for the EVN Pipeline input file and do really minor changes. A final touch
    is required from the user.
    """
    passes2pipeline = [p for p in exp.passes if p.pipeline]
    if len(passes2pipeline) == 1:
        extensions = ['']
        # inpfiles = [f"./{exp.expname.lower()}.inp.txt"]
    else:
        extensions = [f"_{i+1}" for i in range(len(passes2pipeline))]
        # inpfiles = [f"./{exp.expname.lower()}_{i+1}.inp.txt" for i in range(len(passes2pipeline))]

    for i,ext in enumerate(extensions):
        inpfile = f"{exp.expname.lower()}{ext}.inp.txt"
        actions.shell_command("cp", ["/jop83_0/pipe/in/template.inp", inpfile])
        with open(inpfile, 'r') as f:
            filecontent = f.read()

        if args.supsci is 'marcote':
            # only works for the cool and lazy programmer :)
            # TODO: Check that the output is indeed the output from the sh script...
            get_next_userno = actions.shell_command("give_me_next_userno.sh")
            filecontent.replace('3602', get_next_userno)
            print(f"{inpfile} to be executed under AIPS userno. {get_next_userno}.")

        filecontent.replace('n05c3', f"{exp.exname.lower()}{ext}")
        with open(inpfile, 'w') as f:
            f.write(filecontent)

    actions.can_continue("Update the input file(s) before running the EVN Pipeline")




def pre_pipeline(exp, args):
    """
    """
    move2dir(f"/jop83_0/pipe/in/{args.supsci}/{exp.expname.lower()}")

    get_files_from_vlbeer(exp)
    # TODO: VLBA ANTAB files
    actions.shell_command("uvflgall.csh")
    # TODO: run the program (to be written) to get uvflg entries for all telescopes without/empty .uvflg files.
    actions.shell_command("antab_check.py")
    actions.can_continue('Check the ANTAB files and fix them now. Continue aftwards')
    # Unify all antabfs/uvflgfs files into one and copy them to the $IN/{expname} directory.
    actions.shell_command("cat", [f"{exp.expname.lower()}*antabfs", ">", f"{exp.expname.lower()}.antab"],
                          shell=True)
    actions.shell_command("cat", [f"{exp.expname.lower()}*uvflgfs", ">", f"{exp.expname.lower()}.uvflg"],
                          shell=True)
    actions.shell_command("cp", [f"{exp.expname.lower()}.antab", f"/jop83_0/pipe/in/{exp.expname.lower()}/"],
                          shell=True)
    actions.shell_command("cp", [f"{exp.expname.lower()}.uvflg", f"/jop83_0/pipe/in/{exp.expname.lower()}/"],
                          shell=True)

    move2dir(f"/jop83_0/pipe/in/{exp.expname.lower()}")

    prepare_input_file(exp, args)


def pipeline(exp):
    """Runs the EVN Pipeline.
    In case of multiple passes, it runs different processes of the pipeline in parallel.
    """
    move2dir(f"/jop83_0/pipe/in/{exp.expname.lower()}")

    while True:
        outputs = map(lambda inpfile : actions.shell_command("EVN.py", inpfile),
                     [glob.glob(f"{exp.expname.lower()}*inp.txt")])
        # TODO: Check the output, if all the instances say OK.
        if False:
            # actions.can_continue("The Pipeline has finished. Do you want to continue with the post-pipeline operations?")
            answer = actions.yes_or_no_question("Do you want to continue with the post-pipeline operations? No to run the pipeline again (modify files before answering)")
            if answer:
                return True
        else:
            actions.can_continue("It seems like a problem happened in the pipeline. Fix it and then I will rerun it.")


def archive(exp):
    """Archive the output of the EVN Pipeline for the given experiment.
    """
    for folder in ('in', 'out'):
        actions.ssh('jops@jop83',
            f"cd /jop83_0/pipe/{folder}/{exp.expname.lower()};archive -pipe -e {exp.expname}_{exp.obsdate}")

    print("Pipeline results have been archived.")


def post_pipeline(exp, args):
    """Runs all steps required after the EVN Pipeline has run.
    """
    move2dir(f"/jop83_0/pipe/out/{exp.expname.lower()}/")

    # TODO: Check if files already exist.
    actions.shell_command("comment_tasav_file.py", exp.expname.lower())

    passes2pipeline = [p for p in exp.passes if p.pipeline]
    if len(passes2pipeline) == 1:
        extensions = ['']
        # inpfiles = [f"./{exp.expname.lower()}.inp.txt"]
    else:
        extensions = [f"_{i+1}" for i in range(len(passes2pipeline))]
        # inpfiles = [f"./{exp.expname.lower()}_{i+1}.inp.txt" for i in range(len(passes2pipeline))]

    for ext in extensions:
        # TODO: Add the sour flag to avoid the manual input. Check which format properly works.
        actions.shell_command("feedback.pl", ["-exp", f"{exp.expname.lower()}{ext}", "-jss", args.supsci])

    print('You should protect the private sources at http://archive.jive.nl/scripts/pipe/admin.php.')
    actions.print_sourcelist_from_expsum(exp)
    archive(exp)
    actions.can_continue("\nCheck the Pipeline results at:\n" +\
            f"http://old.jive.nl/archive-info?experiment={exp.expname}_{exp.obsdate}" +\
            "\n\nCan continue?")


def ampcal(exp):
    """Runs ampcal.sh script on the $OUT/EXPNAME directory
    """
    move2dir(f"/jop83_0/pipe/out/{exp.expname.lower()}/")
    actions.shell_command("ampcal.sh")




