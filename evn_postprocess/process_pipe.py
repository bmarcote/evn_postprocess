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
import paramiko
from datetime import datetime
from . import metadata
from . import environment as env


def create_folders(expname: str, supsci: str):
    """Creates the folder required for the post-processing of the experiment
    - @eee: /data0/{supportsci}/{exp.upper()}

    Inputs
        - expname: str
            Experiment name (case insensitive).
        - supsci: str
            Surname of the assigned support scientist.
    """
    tmpdir = f"/jop83_0/pipe/in/{supsci.lower()}/{expname.lower()}"
    indir = f"/jop83_0/pipe/in/{expname.lower()}"
    outdir = f"/jop83_0/pipe/out/{expname.lower()}"
    for a_dir in (tmpdir, indir, outdir):
        output = env.ssh('pipe@jop83', f"mkdir -p {a_dir}")






class Pipe(paramiko.SSHClient):
    def __init__(self, user='pipe', host='jop83'):
        self._user = pipe
        self._host = host
        super().__init__()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self._host, username=self._user)

    def execute_commands(self, commands):
        """Execute multiple commands in succession.

        - commands : list of UNIX coomands as strings.
        """
        full_response = []
        for cmd in commands:
            stdin, stdout, stderr = self.client.exec_command(cmd, get_pty=True)
            stdout.channel.recv_exit_status()
            response = stdout.readlines()
            for line in response:
                print(line, end='')

            full_response.append(response)

        return full_response


def disconnect(exp):
    """Closes the connection to Pipe. Throws an exception if it was already closed.
    """
    exp.connections['pipe'].close()
    del exp.connections['pipe']



def get_files_from_vlbeer(exp):
    """Retrieves the antabfs, log, and flag files that should be in vlbeer for the given experiment.
    """
    if 'pipe' not in exp.connections:
        exp.connections['pipe'] = Pipe(user='pipe')

    out = exp.connections['pipe'].execute_commands([f"cd $IN/{exp.supsci}/{exp.expname.lower()} && " \
        "scp evn@vlbeer.ira.inaf.it:vlbi_arch/{exp.obsdatetime.strftime('%b%y').lower()}/{exp.expname.lower()}\*.{ext} ." \
        for ext in ('log', 'antabfs', 'flag')])

    # TODO: check if there are ANTAB files in the previous/following month...
    # WHAT THE HELL I AM DOING BELOW? WHY?
    # Check if there is the same number of files than antennas
    n_log = set([i.strip().replace(exp.expname.lower(), '').replace('.log', '').capitalize() for i in out[0]])
    n_ant = set([i.strip().replace(exp.expname.lower(), '').replace('.antabfs', '').capitalize() for i in out[1]])
    n_total = set(exp.antennas).difference(['Cm', 'Da', 'De', 'Pi', 'Kn'])
    if (n_log < n_total) or (n_ant < n_total):
        print('There are missing files from vlbeer:')
        print(f"Missing log files from: {', '.join(n_total.difference(n_log))}")
        print(f"Missing ANTAB files from: {', '.join(n_total.difference(n_ant))}")



def create_uvflg(exp):
    """Produces the combined uvflg file containing the full flagging from all telescopes.
    """
    if 'pipe' not in exp.connections:
        exp.connections['pipe'] = Pipe(user='pipe')

    out = exp.connections['pipe'].execute_commands([f"cd $IN/{exp.supsci}/{exp.expname.lower()} && uvflgall.csh"])
    # There may be empty uvflg files (not completely empty but without flagging info.
    out = exp.connections['pipe'].execute_commands([f"cd $IN/{exp.supsci}/{exp.expname.lower()} && ls -sa *.uvflgfs"])
    # TODO: from here get who is empty, remove it, and then get that flaggin from the .flag file.
    # The previous line returns the size (in kb) and filename. Empty ones should be ~0 (1 max if rounding)
    exp.connections['pipe'].execute_commands(["cat $IN/{0}/{1}/{1}.uvflgfs > $IN/{1}/{1}.uvflg".format(exp.supsci,
                                                                                            exp.expname.lower())])


def run_antab_editor(exp):
    if 'pipe' not in exp.connections:
        exp.connections['pipe'] = Pipe(user='pipe')

    input('Now run manually antab_editor.py and press continue after you have produced the required ANTAB file.')
    exp.connections['pipe'].execute_commands(["cat $IN/{0}/{1}/{1}.antab > $IN/{1}/{1}.antab".format(exp.supsci,
                                                                                            exp.expname.lower())])




def create_input_file(exp):
    """Copies the template of an input file for the EVN Pipeline and modifies the standard parameters.
    """
    if 'pipe' not in exp.connections:
        exp.connections['pipe'] = Pipe(user='pipe')

    # Parameters to modify inside the input file
    if exp.supsci == 'marcote':
        userno = exp.connections['pipe'].execute_commands(['give_me_next_userno.sh'])
    else:
        userno = 'XXXXX'

    if len(exp.ref_sources) == 0:
        bpass = ', '.join([s.name for s in exp.sources if s.type is metadata.SourceType.fringefinder])
    else:
        bpass = ', '.join(exp.ref_sources)

    to_change = f"'experiment = n05c3' 'experiment = {exp.expname.lower()}' " \
                f"'userno = 3602' 'userno = {userno}' " \
                f"'refant = Ef, Mc, Nt' 'refant = {', '.join(exp.ref_antennas)}' " \
                f"'plotref = Ef' 'plotref = {', '.join(exp.ref_antennas)}' " \
                f"'bpass = 3C345, 3C454.3' 'bpass = {', '.join([])}' " \
                f"'phaseref = ' '# SOURCES THAT MAY BE INCLUDED: {', '.join([s.name for s in exp.sources])}\nphaseref = ' "
    # TODO: Multi-phase centers? then modify the input file too
    out = exp.connections['pipe'].execute_commands([
                 "cp $IN/template.inp $IN/{1}/{1}.inp.txt".format(exp.expname.lower()),
                 f"replace {to_change} -- $IN/{exp.expname.lower()}/{exp.expname.lower()}.inp.txt"])


## Something else?

# Authentification for the credentials

## Run pipeline


def comment_tasav_files(exp):
    """Creates the comment and tasav files after the EVN Pipeline has run.
    """
    if 'pipe' not in exp.connections:
        exp.connections['pipe'] = Pipe(user='pipe')

    out = exp.connections['pipe'].execute_commands(["cd $OUT/{0} && comment_tasav_file.py {0}".format(
                                                                                exp.expname.lower())])


def pipeline_feedback(exp):
    """Runs the feedback.pl script after the EVN Pipeline has run.
    """
    if 'pipe' not in exp.connections:
        exp.connections['pipe'] = Pipe(user='pipe')

    out = exp.connections['pipe'].execute_commands(
        ["cd $OUT/{0} && feedback.pl -exp '{0}' -jsss '{1}' -source '{2}'".format(exp.expname.lower(),
                                                exp.supsci, ' '.join([s.name for s in exp.sources]))])


def archive(exp):
    """Archives the EVN Pipeline results.
    """
    pipe = Pipe(user='jops')
    out = pipe.execute_commands([f"cd ${0}/{1} && archive -pipe -e {1}_{2}".format(f,
                            exp.expname.lower(), exp.obsdate) for f in ('IN', 'OUT')])
    pipe.close()



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
#         if not actions.remote_file_exists(_login, adir):
#             actions.ssh(_login, f"mkdir {adir}")
#             os.makedirs(adir)
#             print(f"Directory {adir} has been created.")

##################################################################################################
#     if not os.path.isfile(f"{exp.expname.lower()}.piletter"):
#         cmd, output = actions.scp(f"jops@jop83:piletters/{exp.expname.lower()}.piletter", '.')
#     return actions.remote_file_exists('jops@ccs',
#                                       f"/ccs/expr/{eEVNname}/{eEVNname.lower()}*.lis")
#     cmd = f"cd /ccs/expr/{eEVNname};/ccs/bin/make_lis -e {eEVNname}"
#     output = actions.ssh('jops@ccs', cmd)
# ##################################################################################################

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




