"""All steps to be performed divided in different terminal commands.
"""
import os
import subprocess





def check_systems_up():
    """Check that all required computers are reachable.
    """
    pass


# Interactive dialog function.


# Wrapper to set the output logging file and the messages when starting and finishing.
# Also to catch all exceptions and repeate the steps if required/continue/stop.

def scp(originpath, destpath):
    """Does a scp from originpath to destpath. If the process returns an error, then it raises ValueError.
    """
    process = subprocess.call(["scp", originpath, destpath], shell=False, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    if process != 0:
        raise ValueError("Error code {} when reading running make_lis in ccs.".format(process))


def ssh(computer, commands):
    """Sends a ssh command to the indicated computer. Returns the output or raises ValueError in case
    of errors. The output is expected to be in UTF-8 format.
    """
    process = subprocess.Popen(["ssh", computer, commands], shell=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    # logger.info(output)
    if process.returncode != 0:
        raise ValueError('Error code {} when reading running make_lis in ccs.'.format(process.returncode))

    return process.communicate()[0].decode('utf-8')


def shell_command(command, parameters=None):
    """Runs the provided command in the shell with some arguments if necessary.
    Returns the output of the command, assuming a UTF-8 encoding, or raises ValueError if fails.
    Parameters must be a single string if provided.
    """
    full_shell_command = [command] if parameters is None else [command, parameters]
    process = subprocess.Popen(full_shell_command, shell=False, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    if process.returncode != 0:
        raise ValueError('Error code {} when reading running make_lis in ccs.'.format(process.returncode))

    return process.communicate()[0].decode('utf-8')


def get_lis_vex(expname, computer, logger, eEVNname=None):
    """Produces the lis file(s) for this experiment in ccs and copy them to eee.
    It also retrieves the vex file and creates the required symb. links.

    --- Params
        expname : str
            Name of the EVN experiment.
        computer : str
            Name of the computer where to create the lis file(s) and get the vex file.
        logger : logging.Logger
            The logger to register the log messages.
        eEVNname : str [DEFAULT: None]
            In case of an e-EVN run, this is the name of the e-EVN run.
    """
    eEVNname = expname if eEVNname is None else expname
    cmd = "cd /ccs/expr/{expname};/ccs/bin/make_lis -e {expname} -p prod".format(expname=eEVNname)
    output = ssh(computer, cmd)

    for ext in ('lis', 'vix'):
        # cmd = ["{}:/ccs/expr/{}/{}*.{}".format(computer, eEVNname, eEVNname.lower(), ext), '.']
        scp(f"{computer}:/ccs/expr/{eEVNname}/{eEVNname.lower()}*.{ext}", '.')

    # Finally, copy the piletter and expsum files
    for ext in ('piletter', 'expsum'):
        scp(f"jops@jop83:piletters/{eEVNname.lower()}.{ext}", '.')

    # In the case of e-EVN runs, a renaming of the lis files may be required:
    if eEVNname != expname:
        for a_lis in glob.glob("*.lis"):
            os.rename(a_lis, a_lis.replace(eEVNname.lower(), expname.lower()))
            output = shell_command("checklis.py", a_lis)
            print(output)

    os.symlink("{}.vix".format(eEVNname.lower()), "{}.vix".format(expname))
    print("\n\nYou SHOULD check now the lis files and modify them if needed.")


def get_data(expname, logger, eEVNname=None):
    """Retrieves the data using getdata.pl and expname.lis file.
    """
    eEVNname = expname if eEVNname is None else expname
    for a_lisfile in glob.glob("{}*lis".format(expname)):
        output = shell_command("getdata.pl", f"-proj {eEVNname} -lis {a_lisfile}")


def pipe_create_dirs(expname, supsci):
    """Create all necessary directories in the Pipeline computer
    """
    for a_midpath in ('in', 'out', 'in/{}'.format(supsci)):
        if not os.path.isdir('/jop83_0/pipe/{}/{}'.format(a_midpath, expname.lower())):
            os.mkdir('/jop83_0/pipe/{}/{}'.format(a_midpath, expname.lower()))


