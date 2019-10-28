"""All steps to be performed divided in different terminal commands.
"""
import os
import sys
import functools
import subprocess
import logging


# All command functions return the terminal command that was executed and the output.

header_comment_log = lambda command : "\n\n{0}\n{0}\n\n>>>>> {1}\n\n".format('#'*82, command)

def decorator_log(func):
    """Decorates each function to log the input and output to the common log file, and individually.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # print(f"Executing {func.__name__} {*args} {**kwargs}")
        output_func = func(*args, **kwargs)
        # output_func can have one or two elements... If only one then it is only the output.
        # Otherwise it has the command that has been run and the output.
        logger1 = logging.getLogger('Executed commands')
        logger2 = logging.getLogger('Commands full log')
        logger1.setLevel(logging.INFO)
        logger2.setLevel(logging.INFO)
        logger1_file = logging.FileHandler('./processing.log')
        logger2_file = logging.FileHandler('./full_log_output.log')
        logger1.addHandler(logger1_file)
        logger2.addHandler(logger2_file)

        if len(output_func) == 1:
            logger2.info(output_func)

        elif len(output_func) == 2:
            for a_cmd, an_output in zip(*output_func):
                logger1.info(f"\n\n{a_cmd}\n")
                logger2.info(header_comment_log(a_cmd))
                logger2.info(an_output)

        return output_func

    return wrapper



def check_systems_up():
    """Check that all required computers are reachable.
    """
    pass


# Interactive dialog function.
def ask_user(text, valtype=str, accepted_values=None):
    """Requests an input from the user via terminal. The input can also be converted to an
    appropriate format if needed (by default str is assumed). If the input is not converted,
    it asks again to the user.

    accepted_values can be a list of all possible values that are accepted.
    """
    answer = input(f"{text} (q to exit): ")
    while True:
        try:
            if answer is 'q':
                sys.exit(0)

            if accepted_values is not None:
                # the answer can be a list (comma-separated) of possible values, so...
                if ',' in answer:
                    answer_values = []
                    for an_answer in answer.split(','):
                        if valtype(an_answer.upper().strip()) not in accepted_values:
                            raise ValueError
                        else:
                            answer_values += valtype(an_answer.strip())

                    return answer_values

            return valtype(answer)
        except ValueError:
            # raise ValueError(f"Invalid input ({text}). Cannot be converted to {valtype}.")
            print(f"Invalid input ({text}). Cannot be converted to {valtype}.\nPlease try again: ")



def can_continue(text):
    """Asks to the user if the program can continue running or if it should stop at this step.
    """
    answer = input(f"{text} (y/yes, or 'q' to exit): ")
    while True:
        try:
            if answer is 'q':
                sys.exit(0)

            if answer.lower() not in ('y', 'yes'):
                raise ValueError

        except ValueError:
            # raise ValueError(f"Invalid input ({text}). Cannot be converted to {valtype}.")
            print(f"Invalid input ({text}). Cannot be converted to {valtype}.\nPlease try again: ")


def station_1bit_in_vix(vexfile):
    """Checks if there is any station in the vex file that recorded at 1 bit.
    Note that this/these station(s) may or may not have recorded at 1 bit in this experiment,
    but only at other moment of the run.
    """
    output = subprocess.call(["grep", "1bit", vexfile], shell=False, stdout=subprocess.PIPE)
    if output == 0:
        # There is at least one station recording at 1 bit.
        return True
    elif output == 1:
        return False
    else:
        # File not found
        raise FileNotFoundError


def scp(originpath, destpath):
    """Does a scp from originpath to destpath. If the process returns an error, then it raises ValueError.
    """
    process = subprocess.call(["scp", originpath, destpath], shell=False, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    if process != 0:
        raise ValueError("Error code {} when reading running make_lis in ccs.".format(process))

    return process


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


@decorator_log
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


@decorator_log
def get_lis_vex(expname, computer_ccs, computer_piletter, eEVNname=None):
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
    cmds, outputs = [], []
    cmd = "cd /ccs/expr/{expname};/ccs/bin/make_lis -e {expname} -p prod".format(expname=eEVNname)
    output = ssh(computer_ccs, cmd)
    cmds.append(computer_ccs + ':' + cmd)
    outputs.append(output)

    for ext in ('lis', 'vix'):
        # cmd = ["{}:/ccs/expr/{}/{}*.{}".format(computer, eEVNname, eEVNname.lower(), ext), '.']
        cmds.append(f"{computer_ccs}:/ccs/expr/{eEVNname}/{eEVNname.lower()}*.{ext} .")
        outputs.append(scp(f"{computer_ccs}:/ccs/expr/{eEVNname}/{eEVNname.lower()}*.{ext}", '.'))


    # Finally, copy the piletter and expsum files
    for ext in ('piletter', 'expsum'):
        scp(f"{computer_piletter}:piletters/{eEVNname.lower()}.{ext}", '.')
        outputs.append(cmds.append(f"{computer_piletter}:piletters/{eEVNname.lower()}.{ext} ."))

    # In the case of e-EVN runs, a renaming of the lis files may be required:
    if eEVNname != expname:
        for a_lis in glob.glob("*.lis"):
            os.rename(a_lis, a_lis.replace(eEVNname.lower(), expname.lower()))
            cmds.append(f"mv {a_lis} {a_lis.replace(eEVNname.lower(), expname.lower())}")
            outputs.append('')
            cmds.append(f"checklis.py {a_lis}")
            outputs.append(shell_command("checklis.py", a_lis))
            print(output)

    os.symlink(f"{eEVNname.lower()}.vix", f"{expname}.vix")
    cmds.append(f"ln -s {eEVNname.lower()}.vix {expname}.vix")
    outputs.append('')
    print("\n\nYou SHOULD check now the lis files and modify them if needed.")
    return cmds, outputs


@decorator_log
def get_data(expname, eEVNname=None):
    """Retrieves the data using getdata.pl and expname.lis file.
    """
    eEVNname = expname if eEVNname is None else expname
    cmds, outputs = [], []
    for a_lisfile in glob.glob(f"{expname.lower()}*lis"):
        cmds.append(f"getdata.pl -proj {eEVNname} -lis {a_lisfile}")
        outputs.append(shell_command("getdata.pl", f"-proj {eEVNname} -lis {a_lisfile}"))

    return cmds, outputs


@decorator_log
def j2ms2(expname):
    """Runs j2ms2 using all lis files found in the directory with the name expname*lis (lower cases).
    """
    cmds, outputs = [], []
    for a_lisfile in glob.glob(f"{expname.lower()}*lis"):
        cmds.append(f"j2ms2 -v {a_lisfile}")
        outputs.append(shell_command("j2ms2", f"-v {a_lisfile}"))

    return cmds, outputs


@decorator_log
def scale1bit(expname, antennas):
    """Scale 1-bit data to correct quentization losses for the given telescopes in all
    MS files associated with the given experiment name.

    Inputs:
        - expaname : str
            The experiment name (preffix used for the associated MS files, typically expname lower cases)
        - antennas : str
            Comma-separated list of antennas that recorded at 1 bit.
    """
    cmds, outputs = [], []
    for a_msfile in glob.glob(f"{expname.lower()}*.ms*"):
        outputs.append(shell_command("scale1bit.py", f"{a_msfile} {antennas.replace(',', ' ')}"))
        cmds.append(f"scale1bit.py {a_msfile} {antennas.replace(',', ' ')}")

    return cmds, outputs


@decorator_log
def standardplots(expname, refant, calsources):
    """Runs the standardplots on the specified experiment using refant as reference antenna and
    calsources as the sources to be picked for the auto- and cross-correlations.

    It picks the first MS file found in glob.glob. In case of regular experiments there should be
    only one and in case of multi-phase centers this behavior may be replaced.

    Inputs:
        - expname : str
            The name of the experiment as preffix of the MS files to read (typically lower cases).
        - refant : str
            The antenna name to use as reference in the plots (showing only baselines to this station).
        - calsources : str
            List (comma-separated, no spaces) of source names to be considered for the plots.
            Usually only the strong sources like fringe-finders.
    """
    output = shell_command("standardplots", f"-weight {glob.glob('expname*.ms*')[0]} {refant} {calsources}")
    # Process the final lines of the output. It should interpretate it and return time range, sources,
    # antennas, and freqs.
    # return get_setup_from_standardplots_output(output)
    # NOTE: NO NEEDED ANYMORE BECAUSE IT IS WITHIN METADATA.PY BUT LAST BITS SHOULD STILL BE PROCESSED FOR THE
    # PROCESSING.LOG FILE.
    return [f"standardplots -weight {glob.glob('expname*.ms*')[0]} {refant} {calsources}"], [output]


@decorator_log
def archive(flag, experiment, rest_parameters):
    """Runs the archive command with the flag and rest_parameters string for the given experiment object
    (metadata class).
    Flag can be -auth, -stnd, -fits,...
    """
    output = shell_command("archive",
                           [flag, "-e", f"{experiment.expname.lower()}_{experiment.obsdate}", rest_parameters])
    return [f"archive {flag} -e {experiment.expname.lower()}_{experiment.obsdate} {rest_parameters}"], [output]



def pipe_create_dirs(expname, supsci):
    """Create all necessary directories in the Pipeline computer
    """
    for a_midpath in ('in', 'out', 'in/{}'.format(supsci)):
        if not os.path.isdir('/jop83_0/pipe/{}/{}'.format(a_midpath, expname.lower())):
            os.mkdir('/jop83_0/pipe/{}/{}'.format(a_midpath, expname.lower()))





