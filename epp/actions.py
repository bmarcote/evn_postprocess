"""All steps to be performed divided in different terminal commands.
"""
import os
import sys
import glob
import functools
import subprocess
import logging


# All command functions return the terminal command that was executed and the output.

header_comment_log = lambda command : "\n{0}\n{0}\n>>>>> {1}\n".format('#'*82, command)
commands_output_to_show = ["checklis.py", "flag_weights.py", "standardplots"]

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

        # print(f"The output is: {output_func}")
        if isinstance(output_func, tuple):
            if len(output_func) == 1:
                logger2.info(output_func)

            elif len(output_func) == 2:
                if not isinstance(output_func[0], list):
                    output_func = [[output_func[0],], [output_func[1],]]

                for a_cmd, an_output in zip(*output_func):
                    logger1.info(f"\n{a_cmd}")
                    # If this is one of the wild commands where the output should be shown, show it!
                    for a_wild_command in commands_output_to_show:
                        if a_wild_command in a_cmd:
                            if a_wild_command is "standardplots":
                                a_mod_output = extract_tail_standardplots_output(an_output)
                                print(f"{a_cmd}:\n{a_mod_output}")
                                logger1.info(a_mod_output)
                            else:
                                print(f"{a_cmd}:\n{an_output}")
                                logger1.info(an_output)

                    logger2.info(header_comment_log(a_cmd))
                    logger2.info(an_output)
        else:
            logger2.info(output_func)

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
    answer = input(f"\n{text} (q to exit): ")
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


def yes_or_no_question(text):
    """Asks to the user for a yes or no question. Accepted values are y/yes/n/no. Always q to quit.
    Returns a bool.
    """
    value = ask_user(f"\n{text} (y/yes/n/no)", accepted_values=['y','yes','n','no'])
    if value is 'y' or value is 'yes':
        return True
    elif value is 'n' or 'no':
        return False

    raise ValueError


def can_continue(text):
    """Asks to the user if the program can continue running or if it should stop at this step.
    """
    answer = input(f"\n{text} (y/yes/enter, or 'q' to exit): ")
    while True:
        try:
            if answer is 'q':
                sys.exit(0)

            if answer.lower() not in ('y', 'yes', ''):
                return False

            return True

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
        raise ValueError(f"Error code {process} when running scp {originpath} {destpath} in ccs.")

    return f"scp {originpath} {destpath}", process


def ssh(computer, commands):
    """Sends a ssh command to the indicated computer. Returns the output or raises ValueError in case
    of errors. The output is expected to be in UTF-8 format.
    """
    process = subprocess.Popen(["ssh", computer, commands], shell=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    # logger.info(output)
    if process.returncode != 0 and process.returncode is not None:
        raise ValueError(f"Error code {process.returncode} when running ssh {computer}:{commands} in ccs.")

    return f"ssh {computer}:{commands}", process.communicate()[0].decode('utf-8')


@decorator_log
def shell_command(command, parameters=None):
    """Runs the provided command in the shell with some arguments if necessary.
    Returns the output of the command, assuming a UTF-8 encoding, or raises ValueError if fails.
    Parameters must be a single string if provided.
    """
    if isinstance(parameters, list):
        full_shell_command = [command] + parameters
    else:
        full_shell_command = [command] if parameters is None else [command, parameters]

    print(f"{' '.join(full_shell_command)}...")
    process = subprocess.Popen(full_shell_command, shell=False, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    if process.returncode != 0 and process.returncode is not None:
        raise ValueError(f"Error code {process.returncode} when running {command} {parameters} in ccs.")

    return ' '.join(full_shell_command), process.communicate()[0].decode('utf-8')


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
    eEVNname = expname if eEVNname is None else eEVNname
    overwrite = True
    cmds, outputs = [], []
    if len(glob.glob("*lis")) > 0:
        overwrite = yes_or_no_question("""lis files already found in the directory.
Do you want to overwrite them (it also applies to vex, piletter, expsum files)?""")

    if overwrite:
        print("Creating lis file...")
        # Commenting out -p prod because of spectral line exper
        cmd = "cd /ccs/expr/{expname};/ccs/bin/make_lis -e {expname}".format(expname=eEVNname)
        output = ssh(computer_ccs, cmd)
        cmds.append(f"ssh {computer_ccs}:{cmd}")
        # If there is a "prod_cont" and "prod_line" in the lis file, remake them to make separate files.
        outputs.append(output)

        print("Getting lis and vix files from ccs...")
        for ext in ('lis', 'vix'):
            # cmd = ["{}:/ccs/expr/{}/{}*.{}".format(computer, eEVNname, eEVNname.lower(), ext), '.']
            cmd, output = scp(f"{computer_ccs}:/ccs/expr/{eEVNname}/{eEVNname.lower()}*.{ext}", '.')
            cmds.append(cmd)
            outputs.append(output)

        # Checks if the lis file(s) contain prod_cont and prod_line profiles. And those cases split
        # the lis files for the two profiles.
        for a_lis in glob.glob("*.lis"):
            split_lis_cont_line(a_lis)

        print(f"Getting the PI letter and .expsum files from {computer_piletter.split('@')[1]}...")
        # Finally, copy the piletter and expsum files
        for ext in ('piletter', 'expsum'):
            cmd, output = scp(f"{computer_piletter}:piletters/{eEVNname.lower()}.{ext}", '.')
            cmds.append(cmd)
            outputs.append(output)

        if not os.path.isfile(f"{expname}.vix"):
            os.symlink(f"{eEVNname.lower()}.vix", f"{expname}.vix")
            cmds.append(f"ln -s {eEVNname.lower()}.vix {expname}.vix")
            outputs.append('')

        # In the case of e-EVN runs, a renaming of the lis files may be required:
        if eEVNname != expname:
            for a_lis in glob.glob("*.lis"):
                os.rename(a_lis, a_lis.replace(eEVNname.lower(), expname.lower()))
                cmds.append(f"mv {a_lis} {a_lis.replace(eEVNname.lower(), expname.lower())}")
                outputs.append('')

        if can_continue('Check the lis file(s) and modify them if needed. Are they ready to be check now?'):
            while True:
                for a_lis in glob.glob("*.lis"):
                    cmd, output = shell_command("checklis.py", a_lis)
                    cmds.append(cmd)
                    outputs.append(output)
                    # print(f"\n{cmds[-1]}:\n{outputs[-1]}")

                if yes_or_no_question('Are lis file(s) OK to continue and get the data?\nNo to check them again'):
                    break




    return cmds, outputs


def split_lis_cont_line(fulllisfile):
    """Given a lis file, it checks if there are jobs set as prod_cont and prod_line.
    If not, it does nothing. Otherwise, it splits the lis file into two lis files,
    one for the continuum pass and another one for the line pass.
    """
    if (subprocess.call(["grep", "prod_line", fulllisfile], stdout=subprocess.DEVNULL) == 0) and \
       (subprocess.call(["grep", "prod_cont", fulllisfile], stdout=subprocess.DEVNULL) == 0):
        print('This is a spectral line experiment with line and continuum passes.')

        lis_cont = fulllisfile.replace('.lis', '_cont.lis')
        lis_line = fulllisfile.replace('.lis', '_line.lis')
        with open(lis_cont, 'w') as f_cont, open(lis_line, 'w') as f_line:
            with open(fulllisfile) as f_full:
                for a_fileline in f_full.readlines():
                    if a_fileline[0] not in ('+', '-'):
                        f_cont.write(a_fileline.replace('.ms', '_cont.ms'))
                        f_line.write(a_fileline.replace('.ms', '_line.ms'))

            f_cont.write(subprocess.check_output(["grep", "prod_cont", fulllisfile]).decode('utf-8'))
            f_line.write(subprocess.check_output(["grep", "prod_line", fulllisfile]).decode('utf-8'))

        os.remove(fulllisfile)



@decorator_log
def get_data(expname, eEVNname=None):
    """Retrieves the data using getdata.pl and expname.lis file.
    """
    eEVNname = expname if eEVNname is None else eEVNname
    cmds, outputs = [], []
    for a_lisfile in glob.glob(f"{expname.lower()}*lis"):
        cmd, output = shell_command("getdata.pl", ["-proj", eEVNname, "-lis", a_lisfile])
        cmds.append(cmd)
        outputs.append(output)

    return cmds, outputs


@decorator_log
def j2ms2(expname):
    """Runs j2ms2 using all lis files found in the directory with the name expname*lis (lower cases).
    """
    cmds, outputs = [], []
    for a_lisfile in glob.glob(f"{expname.lower()}*lis"):
        with open(a_lisfile) as f:
            outms = [a for a in f.readline().replace('\n','').split(' ') if (('.ms' in a) and ('.UVF' not in a))][0]
        if os.path.isdir(outms):
            if yes_or_no_question(f"{outms} exists. Delete and run j2ms2 again?"):
                cmd, output = shell_command("j2ms2", ["-v", a_lisfile])
                cmds.append(cmd)
                outputs.append(output)
        else:
            cmd, output = shell_command("j2ms2", ["-v", a_lisfile])
            cmds.append(cmd)
            outputs.append(output)

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
        cmd, output = shell_command("scale1bit.py", [a_msfile, *antennas.split(',')])
        cmds.append(cmd)
        outputs.append(output)

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
    cmd, output = shell_command("standardplots",
                                ["-weight", glob.glob(f"{expname.lower()}*.ms*")[0], refant, calsources])
    # Process the final lines of the output. It should interpretate it and return time range, sources,
    # antennas, and freqs.
    # return get_setup_from_standardplots_output(output)
    # NOTE: NO NEEDED ANYMORE BECAUSE IT IS WITHIN METADATA.PY BUT LAST BITS SHOULD STILL BE PROCESSED FOR THE
    # PROCESSING.LOG FILE.
    return cmd, output


def extract_tail_standardplots_output(stdplt_output):
    """Given a full log output from standardplots, it returns only the last bits that contain
    the information provided by the "r" command.
    """
    last_lines = []
    for a_line in stdplt_output.split('\n')[::-1]:
        # All "r" output lines always start with those messages
        # (listTimeRage: , listSources: , listAntennas: , listFreqs: ):
        if ('list' in a_line) or (a_line.strip() is ''):
            last_lines.append(a_line)
        else:
            # We are already done
            return last_lines[::-1]

    # Just in case something went unexpected...
    print('\n'.join(last_lines[::-1]))
    return '\n'.join(last_lines[::-1])


@decorator_log
def archive(flag, experiment, rest_parameters):
    """Runs the archive command with the flag and rest_parameters string for the given experiment object
    (metadata class).
    Flag can be -auth, -stnd, -fits,...
    """
    cmd, output = shell_command("archive.pl",
                    [flag, "-e", f"{experiment.expname.lower()}_{experiment.obsdate}", rest_parameters])
    return cmd, output



def pipe_create_dirs(expname, supsci):
    """Create all necessary directories in the Pipeline computer
    """
    for a_midpath in ('in', 'out', 'in/{}'.format(supsci)):
        if not os.path.isdir('/jop83_0/pipe/{}/{}'.format(a_midpath, expname.lower())):
            os.mkdir('/jop83_0/pipe/{}/{}'.format(a_midpath, expname.lower()))






