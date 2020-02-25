"""All steps to be performed divided in different terminal commands.
"""
import io
import os
import sys
import glob
import functools
import subprocess
import logging
from src import metadata

# All command functions return the terminal command that was executed and the output.

header_comment_log = lambda command : "\n{0}\n{0}\n>>>>> {1}\n".format('#'*82, command)
commands_output_to_show = ["checklis.py", "flag_weights.py", "standardplots", "archive"]
# TODO: Add pipeline output and cat the last rows only.


def write_to_log(text, also_print=True):
    """Writes the given text into the two log files created in the program.
    also_print defines if the text should also be written in terminal.
    """
    with open('./processing.log', 'a') as file1, open('./full_log_output.log', 'a') as file2:
        file1.write(text+'\n')
        file2.write(text+'\n')
        if also_print:
            print(text)



def decorator_log(func):
    """Decorates each function to log the input and output to the common log file, and individually.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # output_func can have one or two elements... If only one then it is only the output.
        # Otherwise it has the command that has been run and the output.
        output_func = func(*args, **kwargs)
        logger1 = logging.getLogger('Executed commands')
        logger2 = logging.getLogger('Commands full log')
        logger1.setLevel(logging.WARNING)
        logger2.setLevel(logging.WARNING)
        logger1_file = logging.FileHandler('./processing.log')
        logger2_file = logging.FileHandler('./full_log_output.log')
        logger1.addHandler(logger1_file)
        logger2.addHandler(logger2_file)

        # TODO: For some reason, logger writes multiple times each line.
        # In this way I get the output I want.
        try:
            file1 = open('./processing.log', 'a')
            file2 = open('./full_log_output.log', 'a')
            if isinstance(output_func, tuple):
                if len(output_func) == 1:
                    logger2.info(output_func)
                    file2.write(output_func)

                elif len(output_func) == 2:
                    if not isinstance(output_func[0], list):
                        output_func = [[output_func[0],], [output_func[1],]]

                    for a_cmd, an_output in zip(*output_func):
                        logger1.info(f"\n{a_cmd}")
                        file1.write(f"\n{a_cmd}")
                        # If this is one of the wild commands where the output should be shown, show it!
                        for a_wild_command in commands_output_to_show:
                            if a_wild_command in a_cmd:
                                if a_wild_command is "standardplots":
                                    a_mod_output = extract_tail_standardplots_output(an_output)
                                    # print(f"{a_cmd}:\n{a_mod_output}")
                                    print(f"{a_mod_output}")
                                    logger1.info(a_mod_output)
                                    file1.write(a_mod_output)
                                else:
                                    # print(f"{a_cmd}:\n{an_output}")
                                    print(f"{an_output}")
                                    file1.write(str(an_output))

                        logger2.info(header_comment_log(a_cmd))
                        logger2.info(an_output)
                        file2.write(header_comment_log(a_cmd))
                        if isinstance(an_output, list):
                            file2.write(' '.join(an_output)+ '\n')
                        else:
                            file2.write(str(an_output))
            else:
                logger2.info(output_func)
                file2.write(output_func)
        finally:
            file1.close()
            file2.close()
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
            print(f"Invalid input {text}. Cannot be converted to {valtype}.\nPlease try again: ")


def yes_or_no_question(text):
    """Asks to the user for a yes or no question.
    Accepted values are y/yes/n/no. Always q to quit.
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
            print(f"Invalid input {text}. Cannot be converted to {valtype}.\nPlease try again: ")


def parse_steps(step_list, all_steps, wild_steps=None):
    """The post-processing program can run all steps of the post-process or just a subset of them.
    Given an input comma-separated list of steps to be run, it returns the list of all steps to
    be conducted.

    Inputs:
        - step_list : str
            String with a comma-separated list of steps to be executed. If there is only one step,
            Then all steps from this one (included) to the end will be executed.
        - all_steps : dict
            Dict with all available steps. Therefore, the steps specified in step_list must be
            a subset of this list. The keys are the steps and the values must be the functions
            to execute.
        - wild_steps : list [OPTIONAL; default=None]
            List of steps that will be still included in the returned list even if they are not
            listed in step_list.
    """
    selected_steps = [s.strip() for s in step_list.split(',')]
    # Safety check: all provided steps must be included in all_steps.
    for a_step in selected_steps:
        try:
            assert a_step in all_steps
        except AssertionError as e:
            print(f"{a_step} is not a valid step. Not in {all_steps}")
            raise e

    if len(selected_steps) == 1:
        steps_to_execute = {}
        is_after = False
        for a_step in all_steps:
            if a_step == selected_steps[0]:
                is_after = True

            if is_after or (a_step in wild_steps):
                steps_to_execute[a_step] = all_steps[a_step]

        return steps_to_execute

    elif len(selected_steps) == 0:
        raise ValueError('No steps to run have been specified.')
    else:
        # I do the reverse way because then I can easily add the mandatory steps that
        # pile up information to the current experiment
        steps_to_execute = {}
        for a_step in all_steps:
            if (a_step in selected_steps) or (a_step in wild_steps):
                steps_to_execute[a_step] = all_steps[a_step]

        return steps_to_execute




@decorator_log
def scp(originpath, destpath):
    """Does a scp from originpath to destpath. If the process returns an error, then it raises ValueError.
    """
    process = subprocess.call(["scp", originpath, destpath], shell=False, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    if process != 0:
        raise ValueError(f"Error code {process} when running scp {originpath} {destpath} in ccs.")

    return f"scp {originpath} {destpath}", process


@decorator_log
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
def shell_command(command, parameters=None, shell=False):
    """Runs the provided command in the shell with some arguments if necessary.
    Returns the output of the command, assuming a UTF-8 encoding, or raises ValueError if fails.
    Parameters must be either a single string or a list, if provided.
    """
    if isinstance(parameters, list):
        full_shell_command = [command] + parameters
    else:
        full_shell_command = [command] if parameters is None else [command, parameters]

    print(f"\n\033[1m> {' '.join(full_shell_command)}\033[0m")

    if shell:
        process = subprocess.Popen(' '.join(full_shell_command), shell=shell,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        # TODO: I am changing everything to shell=True...
        process = subprocess.Popen(' '.join(full_shell_command), shell=True, stdout=subprocess.PIPE,
        # process = subprocess.Popen(full_shell_command, shell=shell, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, bufsize=1)

    # for line in process.stdout:
    #     print(line.decode('utf-8').replace('\n', ''))

    while process.poll() is None:
        out = process.stdout.read(1).decode('utf-8')
        sys.stdout.write(out)
        sys.stdout.flush()

# while True:
    #     out = process.stderr.read(1).decode('utf-8')
    #     if (out == '') and (process.poll() != None):
    #         break
    #     if out != '':
    #         sys.stdout.write(out)
    #         sys.stdout.flush()

    if (process.returncode != 0) and (process.returncode is not None):
        raise ValueError(f"Error code {process.returncode} when running {command} {parameters} in ccs.")

    output = process.communicate()
    if isinstance(output, tuple):
        output = [p.decode('utf-8') for p in output]
        for an_output in output:
            if an_output != '':
                return ' '.join(full_shell_command), an_output

        return ' '.join(full_shell_command), output

    return ' '.join(full_shell_command), process.communicate()[0].decode('utf-8')



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
        if yes_or_no_question("Create again the .lis file in ccs?"):
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
            cmd, output = scp(f"{computer_piletter}:piletters/{expname.lower()}.{ext}", '.')
            cmds.append(cmd)
            outputs.append(output)

        if not os.path.isfile(f"{expname}.vix"):
            os.symlink(f"{eEVNname.lower()}.vix", f"{expname}.vix")
            cmds.append(f"ln -s {eEVNname.lower()}.vix {expname}.vix")
            outputs.append('')

        # In the case of e-EVN runs, a renaming of the lis files may be required:
        if eEVNname != expname:
            for a_lis in glob.glob("*.lis"):
                # Modify the references for eEVNname to expname inside the lis files
                update_lis_file(a_lis, eEVNname, expname)
                cmds.append(f"{a_lis} exp name updated from {eEVNname} to {expname}")
                outputs.append('')
                os.rename(a_lis, a_lis.replace(eEVNname.lower(), expname.lower()))
                cmds.append(f"mv {a_lis} {a_lis.replace(eEVNname.lower(), expname.lower())}")
                outputs.append('')

        # if can_continue('Check the lis file(s) and modify them if needed.\n'+\
        #                 'Are they ready to be checked now?'):
        while True:
            for a_lis in glob.glob("*.lis"):
                cmd, output = shell_command("checklis.py", a_lis)
                cmds.append(cmd)
                outputs.append(output)

            if yes_or_no_question('Are the .lis file(s) OK to continue and get the data?\n'+\
                                  '"no" to check them again'):
                break

    return cmds, outputs


def update_lis_file(lisfilename, oldexp, newexp):
    """Updates the lis file (the header lines) referring to an experiment named oldexp
    to newexp. Note that it does not replace all references to oldexp as some of them
    would point to correlator output files that would keep the name.
    """
    with open(lisfilename, 'r') as lisfile:
        lisfilelines = lisfile.readlines()
        for i,aline in enumerate(lisfilelines):
            if aline[0] not in ('+', '-'):
                # Replace the EXP (upper) entries
                lisfilelines[i] = aline.replace(oldexp, newexp)
                # Replace the exp (lower) entries
                lisfilelines[i] = lisfilelines[i].replace(oldexp.lower(), newexp.lower())
                # Replace the exp.vix to EXP.vix (as symb link was done)
                lisfilelines[i] = lisfilelines[i].replace(f"{newexp.lower()}.vix", f"{newexp.upper()}.vix")

    with open(lisfilename, 'w') as lisfile:
        lisfile.write(''.join(lisfilelines))


def get_pi_from_expsum(exp):
    """Obtains the PI name and the email from the .expsum file that is expected to be
    placed in the current directory. Adds this information to the object.
    """
    try:
        with open(f"{exp.expname.lower()}.expsum", 'r') as expsumfile:
            for a_line in expsumfile.readlines():
                if 'Principal Investigator:' in a_line:
                    # The line is expected to be '  Principal Investigator: SURNAME  (EMAIL)'
                    piname, email = a_line.split(':')[1].split('(')
                    exp.piname = piname.strip()
                    exp.email = email.replace(')','').strip()
    except FileNotFoundError as e:
        raise e(f"ERROR: {self.expname.lower()}.expsum not found.")


def get_passes_from_lisfiles(exp):
    """Gets all .lis files in the directory, which mean different correlator passes.
    Append this information to the current experiment (exp object), together with the MS file
    associated for each of them.
    """
    lisfiles = glob.glob(f"{exp.expname.lower()}*.lis")

    for i,a_lisfile in enumerate(lisfiles):
        with open(a_lisfile, 'r') as lisfile:
            for a_lisline in lisfile.readlines():
                if '.ms' in a_lisline:
                    # there is only one .ms input there
                    msname = [elem.strip() for elem in a_lisline.split() if '.ms' in elem][0]
                    exp.add_pass(metadata.CorrelatorPass(a_lisfile, msname))
    if len(exp.passes) > 1:
        print(f"More than one correlation pass to be process. Which passes should be pipelined at due time?")
        print('\n'.join([f"{i}: {a_pass.msfile.replace('.ms', '')}" for i,a_pass in enumerate(exp.passes)]))
        passes2pipeline = ask_user(f"Write the number of the passes to pipeline")
        for i in set(range(len(exp.passes))).difference(set(passes2pipeline)):
            exp.passes[i].pipeline = False


def print_sourcelist_from_expsum(exp):
    """Prints the source list as appears in the .expsum file for the given experiment.
    """
    if os.path.isdir(f"{path}{exp.expname.lower()}.expsum"):
        filepath = f"{path}{exp.expname.lower()}.expsum"
    elif os.path.isdir(f"/export/jive/jops/piletters/{path}{exp.expname.lower()}.expsum"):
        filepath = f"/export/jive/jops/piletters/{path}{exp.expname.lower()}.expsum"
    else:
        raise FileNotFoundError(f"{self.expname.lower()}.expsum not found.")

    with open(filepath, 'r') as expsumfile:
        print('\nSource list:')
        for a_line in expsumfile.readlines():
            if 'src =' in a_line:
                print(a_line)


def append_freq_setup_from_ms_to_exp(exp):
    """Runs the get_setup_from_ms function from the given experiment to import the metadata
    from the frequency setup, read from the MS files, to the object.
    """
    exp.get_setup_from_ms()


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
        raise FileNotFoundError(f"{vexfile} file not found.")


def scale1bit(exp, antennas):
    """Scale 1-bit data to correct quentization losses for the given telescopes in all
    MS files associated with the given experiment name.

    Inputs:
        - exp : Experiment
            The experiment containing all MS files to be scaled from 1 bit.
        - antennas : str
            Comma-separated list of antennas that recorded at 1 bit.
    """
    cmds = []
    for a_pass in exp.passes:
        cmd, output = shell_command("scale1bit.py", [a_pass.msfile, *antennas.split(',')])
        cmds.append(cmd)
        # outputs.append(output)

    return cmds


def standardplots(exp, refant, calsources):
    """Runs the standardplots on the specified experiment using refant as reference antenna and
    calsources as the sources to be picked for the auto- and cross-correlations.

    It picks the first MS file found in glob.glob. In case of regular experiments there should be
    only one and in case of multi-phase centers this behavior may be replaced.

    Inputs:
        - expname : Experiment
            The experiment containing the MS file to read (it will take only the first pass from the experiment).
        - refant : str
            The antenna name to use as reference in the plots (showing only baselines to this station).
        - calsources : str
            List (comma-separated, no spaces) of source names to be considered for the plots.
            Usually only the strong sources like fringe-finders.
    """
    cmd, output = shell_command("standardplots",
                                ["-weight", exp.passes[0].msfile, refant, calsources])
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
            return '\n'.join(last_lines[::-1])

    # Just in case something went unexpected...
    return '\n'.join(last_lines[::-1])


def archive(flag, experiment, rest_parameters):
    """Runs the archive command with the flag and rest_parameters string for the given experiment object
    (metadata class).
    Flag can be -auth, -stnd, -fits,...
    """
    cmd, output = shell_command("archive.pl",
            [flag, "-e", f"{experiment.expname.lower()}_{experiment.obsdate}", rest_parameters], shell=True)
    return cmd, output



################################################################################
################################################################################
##  Functions to be executed in pipe
################################################################################
################################################################################


