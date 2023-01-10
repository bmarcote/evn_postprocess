#! /usr/bin/env python3
import os
import sys
import subprocess
from . import process_eee as eee
from . import process_pipe as pipe


def scp(originpath, destpath, timeout=None):
    """Does a scp from originpath to destpath. If the process returns an error,
    then it raises ValueError.
    """
    print("\n\033[1m> " + f"scp {originpath} {destpath}" + "\033[0m")
    process = subprocess.run(["scp", originpath, destpath], shell=False,
                              stdout=None, stderr=subprocess.PIPE, timeout=timeout)
    if process != 0:
        raise ValueError(f"\nError code {process} when running scp {originpath} {destpath} in ccs.")

    return f"scp {originpath} {destpath}", process


def ssh(computer, commands, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    """Sends a ssh command to the indicated computer.
    Returns the output or raises ValueError in case of errors.
    The output is expected to be in UTF-8 format.
    """
    print("\n\033[1m> " + f"ssh {computer} {commands}" + "\033[0m")
    process = subprocess.Popen(["ssh", computer, commands], shell=shell, stdout=stdout, stderr=stderr)
    # logger.info(output)
    if (process.returncode != 0) and (process.returncode is not None):
        raise ValueError(f"Error code {process.returncode} when running ssh {computer}:{commands} in ccs.")

    if process.communicate()[0] is not None:
        return f"ssh {computer}:{commands}", process.communicate()[0].decode('utf-8')

    return f"ssh {computer}:{commands}"


def shell_command(command, parameters=None, shell=True, bufsize=-1,
                  stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    """Runs the provided command in the shell with some arguments if necessary.
    Returns the output of the command, assuming a UTF-8 encoding, or raises ValueError
    if fails. Parameters must be either a single string or a list, if provided.
    """
    if isinstance(parameters, list):
        full_shell_command = [command] + parameters
    else:
        full_shell_command = [command] if parameters is None else [command, parameters]

    print("\n\033[1m> " + f"{' '.join(full_shell_command)}" + "\033[0m")

    process = subprocess.Popen(' '.join(full_shell_command), shell=shell,
                               stdout=stdout, stderr=stderr, bufsize=bufsize)
    # process = subprocess.Popen(full_shell_command, shell=shell, stdout=subprocess.PIPE,
    # for line in process.stdout:
    #     print(line.decode('utf-8').replace('\n', ''))
    output_lines = []
    while process.poll() is None:
        if process.stdout is not None:
            out = process.stdout.readline().decode('utf-8')
            output_lines.append(out)
            sys.stdout.write(out)
            sys.stdout.flush()

    if (process.returncode != 0) and (process.returncode is not None):
        raise ValueError(f"Error code {process.returncode} when running {command} {parameters} in ccs.")

    return ' '.join(full_shell_command), ''.join(output_lines)


def remote_file_exists(host, path):
    """Checks if a file or path exists in a remote computer returning a bool.
    It may raise an Exception.
    """
    # Test does not work if finds multiple files.
    # status = subprocess.call(['ssh', host, f"test -f {path}"])
    status = subprocess.call(['ssh', host, f"ls {path}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if status == 0:
        return True
    elif (status == 1) or (status == 2):
        return False

    raise Exception(f"SSH connection to {host} failed.")


def grep_remote_file(host, remote_file, word):
    """Runs a grep in a file located in a remote host and returns it.
    It may raise ValueError if there is a problem accessing the host or file.
    """
    cmd = f"grep {word} {remote_file}"
    process = subprocess.Popen(["ssh", host, cmd], shell=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    output = process.communicate()[0].decode('utf-8')
    if process.returncode != 0:
        raise ValueError(f"Errorcode {process.returncode} when searching for {word} in {remote_file} from {host}.")

    return output


def create_all_dirs(exp):
    """Creates all folders (in eee and jop83) for the associated post-processing.
    Input:
        - exp : experiment.Experiment
    """
    eee.create_folders(exp)
    pipe.create_folders(exp)
    return True


def copy_files(exp):
    """Copy all files related to the experiment that already exist when the post-processing
    starts. This includes:
    - vix file
    - expsum file
    - piletter file

    Input:
        - exp : experiment.Experiment

    May Raise: FileNotFound
    """
    exists = []
    files = (exp.vix, exp.expsum, exp.piletter, exp.keyfile, exp.sumfile)
    for a_file in files:
        exists.append(a_file.exists())

    if False in exists:
        raise FileNotFoundError(f"The following files could not be found: "
                                f"{','.join([f.name for f, b in zip(files, exists) if b])}")

    return True


def update_lis_file(lisfilename, oldexp, newexp):
    """Updates the lis file (the header lines) referring to an experiment named oldexp
    to newexp. Note that it does not replace all references to oldexp as some of them
    would point to correlator output files that would keep the name.
    """
    with open(lisfilename, 'r') as lisfile:
        lisfilelines = lisfile.readlines()
        for i, aline in enumerate(lisfilelines):
            if aline[0] not in ('+', '-'):
                # Replace the EXP (upper) entries
                lisfilelines[i] = aline.replace(oldexp, newexp)
                # Replace the exp (lower) entries
                lisfilelines[i] = lisfilelines[i].replace(oldexp.lower(), newexp.lower())
                # Replace the exp.vix to EXP.vix (as symb link was done)
                lisfilelines[i] = lisfilelines[i].replace(f"{newexp.lower()}.vix", f"{newexp.upper()}.vix")

    with open(lisfilename, 'w') as lisfile:
        lisfile.write(''.join(lisfilelines))


def split_lis_cont_line(fulllisfile):
    """Given a lis file, it checks if there are jobs set as prod_cont and prod_line.
    If not, it does nothing. Otherwise, it splits the lis file into two lis files,
    one for the continuum pass and another one for the line pass.
    """
    # Checks that there are more than one PROD pass
    n_prods = set()
    with open(fulllisfile) as f_full:
        for a_fileline in f_full.readlines():
            temp = a_fileline.split()
            if 'PROD' in temp:
                n_prods.add(temp[temp.index('PROD') + 1])

    # TODO: possible problems if > 2 ?
    if ('prod_line' in n_prods) and (len(n_prods) > 1):
        print('This is a spectral line experiment with line and continuum passes.')
        lis_cont = fulllisfile.replace('.lis', '_cont.lis')
        lis_line = fulllisfile.replace('.lis', '_line.lis')
        with open(lis_cont, 'w') as f_cont, open(lis_line, 'w') as f_line:
            with open(fulllisfile) as f_full:
                for a_fileline in f_full.readlines():
                    if a_fileline[0].strip() not in ('+', '-'):
                        f_cont.write(a_fileline.replace('.ms', '_cont.ms'))
                        f_line.write(a_fileline.replace('.ms', '_line.ms'))
                    else:
                        if 'prod_line' in a_fileline:
                            f_line.write(a_fileline)
                            f_cont.write(a_fileline.replace('+', '-'))
                        else:
                            f_line.write(a_fileline.replace('+', '-'))
                            f_cont.write(a_fileline)

        os.remove(fulllisfile)


def check_lisfiles(exp):
    """Checks the existing .lis files to spot possible issues.
    If at least one of the .lis files reports a possible issue (e.g. duplicated scans,
    missing scans, etc), it will return False. Otherwise it will return true.
    """
    all_good = True
    for a_pass in exp.correlator_passes:
        cmd, output = shell_command("checklis.py", a_pass.lisfile.name, shell=True)
        exp.log(f"{cmd}"+"\n#"+output.replace('\n', '\n#'), False)
        # The output has the form:
        #      First scan = X
        #       {errors if any otherwise no extra lines}
        #      Last scan = Y
        temp = [o for o in output.split('\n') if len(o) > 0]  # removing any possible trailing empty line
        all_good = all_good and not (len(temp) > 2)

    return all_good


def update_pipelinable_passes(exp, pipelinable):
    """Updates the attribute of the CorrelatorPasses from exp to define
    if the specific pass should run in the pipeline or not.

    Input
        exp : metadata.Experiment
        pipelinable : list of bool or dict.
            It can be either a list of bool. In that case it must have the same
            dimensions as exp.passes. And the order should be the same. Each
            value would then be applied to each CorrelatorPass from the list in passes.
            If a dict, it must include the associated lisfile as key and then
            the bool as value.
    """
    if isinstance(pipelinable, list):
        assert len(pipelinable) == len(exp.correlator_passes)
        for i, is_pipelinable in enumerate(pipelinable):
            assert isinstance(is_pipelinable, bool)
            exp.correlator_passes[i].pipeline = is_pipelinable
    elif isinstance(pipelinable, dict):
        for a_lisfile in pipelinable:
            for a_exppass in exp.correlator_passes:
                if a_exppass.lisfile == a_lisfile:
                    a_exppass.pipeline = pipelinable[a_lisfile]
                    break


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


def extract_tail_standardplots_output(stdplt_output):
    """Given a full log output from standardplots, it returns only the last bits that contain
    the information provided by the "r" command.
    """
    last_lines = []
    for a_line in stdplt_output.split('\n')[::-1]:
        # All "r" output lines always start with those messages
        # (listTimeRage: , listSources: , listAntennas: , listFreqs: ):
        if 'list' in a_line:
            last_lines.append(f"# {a_line}")
        elif 'ms: Current' in a_line:
            # We are already done for this output
            break
    last_lines.append('\n')

    return '\n'.join(last_lines[::-1])


def archive(flag, exp, rest_parameters):
    """Runs the archive command with the flag and rest_parameters string for the given experiment object
    (metadata class).
    Flag can be -auth, -stnd, -fits,...
    """
    cmd, output = shell_command("archive.pl",
                                [flag, "-e", f"{exp.expname.lower()}_{exp.obsdate}",
                                 rest_parameters], shell=True)
    exp.log(cmd, '# '+'# '.join(output))
    return cmd, output


