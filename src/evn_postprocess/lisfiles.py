import os
import glob
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from . import experiment, utils
from .experiment import Server


def lis_files_in_ccs(exp: experiment.Experiment, server: Server) -> bool:
    """Returns if there are already lis files created in the experiment directory in ccs.
    
    Args:
        exp (experiment.Experiment): Experiment object.
        server (Server): Server object with ccs connection information.
    
    Returns:
        bool: True if lis files exist in ccs, False otherwise.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    return utils.remote_file_exists(f"{server.user}@{server.host}",
                                    str(Path(str(server.path).format(expname=eEVNname)) / f"{eEVNname.lower()}*.lis"))


def get_lis_files(exp: experiment.Experiment) -> bool:
    """Retrieves all lis files available in ccs for this experiment.
    
    Args:
        exp (experiment.Experiment): Experiment object.
    
    Returns:
        bool: True if lis files were retrieved successfully.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    server = experiment.retrieve_servers()['ccs']
    cmds = []
    if len(glob.glob(f"{eEVNname.lower()}*.lis")) == 0:
        utils.scp(f"{server.user}@{server.host}:" + \
                        str(Path(str(server.path).format(expname=eEVNname)) / f"{eEVNname.lower()}*.lis"), '.')

    for a_lis in glob.glob("*.lis"):
        split_lis_cont_line(exp, a_lis)

    # In the case of e-EVN runs, a renaming of the lis files may be required:
    if eEVNname != exp.expname:
        for a_lis in glob.glob("*.lis"):
            # Modify the references for eEVNname to expname inside the lis files
            # if it has not been done yet
            if exp.expname.lower() not in a_lis:
                update_lis_file(a_lis, eEVNname, exp.expname)
                cmds.append(f" Expname updated from {eEVNname} to {exp.expname} in {a_lis}.")

            os.rename(a_lis, a_lis.replace(eEVNname.lower(), exp.expname.lower()))
            cmds.append(f"mv {a_lis} {a_lis.replace(eEVNname.lower(), exp.expname.lower())}")

    return True


def create_lis_files(exp: experiment.Experiment) -> bool:
    """Creates the lis files in ccs.
    
    Args:
        exp (experiment.Experiment): Experiment object.
    
    Returns:
        bool: True if lis files were created successfully.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    server = experiment.retrieve_servers()['ccs']
    if not lis_files_in_ccs(exp, server):
        logger.info("Creating lis file...")
        utils.ssh(f"{server.user}@{server.host}", f"cd {Path(str(server.path).format(expname=eEVNname))};/ccs/bin/make_lis -e {eEVNname}")

    return True


def update_lis_file(lisfilename: str | Path, oldexp: str, newexp: str) -> None:
    """Updates the lis file (the header lines) referring to an experiment named oldexp
    to newexp. Note that it does not replace all references to oldexp as some of them
    would point to correlator output files that would keep the name.
    
    Args:
        lisfilename (str | Path): Path to the lis file to update.
        oldexp (str): Old experiment name to replace.
        newexp (str): New experiment name.
    
    Returns:
        None
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
                lisfilelines[i] = lisfilelines[i].replace(f"{newexp.lower()}.vix",
                                                          f"{newexp.upper()}.vix")

    with open(lisfilename, 'w') as lisfile:
        lisfile.write(''.join(lisfilelines))


def split_lis_cont_line(exp: experiment.Experiment, fulllisfile: str | Path) -> None:
    """Given a lis file, it checks if there are jobs set as prod_cont and prod_line.
    If not, it does nothing. Otherwise, it splits the lis file into two lis files,
    one for the continuum pass and another one for the line pass.
    
    Args:
        exp (experiment.Experiment): Experiment object to update with spectral line flag.
        fulllisfile (str | Path): Path to the lis file to check and potentially split.
    
    Returns:
        None
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
        exp.spectral_line = True
        print('This is a spectral line experiment with line and continuum passes.')
        lis_cont = str(fulllisfile).replace('.lis', '_cont.lis')
        lis_line = str(fulllisfile).replace('.lis', '_line.lis')
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

def _process_single_lisfile(args):
    """Helper function to process a single lisfile in parallel.
    
    Args:
        args: Tuple of (index, lisfile_path, expname, thereis_line, i_lines_done)
    
    Returns:
        experiment.CorrelatorPass or None if no .ms line found
    """
    i, a_lisfile, expname, thereis_line, i_lines_done = args
    
    with open(a_lisfile, 'r') as lisfile:
        for a_lisline in lisfile.readlines():
            if '.ms' in a_lisline:  # The header line
                # there is only one .ms input there
                msname = [elem.strip() for elem in a_lisline.split() if '.ms' in elem][0]
                # In case the output FITS IDI name has already been set
                if '.IDI' in a_lisline:
                    fitsidiname = [elem.strip() for elem in \
                                   a_lisline.split() if '.IDI' in elem][0]
                    to_pipeline = True if ((fitsidiname.split('_')[-2] == '1') or \
                                           thereis_line) else False
                else:
                    if thereis_line:
                        if '_line' in a_lisfile:
                            fitsidiname = f"{expname.lower()}_{2*i_lines_done + 2}_1.IDI"
                        else:
                            fitsidiname = f"{expname.lower()}_{2*i_lines_done + 1}_1.IDI"

                        to_pipeline = True if i_lines_done == 0 else False
                    else:
                        fitsidiname = f"{expname.lower()}_{i+1}_1.IDI"
                        to_pipeline = True if (i == 0) else False

                # Replaces the old *.UVF string in the .lis file with the FITS IDI
                # file name to generate in this pass.
                if '.UVF' in a_lisline:
                    utils.shell_command('sed',
                                        ['-i', f"'s/{msname}.UVF/{fitsidiname}/g'", str(a_lisfile)],
                                        shell=True, bufsize=-1)
                
                return experiment.CorrelatorPass(Path(a_lisfile), Path(msname), fitsidiname,
                                                to_pipeline)
    
    return None


def get_passes_from_lisfiles(exp: experiment.Experiment) -> bool:
    """Gets all .lis files in the directory, which imply different correlator passes.
    Appends this information to the current experiment (exp object),
    together with the MS file associated for each of them.
    
    This function processes lisfiles in parallel using ThreadPoolExecutor.
    
    Args:
        exp (experiment.Experiment): Experiment object to update with correlator pass information.
    
    Returns:
        bool: True if passes were successfully extracted and stored.
    """
    lisfiles = glob.glob(f"{exp.expname.lower()}*.lis")
    thereis_line = True if '_line' in ''.join(lisfiles) else False
    
    # Prepare arguments for parallel processing
    # Calculate i_lines_done for each file index
    args_list = []
    i_lines_done = 0
    for i, a_lisfile in enumerate(lisfiles):
        args_list.append((i, a_lisfile, exp.expname, thereis_line, i_lines_done))
        if thereis_line and (i % 2 == 0) and (i > 0):
            i_lines_done += 1
    
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(_process_single_lisfile, args_list))
    
    exp.correlator_passes = [result for result in results if result is not None]
    exp.store()
    return True


def _check_single_lisfile(args):
    """Helper function to check a single lisfile in parallel.
    
    Args:
        args: Tuple of (correlator_pass, is_multi_phase_center)
    
    Returns:
        bool: True if this pass is valid, False if issues are found.
    """
    a_pass, is_multi_phase_center = args
    
    output = utils.shell_command("checklis.py", a_pass.lisfile.name, shell=True)
    # The output has the form:
    #      First scan = X
    #       {errors if any otherwise no extra lines}
    #      Last scan = Y
    # removing any possible trailing empty line:
    temp = [o for o in output.split('\n') if len(o) > 0]
    
    if is_multi_phase_center:
        # For multi-phase center experiments, we expect to miss some scans in the different passes
        return not len([t for t in temp if '**** Skipped' not in t]) > 2
    else:
        return (not (len(temp) > 2)) and (all(['No scans in' not in t for t in temp]))


def check_lisfiles(exp: experiment.Experiment) -> bool:
    """Checks the existing .lis files to spot possible issues.
    If at least one of the .lis files reports a possible issue (e.g. duplicated scans,
    missing scans, etc), it will return False. Otherwise it will return True.
    
    This function processes lisfiles in parallel using ThreadPoolExecutor.
    
    Args:
        exp (experiment.Experiment): Experiment object with correlator passes to check.
    
    Returns:
        bool: True if all lis files are valid, False if issues are found.
    """
    is_multi_phase_center = len(exp.correlator_passes) > 2 if exp.spectral_line else len(exp.correlator_passes) > 1
    args_list = [(a_pass, is_multi_phase_center) for a_pass in exp.correlator_passes]
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(_check_single_lisfile, args_list))
    
    return all(results)
