"""All steps to be performed divided in different terminal commands.
"""






def check_systems_up():
    """Check that all required computers are reachable.
    """
    pass


# Interactive dialog function.


# Wrapper to set the output logging file and the messages when starting and finishing.
# Also to catch all exceptions and repeate the steps if required/continue/stop.



def get_lis_vex(expname, computer, eEVNname=None, logger):
    """Produces the lis file(s) for this experiment in ccs and copy them to eee.
    It also retrieves the vex file and creates the required symb. links.

    --- Params
        expname : str
            Name of the EVN experiment.
        computer : str
            Name of the computer where to create the lis file(s) and get the vex file.
        eEVNname : str
            In case of an e-EVN run, this is the name of the e-EVN run.
    """
    eEVNname = expname if eEVNname is None
    cmd = "cd /ccs/expr/{expname};/ccs/bin/make_lis -e {expname} -p prod".format(expname=eEVNname)
    process = subprocess.Popen(["ssh", computer, cmd], shell=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    output = process.communicate()[0].decode('utf-8')
    logger.info(output)
    if ssh.returncode != 0:
        raise ValueError('Error code {} when reading running make_lis in ccs.'.format(ssh.returncode))

    for ext in ('lis', 'vix'):
        cmd = "/ccs/expr/{}/{}\*.{} .".format(eEVNname, eEVNname.lower(), ext)
        subprocess.call(["scp", computer, cmd], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = process.communicate()[0].decode('utf-8')
        logger.info(output)

    # Finally, copy the piletter and expsum files
    for ext in ('piletter', 'expsum'):
        cmd = "piletters/{}.{} .".format(expname.lower(), ext)
        subprocess.call(["scp", "jops@jop83", cmd], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = process.communicate()[0].decode('utf-8')
        logger.info(output)
        if ssh.returncode != 0:
            raise ValueError('Error code {} when reading running make_lis in ccs.'.format(ssh.returncode))

    # In the case of e-EVN runs, a renaming of the lis files may be required:
    if eEVNname != expname:
        for a_lis in glob.glob("*.lis"):
            os.rename(a_lis, a_lis.replace(eEVNname.lower(), expname.lower()))
            process = subprocess.Popen(["checklis.py", a_lis], shell=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
            output = process.communicate()[0].decode('utf-8')
            if ssh.returncode != 0:
                raise ValueError('Error code {} when reading running make_lis in ccs.'.format(ssh.returncode))

    os.symlink("{}.vix".format(eEVNname.lower()), "{}.vix".format(expname))
    print("\n\nYou SHOULD check now the lis files and modify them if needed.")



def pipe_create_dirs(expname, supsci):
    """Create all necessary directories in the Pipeline computer
    """
    for a_midpath in ('in', 'out', 'in/{}'.format(supsci)):
    if not os.path.isdir('/jop83_0/pipe/{}/{}'.format(a_midpath, expname.lower())):
        os.makedir('/jop83_0/pipe/{}/{}'.format(a_midpath, expname.lower()))


