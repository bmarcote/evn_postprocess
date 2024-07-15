#! /usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the pipe computer.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.
"""
import glob
from typing import Optional
from rich import print as rprint
from . import experiment
from . import environment as env


def create_folders(exp) -> bool:
    """Creates the folder required for the post-processing of the experiment
    @eee:/data0/{exp.supsci}/{exp.upper()}
    """
    dirs = [f"/data/pipe/{exp.expname.lower()}/in",
            f"/data/pipe/{exp.expname.lower()}/out"]
    if (exp.eEVNname is None) or (exp.eEVNname == exp.expname):
        dirs.append(f"/data/pipe/{exp.expname.lower()}/temp")

    for a_dir in dirs:
        if not env.remote_file_exists('jops@archive2', a_dir):
            env.ssh('jops@archive2', f"mkdir -p {a_dir}")
            exp.log(f"mkdir -p {a_dir}")

    return True


def get_files_from_vlbeer(exp) -> bool:
    """Retrieves the antabfs, log, and flag files that should be in vlbeer for the given experiment.
    """
    cd = f"cd /data/pipe/{exp.expname.lower()}/temp"

    def scp(exp, ext: str):
        return "scp evn@vlbeer.ira.inaf.it:vlbi_arch/" \
               f"{exp.obsdatetime.strftime('%b%y').lower()}/{exp.expname.lower()}" + \
               r"\*" + f".{ext} ."



    cmd, output = env.ssh('jops@archive2', ';'.join([cd, scp(exp, 'flag')]))
    exp.log(cmd)
    for ext in ('log', 'antabfs'):
        cmd, output = env.ssh('jops@archive2', ';'.join([cd, scp(exp, ext)]))
        exp.log(cmd)
        cmd, output = env.ssh('jops@archive2', ';'.join([cd, f"ls {exp.expname.lower()}*{ext}"]))
        the_files = [o for o in output.split('\n') if o != '']  # just to avoid trailing \n
        for a_file in the_files:
            ant = a_file.split('.')[0].replace(exp.expname.lower(), '').capitalize()
            try:
                if ext == 'log':
                    exp.antennas[ant].logfsfile = True
                elif ext == 'antabfs':
                    exp.antennas[ant].antabfsfile = True
            except ValueError:
                # Likely the antenna has a different name in the expsum, or is an e-EVN
                # where this antenna participated but not in this particular experiment
                rprint(f"[yellow]The antenna '{ant}' has a log file but is not found in " \
                       "the .expsum file. Just ignoring this and continuing...[/yellow]")


    exp.log(f"\n# Log files found for:\n# {', '.join(exp.antennas.logfsfile)}")
    if len(set(exp.antennas.names)-set(exp.antennas.logfsfile)) > 0:
        exp.log("# Missing files for: " \
                f"{', '.join((set(exp.antennas.names)-set(exp.antennas.logfsfile)).intersection(set(exp.antennas.observed)))}\n")
    else:
        exp.log("# No missing log files for any station that observed.\n")

    exp.log(f"# Antab files found for:\n# {', '.join(exp.antennas.antabfsfile)}")
    if len(set(exp.antennas.names)-set(exp.antennas.antabfsfile)) > 0:
        exp.log("# Missing files for: " \
                f"{', '.join((set(exp.antennas.names)-set(exp.antennas.antabfsfile)).intersection(set(exp.antennas.observed)))}\n")
    else:
        exp.log("# No missing antab files for any station that observed.\n")

    # In case of high-freq observations, some stations added the "opacity_corrected" flag to
    #the POLY= line, against any standard... Let's remove it so antab_editor (later) can work fine.
    cmd, output = env.ssh('jops@archive2',
        f"grep -l ',opacity_corrected' /data/pipe/{exp.expname.lower()}/temp/{exp.expname.lower()}*.antabfs")
    the_files = [o for o in output.split('\n') if o != '']  # just to avoid trailing \n
    for a_file in the_files:
        cmd, _ = env.ssh('jops@archive2', f"sed -i 's/,opacity_corrected//g' " \
                         f"/data/pipe/{exp.expname.lower()}/temp/{a_file}", \
                         shell=False)
        exp.log(cmd)
        antenna = a_file.split('/')[-1].replace('.antabfs', '').replace(exp.expname.lower(), \
                  '').capitalize()
        exp.antennas[antenna].opacity = True
    return True


def get_vlba_antab(exp) -> Optional[bool]:
    """Retrieves the cal (antab) files from VLBA if needed, and copies the VLBA gains, into the archive temp folder
    for the given experiment.
    """
    if exp.expname.lower()[0] != 'g':
        return True

    cd = f"cd /jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}"

    cmd, output = env.ssh('pipe@jop83', ';'.join([cd, "scp jops@eee:/data0/tsys/vlba_gains.key ."]))
    exp.log(cmd)
    cmd, output = env.ssh('pipe@jop83', ';'.join([cd, "scp jops@ccs:/ccs/var/log2vex/logexp_date/" \
                                                      f"{exp.expname.upper()}_{exp.obsdatetime.strftime('%Y%m%d')}" \
                                                      f"/{exp.expname.lower()}cal.vlba ."]))
    exp.log(cmd)
    return True

    # TODO: grep here which antennas are in the cal (e.g. grep TSYS XX) and update the values.


                # if ext == 'log':
                #     exp.antennas[ant].logfsfile = True
                # elif ext == 'antabfs':
                #     exp.antennas[ant].antabfsfile = True
                #



def run_antab_editor(exp) -> Optional[bool]:
    """Opens antab_editor.py for the given experiment.
    """
    cd = f"cd /data/pipe/{exp.expname.lower()}/temp"
    cdinp = f"/data/pipe/{exp.expname.lower()}/in"
    cdtemp = f"/data/pipe/{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}/temp"
    if env.remote_file_exists('jops@archive2', f"{cdinp}/{exp.expname.lower()}*.antab"):
        print("Antab file already found in {cdinp}.")
        return True

    if env.remote_file_exists('jops@archive2', f"{cdtemp}/" \
            f"{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}*.antab"):
        print("Copying Antab file from {cdtemp} to {cdinp}.")
        cmd, _ = env.ssh('jops@archive2', f"cp {cdtemp}/*.antab {cdinp}/")
        exp.log(cmd)
        if (exp.eEVNname is not None) and (exp.expname != exp.eEVNname):
            # We need to rename to the actual name
            for an_antab in env.ssh('jops@archive2', f"ls {cdinp}/*.antab")[1].split('\n'):
                if an_antab != '':
                    env.ssh('jops@archive2', f"mv {an_antab} "
                f"{'/'.join([*an_antab.split('/')[:-1], an_antab.split('/')[-1].replace(exp.eEVNname.lower(), exp.expname.lower())])}")
        return True

    if exp.eEVNname is not None:
        rprint(f"[bold red]This experiment {exp.expname} is part of the e-EVN run {exp.eEVNname}.\n"
              "Please run antab_editor.py manually to include all experiment associated to the run "
              "(using the '-a' option).\n\nThen run the post-processing again.[/bold red]")
        # I fake it to be sucessful in the object to let it run seemless in a following iteraction
        return None

    if '_line' in ''.join(glob.glob(f"{exp.expname.lower()}*.lis")):
        cmd, _ = env.ssh('-Y '+'jops@archive2', ';'.join([cd, 'antab_editor.py -l']))
        rprint('\n\n\n[bold red]Run `antab_editor.py -l` manually in pipe.[/bold red]')
    else:
        cmd, _ = env.ssh('-Y '+'jops@archive2', ';'.join([cd, 'antab_editor.py']))
        rprint('\n\n\n[bold red]Run antab_editor.py manually in pipe.[/bold red]')

    missing_antabs = [a.name for a in exp.antennas if not a.antabfsfile]
    if len(missing_antabs) > 0:
        rprint(f"[red]Note that you are missing ANTAB files from: {', '.join(missing_antabs)}[/red]")

    exp.log(cmd)
    return None


def create_uvflg(exp) -> Optional[bool]:
    """Produces the combined uvflg file containing the full flagging from all telescopes.
    """
    cdinp = f"/data/pipe/{exp.expname.lower()}/in"
    if env.remote_file_exists('jops@archive2', f"{cdinp}/{exp.expname.lower()}*.uvflg"):
        return True

    if (exp.eEVNname is None) or (exp.expname == exp.eEVNname):
        cd = f"cd /data/pipe/{exp.expname.lower()}/temp"
        if not env.remote_file_exists('jops@archive2', f"{cd}/{exp.expname.lower()}.uvflg"):
            cmd, output = env.ssh('jops@archive2', ';'.join([cd, '~/opt/evn_support//uvflgall.sh']))
            print(output)
            output_tail = []
            for outline in output.split('\n')[::-1]:
                if 'line ' in outline:
                    break
                output_tail.append(outline)

            exp.log(cmd + '\n# ' + ',\n'.join(output_tail[::-1]).replace('\n', '\n# '))
            cmd, _ = env.ssh('jops@archive2', ';'.join([cd, \
                             f"cat *uvflgfs > {exp.expname.lower()}.uvflg"]))
            exp.log(cmd)
    else:
        cd = f"/data/pipe/{exp.eEVNname.lower()}/temp"
        if not env.remote_file_exists('jops@archive2', f"{cd}/{exp.eEVNname.lower()}.uvflg"):
            rprint(f"[bold red]You first need to process the original experiment "
                   f"in this e-EVN run ({exp.eEVNname}).[/bold red]")
            print("Once you have created the .uvflg file for such expeirment "
                  "I will be able to run by myself.")
            return None

    cdinp = f"/data/pipe/{exp.expname.lower()}/in"
    cdtemp = f"/data/pipe/" \
             f"{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}/temp" \
             f"/{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}.uvflg"
    if len(pipepass := [apass.pipeline for apass in exp.correlator_passes if apass.pipeline]) > 1:
        for p in range(1, len(pipepass) + 1):
            cmd, _ = env.ssh('jops@archive2', f"cp {cdtemp} {cdinp}/{exp.expname.lower()}_{p}.uvflg")
            exp.log(cmd)
    else:
        cmd, _ = env.ssh('jops@archive2', f"cp {cdtemp} {cdinp}/{exp.expname.lower()}.uvflg")
        exp.log(cmd)

    return True


def create_input_file(exp) -> bool:
    """Copies the template of an input file for the EVN Pipeline
    and modifies the standard parameters.
    """
    # First copies the final uvflg and antab files to the input directory
    cdinp = f"/data/pipe/{exp.expname.lower()}/in/"
    if env.remote_file_exists('jops@archive2', f"{cdinp}/{exp.expname.lower()}*.inp.txt"):
        return True

    # Parameters to modify inside the input file
    cmd, output = env.ssh('jops@archive2', f"~/opt/evn_support/aips_userno.py {exp.supsci.lower()}")
    if (output is None) or (output.replace('\n', '').strip() == ''):
        exp.log('ERROR: Could not recover your next AIPS user number (from archive2:/data/pipe/aips_userno.txt)')
        rprint('[red bold]Could not recover your next AIPS user number " \
                "(from archive2:/data/pipe/aips_userno.txt)[/red bold]')
        # raise ValueError('Could not recover your next AIPS user number (from archive2:/data/pipe/aips_userno.txt)')

    userno = output.replace('\n', '').strip()

    bpass = ', '.join([s.name for s in exp.sources if s.type is experiment.SourceType.fringefinder])
    pcal = ', '.join([s.name for s in exp.sources if s.type is experiment.SourceType.calibrator])
    targets = ', '.join([s.name for s in exp.sources if (s.type is experiment.SourceType.target) or
                         (s.type is experiment.SourceType.other)])



    to_change = [["experiment = n05c3", f"experiment = {exp.expname.lower()}"],
                  ["userno = 3602", f"userno = {userno}"],
                  ["refant = Ef, Mc, Nt", f"refant = {', '.join(exp.refant)}"],
                  ["plotref = Ef", f"plotref = {', '.join(exp.refant)}"],
                  ["bpass = 3C345, 3C454.3", f"bpass = {bpass}"]]

    if len(pcal) == 0: # no phase-referencing experiment
        to_change += [["#solint = 0", "solint = 2"]]
        to_change += [["phaseref = 3C454.3", "#phaseref ="],
                      ["target = J2254+1341", "#target ="],
                      ["#sources=", f"sources = {targets}, {bpass}"]]
    elif len(targets) == 2*len(pcal):
        pcals = []
        for p in pcal:
            pcals.append(p)
            pcals.append(p)

        to_change += [["phaseref = 3C454.3", f"phaseref = {','.join(pcals)}"],
                      ["target = J2254+1341", f"target = {targets}"]]
    else:
        to_change += [["phaseref = 3C454.3", f"phaseref = {pcal}"],
                      ["target = J2254+1341", f"target = {targets}"]]


    pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
    if (len(exp.correlator_passes) > 2) or \
       ((len(exp.correlator_passes) == 2) and (len(pipepasses) > 1)):
        env.scp(f"{exp.vix}", f"jops@archive2:/data/pipe/{exp.expname.lower()}/in/")
        to_change += [["#doprimarybeam = 1", "doprimarybeam = 1"],
                      ["#setup_station = Ef", f"setup_station = {exp.refant[0]}"]]

    cmd, _ = env.ssh('jops@archive2',
                  "cp /data/pipe/templates/pipeline.inp.txt " \
                  "/data/pipe/{0}/in/{0}.inp.txt".format(exp.expname.lower()),
                  shell=False)
    exp.log(cmd, False)
    for a_change in to_change:
        cmd, _ = env.ssh('jops@archive2', f"sed -i 's/{a_change[0]}/{a_change[1]}/g' " \
                 f"{'/data/pipe/{0}/in/{0}.inp.txt'.format(exp.expname.lower())}", shell=False)
        exp.log(cmd, False)

    if len(pipepasses) > 1:
        cmd, _ = env.ssh('jops@archive2',
                      "mv /data/pipe/{0}/in/{0}.inp.txt "
                      "/data/pipe/{0}/in/{0}_1.inp.txt".format(exp.expname.lower()))
        exp.log(cmd, False)
        a_change = [f"experiment = {exp.expname.lower()}", f"experiment = {exp.expname.lower()}_1"]
        cmd, _ = env.ssh('jops@archive2', f"sed -i 's/{a_change[0]}/{a_change[1]}/g' " \
                         f"{'/data/pipe/{0}/in/{0}_1.inp.txt'.format(exp.expname.lower())}",
                         shell=False)
        exp.log(cmd, False)
        for i in range(2, len(pipepasses) + 1):
            cmd, _ = env.ssh('jops@archive2',
                          "cp /data/pipe/{0}/in/{0}_1.inp.txt "
                          "/data/pipe/{0}/in/{0}_{1}.inp.txt".format(exp.expname.lower(), i))
            exp.log(cmd, False)
            a_change = [f"experiment = {exp.expname.lower()}_1",
                        f"experiment = {exp.expname.lower()}_{i}"]
            cmd, _ = env.ssh('jops@archive2', f"sed -i 's/{a_change[0]}/{a_change[1]}/g' " \
                    f"{'/data/pipe/{0}/in/{0}_{1}.inp.txt'.format(exp.expname.lower(), i)}",
                     shell=False)
            exp.log(cmd, False)

    return True


def run_pipeline(exp) -> Optional[bool]:
    """Runs the EVN Pipeline
    """
    exp.log('# Running the pipeline...', True)
    cd = f"cd /data/pipe/{exp.expname.lower()}/in/"
    rprint('\n\n\n[bold red]Modify the input file for the pipeline and run it manually[/bold red]')
    # TODO:
    exp.last_step = 'pipeline'
    return None
    if len(exp.correlator_passes) > 1:
        cmd = env.ssh('jops@archive2', f"{cd};EVN.py {exp.expname.lower()}_1.inp.txt")
    else:
        cmd = env.ssh('jops@archive2', f"{cd};EVN.py {exp.expname.lower()}.inp.txt")

    exp.log(cmd, False)
    exp.log('# Pipeline finished.', True)
    if len(exp.correlator_passes) == 2:
        # TODO: implement line in the normal pipeline
        cmd = env.ssh('jops@archive2', f"{cd};EVN.py {exp.expname.lower()}_2.inp.txt")

    return True


def comment_tasav_files(exp) -> bool:
    """Creates the comment and tasav files after the EVN Pipeline has run.
    """
    cdin = f"/data/pipe/{exp.expname.lower()}/in"
    cdout = f"/data/pipe/{exp.expname.lower()}/out"
    path = "/home/jops/opt/evn_support"
    if not (env.remote_file_exists('jops@archive2', \
                                   f"{cdout}/{exp.expname.lower()}" + r"\*.comment") and \
            env.remote_file_exists('jops@archive2', \
                                   f"{cdin}/{exp.expname.lower()}" + r"\*.tasav.txt")):
        pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
        if len(pipepasses) > 1:
            for p in range(1, len(pipepasses) + 1):
                if pipepasses[p-1].freqsetup.channels >= 512:
                    # We assume that it is a spectral line experiment
                    cmd = env.ssh('jops@archive2',
                          f"cd {cdin} && {path}/comment_tasav_file.py --line {exp.expname.lower()}_{p}", stdout=None)
                else:
                    cmd = env.ssh('jops@archive2',
                                  f"cd {cdin} && {path}/comment_tasav_file.py {exp.expname.lower()}_{p}", stdout=None)

                exp.log(cmd)
        else:
            if exp.correlator_passes[0].freqsetup.channels >= 512:
                cmd = env.ssh('jops@archive2',
                              f"cd {cdin} && {path}/comment_tasav_file.py --line {exp.expname.lower()}", stdout=None)
            else:
                cmd = env.ssh('jops@archive2',
                              f"cd {cdin} && {path}/comment_tasav_file.py {exp.expname.lower()}", stdout=None)
            exp.log(cmd)

    return True


def pipeline_feedback(exp) -> bool:
    """Runs the feedback.pl script after the EVN Pipeline has run.
    """
    cd = f"cd /data/pipe/{exp.expname.lower()}/out"
    pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
    if len(pipepasses) > 1:
        for p in range(1, len(pipepasses) + 1):
            cmd = env.ssh('jops@archive2',
                          f"{cd} && /home/jops/opt/evn_support/feedback.pl " \
                          f"-exp '{exp.expname.lower()}_{p}' " \
                          f"-jss '{exp.supsci}' -source "
                          f"'{' '.join([s.name for s in exp.sources])}'", stdout=None)
            exp.log(cmd)
    else:
        cmd = env.ssh('jops@archive2',
                      f"{cd} && /home/jops/opt/evn_support/feedback.pl " \
                      f"-exp '{exp.expname.lower()}' " \
                      f"-jss '{exp.supsci}' -source " \
                      f"'{' '.join([s.name for s in exp.sources])}'", stdout=None)
        exp.log(cmd)
    return True


def archive(exp) -> bool:
    """Archives the EVN Pipeline results.
    """
    for f in ('in', 'out'):
        cd = f"cd /data/pipe/{exp.expname.lower()}/{f}/"
        cmd = env.ssh('jops@archive2', f"{cd} && /home/jops/opt/evn_support/archive.pl " \
                      f"-pipe -e {exp.expname.lower()}_{exp.obsdate}", stdout=None)
        exp.log(cmd)

    return True


# Here there should be a dialog about checking pipeline results, do them manually...

def ampcal(exp) -> bool:
    """Runs the ampcal.sh script to incorporate the gain corrections into the Grafana database.
    """
    cd = f"cd /data/pipe/{exp.expname.lower()}/out"
    cmd = env.ssh('jops@archive2', f"{cd} && ampcal.sh")
    exp.log(cmd)
    return True


def get_vlba_antab(exp) -> bool:
    """If the experiment containts VLBA antennas, it retrieves the *cal.vlba file from @ccs.
    """
    # TODO: for VLBA experiments
    return True


