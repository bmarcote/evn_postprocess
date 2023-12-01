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
    - @eee: /data0/{exp.supsci}/{exp.upper()}
    """
    dirs = [f"/jop83_0/pipe/in/{exp.expname.lower()}",
            f"/jop83_0/pipe/out/{exp.expname.lower()}"]
    if (exp.eEVNname is None) or (exp.eEVNname == exp.expname):
        dirs.append(f"/jop83_0/pipe/in/{exp.supsci.lower()}/{exp.expname.lower()}")

    for a_dir in dirs:
        if not env.remote_file_exists('pipe@jop83', a_dir):
            env.ssh('pipe@jop83', f"mkdir -p {a_dir}")
            exp.log(f"mkdir -p {a_dir}")

    return True


def get_files_from_vlbeer(exp) -> bool:
    """Retrieves the antabfs, log, and flag files that should be in vlbeer for the given experiment.
    """
    cd = f"cd /jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}"

    def scp(exp, ext: str):
        return "scp evn@vlbeer.ira.inaf.it:vlbi_arch/" \
               f"{exp.obsdatetime.strftime('%b%y').lower()}/{exp.expname.lower()}" + \
               r"\*" + f".{ext} ."

    cmd, output = env.ssh('pipe@jop83', ';'.join([cd, scp(exp, 'flag')]))
    exp.log(cmd)
    for ext in ('log', 'antabfs'):
        cmd, output = env.ssh('pipe@jop83', ';'.join([cd, scp(exp, ext)]))
        exp.log(cmd)
        cmd, output = env.ssh('pipe@jop83', ';'.join([cd, f"ls {exp.expname.lower()}*{ext}"]))
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
    cmd, output = env.ssh('pipe@jop83',
        f"grep -l ,opacity_corrected /jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}*.antabfs")
    the_files = [o for o in output.split('\n') if o != '']  # just to avoid trailing \n
    for a_file in the_files:
        cmd, _ = env.ssh('pipe@jop83', f"sed -i 's/,opacity_corrected//g' " \
                         f"/jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}/{a_file}", \
                         shell=False)
        exp.log(cmd)
        antenna = a_file.split('/')[-1].replace('.antabfs', '').replace(exp.expname.lower(), \
                  '').capitalize()
        exp.antennas[antenna].opacity = True
    return True


def run_antab_editor(exp) -> Optional[bool]:
    """Opens antab_editor.py for the given experiment.
    """
    cd = f"cd /jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}"
    cdinp = f"/jop83_0/pipe/in/{exp.expname.lower()}"
    cdtemp = f"/jop83_0/pipe/in/{exp.supsci}/" \
             f"{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}"
    if env.remote_file_exists('pipe@jop83', f"{cdinp}/{exp.expname.lower()}*.antab"):
        print("Antab file already found in {cdinp}.")
        return True

    if env.remote_file_exists('pipe@jop83', f"{cdtemp}/" \
            f"{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}*.antab"):
        print("Copying Antab file from {cdtemp} to {cdinp}.")
        cmd, _ = env.ssh('pipe@jop83', f"cp {cdtemp}/*.antab {cdinp}/")
        exp.log(cmd)
        if (exp.eEVNname is not None) and (exp.expname != exp.eEVNname):
            # We need to rename to the actual name
            for an_antab in env.ssh('pipe@jop83', f"ls {cdinp}/*.antab")[1].split('\n'):
                if an_antab != '':
                    env.ssh('pipe@jop83', f"mv {an_antab} "
                f"{'/'.join([*an_antab.split('/')[:-1], an_antab.split('/')[-1].replace(exp.eEVNname.lower(), exp.expname.lower())])}")
        return True

    if exp.eEVNname is not None:
        rprint(f"[bold red]This experiment {exp.expname} is part of the e-EVN run {exp.eEVNname}.\n"
              "Please run antab_editor.py manually to include all experiment associated to the run "
              "(using the '-a' option).\n\nThen run the post-processing again.[/bold red]")
        # I fake it to be sucessful in the object to let it run seemless in a following iteraction
        return None

    if '_line' in ''.join(glob.glob(f"{exp.expname.lower()}*.lis")):
        cmd, _ = env.ssh('-Y '+'pipe@jop83', ';'.join([cd, 'antab_editor.py -l']))
        rprint('\n\n\n[bold red]Run `antab_editor.py -l` manually in pipe.[/bold red]')
    else:
        cmd, _ = env.ssh('-Y '+'pipe@jop83', ';'.join([cd, 'antab_editor.py']))
        rprint('\n\n\n[bold red]Run antab_editor.py manually in pipe.[/bold red]')

    missing_antabs = [a.name for a in exp.antennas if not a.antabfsfile]
    if len(missing_antabs) > 0:
        rprint(f"[red]Note that you are missing ANTAB files from: {', '.join(missing_antabs)}[/red]")

    exp.log(cmd)
    return None


def create_uvflg(exp) -> Optional[bool]:
    """Produces the combined uvflg file containing the full flagging from all telescopes.
    """
    cdinp = f"/jop83_0/pipe/in/{exp.expname.lower()}/"
    if env.remote_file_exists('pipe@jop83', f"{cdinp}/{exp.expname.lower()}*.uvflg"):
        return True

    if (exp.eEVNname is None) or (exp.expname == exp.eEVNname):
        cd = f"cd /jop83_0/pipe/in/{exp.supsci}/{exp.expname.lower()}"
        if not env.remote_file_exists('pipe@jop83', f"{cd}/{exp.expname.lower()}.uvflg"):
            cmd, output = env.ssh('pipe@jop83', ';'.join([cd, 'uvflgall.csh']))
            print(output)
            output_tail = []
            for outline in output.split('\n')[::-1]:
                if 'line ' in outline:
                    break
                output_tail.append(outline)

            exp.log(cmd + '\n# ' + ',\n'.join(output_tail[::-1]).replace('\n', '\n# '))
            cmd, _ = env.ssh('pipe@jop83', ';'.join([cd, \
                             f"cat *uvflgfs > {exp.expname.lower()}.uvflg"]))
            exp.log(cmd)
    else:
        cd = f"/jop83_0/pipe/in/{exp.supsci}/{exp.eEVNname.lower()}"
        if not env.remote_file_exists('pipe@jop83', f"{cd}/{exp.eEVNname.lower()}.uvflg"):
            rprint(f"[bold red]You first need to process the original experiment "
                   f"in this e-EVN run ({exp.eEVNname}).[/bold red]")
            print("Once you have created the .uvflg file for such expeirment "
                  "I will be able to run by myself.")
            return None

    cdinp = f"/jop83_0/pipe/in/{exp.expname.lower()}"
    cdtemp = f"/jop83_0/pipe/in/{exp.supsci}/" \
             f"{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}" \
             f"/{exp.expname.lower() if exp.eEVNname is None else exp.eEVNname.lower()}.uvflg"
    if len(pipepass := [apass.pipeline for apass in exp.correlator_passes if apass.pipeline]) > 1:
        for p in range(1, len(pipepass) + 1):
            cmd, _ = env.ssh('pipe@jop83', f"cp {cdtemp} {cdinp}/{exp.expname.lower()}_{p}.uvflg")
            exp.log(cmd)
    else:
        cmd, _ = env.ssh('pipe@jop83', f"cp {cdtemp} {cdinp}/{exp.expname.lower()}.uvflg")
        exp.log(cmd)

    return True


def create_input_file(exp) -> bool:
    """Copies the template of an input file for the EVN Pipeline
    and modifies the standard parameters.
    """
    # First copies the final uvflg and antab files to the input directory
    cdinp = f"/jop83_0/pipe/in/{exp.expname.lower()}/"
    if env.remote_file_exists('pipe@jop83', f"{cdinp}/{exp.expname.lower()}*.inp.txt"):
        return True

    # Parameters to modify inside the input file
    if exp.supsci == 'marcote':
        cmd, output = env.ssh('pipe@jop83', 'give_me_next_userno.sh')
        if (output is None) or (output.replace('\n', '').strip() == ''):
            raise ValueError('Did not get any output from give_me_next_userno.sh in pipe')
        userno = output.replace('\n', '')
    else:
        userno = 'XXXXX'

    bpass = ', '.join([s.name for s in exp.sources if s.type is experiment.SourceType.fringefinder])
    pcal = ', '.join([s.name for s in exp.sources if s.type is experiment.SourceType.calibrator])
    targets = ', '.join([s.name for s in exp.sources if (s.type is experiment.SourceType.target) or
                         (s.type is experiment.SourceType.other)])
    to_change = [["experiment = n05c3", f"experiment = {exp.expname.lower()}"],
                  ["userno = 3602", f"userno = {userno}"],
                  ["refant = Ef, Mc, Nt", f"refant = {', '.join(exp.refant)}"],
                  ["plotref = Ef", f"plotref = {', '.join(exp.refant)}"],
                  ["bpass = 3C345, 3C454.3", f"bpass = {bpass}"],
                  ["phaseref = 3C454.3", f"phaseref = {pcal}  # VERIFY THIS MANUALLY"],
                  ["target = J2254+1341", f"target = {targets}  # VERIFY THIS MANUALLY"]]

    if len(pcal) == 0:  # no phase-referencing experiment
        to_change += [["#solint = 0", "solint = 2"]]

    pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
    if (len(exp.correlator_passes) > 2) or \
       (len((exp.correlator_passes) == 2) and (len(pipepasses) > 1)):
        env.scp(f"{exp.vix}", f"pipe@jop83:/jop83_0/pipe/in/{exp.expname.lower()}/")
        to_change += [["#doprimarybeam = 1", "doprimarybeam = 1"],
                      ["#setup_station = Ef", f"setup_station = {exp.refant[0]}"]]

    cmd, _ = env.ssh('pipe@jop83',
                  "cp /jop83_0/pipe/in/template.inp " \
                  "/jop83_0/pipe/in/{0}/{0}.inp.txt".format(exp.expname.lower()),
                  shell=False)
    exp.log(cmd, False)
    for a_change in to_change:
        cmd, _ = env.ssh('pipe@jop83', f"sed -i 's/{a_change[0]}/{a_change[1]}/g' " \
                 f"{'/jop83_0/pipe/in/{0}/{0}.inp.txt'.format(exp.expname.lower())}", shell=False)
        exp.log(cmd, False)

    if len(pipepasses) > 1:
        cmd, _ = env.ssh('pipe@jop83',
                      "mv /jop83_0/pipe/in/{0}/{0}.inp.txt "
                      "/jop83_0/pipe/in/{0}/{0}_1.inp.txt".format(exp.expname.lower()))
        exp.log(cmd, False)
        a_change = [f"experiment = {exp.expname.lower()}", f"experiment = {exp.expname.lower()}_1"]
        cmd, _ = env.ssh('pipe@jop83', f"sed -i 's/{a_change[0]}/{a_change[1]}/g' " \
                         f"{'/jop83_0/pipe/in/{0}/{0}_1.inp.txt'.format(exp.expname.lower())}",
                         shell=False)
        exp.log(cmd, False)
        for i in range(2, len(pipepasses) + 1):
            cmd, _ = env.ssh('pipe@jop83',
                          "cp /jop83_0/pipe/in/{0}/{0}_1.inp.txt "
                          "/jop83_0/pipe/in/{0}/{0}_{1}.inp.txt".format(exp.expname.lower(), i))
            exp.log(cmd, False)
            a_change = [f"experiment = {exp.expname.lower()}_1",
                        f"experiment = {exp.expname.lower()}_{i}"]
            cmd, _ = env.ssh('pipe@jop83', f"sed -i 's/{a_change[0]}/{a_change[1]}/g' " \
                     f"{'/jop83_0/pipe/in/{0}/{0}_{1}.inp.txt'.format(exp.expname.lower(), i)}",
                     shell=False)
            exp.log(cmd, False)

    return True


def run_pipeline(exp) -> Optional[bool]:
    """Runs the EVN Pipeline
    """
    exp.log('# Running the pipeline...', True)
    cd = f"cd /jop83_0/pipe/in/{exp.expname.lower()}"
    rprint('\n\n\n[bold red]Modify the input file for the pipeline and run it manually[/bold red]')
    # TODO:
    exp.last_step = 'pipeline'
    return None
    if len(exp.correlator_passes) > 1:
        cmd = env.ssh('pipe@jop83', f"{cd};EVN.py {exp.expname.lower()}_1.inp.txt")
    else:
        cmd = env.ssh('pipe@jop83', f"{cd};EVN.py {exp.expname.lower()}.inp.txt")

    exp.log(cmd, False)
    exp.log('# Pipeline finished.', True)
    if len(exp.correlator_passes) == 2:
        # TODO: implement line in the normal pipeline
        cmd = env.ssh('pipe@jop83', f"{cd};EVN.py {exp.expname.lower()}_2.inp.txt")

    return True


def comment_tasav_files(exp) -> bool:
    """Creates the comment and tasav files after the EVN Pipeline has run.
    """
    cdin = f"/jop83_0/pipe/in/{exp.expname.lower()}"
    cdout = f"/jop83_0/pipe/out/{exp.expname.lower()}"
    if not (env.remote_file_exists('pipe@jop83', \
                                   f"{cdout}/{exp.expname.lower()}" + r"\*.comment") and \
            env.remote_file_exists('pipe@jop83', \
                                   f"{cdin}/{exp.expname.lower()}" + r"\*.tasav.txt")):
        pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
        if len(pipepasses) > 1:
            for p in range(1, len(pipepasses) + 1):
                if pipepasses[p-1].freqsetup.channels >= 256:
                    # We assume that it is a spectral line experiment
                    cmd = env.ssh('pipe@jop83',
                          f"cd {cdout} && comment_tasav_file.py --line {exp.expname.lower()}_{p}")
                else:
                    cmd = env.ssh('pipe@jop83',
                                  f"cd {cdout} && comment_tasav_file.py {exp.expname.lower()}_{p}")

                exp.log(cmd)
        else:
            if exp.correlator_passes[0].freqsetup.channels >= 256:
                cmd = env.ssh('pipe@jop83',
                              f"cd {cdout} && comment_tasav_file.py --line {exp.expname.lower()}")
            else:
                cmd = env.ssh('pipe@jop83',
                              f"cd {cdout} && comment_tasav_file.py {exp.expname.lower()}")
            exp.log(cmd)

    return True


def pipeline_feedback(exp) -> bool:
    """Runs the feedback.pl script after the EVN Pipeline has run.
    """
    cd = f"cd /jop83_0/pipe/out/{exp.expname.lower()}"
    pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
    if len(pipepasses) > 1:
        for p in range(1, len(pipepasses) + 1):
            cmd = env.ssh('pipe@jop83',
                          f"{cd} && /jop83_0/pipe/in/marcote/scripts/evn_support/feedback.pl " \
                          f"-exp '{exp.expname.lower()}_{p}' " \
                          f"-jss '{exp.supsci}' -source "
                          f"'{' '.join([s.name for s in exp.sources])}'", stdout=None)
            exp.log(cmd)
    else:
        cmd = env.ssh('pipe@jop83',
                      f"{cd} && /jop83_0/pipe/in/marcote/scripts/evn_support/feedback.pl " \
                      f"-exp '{exp.expname.lower()}' " \
                      f"-jss '{exp.supsci}' -source " \
                      f"'{' '.join([s.name for s in exp.sources])}'", stdout=None)
        exp.log(cmd)
    return True


def archive(exp) -> bool:
    """Archives the EVN Pipeline results.
    """
    for f in ('in', 'out'):
        cd = f"cd /jop83_0/pipe/{f}/{exp.expname.lower()}"
        cmd = env.ssh('jops@jop83', f"{cd} && /export/jive/jops/bin/archive/user/archive.pl " \
                                    "-pipe -e " \
                                    f"{exp.expname.lower()}_{exp.obsdate}", stdout=None)
        exp.log(cmd)

    return True


# Here there should be a dialog about checking pipeline results, do them manually...

def ampcal(exp) -> bool:
    """Runs the ampcal.sh script to incorporate the gain corrections into the Grafana database.
    """
    cd = f"cd /jop83_0/pipe/out/{exp.expname.lower()}"
    cmd = env.ssh('pipe@jop83', f"{cd} && ampcal.sh")
    exp.log(cmd)
    return True


def get_vlba_antab(exp) -> bool:
    """If the experiment containts VLBA antennas, it retrieves the *cal.vlba file from @ccs.
    """
    # TODO: for VLBA experiments
    return True


