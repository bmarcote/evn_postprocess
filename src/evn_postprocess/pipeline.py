#! /usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps at the pipe computer.
It runs all steps although it requires user interaction to
verify that all steps have been performed correctly and/or
perform required changes in intermediate files.
"""
import os
import glob
import shutil
import subprocess
import traceback
from importlib import resources
from loguru import logger
from pathlib import Path
from typing import Optional
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from rich import print as rprint
from . import experiment
from . import utils
from . import comment_tasav


def get_files_from_vlbeer(exp, server: experiment.Server) -> bool:
    """Retrieves the antabfs, log, and flag files that should be in vlbeer for the given experiment.
    """
    def fetch_file(ext: str):
        try:
            s_formatted = eval(f"f'{server.path}'", {'obsdate': exp.obsdate})
            utils.scp(f"{server.user}@{server.host}:{Path(s_formatted) / f'{exp.expname.lower()}*{ext}'}",
                      str(exp.dirs.pipe_temp) + "/", timeout=120)
        except subprocess.TimeoutExpired:
            rprint(f"[bold yellow]Could not retrieve the {ext} files from vlbeer.[/bold yellow]")
            logger.warning("Could not retrieve {ext} files from vlbeer")
        except ValueError:
            rprint(f"[bold yellow]Could not find the {ext} files in vlbeer.[/bold yellow]")
            logger.warning("Could not retrieve {ext} files from vlbeer")

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_file, a_file) for a_file in ('antabfs', 'log', 'flag')]
        for future in futures:
            future.result()

    for ext in ('antabfs', 'log'):
        for a_file in list(exp.dirs.pipe_temp.glob(f"{exp.expname.lower()}*{ext}")):
            ant = a_file.name.split('.')[0].replace(f"{exp.expname.lower()}", '').split('_')[0].capitalize()
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

    logger.debug(f"\n# Log files found for:\n# {', '.join(exp.antennas.logfsfile)}")
    if len(set(exp.antennas.names)-set(exp.antennas.logfsfile)) > 0:
        logger.debug("# Missing files for: " \
                f"{', '.join((set(exp.antennas.names)-set(exp.antennas.logfsfile)).intersection(set(exp.antennas.observed)))}\n")
    else:
        logger.debug("# No missing log files for any station that observed.\n")

    logger.debug(f"# Antab files found for:\n# {', '.join(exp.antennas.antabfsfile)}")
    if len(set(exp.antennas.names)-set(exp.antennas.antabfsfile)) > 0:
        logger.debug("# Missing files for: " \
                f"{', '.join((set(exp.antennas.names)-set(exp.antennas.antabfsfile)).intersection(set(exp.antennas.observed)))}\n")
    else:
        logger.debug("# No missing antab files for any station that observed.\n")

    # In case of high-freq observations, some stations added the "opacity_corrected" flag to
    #the POLY= line, against any standard... Let's remove it so antab_editor (later) can work fine.
    # Remove ',opacity_corrected' from local antabfs files and add a comment line after
    for antabfs_file in exp.dirs.pipe_temp.glob(f"{exp.expname.lower()}*.antabfs"):
        with open(antabfs_file, 'r') as f:
            content = f.read()
        
        if ',opacity_corrected' in content:
            new_lines = []
            for line in content.split('\n'):
                if ',opacity_corrected' in line:
                    new_lines.append(line.replace(',opacity_corrected', ''))
                    new_lines.append('! opacity_corrected')
                else:
                    new_lines.append(line)
            
            with open(antabfs_file, 'w') as f:
                f.write('\n'.join(new_lines))
            
            antenna = antabfs_file.name.split('.')[0].replace(f"{exp.expname.lower()}_", '').split('_')[0].capitalize()
            if antenna in exp.antennas:
                exp.antennas[antenna].opacity = True

    exp.store()
    return True


def get_vlba_antab(exp) -> Optional[bool]:
    """Retrieves the cal (antab) files from VLBA if needed, and copies the VLBA gains, into the archive temp folder
    for the given experiment.
    """
    rprint("[bold yellow]get_vlba_antab not implemented yet. You need to get the VLBA antab files manually.[/bold yellow]")
    raise NotImplementedError
    if exp.expname.lower()[0] != 'g':
        return True

    cd = f"cd /data/pipe/{exp.expname.lower()}/temp/"

    utils.ssh('jops@archive.jive.eu', ';'.join([cd, "scp jops@eee:/data0/tsys/vlba_gains.key ."]))
    utils.ssh('jops@archive.jive.eu', ';'.join([cd, "scp jops@ccs:/ccs/var/log2vex/logexp_date/" \
                                                      f"{exp.expname.upper()}_{exp.obsdatetime.strftime('%Y%m%d')}" \
                                                      f"/{exp.expname.lower()}cal.vlba ."]))
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
    original_cwd = os.getcwd()
    os.chdir(exp.dirs.pipe_temp)
    if (exp.eEVNname is not None) and (exp.expname != exp.eEVNname):
        os.chdir(original_cwd)
        rprint(f"[bold red]This experiment {exp.expname} is part of the e-EVN run {exp.eEVNname}[/bold red].\n"
              "[red]You should only run antab_editor.py from the main e-EVN experiment run (including the "
              "rest of the run experiments).\nRun it manually in case you indeed want to run it here.[/red]")
        raise ValueError("antab_editor.py should only be run from the main e-EVN experiment run")

    if '_line' in ''.join(glob.glob(f"{exp.expname.lower()}*.lis")):
        utils.shell_command("antab_editor.py", ["-e", exp.expname.lower(), "-f", "..", "-l"], shell=True, stdout=None)
    else:
        utils.shell_command("antab_editor.py", ["-e", exp.expname.lower(), "-p", "1", "-f", ".."], shell=True, stdout=None)
    
    if len(missing_antabs := [a.name for a in exp.antennas if not a.antabfsfile]) > 0:
        rprint(f"[red]Note that you are missing ANTAB files from: {', '.join(missing_antabs)}[/red]")

    os.chdir(original_cwd)
    return None


def create_uvflg(exp) -> Optional[bool]:
    """Produces the combined uvflg file containing the full flagging from all telescopes.
    """
    if len(glob.glob(str(exp.dirs.pipe_temp / "*.uvflg"))) > 0:
        logger.info("uvflg files already created. Skipping.")
        return True

    if len(glob.glob(str(exp.dirs.pipe_temp / "*.log"))) == 0:
        logger.error("No log files found in the temp directory.")
        rprint("[bold red]ERROR:[/bold red] [red]No log files found in the temp directory.[/red]")
        return False
    original_cwd = os.getcwd()
    os.chdir(exp.dirs.pipe_temp)
    utils.shell_command("uvflgall.sh")

    # Check which observed antennas are missing .uvflgfs files and supplement
    # them with a-priori flagging from the experiment .flag file (from vlbeer).
    uvflgfs_files = glob.glob("*.uvflgfs")
    antennas_with_uvflgfs = {
        Path(f).stem.replace(exp.expname.lower(), '').upper()
        for f in uvflgfs_files
    }
    missing_antennas = {a.upper() for a in exp.antennas.observed} - antennas_with_uvflgfs
    if missing_antennas:
        logger.info(f"Antennas missing .uvflgfs files: {', '.join(sorted(missing_antennas))}")
        flag_files = glob.glob(f"{exp.expname.lower()}*.flag")
        if flag_files:
            flag_file = Path(flag_files[0])
            with open(flag_file, 'r') as fh:
                flag_lines = fh.readlines()
            for ant in sorted(missing_antennas):
                ant_lines = [l for l in flag_lines if f"antenna='{ant}'" in l]
                if ant_lines:
                    uvflgfs_out = Path(f"{exp.expname.lower()}{ant.lower()}.uvflgfs")
                    with open(uvflgfs_out, 'w') as fh:
                        fh.write(f"! A-priori flagging for {ant} from {flag_file.name}\n")
                        fh.write("opcode='FLAG'\n")
                        fh.write("dtimrang = 1   timeoff=0\n")
                        fh.writelines(ant_lines)
                    logger.info(f"Created {uvflgfs_out.name} from {flag_file.name} for {ant}")
                else:
                    logger.debug(f"No flagging entries found for {ant} in {flag_file.name}")
        else:
            rprint(f"[yellow]Antennas {', '.join(sorted(missing_antennas))} are missing .uvflgfs files "
                   f"and no .flag file was found in the temp directory.[/yellow]")
            logger.warning(f"Missing .uvflgfs for {', '.join(sorted(missing_antennas))} "
                           "and no .flag file available")

    utils.shell_command("cat", ["*uvflgfs", ">", f"{exp.expname.lower()}.uvflg"])
    if len(pipepass := [apass.pipeline for apass in exp.correlator_passes if apass.pipeline]) > 1:
        for p in range(1, len(pipepass) + 1):
            shutil.copy(f"{exp.expname.lower()}.uvflg",
                        f"{exp.expname.lower()}_{p}.uvflg")

    os.chdir(original_cwd)
    return True


def create_input_file(exp) -> bool:
    """Copies the template of an input file for the EVN Pipeline
    and modifies the standard parameters.
    """
    if len(glob.glob(str(exp.dirs.pipe_in / "*.antab"))) == 0:
        for antabfile in exp.dirs.pipe_temp.glob("*.antab"):
            shutil.copy(antabfile, exp.dirs.pipe_in / antabfile.name)

    if len(glob.glob(str(exp.dirs.pipe_in / "*.uvflg"))) == 0:
        for uvflgfile in exp.dirs.pipe_temp.glob("*.uvflg"):
            shutil.copy(uvflgfile, exp.dirs.pipe_in / uvflgfile.name)

    original_cwd = os.getcwd()
    os.chdir(exp.dirs.pipe_in)
    if len(pipepasses := [apass.pipeline for apass in exp.correlator_passes if apass.pipeline]) > 1:
        if Path(f"{exp.expname.lower()}.antab").exists():
            for p in range(1, len(pipepasses) + 1):
                shutil.copy(f"{exp.expname.lower()}.antab", f"{exp.expname.lower()}_{p}.antab")

        for p in range(1, len(pipepasses) + 1):
            shutil.copy(f"{exp.expname.lower()}.uvflg", f"{exp.expname.lower()}_{p}.uvflg")

    # Copy and modify the pipeline input template
    template_path = resources.files("evn_postprocess.templates").joinpath("pipeline.inp.txt.template")
    pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
    
    for i, apass in enumerate(pipepasses, 1):
        if len(pipepasses) > 1:
            inp_filename = Path(f"{exp.expname.lower()}_{i}.inp.txt")
        else:
            inp_filename = Path(f"{exp.expname.lower()}.inp.txt")
        
        if inp_filename.exists():
            continue
            
        with open(template_path, 'r') as f:
            template_content = f.read()
        
        replacements = {
            '{expname}': exp.expname.lower() if len(pipepasses) == 1 else f"{exp.expname.lower()}_{i}",
            '{userno}': subprocess.run(['aips_userno.py', exp.supsci.lower()], 
                                       capture_output=True, text=True).stdout.strip() or '100',
            '{refant}': exp.refant[0] if len(exp.refant) > 0 else '',
            '{plotref}': exp.refant[0],
            '{bpass}': ', '.join(apass.sources.fringefinder),
            '{dophaseref}': '' if apass.sources.calibrator else '#',
            # '{phaseref}': ', '.join(apass.sources.calibrator),
            # '{target}': ', '.join(apass.sources.target),
            '{target}': ', '.join(apass.sources.target),
            '{phaseref}': ', '.join([apass.sources.calibrator_for_target(tgt) for tgt in apass.sources.target]) if apass.sources.calibrator else '',
            '{dosolint}': '#' if apass.sources.calibrator else '',
            '{solint}': '2',
            '{doprimarybeam}': '1' if exp.multi_phase_center else '-1',
            '{setup_station}': exp.refant[0],
            '{do_all_sources}': '' if exp.multi_phase_center else '#',
            '{all_sources}': ', '.join(set(apass.sources.target + apass.sources.fringefinder + apass.sources.calibrator)) if exp.multi_phase_center else '',
            }
        
        for placeholder, value in replacements.items():
            logger.debug(f"Pipeline input file - replaced {placeholder} with {value}")
            template_content = template_content.replace(placeholder, value)
        
        with open(inp_filename, 'w') as f:
            f.write(template_content)
        
        logger.debug(f"Created pipeline input file: {inp_filename}")

    os.chdir(original_cwd)
    return True


def run_pipeline(exp) -> bool:
    """Runs the EVN Pipeline
    """
    try:
        logger.debug('# Running the pipeline...', True)
        original_cwd = os.getcwd()
        
        if not exp.dirs.pipe_in.exists():
            logger.error(f"Pipeline input directory does not exist: {exp.dirs.pipe_in}")
            return False
            
        os.environ['PIPEFITS'] = str(original_cwd)
        os.chdir(exp.dirs.pipe_in)
        pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
        
        if not pipepasses:
            logger.error("No pipeline passes found")
            return False
            
        logger.info(f"Setting the PIPEFITS environment variable to {os.environ.get('PIPEFITS')}")
        if len(pipepasses) > 1:
            with ProcessPoolExecutor() as executor:
                futures = [executor.submit(utils.shell_command, "EVN.py", [f"{exp.expname.lower()}_{i}.inp.txt"], stdout=None) 
                           for i in range(1, len(pipepasses) + 1)]
                for i, future in enumerate(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Pipeline pass {i+1} failed: {e}")
                        traceback.print_exc()
                        return False
        else:
            utils.shell_command("EVN.py", [f"{exp.expname.lower()}.inp.txt"], stdout=None) #subprocess.PIPE)

        os.chdir(original_cwd)
        return True
    except Exception as e:
        logger.error(f"Unexpected error running pipeline: {e}")
        traceback.print_exc()
        try:
            os.chdir(original_cwd)
        except OSError:
            pass
        return False


def comment_tasav_files(exp) -> bool:
    """Creates the comment and tasav files after the EVN Pipeline has run.
    """
    try:
        # Check if files already exist
        comment_files = list(exp.dirs.pipe_out.glob(f"{exp.expname.lower()}*.comment"))
        tasav_files = list(exp.dirs.pipe_in.glob(f"{exp.expname.lower()}*.tasav.txt"))

        if comment_files and tasav_files:
            logger.debug("Comment and tasav files already exist. Skipping.")
            return True

        if not exp.dirs.pipe_in.exists():
            logger.error(f"Pipeline input directory does not exist: {exp.dirs.pipe_in}")
            return False

        pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]

        if not pipepasses:
            logger.error("No pipeline passes found")
            return False

        if len(pipepasses) > 1:
            for p, ppass in enumerate(pipepasses, start=1):
                if not ppass.freqsetup:
                    logger.warning(f"No frequency setup for pipeline pass {p}")
                    continue
                is_line = ppass.freqsetup.channels >= 512
                comment_tasav.create_comment_and_tasav(exp, f"{exp.expname.lower()}_{p}", is_line)
        else:
            if not exp.correlator_passes[0].freqsetup:
                logger.error("No frequency setup available")
                return False
            is_line = exp.correlator_passes[0].freqsetup.channels >= 512
            comment_tasav.create_comment_and_tasav(exp, exp.expname.lower(), is_line)

        return True
    except Exception as e:
        logger.error(f"Unexpected error creating comment/tasav files: {e}")
        traceback.print_exc()
        return False


def pipeline_feedback(exp) -> bool:
    """Runs the feedback.pl script after the EVN Pipeline has run.
    """
    try:
        original_cwd = os.getcwd()
        os.chdir(exp.dirs.pipe_out)
        pipepasses = [apass for apass in exp.correlator_passes if apass.pipeline]
        sources_str = f"'{' '.join([s.name for s in exp.sources])}'"
        if len(pipepasses) > 1:
            for p in range(1, len(pipepasses) + 1):
                utils.shell_command("feedback.pl",
                                    ["-exp", f"{exp.expname.lower()}_{p}", "-jss", exp.supsci, "-source", sources_str],
                                    stdout=None)
        else:
            utils.shell_command("feedback.pl",
                                ["-exp", exp.expname.lower(), "-jss", exp.supsci, "-source", sources_str],
                                stdout=None)
        return True
    finally:
        os.chdir(original_cwd)


def archive(exp) -> bool:
    """Archives the EVN Pipeline results.
    """
    try:
        original_cwd = os.getcwd()
        for folder in (exp.dirs.pipe_in, exp.dirs.pipe_out):
            os.chdir(folder)
            utils.shell_command("archive.pl", ["-pipe", "-e", f"{exp.expname.upper()}_{exp.obsdate.strftime('%y%m%d')}"], stdout=None)
            os.chdir(original_cwd)
    finally:
        os.chdir(original_cwd)

    return True


# Here there should be a dialog about checking pipeline results, do them manually...

def ampcal(exp) -> bool:
    """Runs the ampcal.sh script to incorporate the gain corrections into the Grafana database.
    """
    original_cwd = os.getcwd()
    os.chdir(exp.dirs.pipe_out)
    utils.shell_command("ampcal.sh")
    os.chdir(original_cwd)
    return True

