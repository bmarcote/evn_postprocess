"""Workflow functions for post-processing EVN experiments.

These functions are called by the Snakemake workflow and wrap the functionality
from process.py, pipeline.py, and pre.py modules.
"""
import sys
import json
import glob
import shutil
from datetime import datetime as dt
from pathlib import Path
from dataclasses import dataclass
from loguru import logger
from rich import print as rprint
from astropy import units as u
from astropy import coordinates as coord
from . import experiment
from . import io
from . import process
from . import pipeline
from . import lisfiles
from . import dialog


@dataclass
class Task(object):
    """Executes the command (which must be a Python function), that has the associated doc
    string for help.
    """
    name: str
    command: str
    doc: str
    done: bool = False

    def to_dict(self) -> dict:
        return {'name': self.name, 'command': self.command, 'doc': self.doc, 'done': self.done}

    @classmethod
    def from_dict(cls, data: dict) -> 'Task':
        return cls(name=data['name'], command=data['command'], doc=data['doc'], done=data.get('done', False))


_WORKFLOW_STEPS = [Task('initialize', 'initialize_experiment',
                        "Creates the directory structure to post-process the experiment. "
                        "Checks that the necessary servers are configured. Retrieves the observing date and "
                        "e-EVN run (if applicable) from the MASTER_PROJECT.LIS file. Retrieves the observing "
                        "files (.key, .sum) from vlbeer, and reads the .jexp files related to the project. "
                        "Verifies if the post-processing has already run before and recovers it."),
                   Task('lisfiles', 'retrieve_lisfiles', "Creates the .lis files in ccs and retrieves them. "
                        "Processes the files to this experiment."),
                   Task('checklis', 'check_lisfiles', "Verifies that the .lis files seem to be fine, "
                        "and sets up the correlator passes to be processed from them."),
                   Task('j2ms2', 'create_msfile', "Creates MS files from .lis files using j2ms2. "
                        "It also retrieves some stats from the visibilities, and creates the notes.md file."),
                   Task('standardplots', 'create_standardplots', "Creates the standard plots from the "
                        "required MS file."),
                   Task('msops', 'msops', "Applies MS operations including weight flagging, polswap, "
                        "and 1-bit scaling."),
                   Task('polconvert', 'polconvert', 'Runs polConvert on all available MS files'),
                   Task('standardplots2', 'msops_post', "Re-runs the standard plots after all msops "
                        "have been performed"),
                   Task('antab', 'antfiles', "Retrieves the .antabfs and .log files from vlbeer. Creates "
                        "the .antab (requires graphical interaction via antab_editor), and the .uvflg file."),
                   Task('pipeline', 'run_pipeline', "Prepares the input file for the EVN Pypeline and runs it."),
                   Task('post_pipe', 'pipeline_diagnostics', 'Runs diagnostics on the pipeline outputs.'),
                   Task('final_data', 'pre_archive', "Prepares the experiment for archiving. Attaches the Tsys "
                        "information to the FITS-IDI files."),
                   Task('archive', 'archive', "Sets the credentials, protechs the files, and archives the "
                        "experiment. In case of an NME, it will prepare the .tex file for the NME feedback.")]


def create_folder_structure() -> experiment.Dirs:
    """Creates the folder structure required for post-processing.

    Returns:
        Iterable[Path]: List of created folders.
    """
    folders = {k: Path(v) for k, v in {'logs': "logs", # 'data': "data", 'results': "results",
                                    #    'diagnostics': "diagnostics",
                                       'pipeline': "pipeline", 'pipe_in': "pipeline/in", 'pipe_out': "pipeline/out",
                                       'pipe_temp': "antenna_files"}.items()}
    for folder in folders.values():
        if not folder.exists():
            Path(folder).mkdir(parents=True, exist_ok=True)
            logger.info(f"Created folder {folder}")
        else:
            logger.debug(f"Folder {folder} already exists. Skipped creation.")

    return experiment.Dirs(**folders)


def initialize_experiment(expname: str, supsci: str) -> experiment.Experiment:
    """Initializes an experiment object with all the relevant metadata, obtained from MASTER_PROJECTS.LIS.

    Args:
        expname (str): Experiment name.
        supsci (str): Support scientist name assigned to this experiment.

    Returns:
        bool: True if experiment was initialized successfully.
    """
    logger.debug(f"Initializing experiment {expname}")
    try:
        servers = experiment.retrieve_servers()
    except FileNotFoundError:
        logger.error("Missing configuration file computers.toml")
        rprint("[red]Expected to be found at ~jops/.config/evn/computers.toml, "
               "or in your local .config directory.[/red]")
        sys.exit(1)

    #if experiment.Experiment.exists(expname):
    #    logger.debug(f"Recovering previously-stored experiment {expname} from file")
    #    return experiment.Experiment.load(expname)

    try:
        obsdate, eEVNname = io.parse_masterprojects(expname, servers['master_projects'])
    except ValueError:
        logger.error("[bold red]The assumed eperiment code {args.expname} "
                     "is not recognized.[/bold red]")
        rprint("[red]Run the program from the experiment folder in /data/exp or use --expname[/red]")
        rprint("[red]Or at least it was not found in MASTER_PROJECTS.LIS[/red]")
        sys.exit(1)

    exp = experiment.Experiment(expname, dt.strptime(obsdate, "%y%m%d").date(), supsci,
                                create_folder_structure(), eEVNname)
    try:
        io.get_init_files(exp, servers)
    except ValueError:
        logger.error("Could not retrieve init files from this experiment (vox/vix, piletter, or expsum)")
        sys.exit(1)

    io.get_vlbeer_sched_files(exp.expname if exp.eEVNname is None else exp.eEVNname,
                              exp.obsdate, servers['vlbeer'])
    exp.get_info_from_vex()
    jexp_info = io.get_jexp_info(exp.expname, servers['jexp'])
    assert jexp_info['piname'] is not None, "piname is None"
    assert jexp_info['pimail'] is not None, "pimail is None"
    exp.pi.append(experiment.PI(jexp_info['piname'], jexp_info['pimail']))
    if jexp_info['coname'] is not None:
        assert jexp_info['coimail'] is not None, "coimail is None"
        exp.pi.append(experiment.PI(jexp_info['coname'], jexp_info['coimail']))

    assert jexp_info['schedsrc'] is not None, "No source information supplied in the jexp file"
    for src in jexp_info['schedsrc'].split(','):
        src_name, src_type_str, src_protected = src.strip().replace('(', '').replace(')', '').replace('|', ' ').split()
        match src_type_str.strip():
            case 'T':
                src_type = experiment.SourceType.target
            case 'R':
                src_type = experiment.SourceType.calibrator
            case 'C':
                src_type = experiment.SourceType.fringefinder
            case 'F':
                src_type = experiment.SourceType.fringefinder
            case _:
                src_type = experiment.SourceType.other

        # Create source if it doesn't exist, then update its type and protection
        if src_name not in exp.sources.names:
            # Use placeholder coordinates (0,0) - will be updated when MS data is processed
            placeholder_coords = coord.SkyCoord(ra=0*u.deg, dec=0*u.deg, frame='icrs')
            exp.sources.append(experiment.Source(name=src_name, coordinates=placeholder_coords, 
                                               type=src_type, protected=False))
        else:
            exp.sources[src_name].type = src_type
            exp.sources[src_name].protected = src_protected.strip() == 'X'

    # TODO: implement this one!
    # io.get_station_feedback_info(exp.expname, servers['station_feedback'])

    exp.store()
    return exp


def retrieve_lisfiles(exp: experiment.Experiment) -> bool:
    """Retrieves and processes .lis files from ccs.

    Args:
        exp (experiment.Experiment): Experiment object.
        server (experiment.Server): Server object with ccs connection information.

    Returns:
        bool: True if lis files were retrieved and processed successfully.
    """
    try:
        if len(glob.glob(f"{exp.expname.lower()}*.lis")) > 0:
            logger.debug(".lis files already exist. Skipping retrieval.")
            return True

        # Check each step individually for better error reporting
        if not lisfiles.create_lis_files(exp):
            logger.error("Failed to create .lis files")
            return False
            
        if not lisfiles.get_lis_files(exp):
            logger.error("Failed to retrieve .lis files")
            return False
            
        if not lisfiles.get_passes_from_lisfiles(exp):
            logger.error("Failed to extract passes from .lis files")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Unexpected error retrieving .lis files: {e}")
        return False


def check_lisfiles(exp: experiment.Experiment) -> bool:
    """Checks and sets up correlator passes from .lis files.

    Args:
        exp (experiment.Experiment): Experiment object with correlator passes.

    Returns:
        bool: True if all lis files are valid.
    """
    try:
        if not exp.correlator_passes:
            logger.error("No correlator passes found. Check .lis file retrieval.")
            return False
            
        if all(p.msfile.exists() for p in exp.correlator_passes):
            logger.debug("MS files already exist. Skipping checklis.")
            return True

        if not lisfiles.check_lisfiles(exp):
            # TODO: In case of e-EVN runs, it needs to do it!
            logger.error("Issues found in .lis files. Please check the files.")
            return False

        return True
    except Exception as e:
        logger.error(f"Unexpected error checking .lis files: {e}")
        return False


def create_msfile(exp: experiment.Experiment) -> bool:
    """Creates MS files from .lis files using j2ms2.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if MS files were created successfully.
    """
    try:
        # Skip if metadata already loaded (check before replacing correlator_passes)
        if exp.correlator_passes and exp.correlator_passes[0].freqsetup is not None:
            logger.debug("MS metadata already loaded. Skipping MS creation and extraction.")
            return True

        if not exp.correlator_passes:
            logger.error("No correlator passes available for MS creation")
            return False

        # Re-doing again in case the lis files were updated externally
        lisfiles.get_passes_from_lisfiles(exp)

        if not all(p.msfile.exists() for p in exp.correlator_passes):
            if not process.getdata(exp):
                logger.error("Failed to get data for MS creation")
                return False

            if not process.j2ms2(exp):
                logger.error("Failed to run j2ms2")
                return False 

            process.update_ms_expname(exp)
        else:
            logger.debug("MS files already exist. Skipping creation.")

        return process.get_metadata_from_ms(exp)
    except Exception as e:
        logger.error(f"Unexpected error creating MS files: {e}")
        return False


def create_standardplots(exp: experiment.Experiment, do_weights: bool = True) -> bool:
    """Creates standardplots from MS files.

    Args:
        exp (experiment.Experiment): Experiment object.
        do_weights (bool): Whether to include weight plots. Default True.

    Returns:
        bool: True if standardplots were created successfully.
    """
    # If it fails, try to go with other sources, refants, etc
    if len(glob.glob("*.ps")) > 0:
        logger.debug("Standardplots already run. Skipping.")
        return True

    # Show scan overview before opening standardplots
    gui = dialog.Terminal()
    if not gui.show_scan_overview(exp):
        logger.info("User cancelled after scan overview.")
        return False
    
    return process.standardplots(exp, do_weights=do_weights) & process.open_standardplot_files(exp)


def msops(exp: experiment.Experiment) -> bool:
    """Applies MS operations including weight flagging, polswap, and 1-bit scaling.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if all MS operations completed successfully.
    """
    if all(len(glob.glob(f"{p.fitsidifile}*")) > 0 for p in exp.correlator_passes):
        logger.debug("FITS IDI files already exist. Skipping creation.")
        return True

    gui = dialog.Terminal()
    if not gui.askMSoperations(exp):
        return False
    
    exp.store()
    return process.flag_weights(exp) & process.ysfocus(exp) & process.polswap(exp) & process.onebit(exp) & process.tconvert(exp)


def polconvert(exp: experiment.Experiment) -> bool:
    """Handles PolConvert if needed.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if PolConvert completed successfully, False if manual intervention needed.
    """
    if not exp.antennas.polconvert:
        logger.debug("No antennas require PolConvert. Skipping.")
        return True

    if all(len(glob.glob(f"{p.fitsidifile}*")) > 0 for p in exp.correlator_passes) and Path('ori_idi').exists():
        logger.debug("FITS IDI files already exist. Skipping creation.")
        return True

    process.prepare_polconvert(exp)
    while (result := process.polconvert(exp)) is None:
        rprint("[bold]Running PolConvert[/bold]")

    if not result:
        rprint("[red]PolConvert doesn't look to have reached a good solution. Try to run it manually[/red]")
        sys.exit(1)

    return process.post_post_polconvert(exp)


def msops_post(exp: experiment.Experiment) -> bool:
    """Applies MS operations including weight flagging, polswap, and 1-bit scaling.

    Args:
        exp (experiment.Experiment): Experiment object.

    Returns:
        bool: True if all MS operations completed successfully.
    """
    return create_standardplots(exp, do_weights=False)


def antfiles(exp: experiment.Experiment) -> bool:
    """Retrieves antenna files from vlbeer for pipeline processing.

    Args:
        expobj_file: Path to experiment JSON file
    """
    if len(glob.glob("pipeline/in/*.antab")) > 0:
        logger.debug("Antenna ANTAB files already exist. Skipping.")
        return True

    if len(glob.glob("pipeline/temp/*.antab")) > 0:
        for afile in Path(exp.dirs.pipe_temp).glob("*.antab"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name)

        logger.debug("Antenna ANTAB files already created. Copied to the pipeline input directory.")
        return True

    if (exp.eEVNname is None) or (exp.expname == exp.eEVNname):
        if not pipeline.create_uvflg(exp):
            logger.error("uvflg creation needs manual intervention.")
            rprint("[bold red]STOPPED PROCESS:[/bold red] [red]uvflg creation needs manual intervention.[/red]")
            return False

        pipeline.get_files_from_vlbeer(exp, experiment.retrieve_servers()['vlbeer'])
        if any(s.lower() in ('br', 'kp', 'la', 'yy', 'mk') for s in exp.antennas.names):
            pipeline.get_vlba_antab(exp)

        if not pipeline.run_antab_editor(exp):  # TODO: use the correct codes if eEVN or line
            rprint("[bold yellow]STOPPED PROCESS:[/bold yellow] [yellow]antab_editor needs manual intervention.[/yellow]")
            return False

        for afile in Path(exp.dirs.pipe_temp).glob("*.antab"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name)

        for afile in Path(exp.dirs.pipe_temp).glob("*.uvflg"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name)
    else:
        eEVNpath = Path(str(experiment.retrieve_servers()['eee'].path).format(expname=exp.eEVNname)) \
                    / "pipeline" / "in"
        if not (antabfiles := eEVNpath.glob("*.antab")):
            logger.error(f"Create the antab/uvflg files from {exp.eEVNname} before continue here.")
            return False

        for afile in antabfiles:
            shutil.copy(afile, exp.dirs.pipe_in / afile.name.replace(exp.eEVNname.lower(), exp.expname.lower()))

        for afile in eEVNpath.glob("*.uvflg"):
            shutil.copy(afile, exp.dirs.pipe_in / afile.name.replace(exp.eEVNname.lower(), exp.expname.lower()))

    return True


def run_pipeline(exp: experiment.Experiment) -> bool:
    """Prepares input files for EVN Pipeline.

    Args:
        expobj_file: Path to experiment JSON file
    """
    return pipeline.create_input_file(exp) & pipeline.run_pipeline(exp)


def pipeline_diagnostics(exp: experiment.Experiment) -> bool:
    """Creates diagnostic files after pipeline completion.

    Args:
        expobj_file: Path to experiment JSON file
    """
    return pipeline.comment_tasav_files(exp) & pipeline.pipeline_feedback(exp)


def pre_archive(exp: experiment.Experiment) -> bool:
    """Appends Tsys/GC information to FITS-IDI files.

    Args:
        expobj_file: Path to experiment JSON file
    """
    return process.append_antab(exp)


def archive(exp: experiment.Experiment) -> bool:
    """Archives experiment files to the EVN archive.

    Args:
        expobj_file: Path to experiment JSON file
    """
    return process.set_credentials(exp) & process.protect_experiment_files(exp) & process.print_exp(exp, display_in_terminal=False) & \
           process.archive(exp) & process.send_letters(exp) & process.antenna_feedback(exp) & process.nme_report(exp)


def list_tasks(expname: str, print_docs: bool = False):
    """Lists all tasks avaliable to be executed.

    Args
        exp : experiment.Experiment | None
            The experiment associatd to this post-processing. If not provided, it will just list the general tasks.
        print_docs : bool = True
            In addition to list the tasks, it will also print the documentation associated to each one.
    """
    rprint(f"\n\n[bold]Post-processing of {expname}:[/bold]")
    exp = experiment.Experiment.load(expname)
    if exp:
        steps = [Task.from_dict(s) for s in exp.steps]
    else:
        steps = _WORKFLOW_STEPS
    
    for s in steps:
        rprint(f"{'🟢' if s.done else '🔴'}"
               f" [bold {'green' if s.done else 'red'}]{s.name}[/bold {'green' if s.done else 'red'}]\n" + \
                      (f"   [dim]{s.doc}[/dim]" if print_docs else ""))


def run_isolated_task(task_name: str, expname: str | None = None):
    """Runs a single task independently.
    Note that it may require that all previos steps to have run, as some metadata from them may be
    required to run the desired task.

    Args:
        expname : str
            The name of the experiment to process (case insensitive).
        task_name : str | None
            Name of the task to run (run the help to know the available tasks).
            If not provided, assumes that the name of the current directory is the experiment name.
    """
    try:
        exp = experiment.Experiment.load(expname)
    except FileNotFoundError:
        rprint(f"[bold red]Could not find the stored information for {expname if expname is not None else Path().name}"
               "[/bold red].\n[red]Maybe the experiment was never initialized?[/red]")
        sys.exit(1)
    except (json.JSONDecodeError, KeyError) as e:
        rprint(f"[bold red]Error loading experiment data: {e}[/bold red]")
        rprint("[red]The experiment file may be corrupted. Consider reinitializing the experiment.[/red]")
        sys.exit(1)

    if task_name not in globals():
        rprint(f"[red]Task '{task_name}' not found.[/red]")
        available_tasks = [name for name in globals() if callable(globals()[name]) and not name.startswith('_')]
        rprint(f"[dim]Available tasks: {', '.join(available_tasks)}[/dim]")
        sys.exit(1)

    try:
        return globals()[task_name](exp)
    except Exception as e:
        rprint(f"[red]Error running task '{task_name}': {e}[/red]")
        sys.exit(1)


def validate_steps(from_step: str, to_step: str | None = None) -> tuple[bool, str]:
    """Validates that the given step names exist and are in the correct order.

    Args:
        from_step: Name of the starting step.
        to_step: Name of the ending step (optional).

    Returns:
        Tuple of (is_valid, error_message). If valid, error_message is empty.
    """
    if not _WORKFLOW_STEPS:
        return False, "No workflow steps defined"
    
    step_names = [s.name for s in _WORKFLOW_STEPS]

    if not from_step:
        return False, "Starting step cannot be empty"
    
    if from_step not in step_names:
        return False, f"Step '{from_step}' not found. Available steps: {', '.join(step_names)}"

    if to_step is not None:
        if to_step not in step_names:
            return False, f"Step '{to_step}' not found. Available steps: {', '.join(step_names)}"

        from_idx = step_names.index(from_step)
        to_idx = step_names.index(to_step)
        if from_idx > to_idx:
            return False, f"Step '{from_step}' comes after '{to_step}'. Order should be reversed."

    return True, ""


def run_workflow(exp: experiment.Experiment, archive: bool = True, debug: bool = False,
                 from_step: str | None = None, to_step: str | None = None):
    """Run the workflow for the given experiment.
    
    Args:
        exp: The experiment object.
        archive: Whether to include the archive step.
        debug: Whether to enable debug logging.
        from_step: Starting step name (optional).
        to_step: Ending step name (optional).
        
    Returns:
        bool: True if workflow completed successfully, False otherwise.
    """
    try:
        if debug:
            # Debug mode: show all information (time, level, module, function, line)
            debug_format = (
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            )
            logger.add(exp.dirs.logs / 'post_process.log', colorize=True,
                       level="DEBUG", backtrace=True, diagnose=True, format=debug_format)
        else:
            # Info mode: show only time (HH:MM:SS) and level in dim color, then message on new line
            info_format = (
                "<dim>{time:HH:mm:ss}</dim> <dim>{level: <8}</dim>\n"
                "{message}"
            )
            logger.add(exp.dirs.logs / 'post_process.log', colorize=True,
                       level="INFO", backtrace=False, diagnose=False, format=info_format)
    except (OSError, PermissionError) as e:
        rprint(f"[yellow]Warning: Could not create log file: {e}[/yellow]")

    # TODO: put this in the commands.log file
    #logger.info(f"\n\n\n{'#'*37}\n# Post-processing of {exp.expname} ({exp.obsdate}).\n"
    #            f"# Running on {dt.today().strftime('%d %b %Y %H:%M')} by {exp.supsci}.\n"
    #            f"Using evn_postprocess version {version('evn_postprocess')}.")

    if not archive:
        logger.debug("The data will not be stored in the EVN archive.")

    exp.steps = [s for s in _WORKFLOW_STEPS if (archive or (s.name != 'archive')) and (s.name != 'initialize')]
    exp.store()

    # Filter steps based on from_step and to_step
    if from_step is not None:
        step_names = [s.name for s in exp.steps]
        try:
            from_idx = step_names.index(from_step)
            to_idx = step_names.index(to_step) + 1 if to_step is not None else len(step_names)
            steps_to_run = exp.steps[from_idx:to_idx]
        except ValueError as e:
            logger.error(f"Error filtering steps: {e}")
            return False
    else:
        steps_to_run = exp.steps

    if not steps_to_run:
        rprint("[yellow]No steps to run.[/yellow]")
        return True

    for step in steps_to_run:
        logger.info(f"Running step: {step.name}")
        try:
            if step.command not in globals():
                logger.error(f"Command '{step.command}' not found for step '{step.name}'")
                return False
                
            if not globals()[step.command](exp):
                logger.error(f"Step {step.name} failed.")
                rprint(f"[red]Step {step.name} failed. Check logs for details.[/red]")
                return False

            step.done = True
            exp.store()
            logger.info(f"Step {step.name} completed successfully")
        except Exception as e:
            logger.error(f"Unexpected error in step {step.name}: {e}")
            rprint(f"[red]Unexpected error in step {step.name}: {e}[/red]")
            return False

    rprint(f"[italic green]The processing of {exp.expname} seems to have finalized properly.[/italic green]")
    return True
