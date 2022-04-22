
from . import experiment
from . import environment as env
from . import process_ccs as ccs
from . import process_eee as eee
from . import process_pipe as pipe


# Create processing_log?  log dir.
# - In the case of e-EVN, it should be run until the ANTAB steps in all the other experiments.

class ManualInteractionRequired(Exception):
    pass


def dispatcher(exp: experiment.Experiment, functions):
    """Runs all functions one-after-the-next-one.
    All functions are expected to only require the {exp} parameter, and to return a bool
    if they run sucessfully or not. It one fails, then the dispatcher will stop, storing the
    current {exp}.
    """
    try:
        for a_step in functions:
            if (output := a_step(exp)) is None:
                raise ManualInteractionRequired(f"Stopping for manual intervention at {a_step.__name__}.")
            elif not output:
                raise RuntimeError(f"The function {a_step.__name__} did not run properly for {exp.expname}.")
    # except RuntimeError: # Not handled, raised to above
    finally:
        exp.store()
        # print('ERROR: Pipeline ending here. Experiment has been correctly stored.')

    return True


def setting_up_environment(exp: experiment.Experiment):
    """Sets up the environment for the post-processing of the experiment.
    This implies to create the
    """
    output = dispatcher(exp, (env.create_all_dirs, env.copy_files, eee.set_credentials_pipelet))
    exp.parse_expsum()
    output = dispatcher(exp, (pipe.get_files_from_vlbeer, ))
    exp.store()
    return output


def preparing_lis_files(exp: experiment.Experiment):
    """Checks that the .lis file(s) already exists.
    Otherwise it creates it in ccs and copy it to the experiment folder.
    """
    output = dispatcher(exp, (ccs.create_lis_files, ccs.get_lis_files, eee.get_passes_from_lisfiles))
    exp.store()
    return output


def first_manual_check(exp: experiment.Experiment):
    """It is only executed for complex experiments: those with
    """
    output = env.check_lisfiles(exp)
    if not output:
        temp = 'file', 'seems' if len(exp.correlator_passes) == 1 else 'files', 'seem'
        print(f"\n\n{'#'*10}\n# Stopping here...")
        print(f"The .lis {temp[0]} for {exp.expname} {temp[1]} to have issues to "
              f"be solved manually.\n{'#'*10}\n")
        print("Note that if you change the name of the .lis file, you will need to re-run the step 'lisfile'.")

    if exp.eEVNname is not None:
        print(f"\n\n{exp.expname} is part of an e-EVN run. Please edit manually the lis file now.")
        exp.last_step = 'checklis'
        output = None
        exp.store()
        print("Note that if you change the name of the .lis file, you will need to re-run the step 'lisfile'.")
        raise ManualInteractionRequired('The lis file needs to be manually edited.')

    return output


def creating_ms(exp: experiment.Experiment):
    """Steps from retrieving the cor files to create the MS and standardplots
    """
    output = dispatcher(exp, (eee.getdata, eee.j2ms2, eee.update_ms_expname, eee.get_metadata_from_ms))
    exp.last_step = 'ms'
    exp.store()
    return output


def standardplots(exp: experiment.Experiment):
    output = dispatcher(exp, (eee.standardplots, eee.open_standardplot_files))
    exp.last_step = 'plots'
    exp.store()
    return output


def ms_operations(exp: experiment.Experiment):
    exp.gui.askMSoperations(exp)
    output = dispatcher(exp, (eee.ysfocus, eee.polswap, eee.flag_weights, eee.onebit,
                              eee.update_piletter))
    exp.store()
    # To get plots on, specially, ampphase without the drops that have been flagged here:
    eee.standardplots(exp, do_weights=False)
    exp.last_step = 'msops'
    exp.store()
    return output


def tconvert(exp: experiment.Experiment):
    output = dispatcher(exp, (eee.tconvert, eee.polconvert))
    if len(exp.antennas.polconvert) > 0:
        # TODO: if polconvert runs, then create again the MS and run standardplots
        pass
    exp.last_step = 'tconvert'
    exp.store()
    return output


def post_polconvert(exp: experiment.Experiment):
    output = dispatcher(exp, (eee.post_polconvert, ))
    exp.last_step = 'post_polconvert'
    exp.store()
    return output


def archive(exp: experiment.Experiment):
    output = dispatcher(exp, (eee.archive, ))
    exp.last_step = 'archive'
    exp.store()
    return output


def antab_editor(exp: experiment.Experiment):
    output = dispatcher(exp, (pipe.run_antab_editor,))
    exp.last_step = 'antab'
    exp.store()
    return output


def getting_pipeline_files(exp: experiment.Experiment):
    """Retrieves the files that are required to run the EVN Pipeline in the associated experiment
    """
    # THIS MAY ONLY RUN FOR SOME OF THE EXPERIMENTS IN AN E-EVN EXPERIMENT
    output = dispatcher(exp, (pipe.create_uvflg, pipe.create_input_file))
    # Here there may be a waiting task for e-EVN experiments until all the others are in.
    # Copy antab file and uvflg to input
    exp.last_step = 'pipeinputs'
    exp.store()
    return output


def protect_archive_data(exp: experiment.Experiment):
    """Opens a web browser to the authentification page for EVN experiments
    """
    if len([s.name for s in exp.sources if s.protected]) > 0:
        print("\n\nYou now need to protect the archived data.")
        print("Open http://archive.jive.nl/scripts/pipe/admin.php")
        print(f"And protect the following sources: {', '.join([s.name for s in exp.sources if s.protected])}")
    else:
        print("No sources require protection.")

    return None


def pipeline(exp: experiment.Experiment):
    output = dispatcher(exp, (pipe.run_pipeline,))
    exp.last_step = 'pipeline'
    exp.store()
    return output


def after_pipeline(exp: experiment.Experiment):
    # TODO: authentification for pipeline results
    output = dispatcher(exp, (pipe.comment_tasav_files, pipe.pipeline_feedback, pipe.archive))
    # output = dispatcher(exp, (pipe.comment_tasav_files, pipe.archive))
    exp.last_step = 'postpipe'
    exp.store()
    print('\n\nNow check manually the Pipeline results in the browser.')
    print('You may need to re-run the pipeline if you want to improve the results.')
    print('Re-run me only once you are happy with the final results and you have archived them again.')
    print('And once you have updated the PI letter.')
    return None if output else output


def final_steps(exp: experiment.Experiment):
    output = dispatcher(exp, (eee.append_antab, pipe.ampcal, eee.send_letters, eee.antenna_feedback, eee.nme_report))
    exp.last_step = 'last'
    exp.store()
    return output



