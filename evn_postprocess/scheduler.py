
from . import experiment
from . import environment as env
from . import process_ccs as ccs
from . import process_eee as eee
from . import process_pipe as pipe


# Create processing_log?  log dir.
# - In the case of e-EVN, it should be run until the ANTAB steps in all the other experiments.


def dispatcher(exp: experiment.Experiment, functions):
    """Runs all functions one-after-the-next-one.
    All functions are expected to only require the {exp} parameter, and to return a bool
    if they run sucessfully or not. It one fails, then the dispatcher will stop, storing the
    current {exp}.
    """
    try:
        for a_step in functions:
            if not a_step(exp):
                raise RuntimeError(f"The following function did not run properly for {exp.expname}: {a_step.__name__}.")
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
    output = dispatcher(exp, (ccs.create_lis_files, ccs.get_lis_files, \
                              eee.get_passes_from_lisfiles))
    exp.store()
    return output


def first_manual_check(exp: experiment.Experiment):
    """It is only executed for complex experiments: those with
    """
    output = env.check_lisfiles(exp)
    if not output:
        temp = 'file','seems' if len(exp.correlator_passes) == 1 else 'files','seem'
        print(f"\n\n{'#'*10}\n# Stopping here...")
        print(f"The .lis {temp[0]} for {exp.expname} {temp[1]} to have issues to " \
              f"be solved manually.\n{'#'*10}\n")

    if exp.eEVNname is not None:
        print(f"{exp.expname} is part of an e-EVN run. Please edit manually the lis file now.")
        exp.last_step = 'checklis'
        output = False

    exp.store()
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
        #TODO: if polconvert runs, then create again the MS and run standardplots
        pass
    exp.last_step = 'tconvert'
    exp.store()
    return output


def archive(exp: experiment.Experiment):
    output = eee.archive(exp)
    if output:
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


def pipeline(exp: experiment.Experiment):
    # TODO: authentification for pipeline results
    output = dispatcher(exp, (pipe.run_pipeline,))
    exp.store()
    output = dispatcher(exp, (pipe.comment_tasav_files, pipe.pipeline_feedback, pipe.archive))
    exp.last_step = 'pipeline'
    exp.store()
    # This is to force the manual check of the pipeline results
    return False # output



def after_pipeline(exp: experiment.Experiment):
    pipe.ampcal(exp)
    exp.last_step = 'post_pipeline'
    exp.store()
    return True








