
from . import experiment
from . import environment as env
from . import process_css as css
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
                raise RuntimeError(f"The following function did not run properly for {exp.expname}: {a_step}.")
    # except RuntimeError: # Not handled, raised to above
    finally:
        exp.store()
        print('ERROR: Pipeline ending here. Experiment has been correctly stored.')

    return True


def setting_up_environment(exp: experiment.Experiment):
    """Sets up the environment for the post-processing of the experiment.
    This implies to create the
    """
    output = dispatcher(exp, (env.create_all_dirs, env.copy_files, eee.set_credentials_pipelet))
    exp.parse_expsum()
    exp.store()
    return output

# MODIFY THE FOLLOWING FUNCTIONS WITH THE SAME STRUCTURE AS IN THE PREVIOUS ONE


def preparing_lis_files(exp: experiment.Experiment):
    """Checks that the .lis file(s) already exists.
    Otherwise it creates it in ccs and copy it to the experiment folder.
    """
    for a_pass in exp.correlator_passes:
        if not a_pass.lisfile.exists():
            if not ccs.lis_files_in_ccs(exp):
                ccs.create_lis_files(exp)
            ccs.get_lis_files(exp)

    eee.get_passes_from_lisfiles(exp)
    exp.store()
    return True


def first_manual_check(exp: experiment.Experiment):
    """It is only executed for complex experiments: those with
    """
    if not env.check_lisfiles(exp):
        temp = 'file','seems' if len(exp.correlator_passes) == 1 else 'files','seem'
        print(f"{'#'*10}\n# Stopping here...")
        print(f"The list {temp[0]} for {exp.expname} {temp[1]} to have issues to solve manually.\n{'#'*10}\n")
    exp.store()

    return env.check_lisfiles(exp)


def creating_ms(exp: experiment.Experiment):
    """Steps from retrieving the cor files to create the MS and standardplots
    """
    output = dispatcher(exp, (eee.getdata, eee.j2ms2, eee.update_ms_expname, eee.get_metadata_from_ms))
    return output


def standardplots(exp: experiment.Experiment):
    output = dispatcher(exp, (eee.standardplots, eee.open_standardplot_files))
    return output


def ms_operations(exp: experiment.Experiment):
    # DIALOG for
    # - flag weights
    # - polswap
    # - onebit if there is a mention on it
    # - tConvert
    # - polconvert
    output = dispatcher(exp, (eee.ysfocus, eee.polswap, eee.flag_weights, eee.onebit, eee.update_piletter))
    exp.store()
    # To get plots on, specially, ampphase without the drops that have been flagged here:
    eee.standardplots(exp, do_weights=False)
    return output


def tconvert(exp: experiment.Experiment):
    output = dispatcher(exp, (eee.tconvert, eee.polConvert))
    did_polConvert_run = False
    for a_pass in exp.passes:
        if len(a_pass.antennas.polconvert) > 0:
            did_polConvert_run = True
    if did_polConvert_run:
        #TODO: if polconvert runs, then create again the MS and run standardplots
        pass
    exp.store()
    return output


def archive(exp: experiment.Experiment):
    return eee.archive(exp)


def getting_pipeline_files(exp: experiment.Experiment):
    """Retrieves the files that are required to run the EVN Pipeline in the associated experiment
    """
    # THIS MAY ONLY RUN FOR SOME OF THE EXPERIMENTS IN AN E-EVN EXPERIMENT
    output = dispatcher(exp, pipe.get_files_from_vlbeer, pipe.create_uvflg, pipe.run_antab_editor)
    # Here there may be a waiting task for e-EVN experiments until all the others are in.
    # Copy antab file and uvflg to input
    exp.store()
    return output


def pipeline(exp: experiment.Experiment):
    # TODO: authentification for pipeline results
    output = dispatcher(exp, pipe.create_input_file)
    # TODO: Run pipeline
    output = dispatcher(exp, pipe.comment_tasav_files, pipe.pipeline_feedback, pipe.archive)
    exp.store()
    return output



def after_pipeline(exp: experiment.Experiment):
    pipe.ampcal(exp)








