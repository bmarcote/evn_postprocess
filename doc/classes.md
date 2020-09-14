
# Structure of the program


## main.py

*NOTE: Needs to be updated in the code*

'*initialize*'  : [eee.folders],
'showlog'       : [eee.ccs],
'*pi_expsum*'   : [actions.get_pi_from_expsum, actions.get_passes_from_lisfiles],
'j2ms2'         : [eee.getdata, eee.j2ms2, eee.onebit],
'*MSmetadata*'  : [actions.append_freq_setup_from_ms_to_exp],
'standardplots' : [eee.standardplots],
'MSoperations'  : [eee.MSoperations],
'tConvert'      : [eee.tConvert, eee.polConvert],
'archive'       : [eee.letters, eee.archive],
'*pipe_folders*': [pipe.folders],
'prepipeline'   : [pipe.pre_pipeline],
'pipeline'      : [pipe.pipeline],
'postpipeline'  : [pipe.post_pipeline],
'letters'       : [pipe.ampcal, eee.send_letters]



## metadata.py

Credentials
    - username : str
    - password : str
    + __init__(username, password)

SourceType(Enum)
    - target
    - calibrator
    - fringefinder
    - other

Source
    - name : str
    - type : SourceType
    - protected : bool
    + __init__(name, type, protected=False)


Subbands
    - n_subbands : float
    - channels : (n_subbands)-array
    - freqs : (n_subbands X channels)-array (Ref. freqs)
    - bandwidths : (n_subbands)-array
    + __init__(chans, freqs, bandwidths)


CorrelatorPass
    - lisfile : str
    - msfile : str
    - fitsidifile : str
    - pipeline : bool (if this pass should be pipelined)
    - sources : list of Source
    - freqsetup : Subbands
    + __init__(lisfile, msfile, fitsidifile, pipeline=True)
    + freqsetup(channels, frequencies, bandwidths)


Experiment
    - expname : str
    - eEVNname : str or None  (if case of associated to a different e-EVN job name)
    - piname : str or list
    - email : str or list
    - obsdate : str (YYMMDD format)
    - obsdatetime : datetime
    - processdate : datetime
    - timerange : tuple (starttime, endtime)
    - antennas : list of str
    - ref_antennas : list of str
    - onebit_antennas : list of str
    - polswap_antennas : list of str
    - polconvert_antennas : list of str
    - flagged_weights : FlagWeights or None
    - sources : list of Source   (taken from the expsum file)
    - ref_sources : list of str
    - correlator_passes : int
    - passes : list of CorrelatorPass
    - credentials : Credentials
    - existing_piletter : bool  (from before postprocessing)
    - existing_lisfile  : bool  (from before postprocessing)
    - stored_outputs : dict    # key with the name of functions (checklis, flag_weights,..)
    + __init__(expname)
    + add_pass(a_new_pass : CorrelatorPass)
    + set_credentials(username, password)
    + get_setup_from_ms()
    + parse_expsum()


FlagWeight
    - threshold : float
    - percentage : float  [-1 is not set]

## actions.py

~~several functions for IO that will be overwritten~~
+ ask_user(text, valtype=str, accepted_values=None)
+ yes_or_no_question(text)
+ can_continue(text)
~~  ~~

+ scp(originpath, destpath)  --> exec_command, outputcode
+ ssh(computer, commands)  --> exec_command, output
+ shell_command(command, parameters=None, shell=False)  --> exec_command, output
+ remote_file_exists(host, path) --> bool
+ update_lis_file(listfilename, oldexp, newexp)
+ split_lis_cont_line(fulllisfile)
+ update_pipelinable_passes(exp, pipelinable : lis[bool] or dict{lisfile:bool})
+ station_1bit_in_vix(vexfile) --> bool
+ extract_tail_standardplots_output(stdplt_output) --> str
+ archive(flag, experiment, rest_parameters)


+ end_program(exp)


## process_css.py

+ *parse_masterprojects(exp)*
+ get_vixfile(exp)
+ get_expsumfile(exp)
+ *parse_expsumfile(exp)*
+ get_piletter(exp)
+ lis_files_in_ccs(exp) --> bool
+ lis_files_in_local(exp) --> bool
+ create_lis_files(exp)
+ get_lis_files(exp)
+ *get_files(exp)*
+ check_lisfiles(exp)



## process_eee.py

+ *folders(exp, args)*
+ get_passes_from_lisfiles(exp)
+ getdata(exp)
+ j2ms2(exp)
+ onebit(exp)
+ ysfocus(exp)
+ standardplots(exp)
+ open_standardplot_files(exp)
+ polswap(exp, antennas)
+ flag_weights(exp, threshold)
+ ms_operations(exp)
+ update_pilatter(exp, weightthreshold, flaggeddata)
+ tConvert(exp)
+ polConvert(exp)


## process_pipe.py



## dialog.py

Choice(Enum)
    - ok
    - abort
    - repeat

FirstDialogForm



+ warning_dialog(text)  --> bool
+ first_dialog(exp)
+ standardplots_dialog(exp) --> {'choice', 'polswap', 'polconvert', 'flagweight', 'ref_ant', 'cal_sources'}






