

## To Implement

[ ] Read the expsum to retrieve the sources, type of sources and if they need to be protected.
This will be stored under the Store() object, and can be reminded to the SuSci when the auth needs to be performed.
[ ] Retrieve the sources and allow the SupSci to mark them as ff, pcal, target, check, other. Etc.


## Bugs

[X] While copying the polconvert_inputs.ini, error (Errno 18: invalid-cross-device link)
[ ] In the pipeline part, for multi-pass experiments it needs to name the files as {exp}\_N. Retrieve from FITS IDI name?
[ ] When doing archive_pipe. Before it seems that moved directly from comment_tasav to archive, without feedback. 
[ ] When doing archive_pipe it crashes with:
    ```
    File "/data0/marcote/scripts/evn_postprocess/evn_postprocess/process_pipe.py", line 253, in archive
        cmd, output = env.ssh('jops@jop83', f"{cd} && archive -pipe -e {exp.expname.lower()}_{exp.obsdate}", stdout=None)
    ValueError: too many values to unpack (expected 2)
    ```
    But the previous comment_tasav_file.py works fine. I think it needs the full path to archive.




experiment.py - Experiment
__init__(expanse, support_scientist)
get_setup_from_ms()
parse_expsum()
