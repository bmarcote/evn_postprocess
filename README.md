# EVN Postprocess Pipeline

*NOTE*: This code is still under heavy development.


Pipeline to run all post-processing of EVN data in a semi-interactive and semi-smart way. This code will operate all steps required from post-correlation at JIVE to user delivering of the data. That is, it runs the steps defined in the _SFXC Post-Correlation Checklist_.

To summarize them, this pipeline will (for a given EVN experiment):
- Select the correct jobs produced by SFXC in *ccs*.
- Build the required .lis file(s) to process the data in *eee*.
- Retrieve the data and create the corresponding Measurement Set(s).
- Produce standard plots.
- Run the required MS operations.
- Create the corresponding FITS IDI files.
- Archive the data to the EVN Archive.
- Generate all required files to be able to run the EVN Pipeline.
- Run the EVN Pipeline.
- Check the EVN Pipeline output and prepare the emails for the PI.
- Finalize the EVN experiment post-processing.





## Bug List



- [ ] Log all command outputs into the log files.
- [ ] Check if for e-EVN experiments it changes the experiment name.
- [ ] Standard plot breaks in the script: ('list has no split').
- [X] Say no to create new lis files but still retrieving the vex file if n/a.
- [ ] If error produced in archive.pl due to Proposal Tool it breaks...
- [X] Output message for archiving too.
- [X] 'Create again the lis file in ccs?' Asks even if it doesn't exist.



## Feature requests

- [ ] When multiple values are requested, do it in a CLI form approach:
      ----------------------
      |____________________|
      |                    |
      | Introduce...:      |
      | [ ] Swap pols      |
      | [ ] Pol convert    |
      | ___ Weight cut     |
      | ...                |
      |                    |
      |                    |
      |                    |
      |                    |
      ----------------------
- [ ] Get flag_weight.py output and store it in the metadata.
- [ ] Modify PI letter to include flag_weight.py output and cut experiment name.
- [ ] Parallelize when multiple MS available (for j2ms2, tConvert).
- [ ] Make standardplots smarter (both in processing & jplotter).
- [ ] tConvert: if IDI exists, remove them.
- [ ] "-h" formatting.
- [ ] No credentials for NME.
- [ ] If different FREQ IDs. Does it work properly?
- [ ] Create checklist.
- [ ] Retrieve calibrators/targets from .expsum.
- [ ] (Maybe independent program): check stations with empty .uvflgfs files and append the flags from SCHED.
- [ ] Add spectral line EVN Pipeline support (under `$IN/immer/script/EVN_line.py`).






