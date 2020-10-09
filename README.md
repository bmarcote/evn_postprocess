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





*NOTE*: The following is no updated and belongs to before the current refactoring period. To be checked once I have again a running version of the code.


## Bug List

- [ ] If '--steps archive' and plots have been already compressed, it crashes. Check if they do exist.
- [ ] Second standardplots had no stored the sources to plot.
- [ ] If plots cannot be open, it must be said in the next "Question" dialog.
- [ ] Log all command outputs into the log files.
- [X] Check if for e-EVN experiments it changes the experiment name.
- [X] Standard plot breaks in the script: ('list has no split').
- [X] Say no to create new lis files but still retrieving the vex file if n/a.
- [ ] If error produced in archive.pl due to Proposal Tool it breaks...
- [X] Output message for archiving too.
- [X] 'Create again the lis file in ccs?' Asks even if it doesn't exist.
- [X] If sources provided in the command line, then update that information for the Source for Plotting in the first dialog.
- [ ] Printing stdout is slow (doesn't show the sifting values like in j2ms2..). Refresh rate?
- [ ] getdata doesn't print stdout while running.
- [ ] dialog about updating pi letter. Overplots on top of previous one. Then it does not disappear and new messages are written on top of it, breaking newlines.
- [ ] Check that I don't pass an empty list as default value in an attribute. Remember that it is by reference so gives problems. I ALWAYS FORGET.

- [ ] In multiphase centers... If I pick to pipeline one that is not the first one, rename the expected IDI files to make that one the \_1\_1.
- [ ] If going directly to standardplots, then it doesn't have the plotsource/refant info..


## Feature requests

- [ ] When aborting, raise an Exception and then in main store the {exp} obj.
- [X] Get flag_weight.py output and store it in the metadata.
- [X] Modify PI letter to include flag_weight.py output and cut experiment name.
- [ ] Modify PI letter with non-observing stations comment (e.g. "OUT")
- [ ] Parallelize when multiple MS available (for j2ms2, tConvert).
- [ ] Make standardplots smarter (both in processing & jplotter).
- [X] tConvert: if IDI exists, remove them.
- [X] Include IDI output names in the .lis file.
- [ ] "-h" formatting.
- [X] No credentials for NME.
- [ ] If different FREQ IDs. Does it work properly?
- [ ] Create checklist.
- [X] Retrieve calibrators/targets from .expsum.
- [ ] (Maybe independent program): check stations with empty .uvflgfs files and append the flags from SCHED.
- [ ] Add spectral line EVN Pipeline support (under `$IN/immer/script/EVN_line.py`).
- [ ] --nogui Option in the command line to not show e.g. standardplots or other visual tasks if any.
- [ ] Write into the log all the metadata (number of correlation passes, etc).






