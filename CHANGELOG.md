
# Changelog of evn_postprocess

This is the change log for the different production (master) versions of the program.



## Version 0.4 -- 14 November 2019

First version that fully works for all steps than in the eee computer.

Fixed:
    - Several bugs across the entire program.
Known errors:
    - Log files show multiple repeated lines.


## Version 0.3 -- 13 November 2019

Changed:
    - Checklis is done after the manual modification of the .lis file. It repeats the check if user not happy.
Fixed:
    - Output line 'j2sm2' -> 'j2ms2'.
    - Output from 'r' command during standardplots in the default log file.
    - Construction of the touch credential auth file.
    - archive command was not recognized in the session. Changed to archive.pl.
    - Wrong experiment name use when getting lis/vix files and getdata for e-EVN that are not the master name.
    - Bad parsing of the experiment names from ccs MASTER_PROJECTS.LIS in e-EVN experiments.
Added:
Deprecated:
Removed:
Known errors:
    - Log files show multiple repeated lines.

## Version 0.2 -- 7 November 2019

First real test for the eee machine related part.
