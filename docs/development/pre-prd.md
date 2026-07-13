
# Towards a cleaner version of evn_postprocess

## Cleaning up the code

The last updates included a significant number of changes, refactoring, and new features. Now the code become more obscure and it can be largely improved, simplified, and reduced.

This PRD has this in mind.


### Broad overview of the high-level structure for evn_postprocess.


- Post_process will be able to run steps from 'init' to 'archive' locally in the same server.
- In 'init', it would call a completely encapsulated class that will retrieve the required files from the external servers in order to start the post-processing. There will be two implemented uses:
  * If the username running the task is 'jops' or the user belongs to the group 'supsci', then it will run the standard initialization (equivalent to the current one), which sets the default `--mode supsci`.
  * Otherwise (and if '--mode supsci' is not specified), it will run a different initialization, where the required input files are expected to be already hosted in the local directory.
- All steps will run automatically unless an error is reported (then `evn_postprocess` will notify about that to the user and will require a re-launch to continue).
- The steps will be as asynchronous as possible, considering that the MS can only be accessed by one instance at the time.
- `evn_postprocess` will also encapsulate the pipeline part at the end. In the first interaction, it will just replicate and use the current EVN Pypeline, but writing the module to work with it as separate as possible with metaclasses so it can easily be replaced by another pipeline in the coming future.
- The final `archive` (to be renamed to `distribute`) step, will also be modular. Only in the case of the standard initialization (`supsci` mode), it will archive the data. Which also should be an encapsulated module. Otherwise, it should run another distribution mode (using the same metaclass) that will be defined in the future.
- Independently of the mode, `evn_postprocess` should operate in the same way after initialization until distribution and all needed files should be expected to be in place.



### Server agnostic

The previous versions of `evn_postprocess` relied on multiple calls to external servers to run different steps and retrieve external files. But this is no longer required as it has been largely encapsulated. I hence want to get all the code related to this removed (for example, the Servers classes, the calls to masterprojects, jexp, etc).



### Initialization

The initialization has a part that depends on the mode, and another one that will be shared to all of them.


#### Mode supsci

If mode `supsci` (which is the default), then it will have a separated class defining all steps required to retrieve the needed inputs.
It will need to retrieve the .vix file (following the same rules as now), remotely create and retrieve the .lis files.


#### Mode regular user

Then it will assume that the .vex, .lis, and any other required file will already exist in the current directory.


#### mode SWEEPS

It will implement a encapsulated class, alternative to the supsci one (sharing the same abstract class), that will retrieve from where they are needed, the required files. It will require the .json config file containing the information required to run everything blindly (the supsci would have run before and provided this), with all needed choices as antennas to polconvert, flag weight threshold, sources, etc.
It will have a list of steps to skip in the processing.


#### All modes

It will create the directory structure, and verify that the required files are present.
If something is missing, it should report it (with the given notification, message, etc as configured) and exit with an error in the terminal.


### Processing steps

It will essentially run the same steps as now. It just needs to remove dependencies, and also standard plots will only run once now, after postpolconvert.  Only in the case that the weight threshold is not set automatically (or it cannot guess if there are linear polarization antennas), the plots will be generated.


### Antenna files

This should now run after `post_polconvert` and `standardplots` and before the pipeline steps.
It is the equivalent of retrieving the information from `vlbeer` and producing the ANTAB and UVFLG files.

This should only run in the case of supsci mode. The other modes will expect to have these files already produced.


### Pipeline

Also to be encapsulated, so it is easy to migrate the pipelines.


### Distribution

Should also be defined as an encapsulated module.


#### supsci

It will prepare the PI letter, ask the user to check the dashboard and then proceed with either re-running some of the steps or continue.  Once it reaches this step and runs to move forward, it will produce the credentials, the final (auth) PI letter, and archive all data, finalizing with the PI letter template to be delivered.

#### Otherwise

It will skip all archive/PI letter-related steps. Will just make sure that the final FITS-IDI files are in order.



## For each step

For each step, I want a more clear distinction between the log message, the ones that the user sees, and the CLI interaction.

Each step must have a part that prints a message in the terminal (colorful via rich) so the user gets a useful message and clear to see. At the same time, a more specific logging needs to be achieved via loguru, but a bit hidden for the user side. The evn_postprocess should produce a `post_process.log` file that contains all steps that have been run, in case a user wants to run them manually (e.g. `flagweights msdata threshold`, etc).





