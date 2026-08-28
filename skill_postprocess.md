---
name: postprocess
description: Conducts the post-processing of an EVN (European VLBI Network) observation. This is the internal processing of the data from correlation to distribution to diagnose and prepare the data to be analyzed by the users.

---

# Post-process

Each correlated experiment needs to be processed and verified in order to produce an output as good as possible to the users and to unveil all possible issues that the antennas participating in the observation had.

Each experiment is defined by a given name (referred to as `EXP` hereafter), and it will contain:

- Data from an EVN observation that has been already correlated.
- In the observation there will be a given number of antennas that were supposed to participate. In some cases some of them will be absent for different reasons, or presenting issues during the observation.
- During the observation, different sources are observed: target sources (one or more), and different calibrator sources (phase-calibrators, bracketing each scan on the target(s) are present for most observations; bright fringe-finder sources for instrumental calibration and diagnostic purposes), and other additional calibrators.
- The standard observation mode is the so-called “continuum” observation, where there is a unique dataset (“one pass”) to be processed from correlation -> MS -> FITS-IDI -> pipeline. But other options are:
  - Spectral line observations: where two (or more sometimes) passes are present, in general one is the “continuum” and another one the “spectral line(s)”, containing data correlated with a much larger number of spectral channels per subband (>512 channels).
  - Multi phase centers: during the target scans, correlations on different positions were performed, providing multiple passes that will contain different targets observed at the same time. The data from the calibrators may be included only in one pass (to save space as it is identical in all passes), or in all of them.
  - e-EVN observations, where a experiment is actually part of a longer session that may have included more experiments. In this case, there is only one “master” experiment (the first one to be observed), containing the input files, although the data are at our stages separated for each experiment independently.
- Roughly speaking, we take the correlated data, convert it to a measurement set (MS) format, where we can do multiple diagnostics, corrections; then convert it to the FITS-IDI formats that are the ones to be distributed, and run the pipeline on these data to get some first-rough results.
- Once the data are ready, we archive them to the EVN archive and inform the principal investigator (PI) on how to access the data.



## Working environment

Everything runs on the `eee2` machine, that we connect remotely via `ssh`. There, we always work on a directory `/data/exp/{EXP}`.

The `postprocess` program will run in a semi-automatic way all the following steps. Here I indicate what it runs or we need to run manually on each step.



## Retrieval of context files

Some files are necessary to understand the experiment to be processed.

In the server `ccs`, under `/ccs/expr/{EXP}/` there will be some files to be retrieved, or created and retrieved:

- A `{exp}.vix` (or `.vox` in some cases, which should be used first if exists) should have been created, containing all information related to how the observations were performed.

- A `{exp}.lis` (or multiple as `{exp}*.lis`) needs to be created if they still don’t exist via

  ```sh
  make_lis -e {exp} -p {profile} -s {exp}.lis -m {SRC}
  ```

  Note that `-p` `-s` or  `-m` options are not necessary for a standard experiment. `-m` can be used for multi phase center experiment, where we only want the data from the calibrator sources to be included in the pass containing the given source `SRC`.

We can start retrieving the `vix` file as from there we will be able to extract the following information:

- The antennas that were expected to participate in the observation.
- The PI name and email (`PI_name`, `PI_email`) and title of the experiment (`expr_description`).
- The scans in the observation (time ranges for each of them, which source was observed, and which antennas were expected on each scan). Only in multi phase centers, some scans will contain more than one source. The first one listed shall be the one where antennas were pointing to, and in general the reference (first) pass.  In the case of `e-EVN observations` (when the field `exper_description` starts with “e-EVN: EXP1, EXP2, …”), there will be a single .vix file containing multiple experiments (EXP1, EXP2, …). Therefore only a subset of the scans will be relevant to each experiment. Note that if the given experiment was part of an e-EVN run and was not the first one, then all these files in `ccs` only exist for the first experiment, and for the other ones they will need to be copied to `eee` directly from this one. Create a folder for each experiment, and the files will need to be copied to them.

We copy these files to our working directory in  `eee2`.

And we create a symbolic link to the .vix file as:

````sh
ln -s {exp}.vix {EXP}.vix
````

Note that in case of e-EVN experiments, the `{EXP}` will imply the (upper case) name of our experiment, while the `{exp}` is the (lower-case) experiment name of the reference one in the run, and hence different.

The name of the lis files should also be changed to the experiment name, instead of the master e-EVN experiment.



### Feedback from the stations

Except for e-EVN runs, most observations will appear at `https://services.jive.eu/Feedback/experiment/{EXP}`, which shows a table with the feedback the antennas provide during the observations. It contains the following columns: “Friend”, “Site” (the antenna name, expanded), “Result” (Success/Minor failures/Major/…), “Problem” (if it has a keyword, in general empty), and “Text” (where the operator explains the issue). Sometimes under “Success” the operators write in the Text section something like “success”, “ok”, “No known problems”,… Which could be avoided in our later report.



### JIVE internal metadata

Some extra information that is not present in the vix file is available in the `.jex` files associated to each experiment. Those can be retrieved from the `archive` machine, under `Expadmin/Jexp/{exp}.jex`. Most of the information is already known but the relevant one is:

- `piname` (the name of the PI of the experiment, although the one in the vix file should be more complete). Sometimes the `coname` will also be defined, indicating a co-PI that should be notified.
- `pimail` (the email of the PI, likely the same as in the vix file), and similarly to the previous one, `coimail`.
- `support`, the associated support scientist that is assigned to this experiment. In a few cases it may show two names (separated by a `/`), if two scientists worked on it. Only one (assume the later) should be considered for us here.
- `schedsrc`, a list (comma separated) of the sources from this experiment, with (in brackets) the tentative source type and if the source should be protected or not when uploaded to the archive, in the form `(A|B)`, where `A` is either “C” (calibrator), “T” (target), “R” (phase calibrator), and `B` is either “P” (public source) or “X” (to be protected). It may in some cases contain a “?” if this information is not known at the moment.



## Preparation of the lis files

Lis files contain a single header line with the following information:

````sh
EXP exp.vix Prod exp*.ms exp*.ms.UVF
````

Where it defines the experiment name, the used .vix file, and the expected outputs in MS and UVF format. Currently we change manually the later to `exp_*_1.IDI`, where the \* = 1, 2, … depending on the different existing passes.

The following lines define the different jobs that the correlator processed. The first character is a “+” or a “-”, defining if such job should be used or not in the following process to retrieve the data.



In general there is no further action required on these retrieved files. Except in these two occasions:

- In the case of spectral line experiments, there will be two (or more) correlation profiles, including in their names `_cont` (for the continuum passes) and `_line` (for the spectral line passe(s)). The lis file should be split in different files: `{exp}_cont.lis` and `{exp}_line.lis` (or even more if multiple passes exist for different lines for example). That means that the suffix should correspond to the jobs that are enable or disable (via the “-” or “+” at the beginning of each job). Only jobs using the `prod_line` profile should go to the `_line.lis` and the same with the `_cont` ones.
- In the case of e-EVN runs, there will be only a set of jobs that belong to each given experiment. This information is currently only known by us and hence needs to be manually done. The original lis file needs to be copied to the folders to all experiments in the session, and edited manually on each of them.



Then, we verify that the `.lis` file(s) pointing to the data to be retrieved are correct. For that we ran:

````sh
checklis {lis-file}
````

On the .lis file (or on each file if multiple exist), an output as the following one will appear:

````sh
  First scan = 1
   Last scan = 185
````

Indicating the first and last scan to be included.

In some cases it will warn in between if some missing scans are present in the file. That should be fine and only errors raising that there are **duplicated** scans would be a stopover. In those cases, means that the same scan has been correlated multiple times and only one should be included (most of the times, there will be different correlation profiles, so one can note which one is the final one and only scans using such profile should be included).



## Retrieval of the data

Once ready, we can retrieve all correlated data and convert them to MS files. For each lis file:

````sh
getdata.pl -proj {EXP} -lis {lisfile}
j2ms2 -v {lisfile} [options]
````

Only in the case of multi phase centers, `j2ms2` will need the extra option `fo:nosquash_source_table` (to keep all sources information in the headers, otherwise it drops the source information of those that are not present in the data).

In case of e-EVN experiments, it is necessary to replace the project name in the MS file:

```sh
expname.py {ms-file} {EXP}
```



## Inspection of the data

The MS (if there are multiple, the first one that is taken as reference pass should be enough) is then used to inspect the quality of the observations.

If automatic, one can create a lag-spectrum MS from the same data with `j2ms2 -d frequency -v lisfile fo:filter/source=comma-separated-list-of-sources -o output-ms`. This would allow to have one MS on the frequency domain (only data from all calibrator sources is relevant).  A statistical analysis can be done on a per-scan basis now:

Per scan, determine the signal-to-noise that the source is detected with, for the different antennas. This will give us:

- The scans where some antennas dropped (no signal was detected).
- The antennas providing the highest signal-to-noise and when they were in, so they can be used as reference antennas in the following.



And we create some visual diagnostic plots with:

```sh
standardplots -weight [-scan {scan_no}]  ms-file {refant} {source}
```

That will create different plots that we can look at. Ideally we generate:

- Plots showing the auto- and cross-correlations (amplitudes and phase and amplitudes, respectively, versus frequency for each baseline to the referent antenna) for each scan on the fringe finders. Only if there are antennas that did not observe such sources but still observe the others, we may do a plot on a phase calibrator scan too.
  We look at which antennas show signal in those scans (some of them may only show noise with no source signal indicating that they were not working), if the different polarizations were correct (we expect a similar amplitude level for both RR and LL and much lower amplitudes in RL and LR), indicating if there may be antennas with bad polarizations, or swapped ones, or if they observed in linear polarization instead (and thus it would need to be corrected later with PolConvert).
- It also creates the weight plot showing the weights vs time per antenna.   It is expected that all antennas would show a weight of 1 most of the times (implying we receive as much data as expected). Weights of 0 are common on the subbands or parts of the observation where the antennas were not observing. Values in between imply dropped data and should always be a small fraction of a few percent. In a few cases some antennas may have transmitted constantly less data than expected and thus the ceil may be a value of 0.4 ,0.5, 0.7 or similar for example. Determine what is the minimum value of weights that contain most of the data (that would represent the good data) and set a threshold that will flag everything below that one.
- There are some plots showing the phases and amplitudes along the time for the full observation and for a part of it, for each baseline to some reference antenna(s). As the target sources are in general too faint, it will only show noise, but the calibrators should indicate good signal. This can indicate times where some antennas dropped unexpectedly.
- The previous things are ones done by eye, although the lag MS diagnostics should reveal the same in an automatic way.



In summary, from these steps we will now know:

- The reference antenna to be used (the one providing the highest SNR and available during the whole observation; if not possible use more than one).
- The threshold to flag the data based on the antenna weights.
- If some antennas require a polarization swap or PolConvert.



## Fixing the MS data

All this should run (it can be in parallel) for each MS created (we ignore the lag-spectrum one as that one was only for internal diagnostics).

In case Yebes (or Hobart from the LBA) is in the array, run this script to fix its mount type in the MS file:

```bash
ysfocus.py {exp}.ms
```

Flag weights below the specified threshold (typically 0.9, but check the weight plot from before):

```bash
flag_weights.py {exp}.ms {threshold}
```

This script will output how much data have been flagged, which should be added in the summary report.

In the cases where a station has swapped polarizations, it needs to be fixed by using:

```bash
polswap.py {exp}.ms {antenna}
```

Optionally you can provide a time range for which the swap must be applied, by using the optional parameters `-t1 STARTTIME` and/or `-t2 ENDTIME`. 

In the cases where a station recorded 1-bit data instead of the usual 2-bits (at the moment it should only happen in RadioAstron observations), then the data must be scaled to the usual 2 bits to correct for quantization losses. In that case use:

```bash
scale1bit.py {exp}.ms {antenna}
```

Optionally you can also scale the weights with the parameter `-w`. In general this never happens, but it is wise to search the vix file for “1bit” or “onebit” sampling in case one did.



## Creating the FITS-IDI files

Needs to run tConvert on each MS file (it can be in parallel), using each lis file:

```sh
tConvert -v {lis-file}
```

It will create the FITS-IDI files from the MS as indicated in the header of the lis file.

It allows options on how large each file should be, the pipeline uses a `-o chunk_size=4GB`, meaning that for each pass (_n_, it will create the files `exp_n_1.IDI*`, where \* = 1, 2, 3,… (the full data are divided in different files). Only if there is only one file created, there will be no trailing number at the end.



## Pol Convert (if needed)

If some antennas recorded with linear polarization, this must be converted to circular polarizations. For that we have the `PolConvert` program, by running:

```sh
polconvert.py  polconvert_inputs.toml  --compute
```

Where the `polconvert_inputs.toml` is the input file defining the FITS-IDI to take as reference (to be left as `{exp}_1_1.IDI*` as it will search for the good one), the files to apply the solutions, the antennas to correct and to use for the computation and the data to select. Here it is critical to select a time range with good data in all antennas that are selected. Therefore, you will need to exclude the antennas that did not observe during that time, or present bad polarization information, or didn’t observe all subbands as the linear polarization antenna.  And then one can play with the averaging (in frequency and time) and the solution weight to get a good solution.

The script outputs the signal-to-noise values of the obtained corrected data (also creates a plot showing it), which can be compared. The RL, LR signal should now be much lower than the RR and LL one, for all subbands.

Sometimes one of the each subbands is not properly corrected (due to a lower SNR) but different parameters can solve it.

The `--compute` flag tells PolConvert to only compute the solutions, but not to apply them (for speed purposes). Once the inputs are found to provide good results, one can run the application of the correction with

```sh
polconvert.py  polconvert_inputs.toml  --apply
```

On all FITS-IDI files.

If there are multiple passes and all have the same setup (same number of channels), then this can be applied to all existing FITS-IDI files directly. For spectral line passes, I still need to check if the difference in number of channels produce different results but in principle the same approach can be done.



### Verification of the conversion

The program will convert each file and that will end up in a new file `{exp}_*_1.IDI*.PCONVERT`.  Before making these the final ones, we convert back these data to a (new) MS and produce again updated standardplots that would confirm that now the signal is good in the converted station.

```sh
# Converts the FITS IDI files into a new MS
idi2ms.py  {exp}-pconv.ms  '{exp}_1_1.IDI*PCONVERT'

# Re-generate standardplots
standardplots [-scan {scan_no}] {exp}-pconv.ms {refant} {calsrcs}
```

If everything worked as expected, then, for safety, the original FITS-IDI are stored in a `idi_ori/` folder, and then the “.PCONVERT” ones are renamed, dropping such suffix.



## Pipeline of the data



### Preparing the ANTAB and UVFLG input files from the stations

The stations should have uploaded to the `vlbeer` server their “.antabfs” and “.log” files from each observation.

Those files are downloaded to the `antenna_files/` directory via:

```sh
rsync -avz evn@vlbeer.ira.inaf.it:vlbi_arch/mmmYY/{exp}\*antabfs .
rsync -avz evn@vlbeer.ira.inaf.it:vlbi_arch/mmmYY/{exp}\*log .
```

Where `mmmYY` is the month and year of the start of the observation.

In case of global observations (if the include the VLBA antennas), then download the *{exp}cal.vlba* file, which contains the flagtables, the antab, and the weather information. All these files should also be at *ccs* under `/ccs/var/log2vex/logexp_date/{EXP}_{YYYYMMDD}/`. The antab information from the VLBA stations typically lacks the GAIN and INDEX headers. The former one can be copied from the file */data/tsys/gbt_gains.key* (in _eee_). This file contains the GAINs for each antenna and at each frequency. Copy into the _antab_ file the corresponding one. You will need to copy both files (the *cal.vlba* file) and the *gains.key* to the directory. Then, `antab_editor.py` will do all the work parsing these files into individual antabfs files.



#### ANTAB files

We have the visual `antab_editor.py` program that can run on these files and we can edit the data:

```sh
# Being at /data/exp/{EXP}/antenna_files/.  It will understand you are processing {exp}
antab_editor.py -e {exp} -f ../

# Optional parameters
# -l (or --spectralline) for spectral line experiments
# -a EXP2 [EXP3 ...] for check the FITS IDI files from other experiments when loading the
# current one. Useful for e-EVN runs, when an ANTAB file is produced for multiple experiments
# observed with the same vexfile named for EXPERIMENT (exp).
```

We manually edit it so the resulting `{exp}.antab` file contains valid gain data.



Note that in the case of e-EVN runs, this would mean only the first experiment in the run, but the files will contain the data for the whole run. It will need to provide the different directories where the FITS-IDI from the other experiments lie (they all must be created before antab_editor runs), as a spaced-separated list in `-f`.

#### Creating uvflg files

Almost all stations would provide the _log_ file, from which we can generate the _uvflgfs_ files. These ones would contain the a-priori flags to apply to the data, taking into account the times when the station was off source, slewing, etc. They can be created with:

```bash
uvflgall.sh  # to run in the directory containing all log files

# Or individually for each stations:
uvflg.pl {exp}{ant}.log
```

Check that all the _uvflgfs_ files have roughly the same size. If there is one much smaller, this is likely because there was a problem with the log file interpretation.



### Input files for the pipeline

Then under `data/exp/{EXP}/pipeline/in/` one needs to copy the generated `.antab` file, the `.uvflg` file (or multiple if multiple passes are expected to be pipelined - in general only if there are spectral line ones), and create the input file for the pipeline (`exp.inp.txt`), with the relevant information that is already known.

Then the pipeline runs as:

```sh
EVN.py {exp}.inp.txt
```



### Parsing pipeline output

If the pipeline runs properly, then it needs to run:

```sh
# Under /data/exp/{EXP}/pipeline/in/
comment_tasav_file.py '{exp}'

# Under /data/exp/{EXP}/pipeline/out/
feedback.pl -exp '{exp}' -jss '{supportscientist_name}' -source 'source1 source2 ...'
# Where the sources are all of them that were included for the pipeline
```

It will generate a `.html` file that can be viewed in the browser. The current `postprocess` also appends that one into the dashboard so it can be seen there.



**Check the EVN Pipeline output**

Open the generated *html* file from the EVN Pipeline and check the full output. Among other plots, please consider checking in detail the following sections:

- *Plots of the uncalibrated amplitude and phase against frequency channel*. Do you see fringes for all stations (at least for the fringe finders, or phase calibrator?
- *Telescope sensitivities*. Check that there is Tsys information for all participating stations, for all time ranges, and all observed subbands.
- *Fringe-fit (delay and rate) solutions*. Check that all stations got consistent solutions.
- *Telescope bandpasses*. Check that the bandpasses are correct (roughly flat across the band).
- *Calibrated amplitude and phase against time*. Do all stations have data? Did you miss any of them?
- *Calibrated amplitude and phase against frequency*. Do you see flat phases? And amplitudes?
- *Statistical summary* (from the amplitude corrections applied to the fringe finders). Do all stations have corrections around one with low median errors? If some stations exhibit large correction factors you may want to take a closer look to the data and the ANTAB files to fix it.
- *Telescope sensitivities*. How are the gains applied to all stations/IFs along the time? Did some stations drop data?



If some antenna has a big gain correction (you can also verify that by running `gscale` in `Difmap` on one of the fringe-finders or phase calibrator datasets), then modify it in the _antab_ file: Search for “GAIN {ANT}” and some lines below change the “FT=” value to the square of the obtained correction.

Same if there are significant fractions of bad data for a given antenna (e.g. if one full polarization is broken). You can add manually those flaggs into the `{exp}.uvflg` file, and rerun the pipeline (with `tmask =2`).



## Preparing the data for distribution

### PI letter

An email will be sent to the PI (and a copy is also stored in the EVN Archive) with the summary of the observations and how to retrieve them.

This has a `{exp}.piletter` template that can be filled with everything we noticed.

A good summary shall include the issues on the different stations, if some dropped some times, etc.



## Credentials

The data has a proprietary period so it can only be accessed by the PI (or their team).

First, we need to protect the pipeline results for the sources that were indicated to be protected at the beginning (from the .jex file). Do not run it if no sources had to be protected (for example in NME experiments):

```sh
auth_pipe.py -e {EXP} -s 'SRC1 SRC2 ...' -p source
```

Then we need to set the credentials for the experiment. Get a random password using your favorite method (a quick one is via `date | md5sum | cut -b 1-12`), and keep an empty file in the experiment directory (`/data/exp/{EXP}/`) whose name contains the username and password:

```sh
touch {exp}_{password}.auth
```

And we protect and archive all data:

```sh
# sets the credentials
archive -auth -e {exp}_{YYMMDD} -n {exp} -p {password}

# appends first the ANTAB information to the FITS-IDI files
append_antab_idi.py --antab pipeline/in/{exp}.antab --files '{exp}_*_1.IDI*'
# note that for multiple pases pipelined, different antabs should be applied to the different passes

# archives the data
archive -fits -e {exp}_{YYMMDD} *IDI*
cd pipeline/in/
archive -pipe -e {exp}_{YYMMDD}

cd ../out/
archive -pipe -e {exp}_{YYMMDD}

# copies the gain corrections from each station to the database (currently it doesn't work)
ampcal.sh

# Modify the PI letter accordingly and archive it together with the standard plots:
gzip *ps
archive -stnd -e {exp}_{YYMMDD} {exp}.piletter *ps.gz
```

And then create a piletter containing the credentials so the user will receive them:

```sh
pipelet.py {exp} {support-scientist}
```

Which produces the `{exp}.piletter_auth` file that we copy and send via email.



## NME Reports

If you are post-processing a NME (all experiments that start their names with a `N` or a `F` but not `FT`), then you need to write the NME Report.

This is the website in the Wiki where all the NME Reports are stored:
http://www.jive.eu/jivewiki/doku.php?id=evntog:nme_reports

There you have a LaTeX template and you can upload it to the corresponding place.

However, I created a script `create_nme_report_template.py` to produce the LaTeX template with the correct structure for your NME (e.g. the table will show all the stations that participated). This will save you time modifying the table (just run with a `-h` to know the necessary inputs).

If you do not have this script, download it from the Gitea webpage:
https://code.jive.eu/marcote/science_support_doc

Finally, send an email to EVNTech (evntech@jive.eu) informing everyone that the Report has been uploaded.
