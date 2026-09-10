# EVN Post-Correlation Processing

Date: 7 September 2026
Version: 7.0

This guide covers everything to be done with the data of an EVN experiment after correlation: from the moment the correlation is finished until the data are delivered to the PI.

There are **two ways to do it, and this guide documents both for every step**:

- **Automatically** (Section 2), with the `postprocess` program, which walks the whole workflow and stops only where a human decision is genuinely needed.
- **Manually** (Section 3), running the underlying tool of each step by hand. This is what you need when a step needs a one-off tweak, or something failed and you want to understand why.

The two are interchangeable at any point: `postprocess` records every local command it runs into `logs/commands.sh`, and every step can be resumed from where a manual intervention left off. There is no "you started manually, so you must finish manually".

> **Convention used throughout**: anything between `{}` must be replaced by the value for your experiment. `{exp}` is the experiment name in lowercase (`em111a`), `{EXP}` in uppercase (`EM111A`), `{YYMMDD}` the observing date.



---

## 1. General information

### 1.1 Computers

| Machine | What lives there |
| --- | --- |
| **eee/eee2** | Where you do essentially everything: the experiment directory, the MS and FITS-IDI files, the EVN Pipeline, the archiving commands. |
| **ccs** | Correlator control files: the `.vix` file and the correlator output. `make_lis`/`showlog_new` live in `/ccs/bin`. |
| **archive** | The EVN Data Archive machine. Holds the `.jex` files with the PI contacts and the per-source protection. |
| **vlbeer** (`vlbeer.ira.inaf.it`) | FTP/SFTP server where the stations upload their `log`, `antabfs` and `uvflgfs` files. |

The host names, users and paths are not hardcoded: they are read from `computers.toml` (searched in `$XDG_CONFIG_HOME/evn/`, then `~/.config/evn/`, then `~jops/.config/evn/`, or pointed at with `EVN_COMPUTERS_TOML`). If a step cannot reach a machine, check that file first.

Support-scientist documentation, issue tracking and the session pages are on the JIVE
RedMine: <https://jrm.jive.nl/projects/science-support>.

### 1.2 V\*x files

`.vex` / `.vax` / `.vixfile` / `.vix` are all the same vex format, at different stages:

- **vex** — produced by SCHED; the file the stations load to run the observation.
- **vax** — same, with the clock section and the actual epoch added; used for rapid correlation control (e.g. FTP fringe tests in NMEs).
- **vexfile** — the output of `log2vex`; may differ slightly from the `.vax`.
- **vix** — the version that went through production correlation (updated clocks, PI-requested source coordinates, …). **This is the one post-processing uses.**

**e-EVN note:** there is normally one `.vix` per project, but an e-EVN run correlates several experiments under a single vex file, named after the *first* experiment of the session. The whole workflow is aware of this (see the e-EVN notes in the individual steps).

### 1.3 The experiment directory

Everything for one experiment lives in a single directory on `eee2`, conventionally `/data/exp/{EXP}`. Both the automatic and the manual route use the same layout:

```
/data/exp/{EXP}/
├── {exp}.vix                 the vex file from ccs ({EXP}.vix is a symlink to it)
├── {exp}.lis                 the lis file(s), one per correlator pass
├── {exp}.ms                  the Measurement Set(s)
├── {exp}-lag.ms              auxiliary lag-space MS (diagnostics only, deletable)
├── {exp}_1_1.IDI*            the FITS-IDI files that go to the archive
├── {exp}.toml                experiment configuration + record of every decision taken
├── {exp}.json                internal state of postprocess (which steps ran, metadata)
├── {exp}.piletter[.html|.eml] the PI letter, in its three forms
├── {exp}_{password}.auth     the archive credentials
├── logs/                     commands.sh, logging_messages.log, j2ms2.log, tconvert.log, …
├── plots/                    the standard plots, as PNG (the .ps files stay in the root)
├── antenna_files/            station .log/.antabfs/.uvflgfs files, and the antab_editor work area
└── pipeline/
    ├── in/                   pipeline input: {exp}.inp.txt, {exp}.antab, {exp}.uvflg, .tasav.txt
    └── out/                  pipeline output: AIPS.LOG, plots, {exp}.comment, {exp}.html
```

Two files are worth knowing about before anything else:

- **`logs/commands.sh`** — every local command `postprocess` ran, in order, with the step it belonged to, as a runnable shell script. This is the single best answer to "what would I have to type to do this by hand?" for *your* experiment, with the real file names filled in. Read it whenever this guide's generic `{exp}` form is not enough.
- **`logs/logging_messages.log`** — the full run log, including the warnings and errors that scrolled past.

---

## 2. The automatic route: `postprocess`

### 2.1 Running it

```bash
ssh -Y username@eee2                 # or your own account
mkdir -p /data/exp/{EXP}
cd /data/exp/{EXP}
postprocess
```

With no arguments it derives the experiment name from the directory, the support scientist from the user running it, and runs the whole workflow — continuing from the last step that finished successfully if it has run before. Use `--expname` / `--supsci` if you are somewhere else or processing on behalf of somebody else.



**NOTE:** There is a bash script `evn-yellow-folder` that you can get internally to put it in your local computer, and then you will be able to run the following just from your computer, as long as you have the proper credentials and authentication to connect to the servers:

`````sh
# launch the parts above, running it in a tmux session in eee2
evn-yellow-folder  {exp}
# You will get the progress in Mattermost

# To resume a previously launched session
evn-yellow-folder {exp} --resume
`````

Note that this may still not be available for you. From the following, only the first option when you launch it via `postprocess` is considered.



### 2.2 The commands you will actually use

```bash
postprocess                       # run everything (or continue from the last good step)
postprocess list                  # all steps, and which ones have completed
postprocess last                  # the last step that finished successfully
postprocess info                  # everything postprocess knows about the experiment
postprocess dashboard             # the same, as a web page (plots, scans, pipeline, comments)
postprocess run STEP [STEP2]      # run from STEP (to STEP2, both included)
postprocess exec COMMAND          # run ONE underlying command, with the stored parameters
postprocess edit FIELD [VALUES]   # fix stored metadata (refant, source types)
postprocess -h                    # full help; also `postprocess <command> -h`
```

`postprocess exec` is the bridge between the two routes: it runs a single command with the parameters already known for this experiment, so you do not have to look up the reference antenna, the pass names or the calibrator list yourself. `postprocess exec` with no argument lists everything it can run. See the mapping table in §2.7.

`postprocess edit` fixes the two things most often guessed wrong:

```bash
postprocess edit refant Ef Mc        # reference antennas, in order of preference
postprocess edit {type} {SOURCE}     # set a source type (target, phasecal, fringefinder)
postprocess edit refant              # no value: list what is available
```

Useful global options:

| Option | Effect |
| --- | --- |
| `-e/--expname`, `-jss/--supsci`, `-d/--dir` | Override what is inferred from the cwd/user. |
| `--refant Ef Mc` | Reference antenna(s), overriding the auto-selection. |
| `-a/--no-archive` | Run everything but do not deliver anything to the archive. |
| `--no-lag` | Skip the auxiliary lag-space MS (and with it the per-scan antenna SNR). |
| `--mode {supsci,regular,sweeps}` | Which backends to use (see §2.4). |
| `--config FILE` | Experiment TOML (default: `{exp}.toml` in the directory). |
| `--policy FILE`, `--batch` | Unattended running (see §2.6). |
| `--comms FILE` | Notification settings (see §2.5). |
| `--debug` | Verbose output. Use it when reporting a problem. |

### 2.3 Configuration files

| File | Who writes it | What it is for |
| --- | --- | --- |
| `{exp}.toml` | Mostly the program, but you can modify it | Source types, PI contacts, and the record of every decision taken (weight threshold, polswap/polconvert/onebit antennas, refant, comments). A run whose `[postprocess]` section is complete repeats itself silently. |
| `computers.toml` | your config | Host/user/path of ccs, archive, vlbeer. |
| `comms.toml` | Your config | Where notifications go (see §2.5), if you terminal notifications, Mattermost chat notifications, etc. |
| `policy.toml` | You, for batch runs | The decisions to take without asking (see §2.6). **NO ACTIVE AT THE MOMENT** |

The experiment TOML is the one to know. A minimal, useful version:

```toml
[observation]
expname = "GS046"
supsci  = "marcote"

[[pi]]
name  = "Jane Doe"
email = "jane.doe@institute.edu"

# type: "target" | "calibrator" | "fringefinder" | "other"
# protected = true  -> archive credentials required to download it
# guessed = true    -> the program's own heuristic guess; correct it freely
[sources."J1848+3244"]
type = "target"
protected = true

[sources."3C345"]
type = "fringefinder"
```

Source names containing `+`, `-` or `.` must be quoted (TOML-format requirements). Everything is optional: what is missing is inferred, asked, or resolved from external paths.

### 2.4 Operating modes

The mode decides *where inputs come from* and *where results go*. It is auto-detected from the OS user/group and can be forced with `--mode`:

- **`supsci`** — the JIVE support-scientist job: retrieve from ccs, station files from vlbeer, archive and deliver. This is what this guide assumes throughout.
- **`regular`** — everything is already local; nothing is retrieved and nothing is archived. `distribute` only verifies that the FITS-IDI files are all there. This is meant for an external user using SFXC to correlate data so they can post-process their data in the same way as us.
- **`sweeps`** — the automated system for the SWEEPS project at JIVE. _Not implemented yet_.

### 2.5 Notifications

With a `comms.toml` in place (`./comms.toml`, `~/.config/evn/comms.toml`, or `~jops/.config/evn/comms.toml`), `postprocess` sends you a message by email or Mattermost when a step fails, when it pauses for review, and when the run finishes with its summary of what was spotted. On the shared `jops` account, leave `username` empty and fill the `[[people]]` table: each experiment then notifies its own support scientist.

Secrets go in the environment rather than the file if you prefer: `POSTPROCESS_SMTP_PASSWORD`, `POSTPROCESS_MM_TOKEN`.

### 2.6 Unattended (batch) runs

```bash
postprocess --batch --policy /path/to/policy.toml run
```

In batch mode nothing interactive is ever opened. When a decision is needed, the run writes a `REVIEW_REQUIRED` file in the experiment directory, exits with status 0, and waits to be resumed by a later `postprocess run`. A minimal policy:

```toml
weight_threshold = 0.85          # required in batch mode (0.0..1.0)
polswap          = ["Wb"]
polconvert       = ["Kt"]
onebit           = []
refant           = ["Ef"]
pause_after      = ["postpipe"]  # steps after which to stop for review
skip_archive     = false
batch            = true
```

### 2.7 Where automatic meets manual

| Step | `postprocess exec` command(s) | Underlying tool(s) |
| --- | --- | --- |
| 1. `initialize` | — | `scp` of the `.vix` from ccs and `mkdir` to create required folders. |
| 2. `lisfiles` | `makelis`, `getlis`, `modlis` | `make_lis` (ccs), `scp`, `sed` |
| 3. `checklis` | `checklis` | `checklis.py` |
| 4. `j2ms2` | `getdata`, `j2ms2`, `expname`, `metadata`, `lagsnr` | `getdata.pl`, `j2ms2`, `mstools run expname` |
| 5. `standardplots` | `standardplots` | `standardplots` / `jplotter`, `gv` |
| 6. `msops` | `ysfocus`, `flag_weights`, `polswap`, `onebit` | `mstools run …` (or `ysfocus.py`, `flag_weights.py`, `polswap.py`, `scale1bit.py`) |
| 7. `tconvert` | `tconvert` | `tConvert` |
| 8. `polconvert` | `polconvert` | `polconvert.py` |
| 9. `post_polconvert` | `postpolconvert` | file renaming, `idi2ms.py` |
| 10. `standardplots2` | `standardplots` | `standardplots` |
| 11. `antab` | `vlbeer`, `uvflg`, `antab` | `scp`/`sftp` from vlbeer, `uvflgall.sh`, `antab_editor.py` |
| 12. `pipeinputs` | `pyinput` | template → `pipeline/in/{exp}.inp.txt` |
| 13. `pipeline` | `pipe` | `EVN.py` |
| 14. `postpipe` | `comment_tasav`, `feedback`, `piletter` | `comment_tasav_file.py`, `feedback.pl`, PI-letter template |
| 15. `prearchive` | `append` | `append_antab_idi.py` (`append_tsys.py` + `append_gc.py`) |
| 16. `verification` | `verify` | `check_antab_idi.py`, `check-multipart-fits.py`, `compare-ms-idi.py` |
| 17. `distribute` | `auth`, `protect`, `archive-fits`, `archive-pilet`, `nme` | `auth_pipe.py`, `archive.pl` |

### 2.8 When a step fails

1. Read the error in the terminal, then `logs/logging_messages.log` for the context, then the step's own log (`logs/j2ms2.log`, `logs/tconvert.log`, `logs/getdata.log`, `logs/verification.log`, …).
2. Fix the cause (often: a missing file, a wrong source type, a wrong reference antenna — see `postprocess edit`).
3. Re-run just that step: `postprocess run {step}` (it continues to the end), or `postprocess run {step} {step}` to run only it, or `postprocess exec {command}` to retry a single command in isolation, bypassing the step ordering.

Steps are smart: `j2ms2` skips passes whose MS already exists, `tconvert` skips passes whose FITS-IDI files exist, `msops` refuses to re-apply itself once the FITS-IDI files are there, and vlbeer downloads never overwrite a file you have edited by hand.



### 2.9 Manual inspection required within the processing

When `postprocess` runs, it should guess anything that is required for most of the post-processing. The support scientist should be prompted to do a manual inspection of the outcome. These are the steps where such intervention will be required (in the cases where no unexpected problems are raised):

- When running PolConvert (if needed), `postprocess` tries to get good parameters that provide an accurate correction. This sometimes is not met and the support scientist will be prompt to try to run polconvert manually and try to get a good correction.
- The support scientist will be asked to run `antab_editor.py` manually to provide a good ANTAB file for the experiment. This is still a fully manual step to be done.
- After the pipeline runs, the support scientist will be prompted to open the dashboard containing the full summary of the experiment and plots to diagnose. Here is where you need to spend your time: verify everything that may have been wrong, fill the comments per station and global also taking a look at what stations said in the station feedback, and check the pipeline results. Try to improve the gains of the different antennas so the gain calibration is more accurate, and re-run the pipeline if needed.
- At the very end, the support scientist will be told to send the PI letter to the PI (in different formats, use the one you feel more comfortable with).



---

## 3. The manual route

### 3.1. Initialize the experiment directory and copy the vex file

Creates the directory structure (`logs/`, `plots/`, `pipeline/{in,out}`, `antenna_files/`), gets the `.vix` file, and derives *all* metadata from it: observing date, e-EVN?, scheduled stations, sources and scans. It then applies `{exp}.toml` (source types, PI, support scientist) and classifies heuristically any source still untyped.

```bash
scp jops@ccs:/ccs/expr/{EXP}/{exp}.vix .
ln -s {exp}.vix {EXP}.vix
mkdir -p pipeline/in pipeline/out antenna_files/
scp -r evn@vlbeer.ira.inaf.it:vlbi_arch/mmmYY/{exp}.key .
scp -r evn@vlbeer.ira.inaf.it:vlbi_arch/mmmYY/{exp}.sum .
```

For an e-EVN experiment the vix file is the one of the *first* experiment of the e-EVN run — copy that one and the symbolic link should be `{EXP}`, the one being processed. Retrieve the .key and .sum schedule files so you have those files as reference for what was scheduled.



### 3.2. Create and retrieve the lis files

Creates the `.lis` file(s) on ccs if they are not there, copies them over, and fixes their header line. so the FITS-IDI basename is `{exp}_1_1.IDI` instead of `{exp}.ms.UVF`. For e-EVN, the files are renamed and re-pointed to this experiment.

```bash
ssh -Y jops@ccs
cd /ccs/expr/{EXP}
/ccs/bin/showlog_new {EXP}
```

In the window, check which runs are `PRODUCTION` (there will usually be `CLOCK SEARCH` or `TEST` runs too). If any run shows `UNKNOWN` status (only happens in e-EVN), mark it `GOOD` from *Inspect Data* with the bottom-right options. Then switch to *ExportFile*, verify all the scans you want carry a `+`, click *ExportFile* again and save the lis file into the `{EXP}` directory.

For multi-phase-centre or pulsar-binning experiments, the *save* dialog offers a name like `{exp}*{key}.lis` — **do not change the `{key}` part**: it is expanded automatically into one lis file per phase centre (or per pulsar bin).

If you want to avoid the GUI version and know what to expect, you can run instead:
```sh
# Instead of showlog_new:
make_lis -e {exp} -p {profile} -s {exp}.lis
```

Where `-s` names the output file. `-p` selects jobs by profile (`prod`, or `prod_cont` / `prod_line` for spectral-line experiments); omitted, it takes all production jobs and skips the clock-search ones. For multi-phase-centre experiments, `-m {SRC}` keeps the calibrator data in the pass of one source instead of repeating them in every phase centre — this is what `postprocess` adds automatically above 5 phase centres in a scan.

Then, on eee:

```bash
scp jops@ccs:/ccs/expr/{EXP}/{exp}\*.lis .

# Fixes the output FITS-IDI file name, on each file:
sed -i 's/{exp}.ms.UVF/{exp}_1_1.IDI/g' {exp}.lis
```

**What to check.** Open the lis file. The **first line** must name the correct `.vix` file and the correct output MS (normally `{exp}.ms`). The **last entry of the header** must be the FITS-IDI basename `{exp}_1_1.IDI`, and it must be *unique across passes* (see the naming rule in step 7). You may also want to change the reference station used to define the frequency setup.

Spectral-line, multi-phase-centre and pulsar-binning experiments need several lis files, producing several MS files (e.g. a continuum pass and a line pass).



Now you need to check each lis file for duplicated or missing scans, and that all passes have unique `.lis` / MS / FITS-IDI names. It also extracts the list of correlator passes that the rest of the workflow works on.

```bash
checklis.py {exp}.lis  # or for each .lis file
```

This checks the correlated jobs that will be retrieved and checks for which scans will then be processed. It gives you the first and last scan to be processed, and it will warn you if you have selected duplicated or missing scans.  **Missing scans** sometimes are expected in case of e-EVN runs for example, or when some scans were dedicated to phasing-up interferometers in the array, but otherwise it should never happen. If you are warned about **duplicated scans**, you need to fix this. You can always select or deselect the jobs to be retrieved by writing a “+” or “-” at the beginning of the line, respectively. Find which jops are providing the duplicated scan and leave only the current one (it may happen that such scan was correlated multiple times, with different profiles, and there should be only one final one to be use).

### 3.3. Get the correlator output and make the MS

Fetch the correlator output for the jobs listed in the lis file, converts it to a Measurement Set, fixes the project name for e-EVN.

```bash
getdata.pl -proj {EXP} -lis {exp}.lis  # do it for all lis files you may have
j2ms2 -v {exp}.lis
```

By default `j2ms2` keeps the full antenna table from the vex file but reduces the source table to the sources that actually have data in this MS. That is what you want for most experiments, and in particular for e-EVN, where the vex file carries the sources of *other* experiments too. 

Use `fo:nosquash_source_table`  in `j2ms2` to keep the *full* source table instead, that is required in multi-phase-centre runs.

**Note** that `j2ms2` appends the data from each job into the output MS (defined inside the lis file). If the MS exists, then it will append such data into the existing file. This may create duplicated data if you run it more than once without removing the previously created MS.

In **e-EVN** observations, only the first experiment's name propagates through the correlator jobs, so the project name in the MS of every other experiment has to be corrected:

```bash
mstools run expname {exp}.ms {EXP}
# equivalently, the standalone script:
expname.py {exp}.ms {EXP}
```



### 3.4. Inspect the data

Produce the standard plots, converts them to PNG for the dashboard, and opens them, so you can verify the correlated data from the different antennas.

```bash
standardplots -weight [-scan {scan_no}] {exp}.ms {refant} {calsrcs}
```

where `-scan` optionally forces a reference scan instead of the automatically chosen ones from the `{calsrcs}` (sources to be used for that). `{refant}` is the two-letter reference antenna code, and `{calsrcs}` is one or more comma-separated calibrator names (matched exactly). `standardplots -h` for the rest.

**The plots, and what to look for in each.**

| File | What it shows | What to check |
| --- | --- | --- |
| `{exp}-weight.ps` | Weight vs time, per antenna | Weights should sit at ~1. Note if some antennas are significantly dropping data along the observation: it sets the flagging threshold in the next steps (typically ~0.9). |
| `{exp}-auto-scan{N}.ps` | Autocorrelations, amplitude vs frequency, per antenna | Bandpasses should be roughly flat. L band will show RFI. A missing or flat-zero band means the antenna did not record that subband. |
| `{exp}-cross-scan{N}.ps` | Cross-correlations to the reference antenna, amplitude and phase vs frequency | Fringes present? Both polarizations comparable? A station with signal in RL/LR instead of RR/LL has **swapped polarizations** (next step). A station with similar amplitudes across all four polarizations may have recorded **linear** polarization (future step to fix). |
| `{exp}-ampphase-0.ps` | Amplitude and phase vs time, all baselines to the refant, whole experiment | Which antennas dropped out, when, and for how long. |
| `{exp}-ampphase-1.ps` | The same, zoomed on the first hour | The detailed time behaviour at the start. |
| `{exp}-amptime-scan{N}.ps` | Autocorrelation amplitude vs time around a scan | Level changes, dropouts. **Not produced by the standard run** — make it by hand from `jplotter` (Appendix A) when you need it. |

The `.ps` files stay in the experiment directory; if you do PNG copies (one per page) go to `plots/` and are what the dashboard shows from `postprocess`.

**What can fail.** If `standardplots` produces nothing useful (a source name that matches no scan, a reference antenna absent from the calibrator scans), the fallback is to drive `jplotter` by hand — see Appendix A, which reproduces exactly the commands `postprocess` issues. Note that the reference antenna is chosen *per scan*: if your preferred one is missing from a scan, another is picked automatically and said so in the log.

### 3.5. Fix the Measurement Set

Apply some corrections if required to the existing data:

1. `ysfocus` — corrects the mount type of **Yebes** (Nasmyth) and **Hobart** (X-Y east-west) in the MS `ANTENNA` table, so `tConvert` writes the right `MNTSTA` and the parallactic-angle correction is right.
2. `flag_weights` — flags visibilities whose weight is below the threshold. Usually ~0.9 but check the weight plot for that. Everything below such weight threshold will be flagged.
3. `polswap` — swaps the polarizations of antennas that labelled them the wrong way round.
4. `onebit` — scales data recorded with 1 bit to the usual 2-bit level (quantization correction).

```bash
mstools run ysfocus       {exp}.ms                 # Yebes/Hobart mount fix
mstools run flag_weights  {exp}.ms {threshold}     # flags, and reports the % flagged
mstools run polswap       {exp}.ms {antenna}       # swapped polarizations
mstools run scale1bit     {exp}.ms {ant} [{ant} …] # 1-bit → 2-bit scaling
mstools view {exp}.ms                              # overview of what is in the MS
```

The standalone scripts still exist and do the same thing, with a couple of extra options:

```bash
ysfocus.py {exp}.ms
flag_weights.py [-v] {exp}.ms {threshold}          # -v: report only, do not flag
polswap.py [-t1 START] [-t2 END] {exp}.ms {antenna}   # AIPS time format
scale1bit.py [-w] [-u] {exp}.ms {ant} [{ant} …]    # -w: also scale weights; -u: undo
```

`-t1/-t2` take `YYYY/MM/DD/hh:mm:ss` or `YYYY/DOY/hh:mm:ss` — use them when a station fixed its swap partway through the run.



### 3.6. Create the FITS-IDI files

Convert each correlator pass from MS to FITS-IDI:

```bash
tConvert -v {exp}.lis [-o {chunksize}]
```

The FITS-IDI naming rule is `{exp}_{corr}_{pass}.IDI{chunk}` where:

- **corr** (1..*n*) — different correlations: continuum vs spectral line, different phase centres, pulsar bins.
- **pass** (1..*n*, normally 1) — correlation passes for experiments using two heads, or too many lags/subbands to correlate in one go. These can generally be glued back together with `VBGLU` in AIPS.
- **chunk** — a large pass is split over `.IDI1`, `.IDI2`, … Only `.IDI1` carries the Tsys and gain-curve tables (future step). In general we currently set a chunk size of 4GB (max size of each individual file).



### 3.7. Linear → circular polarization conversion (only if needed)

EVN observations are always in circular polarization. Occasionally a station records linear instead, and those data have to be converted. **PolConvert** (Martí-Vidal et al. 2016, A&A 587, A143) derives the conversion from a bright source and applies it to the whole dataset. It works on the FITS-IDI files, not on the MS — which is why this step comes after the previous step.

You can retrieve the input file for PolConvert, modify it and run it:

```bash
cp /home/jops/opt/evn_support/polconvert_inputs.toml .
# edit it: the comments explain every field
polconvert.py polconvert_inputs.toml --compute
# --compute  : only derive the solutions, overwriting what is in the file

# Once you are hhappy with the solutions,you can apply them to all FITS-IDI:
polconvert.py polconvert_inputs.toml --compute
# If you run it with no flags, it will do what is written in the input file. 
```

The clearest signal is the per-IF fringe-SNR table `polconvert.py` prints when it finishes. A converted IF puts its power into RR/LL and leaves RL/LR small; a failed one leaves all four comparable (IF5 below):

```
        PolConvert fringe SNR  ·  SCAN 0 EF-JB

  Pol   IF1   IF2   IF3   IF4   IF5   IF6   IF7   IF8
 ─────────────────────────────────────────────────────
  RR    265   307   208   285   326   395   229   236
  LL    230   273   287   365   390   209   224   316
  RL     21     6     7     8   287    26    13    21
  LR      7    18     7    11   260    12    25    14
```

You can also open the plot at `polconvert_logs/FRINGE_PLOTS/ALL*.png`, that will show this in a plot with the initial and corrected powers.

If PolConvert dies before printing it, run it again.

**What can fail:**

- *Only `nan` in the solutions.* It is finding no data in the parameter space you gave it: wrong scan, wrong time range, wrong IFs, or a reference antenna that is not in that scan.
- *Segmentation fault.* Common, try to run it again.
- The amplitudes for RR and LL should be much higher than for RL, LR, and consistent across all IFs (with the possible exception if the antenna has already much lower signal on some side IFs). If you do not see that, you will need to try to tweak the parameters, either a different time range, different reference antenna, taking some stations out, or modifying the weight parameter.

**Remember to mention it in the PI letter.** The conversion is transparent to the user, so a note belongs in "Further remarks":

> Note that "ANTENNA NAME" ("XX") originally observed linear polarizations, which were transformed to circular ones during post-processing using the PolConvert program (Martí-Vidal et al. 2016, A&A 587, A143). Thanks to this correction, you can automatically recover the absolute EVPA value when using "XX" as reference station during fringe-fitting.

### 3.8. Put the converted files in place

Back up the original FITS-IDI files into `idi_ori/`, then renames the `*.PCONVERT` files to the standard FITS-IDI names.

```bash
mkdir -p idi_ori && mv {exp}_1_1.IDI* idi_ori/
# in bash
for f in {exp}_1_1.IDI*.PCONVERT; do mv "$f" "${f%.PCONVERT}"; done
# in zsh
zmv '{exp}_1_1.IDI(*).PCONVERT' '{exp}_1_1.IDI$1'
```

To verify the conversion visually, convert back to an MS and re-plot:

```bash
idi2ms.py [-d] {exp}-pconv.ms '{exp}_1_1.IDI*'
standardplots [-scan {scan_no}] {exp}-pconv.ms {refant} {calsrcs}
```

(The quotes around the wildcard are required.) Check that the previously linear antenna now shows its signal in RR/LL. Keep the pre-conversion files until you are satisfied.



---

## 3.9. Towards the pipeline

### 3.9.1. Retrieve station files

Note that **for e-EVN observations**, you will need to do this for the original experiment in the run only.

1. Download the station `.log`, `.antabfs` and `.flag` files from vlbeer into `antenna_files/`. 
3. Creates the `.uvflg` files from the `.log` files.
3. Symlinks the `.vix` into `antenna_files/` so the schedule sits beside the station files.
6. Opens `antab_editor.py` to prepare a good ANTAB file with the Tsys and gain curve information for all antennas.

```bash
cd /data/exp/{EXP}/antenna_files
scp -r evn@vlbeer.ira.inaf.it:vlbi_arch/{monthYY}/\*.log .
scp -r evn@vlbeer.ira.inaf.it:vlbi_arch/{monthYY}/\*.antabfs .
scp -r evn@vlbeer.ira.inaf.it:vlbi_arch/{monthYY}/\*.uvflgfs .

# Produces a uvflgfs file from each log:
uvflgall.sh
cat *uvflgfs > {exp}.uvflg

ln -s ../{EXP}.vix {EXP}.vix

antab_editor.py -e {EXP} -f ..
#  -l / --spectralline : spectral-line experiment (produces continuum + line ANTAB files)
#  -a EXP2 …           : also read the FITS-IDI of the other e-EVN experiments of the run (that must already have had their FITS-IDI produced)

cp {exp}.antab {exp}.uvflg ../pipeline/in/
```

`antab_editor.py` is the tool for everything ANTAB. It shows the gain curves, lets you edit each station's ANTAB, interpolate sparse Tsys measurements, and create nominal files for missing stations. It reads the FITS-IDI files to verify there is a Tsys measurement for at least every scan (slow the first time, cached afterwards).

Save the per-station corrected files (they get an `.editor` extension, so the originals stay recognizable) and the combined `{exp}.antab`. That combined file, plus `{exp}.uvflg`, are what the pipeline needs in `pipeline/in/`.

**High-frequency (K/Q band) experiments.** Some stations write `, opacity_corrected` into the
`POLY =` line of their ANTAB, which is not standard and which `antab_editor.py` cannot parse.
`postprocess` strips it automatically (leaving an `! opacity_corrected` comment as the
record); by hand, `grep opacity *.antabfs` and remove it. Either way, report it in the PI
letter under "Further remarks":

> The following antennas provided opacity-corrected gain calibration: ANTENNA1, ANTENNA2, …

**Globals (VLBA stations).** NRAO publishes the calibration at <http://www.vlba.nrao.edu/astro/VOBS/astronomy/>; download `{exp}cal.vlba`, which holds the flag tables, the antab and the weather information. The same files are on ccs under `/ccs/var/log2vex/logexp_date/{EXP}_{YYYYMMDD}/`. The VLBA antab typically lacks the `GAIN` and `INDEX` headers; the gains come from `/data/tsys/gbt_gains.key` and `vlba_gains.key`. Copy both files into `antenna_files/` and `antab_editor.py` parses them into per-station `.antabfs` files.

**If the VLA is in the array**, its individual antab should be in that same directory or in the NRAO webpage. If it is not, contact the VLBA data analysts at <vlbiobs@nrao.edu> (they often forget to produce it and looks to be a manual thing).

**What to check.** A careful look at all these files is mandatory. Pay attention to the frequency ranges (`FREQ`) and the column indexes in the headers, and to suspicious Tsys numbers. Check that all the `.uvflgfs` files are roughly the same size — one much smaller than the rest usually means its log file was not interpreted correctly.

**What can fail.**

- *A station has not uploaded its `.antabfs`.* Ask them by email. In the meantime, `antab_editor.py` can generate a nominal file from the EVN Status Table SEFDs. Some stations may slightly different file names — check before concluding the file is missing.
- Sometimes `antab_editor.py` breaks while reading an antab file because of syntax errors on the file. Check what it writes in the terminal to be sure this is not the case.
- 

### 3.9.2. Preparing the pipeline input file and running it

Once copied the `.antab` and `.uvflg` files from `antenna_files/` into `pipeline/in/` (multiple files for spectral line or multiple phases centers if they are required to be pipelined), you need to prepare the `pipeline/in/{exp}.inp.txt` input template for the pipeline. Retrieve it from a previous experiment or from `~jops/opt/evn_postprocess/src/evn_postprocess/templates/`, filling in: the AIPS user number, the reference antenna, the bandpass (fringe-finder) sources, the phase-reference/target pairs, the solution interval, and the primary-beam / all-sources settings if this is a multi-phase-centre run. List at the bottom all sources in the observation if this one includes less sources in the data than scheduled (e.g. in e-EVN runs).

This is a separate step from running the pipeline precisely so you can read and edit the input file first.

The `tmask` parameter selects which parts run:

| # | Stage |
| --- | --- |
| 1 | Load and sort the data |
| 2 | A-priori data flagging |
| 3 | Plot the raw data |
| 4 | Amplitude calibration and parallactic-angle correction |
| 5 | Fringe-fitting |
| 6 | Bandpass calibration |
| 7 | Plot the results after ampcal, fringe-fitting and bandpass |
| 8 | Split |
| 9 | Create multi files, dirty maps and first clean maps |
| 10 | Continue mapping |
| 11 | Plot the final data |
| 12 | Calculate the antenna sensitivities |
| 13 | Save useful data and plot the final map |

```bash
cd /data/exp/{EXP}/pipeline/in
EVN.py {exp}.inp.txt    # or {exp}_1.inp.txt, {exp}_2.inp.txt … for multiple passes
```

**Note that for spectral line or multiple passes to pipeline** experiments, the experiments name should be “{exp}_n”, where _n_ is the correlator pass to pipeline. This should also be extrapolated to the experiment name defined inside the input file, and to the names of the antab and uvflg tables, and the vix file (if copied, which is required for multi-phase center observations).

Multi-pass experiments may be pipelined one process per pass, in parallel (although in the general case, we only pipeline one pass as the calibrator information is the same). All output lands in `pipeline/out/`.



### 3.9.3. Post-pipeline: diagnostics, feedback page and PI letter

You will need to create the `.tasav.txt` (in `pipeline/in/`) and `.comment` (in `pipeline/out/`) files, generates the pipeline feedback HTML page, and writes the PI letter from its template. Then you can open the created HTML file and go through the pipeline plots to inspect the data:

```bash
comment_tasav_file.py [-l] -oc pipeline/out -ot pipeline/in '{exp}'
feedback.pl -exp '{exp}' -jss '{your_surname}' [-source 'src1 src2 …'] [-nme=T]
```

`{exp}` is the *pass* name: `{exp}`, or `{exp}_1` / `{exp}_2` for a continuum and a spectral line pass. It is case-insensitive. `-l` marks a spectral-line pass.

**Check the `.comment` file carefully**, especially the frequency setup — it can get it wrong in a few particular cases.

**Reviewing the pipeline output.** Open the generated HTML page (`pipeline/out/{exp}.html` and go through:

- *Uncalibrated amplitude and phase vs frequency channel* — do you see fringes for all stations, at least on the fringe-finders and the phase calibrator?
- *Telescope sensitivities* — is there Tsys for every station, every time range, every subband?
- *Fringe-fit (delay and rate) solutions* — are they consistent across stations?
- *Telescope bandpasses* — roughly flat?
- *Calibrated amplitude and phase vs time* — do all stations have data? Did you lose one?
- *Calibrated amplitude and phase vs frequency* — flat phases? sensible amplitudes?
- *Statistical summary* (amplitude corrections from the fringe-finders) — corrections around 1 with small median errors? A station with a large correction factor needs a closer look at its data and its ANTAB.

**Fixing what the pipeline reveals.**

- *A large gain correction for one antenna.* Confirm it with `gscale` in `Difmap` on a fringe-finder or the phase calibrator (`gscale2avg.py` reformats the Difmap output per antenna and gives you the average factor). Then find `GAIN {ANT}` in the `.antab` file and change the `FT=` value a few lines below to the **square** of the correction. Re-run the pipeline (you can do it from `tmask = 4`, that is where the ANTAB info gets loaded, of `tmask = 2` if you are adding new flagging information).
- *A significant fraction of bad data* (e.g. one polarization broken). Add the flags by hand to `pipeline/in/{exp}.uvflg` and re-run the pipeline with `tmask = 2`.

**Update the PI letter here.** If you are running it manually, currently we do not have a proper way to get the template of the PI letter for this experiment, beyond the old-style one that `jexp` generates in `archive`. You can copy this from:

 `````shell
 scp jops@archive:piletters/{exp}.piletter .
 scp jops@archive:piletters/{exp}.expsum .
 `````

Edit the PI letter with all information you have seen from the experiment and what is worth to mention to the PI.

The `.expsum` file contains the information of which **sources should be protected**.



---

## 4. Delivering the data

### 4.1 Put the Tsys and gain curves into the FITS-IDI files

We append the `SYSTEM_TEMPERATURE` and `GAIN_CURVE` tables from the final `.antab` file(s) into the FITS-IDI files:

```bash
append_antab_idi.py                                   # in the {EXP}/ directory
append_antab_idi.py --antab pipeline/in/{exp}.antab   # a specific ANTAB file
append_antab_idi.py --fits '{exp}_*_1.IDI*'           # specific FITS-IDI files (quote the wildcard!)
append_antab_idi.py --replace …                       # overwrite tables already present
```

It finds the `.antab` file(s) in `pipeline/in/` and runs `append_tsys.py` and `append_gc.py` underneath. Only the `.IDI1` chunk of each pass carries these tables.



### 4.2. Verifying all the data

Three independent, read-only checks on the finished FITS-IDI files:

**The ANTAB tables really got into the FITS-IDI files (and all passes):**

```bash
check_antab_idi.py [--fits '{exp}_*_1.IDI*']
```

**No data lost between the FITS-IDI chunks:** A pass split over `.IDI1`, `.IDI2`, … is checked by comparing the end time of each chunk with the start time of the next:

```bash
check-multipart-fits.py '{exp}_1_1.IDI*' '{exp}_2_1.IDI*' ...
```

It prints a line only where something is off, e.g. `es124_1_1 loss=1.9999891519546509s gain=0.0s nZero=0`. A **loss under 10 s** is the normal rounding of one integration at a chunk boundary and passes; more than that means visibilities were silently dropped, and the step fails. Overlapping chunks (`gain`) and zero timestamps (`nZero`) are logged as warnings — worth a look, but not missing data.

**3. The FITS-IDI still holds what the MS had.** Per pass, exposure time, weight and number of visibilities are accumulated per (baseline, source) on both sides and compared:

```bash
compare-ms-idi.py --ms {exp}.ms --idi '{exp}_1_1.IDI*'
```

The tool reports every pair that differs *anywhere*, which on a perfectly good conversion is most of them:

```
('EfEf', 'Ic2810nuc') :
      15394.0000s wgt=245480.8214   7697 times in   MS: es124-IC2810NUC.ms
      15394.0000s wgt=243547.6475   7697 times in  IDI: : es124_2_1.IDI*
Checked 2 data sets, 225 common keys with 196 problems identified
```

Those 196 "problems" are not problems: the **seconds and the visibility counts match**, and only the weight differs, because it is recomputed during the conversion. The step applies the real rule instead — seconds and visibility counts must be identical, weights may differ by up to **< 10 %** — and fails only on a pair that breaks it, on negative weights, or on a (baseline, source) that one of the two datasets does not have at all, like because it was flagged during the post-process. There may be exceptions but check that the differences track to something plausible.

### 4.3. Credentials, protection, archive, and PI letter

1. Read the PI/co-I contacts and the per-source protection from the `.expsum`  file (some times you may have a newer email in the .key file).
2. Set (or recover) the archive credentials.
3. Protect the sources the PI scheduled as protected.
4. Archive the standard plots, the FITS-IDI files, the PI letter, and the pipeline directories.
5. Regenerate the PI letter with the credentials and hand it to you to send.

```bash
# A "safe" password, and the file that records the credentials:
date | md5sum | cut -b 1-12
touch {exp}_{password}.auth

archive.pl -auth  -e {EXP}_{YYMMDD} -n {exp} -p {password}

# Source protection (per source, and for the pipeline products):
auth_pipe.py -e {EXP}_{YYMMDD} -s '{SRC1} {SRC2}' -p source
auth_pipe.py -e {EXP}_{YYMMDD} -s '{SRC1} {SRC2}' -p pipe
# Also, manually via: http://archive.jive.nl/scripts/pipe/admin.php

gzip *ps
archive.pl -stnd  -e {EXP}_{YYMMDD} *ps.gz
archive.pl -stnd  -e {EXP}_{YYMMDD} {exp}.piletter
archive.pl -fits  -e {EXP}_{YYMMDD} *IDI*

(cd pipeline/in  && archive.pl -pipe -e {EXP}_{YYMMDD})
(cd pipeline/out && archive.pl -pipe -e {EXP}_{YYMMDD})

# Only if the experiment was not submitted through NorthStar (Bob usually does this):
archive.pl -abstract {abstract.txt} -e {EXP}_{YYMMDD}
```

`archive` (no `.pl`) prints the full list of options and the file-name conventions. `auth_pipe.py -h` lists the fields that can be protected or released, per experiment and per source. You can also set the authentication from <http://archive.jive.nl/scripts/pipe/admin.php>.

> **No password protection is needed** for NMEs, tests, or for experiments where the PI has waived the proprietary period. `postprocess` skips the credentials for NMEs automatically.

For the PI letter, run:

```bash
pipelet.py [-o OUTDIR] [-c CREDENTIALS | -u USER -p PASS] {exp} {your_surname}
```

To build the credentials letter by hand. It takes the credentials from the `{username}_{password}.auth` file in the current directory.

**Send the email** to the PI (and co-PI if listed in the .expsum too) and always CC `jops@jive.eu`.



---

## 6. After delivery

### 6.1 Station feedback and issue tracking

Add the important station issues to the **Feedback experiment pages**
(<http://old.evlbi.org/session/feedback.html>, or directly from the experiment's Archive
page). This is the operations-facing record, so it can be more technical than the PI letter.
Only add what the stations have not reported themselves.

Then report the major issues on the JIVE RedMine news page,
<https://jrm.jive.nl/projects/science-support/news>, where problems are tracked per session
and per station (create the news item if the session does not exist yet). Use *Issues* for
anything that needs investigating inside JIVE.

> `parsePIletter.py` used to do the first of these from the PI letter. It is no longer
> installed; the pages are filled in by hand.

### 6.2 Gain corrections into the database

```bash
cd /data/exp/{EXP}/pipeline/out
ampcal.sh [-e {eEVN_name}] [{source}]
```

With no argument it uses the BPASS and PHASEREF source lists from the pipeline input file.
For an e-EVN run, pass the official name of the run (the first experiment observed). This is
one of the few steps with no automated equivalent — run it by hand.

### 6.3 House-keeping

Once the experiment has been delivered and the usual two weeks of waiting for PI feedback have
passed, you can free the disk: the AIPS files and the FITS-IDI files can go (a copy of the
latter is in the EVN Data Archive). The `-lag.ms` file is a diagnostic and can go at any time.
Keep the Measurement Sets until they have been backed up to tape.

---

## 7. NME reports

For a Network Monitoring Experiment you also write the NME report. The reports live on the
wiki: <http://www.jive.eu/jivewiki/doku.php?id=evntog:nme_reports>, where there is a LaTeX
template.

`postprocess` reminds you about it at the end of the delivery (`postprocess exec nme`). The
helper that generated a pre-filled LaTeX template with the participating stations
(`create_nme_report_template.py`) is not installed on eee at the moment; get it from the Gitea
repository <https://code.jive.eu/marcote/science_support_doc> if you want it.

When the report is uploaded, email EVNTech (<evntech@jive.eu>) to say so.

### Retrieving the clocks used in correlation

A `klx{band}` file with the history of clocks used during correlation is usually in the
experiment folder on ccs. If it is not:

```bash
ssh jops@ccs
/ccs/bin/clocks {EXP} > {EXP}.clocks     # it prints to stdout by default
```

---

## Appendix A — driving `jplotter` by hand

If `standardplots` does not give you what you need, these are the commands `postprocess`
itself issues, so the plots come out identical. Start with:

```bash
jplotter
```

```
ms {exp}.ms
indexr
r                 # frequency setup, stations, sources, times
```

**Weights vs time**

```
bl auto; fq */p
src none
time none
ch mid
pt wt
new all f bl t
y global
sort bl
refile {exp}-weight.ps/cps
wt 0.1
pl
```

**Autocorrelations, amplitude vs frequency** (pick a scan with a strong source; `listr` lists
them)

```
bl auto
fq */p; ch none
avt scalar; avc none
time none
pt ampfreq
y 0 2
scan mid-30s to mid+30s where scan_number={N}
new all false bl true sb false time true
multi true
sort bl
ckey p[rr]=2 p[ll]=3 p[rl]=4 p[lr]=5 p[none]=1
nxy 2 4
refile {exp}-auto-scan{N}.ps/cps
pl
```

**Cross-correlations to the reference antenna, amplitude and phase vs frequency**

```
bl {refant}* -auto
fq *; ch none
avt vector; avc none
pt anpfreq
y local
scan mid-30s to mid+30s where scan_number={N}
new all false bl true sb false
multi true
sort bl
ckey p[rr]=2 p[ll]=3 p[rl]=4 p[lr]=5 p[none]=1
nxy 2 4
refile {exp}-cross-scan{N}.ps/cps
pl
```

**Amplitude and phase vs time, all baselines to the reference antenna**

```
bl {refant}* -auto
fq {sb}/p; ch 0.1*last:0.9*last
new all f bl t
avt none; avc vector
pt anptime
y local
src none
time none                      # or: time $start-5m to +55m   for the zoomed version
sort bl
ckey src src[none]=1
ptsz 2
refile {exp}-ampphase-0.ps/cps
pl
```

Then `exit`. The polarization colour scheme
(`ckey p[rr]=2 p[ll]=3 p[rl]=4 p[lr]=5 p[none]=1`) is the JIVE standard: single-polarization
data comes out black.

---

## Appendix B — tools, and what happened to the old ones

**Available on eee** (`/home/jops/opt/evn_support`, `/home/jops/opt/bin`, `/usr/local/bin`,
and the `pyjops` environment):

`postprocess`, `mstools`, `getdata.pl`, `j2ms2`, `tConvert`, `checklis.py`, `expname.py`,
`standardplots`, `jplotter`, `ysfocus.py`, `flag_weights.py`, `polswap.py`, `scale1bit.py`,
`polconvert.py`, `idi2ms.py`, `append_antab_idi.py`, `append_tsys.py`, `append_gc.py`,
`check_antab_idi.py`, `check-multipart-fits.py`, `compare-ms-idi.py`, `auth_pipe.py`,
`archive` / `archive.pl`, `uvflgall.sh`, `uvflg.pl`, `antab_editor.py`,
`antabfs_nominal.py`, `EVN.py`, `comment_tasav_file.py`, `feedback.pl`, `ampcal.sh`,
`pipelet.py`, `gscale2avg.py`, `create_processing_log.py`, `casa`, `gv`.

**On ccs**, in `/ccs/bin` (not in a non-interactive `PATH` — use the full path over ssh):
`showlog`, `showlog_new`, `make_lis`, `checklis`, `clocks`, `log2vex`.

**No longer available, and what replaced them:**

| Old tool | Status |
| --- | --- |
| `antabfs.py` | Not installed. ANTAB creation from log files is done by the stations; use `antab_editor.py`. |
| `antabfs_interpolate.py` | Folded into `antab_editor.py` (Tsys interpolation). |
| `antab_check.py` | Folded into `antab_editor.py` (sanity checks and plots). |
| `parsePIletter.py` | Not installed. Fill the feedback pages by hand (§6.1). |
| `create_nme_report_template.py` | Not installed on eee. Available from the Gitea repository. |
| `jexp` (GUI) | Gone. The `.jex` file is read directly at delivery time; there is no `.expsum` step any more. |
| `find_idi_with_time.py` | Present but **broken**: its shebang points at `/usr/bin/python`, which does not exist. Run it as `python3 /home/jops/opt/evn_support/find_idi_with_time.py …` if you need it. |
| `rename` | Not installed. Use a shell loop, or `zmv` under zsh (see step 9). |
| `create_processing_log.py` | Still works, but `postprocess` keeps the equivalent record itself: `{exp}.toml` (decisions), `logs/commands.sh` (commands), `logs/logging_messages.log` (the run), `notes.md` (summary). |
| `pipe@jop83`, `$IN`, `$OUT` | Gone. The pipeline runs on eee in `pipeline/in` and `pipeline/out` inside the experiment directory. |

---

## Appendix C — quick troubleshooting index

| Symptom | Where to look |
| --- | --- |
| A step failed and you want the exact command it ran | `logs/commands.sh` |
| Full run log, warnings included | `logs/logging_messages.log` |
| `j2ms2` / `getdata.pl` / `tConvert` output | `logs/j2ms2.log`, `logs/getdata.log`, `logs/tconvert*.log` |
| Verification details | `logs/verification.log` |
| Pipeline broke | `pipeline/out/AIPS.LOG` + the ParselTongue output; usually an ANTAB `FREQ` range too narrow |
| Wrong source types / reference antenna | `postprocess edit …`, or `{exp}.toml` |
| Everything postprocess believes about the experiment | `postprocess info`, or `postprocess dashboard` |
| Which steps have run | `postprocess list`, `postprocess last` |
| The run stopped and wrote `REVIEW_REQUIRED` | Read the file: it names the step and the reason. Resolve, then `postprocess run` |
| `command not found` on ccs over ssh | Use the full path `/ccs/bin/…` |
| A tool is installed somewhere else | `EVN_<TOOL>=/path/to/tool`, or an entry in `computers.toml` |
