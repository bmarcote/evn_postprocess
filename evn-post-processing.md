# EVN Post-Correlation Processing

Date: 7 September 2026
Version: 7.0

This guide covers everything to be done with the data of an EVN experiment after
correlation: from the moment the correlation is finished until the data are delivered to
the PI.

There are **two ways to do it, and this guide documents both for every step**:

- **Automatically**, with the `postprocess` program, which walks the whole workflow and
  stops only where a human decision is genuinely needed.
- **Manually**, running the underlying tool of each step by hand. This is what you need
  when a server is down, a step needs a one-off tweak, or something failed and you want to
  understand why.

The two are interchangeable at any point: `postprocess` records every local command it
runs into `logs/commands.sh`, and every step can be resumed from where a manual
intervention left off. There is no "you started manually, so you must finish manually".

> **Convention used throughout**: anything between `{}` must be replaced by the value for
> your experiment. `{exp}` is the experiment name in lowercase (`gs046`), `{EXP}` in
> uppercase (`GS046`), `{YYMMDD}` the observing date.

---

## 1. General information

### 1.1 Computers

| Machine | What lives there |
| --- | --- |
| **eee** (`eee2`) | Where you do essentially everything: the experiment directory, the MS and FITS-IDI files, the EVN Pipeline, the archiving commands. |
| **ccs** | Correlator control files: the `.vix` file and the correlator output. `make_lis`/`showlog_new` live in `/ccs/bin`. |
| **archive** | The EVN Data Archive machine (this replaced the old *jop83*). Holds the `.jex` files with the PI contacts and the per-source protection. |
| **vlbeer** (`vlbeer.ira.inaf.it`) | FTP/SFTP server where the stations upload their `log`, `antabfs` and `uvflgfs` files. |

> **Note on *jop83* and `$IN` / `$OUT`.** The EVN Pipeline no longer runs on a separate
> machine under `$IN`/`$OUT`. It runs on **eee**, inside the experiment directory, in
> `pipeline/in` and `pipeline/out`. If a script or an old note tells you to
> `ssh pipe@jop83`, that machine does not exist any more — see §5.

The host names, users and paths are not hardcoded: they are read from `computers.toml`
(searched in `$XDG_CONFIG_HOME/evn/`, then `~/.config/evn/`, then `~jops/.config/evn/`, or
pointed at with `EVN_COMPUTERS_TOML`). If a step cannot reach a machine, check that file
first.

Support-scientist documentation, issue tracking and the session pages are on the JIVE
RedMine: <https://jrm.jive.nl/projects/science-support>.

### 1.2 V\*x files

`.vex` / `.vax` / `.vixfile` / `.vix` are all the same vex format, at different stages:

- **vex** — produced by SCHED; the file the stations load.
- **vax** — same, with the clock section and the actual epoch added; used for rapid
  correlation control (e.g. FTP fringe tests in NMEs).
- **vexfile** — the output of `log2vex`; may differ slightly from the `.vax`.
- **vix** — the version that went through production correlation (updated clocks, PI-requested
  source coordinates, …). **This is the one post-processing uses.**

**e-EVN note:** there is normally one `.vix` per project, but an e-EVN run correlates several
experiments under a single vex file, named after the *first* experiment of the session. The
whole workflow is aware of this (see the e-EVN notes in the individual steps).

### 1.3 The experiment directory

Everything for one experiment lives in a single directory on eee, conventionally
`/data/exp/{EXP}`. Both the automatic and the manual route use the same layout:

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

- **`logs/commands.sh`** — every local command `postprocess` ran, in order, with the step it
  belonged to, as a runnable shell script. This is the single best answer to "what would I
  have to type to do this by hand?" for *your* experiment, with the real file names filled
  in. Read it whenever this guide's generic `{exp}` form is not enough.
- **`logs/logging_messages.log`** — the full run log, including the warnings and errors that
  scrolled past.

---

## 2. The automatic route: `postprocess`

### 2.1 Running it

```bash
ssh -Y jops@eee                 # or your own account
mkdir -p /data/exp/{EXP}
cd /data/exp/{EXP}
postprocess
```

With no arguments it derives the experiment name from the directory, the support scientist
from the user running it, and runs the whole workflow — continuing from the last step that
finished successfully if it has run before. Use `--expname` / `--supsci` if you are
somewhere else or processing on behalf of somebody else.

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

`postprocess exec` is the bridge between the two routes: it runs a single command with the
parameters already known for this experiment, so you do not have to look up the reference
antenna, the pass names or the calibrator list yourself. `postprocess exec` with no argument
lists everything it can run. See the mapping table in §2.7.

`postprocess edit` fixes the two things most often guessed wrong:

```bash
postprocess edit refant Ef Mc        # reference antennas, in order of preference
postprocess edit target {SOURCE}     # set a source type (also: phasecal, fringefinder)
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
| `{exp}.toml` | You **and** the program | Source types, PI contacts, and the record of every decision taken (weight threshold, polswap/polconvert/onebit antennas, refant, comments). A run whose `[postprocess]` section is complete repeats itself silently. |
| `computers.toml` | Site config | Host/user/path of ccs, archive, vlbeer. |
| `comms.toml` | You, once | Where notifications go (see §2.5). |
| `policy.toml` | You, for batch runs | The decisions to take without asking (see §2.6). |

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

Source names containing `+`, `-` or `.` must be quoted (TOML bare-key rule). Everything is
optional: what is missing is inferred, asked, or resolved from the policy file.

### 2.4 Operating modes

The mode decides *where inputs come from* and *where results go*. It is auto-detected from
the OS user/group and can be forced with `--mode`:

- **`supsci`** — the JIVE support-scientist job: retrieve from ccs, station files from
  vlbeer, archive and deliver. This is what this guide assumes throughout.
- **`regular`** — everything is already local; nothing is retrieved and nothing is archived.
  `distribute` only verifies that the FITS-IDI files are all there.
- **`sweeps`** — the automated system. Not implemented yet.

### 2.5 Notifications

With a `comms.toml` in place (`./comms.toml`, `~/.config/evn/comms.toml`, or
`~jops/.config/evn/comms.toml`), `postprocess` sends you a message by email or Mattermost
when a step fails, when it pauses for review, and when the run finishes with its summary of
what was spotted. On the shared `jops` account, leave `username` empty and fill the
`[[people]]` table: each experiment then notifies its own support scientist.

Secrets go in the environment rather than the file if you prefer:
`POSTPROCESS_SMTP_PASSWORD`, `POSTPROCESS_MM_TOKEN`.

### 2.6 Unattended (batch) runs

```bash
postprocess --batch --policy /path/to/policy.toml run
```

In batch mode nothing interactive is ever opened. When a decision is needed, the run writes
a `REVIEW_REQUIRED` file in the experiment directory, exits with status 0, and waits to be
resumed by a later `postprocess run`. A minimal policy:

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
| 1. `initialize` | — | `scp` of the `.vix` from ccs |
| 2. `lisfiles` | `makelis`, `getlis`, `modlis` | `make_lis` (ccs), `scp`, `sed` |
| 3. `checklis` | `checklis` | `checklis.py` |
| 4. `j2ms2` | `getdata`, `j2ms2`, `expname`, `metadata`, `lagsnr` | `getdata.pl`, `j2ms2`, `mstools run expname` |
| 5. `standardplots` | `standardplots`, `gv` | `standardplots` / `jplotter`, `gv` |
| 6. `msops` | `ysfocus`, `flag_weights`, `polswap`, `onebit` | `mstools run …` (or `ysfocus.py`, `flag_weights.py`, `polswap.py`, `scale1bit.py`) |
| 7. `tconvert` | `tconvert` | `tConvert` |
| 8. `polconvert` | `polconvert` | `polconvert.py` |
| 9. `post_polconvert` | `postpolconvert` | file renaming, `idi2ms.py` |
| 10. `standardplots2` | `standardplots` | `standardplots` |
| 11. `antab` | `vlbeer`, `uvflg`, `antab` | `scp`/`sftp` from vlbeer, `uvflgall.sh`, `antab_editor.py` |
| 12. `pipeinputs` | `pyinput` | template → `pipeline/in/{exp}.inp.txt` |
| 13. `pipeline` | `pipe` | `EVN.py` |
| 14. `postpipe` | `comment_tasav`, `feedback`, `piletter` | `comment_tasav_file.py`, `feedback.pl` (ported in-tree), PI-letter template |
| 15. `prearchive` | `append` | `append_antab_idi.py` (`append_tsys.py` + `append_gc.py`) |
| 16. `verification` | `verify` | `check_antab_idi.py`, `check-multipart-fits.py`, `compare-ms-idi.py` |
| 17. `distribute` | `auth`, `protect`, `archive-fits`, `archive-pilet`, `nme` | `auth_pipe.py`, `archive.pl` |

### 2.8 When a step fails

1. Read the error in the terminal, then `logs/logging_messages.log` for the context, then
   the step's own log (`logs/j2ms2.log`, `logs/tconvert.log`, `logs/getdata.log`,
   `logs/verification.log`, …).
2. Fix the cause (often: a missing file, a wrong source type, a wrong reference antenna —
   see `postprocess edit`).
3. Re-run just that step: `postprocess run {step}` (it continues to the end), or
   `postprocess run {step} {step}` to run only it, or `postprocess exec {command}` to
   retry a single command in isolation, bypassing the step ordering.

Steps are idempotent where it matters: `j2ms2` skips passes whose MS already exists,
`tconvert` skips passes whose FITS-IDI files exist, `msops` refuses to re-apply itself once
the FITS-IDI files are there, and vlbeer downloads never overwrite a file you have edited
by hand.

### 2.9 Tunables

| Variable | Default | Purpose |
| --- | --- | --- |
| `EVN_SSH_TIMEOUT_S` | 60 | Connect timeout for every SSH/SCP call. |
| `EVN_SCP_TIMEOUT_S` | 600 | Wall-clock timeout for an SCP transfer. |
| `EVN_SSH_RETRIES` | 2 | Retries with backoff on transient SSH failures. |
| `EVN_SSH_BACKOFF_S` | 3.0 | Base backoff between SSH retries. |
| `EVN_MAX_PASS_WORKERS` | min(cpus,16) | Passes worked on at once for in-process (casacore) work. |
| `EVN_MAX_PASS_IO_WORKERS` | 10 | Passes at once for subprocess-per-pass steps (`j2ms2`, `getdata.pl`, `tConvert`). |
| `EVN_<TOOL>` | — | Override the path of an external binary, e.g. `EVN_TCONVERT=/opt/…/tConvert`. Otherwise `computers.toml`, then `$PATH`. |

---

## 3. Get the data and inspect it

### Step 1 — `initialize`: the experiment directory and the vex file

**What it does.** Creates the directory structure (`logs/`, `plots/`, `pipeline/{in,out}`,
`antenna_files/`), gets the `.vix` file, and derives *all* metadata from it: observing date
(`$EXPER exper_nominal_start`), e-EVN membership (`exper_description`), stations, sources and
scans. It then applies `{exp}.toml` (source types, PI, support scientist) and classifies
heuristically any source still untyped.

**Automatic**

```bash
cd /data/exp/{EXP}
postprocess run initialize
```

**Manual**

```bash
scp jops@ccs:/ccs/expr/{EXP}/{exp}.vix .
ln -s {exp}.vix {EXP}.vix
```

For an e-EVN experiment `{EXPn}` (n>1), the vex file is the one of the *first* experiment of
the run — copy that one and keep its name.

**What to check.** `postprocess info` should show the right observing date, the full station
list, and sensible source types. Heuristic guesses are marked `guessed = true` in
`{exp}.toml` and are meant to be corrected.

**What can fail.**

- *Wrong or missing source types.* This propagates all the way into the pipeline input file
  and the plots. Fix with `postprocess edit target {SRC}` / `phasecal` / `fringefinder`, or
  edit `{exp}.toml` directly.
- *No `.vix` on ccs.* For an e-EVN run you are looking under the wrong experiment name; use
  the first experiment of the session.
- *`FileNotFoundError: computers.toml`.* The server configuration is not where the program
  looks — see §1.1.

> **Note.** There is no longer a `MASTER_PROJECTS.LIS`, `.jexp` or `.expsum` lookup at this
> stage: everything comes from the vex file plus `{exp}.toml`. The `.jex` file (PI contacts
> and per-source protection) is read later, at delivery time.

### Step 2 — `lisfiles`: create and retrieve the lis files

**What it does.** Creates the `.lis` file(s) on ccs if they are not there, copies them over,
and rewrites their header so the FITS-IDI basename is `{exp}_1_1.IDI` instead of
`{exp}.ms.UVF`. For e-EVN, the files are renamed and re-pointed to this experiment.

**Automatic**

```bash
postprocess run lisfiles
# or a single piece:
postprocess exec makelis     # create them on ccs
postprocess exec getlis      # copy them here
postprocess exec modlis      # fix the header (FITS-IDI basename, correlator passes)
```

**Manual — with the GUI**

```bash
ssh -Y jops@ccs
cd /ccs/expr/{EXP}
/ccs/bin/showlog_new {EXP}
```

In the window, check which runs are `PRODUCTION` (there will usually be `CLOCK SEARCH` runs
too). If any run shows `UNKNOWN` status (only happens in e-EVN), mark it `GOOD` from
*Inspect Data* with the bottom-right options. Then switch to *ExportFile*, verify all the
scans you want carry a `+`, click *ExportFile* again and save the lis file into the `{EXP}`
directory.

For multi-phase-centre or pulsar-binning experiments, the *save* dialog offers a name like
`{exp}*{key}.lis` — **do not change the `{key}` part**: it is expanded automatically into one
lis file per phase centre (or per pulsar bin).

**Manual — without the GUI**

```bash
ssh jops@ccs
cd /ccs/expr/{EXP}
/ccs/bin/make_lis -e {exp} -p {profile} -s {exp}.lis
```

`-s` names the output file. `-p` selects jobs by profile (`prod`, or `prod_cont` /
`prod_line` for spectral-line experiments); omitted, it takes all production jobs and skips
the clock-search ones. For multi-phase-centre experiments, `-m {SRC}` keeps the calibrator
data in the pass of one source instead of repeating them in every phase centre — this is
what `postprocess` adds automatically above 5 phase centres in a scan.

Then, on eee:

```bash
scp jops@ccs:/ccs/expr/{EXP}/{exp}\*.lis .
sed -i 's/{exp}.ms.UVF/{exp}_1_1.IDI/g' {exp}.lis
```

**What to check.** Open the lis file. The **first line** must name the correct `.vix` file and
the correct output MS (normally `{exp}.ms`). The **last entry of the header** must be the
FITS-IDI basename `{exp}_1_1.IDI`, and it must be *unique across passes* (see the naming
rule in step 7). You may also want to change the reference station used to define the
frequency setup.

Spectral-line, multi-phase-centre and pulsar-binning experiments need several lis files,
producing several MS files (e.g. a continuum pass and a line pass).

**What can fail.** `/ccs/bin` is not in a non-interactive `PATH`, so `ssh jops@ccs make_lis …`
silently fails with "command not found" — use the full path, as above.

### Step 3 — `checklis`: validate the lis files

**What it does.** Checks each lis file for duplicated or missing scans, and that all passes
have unique `.lis` / MS / FITS-IDI names. It also extracts the list of correlator passes that
the rest of the workflow works on.

**Automatic**

```bash
postprocess run checklis        # or: postprocess exec checklis
```

**Manual**

```bash
checklis.py {exp}.lis           # on eee
# or, on ccs:  /ccs/bin/checklis {exp}.lis
```

**What to check / what can fail.** This step is a gate: it fails on duplicated or missing
scans, and on two passes that would write to the same MS or FITS-IDI name. Both are fixed by
editing the lis file (or by re-exporting it from `showlog_new`). Do not skip past a failure
here — a duplicated scan silently corrupts everything downstream.

### Step 4 — `j2ms2`: get the correlator output and make the MS

**What it does.** Fetches the correlator output for the jobs listed in the lis file, converts
it to a Measurement Set, fixes the project name for e-EVN, reads the metadata (antennas,
sources, frequency setup) back from the MS, and — unless `--no-lag` — builds an auxiliary
lag-space MS from which it computes a per-scan, per-antenna, per-polarization SNR. That SNR
is what lets step 6 decide by itself which antenna has swapped polarizations and when.

**Automatic**

```bash
postprocess run j2ms2
# or the individual pieces:
postprocess exec getdata
postprocess exec j2ms2
postprocess exec expname     # e-EVN only
postprocess exec metadata
postprocess exec lagsnr
```

**Manual**

```bash
getdata.pl -proj {EXP} -lis {exp}.lis
j2ms2 -v {exp}.lis fo:nosquash_source_table
```

By default `j2ms2` keeps the full antenna table from the vex file but reduces the source
table to the sources that actually have data in this MS. That is what you want for most
experiments, and in particular for e-EVN, where the vex file carries the sources of *other*
experiments too.

`fo:nosquash_source_table` keeps the *full* source table instead. `postprocess` adds it for
non-e-EVN experiments so that, in a multi-phase-centre run, the calibrator information
survives into every phase centre. For an e-EVN experiment it is deliberately **not** added —
you do not want the other experiments' sources in your MS. If you are running by hand, make
the same choice.

**e-EVN.** Only the first experiment's name propagates through the correlator jobs, so the
project name in the MS of every other experiment has to be corrected:

```bash
mstools run expname {exp}.ms {EXP}
# equivalently, the standalone script:
expname.py {exp}.ms {EXP}
```

**What to check.** `postprocess info` afterwards shows the antennas, the sources, the
frequency setup, and the scan overview with the per-antenna SNR. Antennas with no data
anywhere, or a frequency setup that does not match what was scheduled, are worth chasing
before going any further.

**What can fail.**

- *Not enough disk space.* The step estimates the size from the correlator output and stops
  before starting. Free space or move the experiment to a bigger volume.
- *A missing job in the correlator output.* `getdata.pl` reports it; the fix is on ccs (the
  job was not correlated, or the lis file names a job that does not exist).
- *`j2ms2` fails for one pass.* Its output is in `logs/j2ms2.log`. The other passes still
  finish; re-run the step and only the missing MS is rebuilt.

### Step 5 — `standardplots`: inspect the data

**What it does.** Produces the JIVE standard plots, converts them to PNG for the dashboard,
and opens them for you.

**Automatic**

```bash
postprocess run standardplots
postprocess exec standardplots      # just re-make the plots
postprocess exec gv                 # just open them
postprocess dashboard               # look at them in a browser (prints the SSH tunnel command)
```

**Manual**

```bash
standardplots -weight [-scan {scan_no}] {exp}.ms {refant} {calsrcs}
```

where `-scan` optionally forces a reference scan instead of the automatically chosen ones,
`{refant}` is the two-letter reference antenna code, and `{calsrcs}` is one or more
comma-separated calibrator names (matched exactly). `standardplots -h` for the rest.

**The plots, and what to look for in each.**

| File | What it shows | What to check |
| --- | --- | --- |
| `{exp}-weight.ps` | Weight vs time, per antenna | Weights should sit at ~1. Note the lowest one: it sets the flagging threshold in step 6 (typically 0.9). |
| `{exp}-auto-scan{N}.ps` | Autocorrelations, amplitude vs frequency, per antenna | Bandpasses should be roughly flat. L band will show RFI. A missing or flat-zero band means the antenna did not record that subband. |
| `{exp}-cross-scan{N}.ps` | Cross-correlations to the reference antenna, amplitude and phase vs frequency | Fringes present? Both polarizations comparable? A station with signal in RL/LR instead of RR/LL has **swapped polarizations** (step 6). A station with structure in only one polarization pair may have recorded **linear** polarization (step 8). |
| `{exp}-ampphase-0.ps` | Amplitude and phase vs time, all baselines to the refant, whole experiment | Which antennas dropped out, when, and for how long. |
| `{exp}-ampphase-1.ps` | The same, zoomed on the first hour | The detailed time behaviour at the start. |
| `{exp}-amptime-scan{N}.ps` | Autocorrelation amplitude vs time around a scan | Level changes, dropouts. **Not produced by the standard run** — make it by hand from `jplotter` (Appendix A) when you need it. |

The `.ps` files stay in the experiment directory; PNG copies (one per page) go to `plots/`
and are what the dashboard shows. `postprocess` makes the weight plot by default (for the
first correlator pass only); the standalone `standardplots` needs `-weight` for it.

**What can fail.** If `standardplots` produces nothing useful (a source name that matches no
scan, a reference antenna absent from the calibrator scans), the fallback is to drive
`jplotter` by hand — see Appendix A, which reproduces exactly the commands `postprocess`
issues. Note that the reference antenna is chosen *per scan*: if your preferred one is
missing from a scan, another is picked automatically and said so in the log.

### Step 6 — `msops`: fix the Measurement Sets

**What it does.** Applies, in this order and stopping at the first failure:

1. `ysfocus` — corrects the mount type of **Yebes** (Nasmyth) and **Hobart** (X-Y east-west) in
   the MS `ANTENNA` table, so `tConvert` writes the right `MNTSTA` and the parallactic-angle
   correction is right.
2. `flag_weights` — flags visibilities whose weight is below the threshold.
3. `polswap` — swaps the polarizations of antennas that labelled them the wrong way round.
4. `onebit` — scales data recorded with 1 bit to the usual 2-bit level (quantization
   correction).

**How the parameters are decided.** In this precedence order: the `[postprocess]` section of
`{exp}.toml` → the lag-MS diagnostics, when they are confident → an interactive dialog (or the
dashboard / a Mattermost reply) → in batch mode, the policy file, or a `REVIEW_REQUIRED` stop.
Whatever path decided them, they are written back into `{exp}.toml`, so a re-run applies them
silently.

For `polswap` in particular, the program works out **when** each antenna was swapped, from the
per-scan lag SNRs (a swapped antenna carries its signal in RL/LR instead of RR/LL). Stations
often fix the swap partway through a run, so swapping the whole observation would corrupt the
part that was already correct:

- swapped in every checked scan → the whole observation;
- exactly one change of state → everything before (or after) the scan where it changed;
- no scan swapped, or several changes of state → a warning in the log, and the whole
  observation is swapped for you to review.

**Automatic**

```bash
postprocess run msops
# or individually:
postprocess exec ysfocus
postprocess exec flag_weights
postprocess exec polswap
postprocess exec onebit
```

**Manual**

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

`-t1/-t2` take `YYYY/MM/DD/hh:mm:ss` or `YYYY/DOY/hh:mm:ss` — use them when a station fixed
its swap partway through the run.

**What to check.**

- The weight threshold: 0.9 is the default and is right most of the time, but look at the
  weight plot first. `postprocess` warns explicitly about antennas whose weights look
  unexpectedly low.
- **Write down how much data `flag_weights` flagged** — it goes into the PI letter. It is
  recorded automatically in `{exp}.toml` as `flagged_percent`.
- After a polswap, re-check the cross plots: the signal must have moved into RR/LL.

**What can fail.**

- *msops silently does nothing.* If the FITS-IDI files already exist, the step assumes the
  operations were already applied and skips — this is deliberate, since they modify the MS in
  place and are not idempotent. To genuinely redo them, delete the FITS-IDI files first.
- *The wrong antenna gets swapped.* The lag diagnostics are a heuristic. Check the plots, and
  put the right answer in `{exp}.toml` (`polswap = [...]`) before re-running.
- *`BatchInteractionError` in batch mode.* The decisions are not in the TOML and the
  diagnostics were not confident: review the plots and write the values into `{exp}.toml`,
  or run without `--batch`.

> **Linear polarizations** are *not* fixed here. That conversion happens after the FITS-IDI
> files exist — see step 8.

#### Update the PI letter as you go

`postprocess` writes the PI letter from a template after the pipeline (step 14), but the
things that go into it are noticed *here*. Keep a note of:

- the weight threshold used, and how much data `flag_weights` flagged;
- per station: anything that happened during observation, correlation or post-processing;
- the trailing letter of the experiment name in the acknowledgement section, if it has to go.

Everything you put in the `[comments]` section of `{exp}.toml` (or type in the dashboard's
Comments tab) ends up in the letter.

### Step 7 — `tconvert`: create the FITS-IDI files

**What it does.** Converts each correlator pass from MS to FITS-IDI, several passes at once
(bounded by `EVN_MAX_PASS_IO_WORKERS`, default 10). Passes whose FITS-IDI files already exist
are skipped, so the step is safe to re-run. A pass that fails does not abandon the others;
every failure is named together at the end.

**Automatic**

```bash
postprocess run tconvert        # or: postprocess exec tconvert
```

**Manual**

```bash
tConvert -v {exp}.lis -o {chunksize}
```

`postprocess` drives `tConvert` from the lis file (which already names the output basename)
and computes the chunk size so the result fits on the current disk. The older form
`tConvert {exp}.ms {exp}_1_1.IDI` also works if you prefer to name the files explicitly.

**The FITS-IDI naming rule.** `{exp}_{corr}_{pass}.IDI{chunk}` where:

- **corr** (1..*n*) — different correlations: continuum vs spectral line, different phase
  centres, pulsar bins.
- **pass** (1..*n*, normally 1) — correlation passes for experiments using two heads, or too
  many lags/subbands to correlate in one go. These can generally be glued back together with
  `VBGLU` in AIPS.
- **chunk** — a large pass is split over `.IDI1`, `.IDI2`, … Only `.IDI1` carries the Tsys and
  gain-curve tables (step 15).

Names must be **unique across passes**; step 3 enforces this.

**What to check.** One set of FITS-IDI files per correlator pass, with sensible sizes. When a
single pass is converting, `tConvert`'s output is on the terminal; from two upwards each pass
writes to its own `logs/tconvert.log` sibling (`tconvert-2.log`, …), each naming the lis file
it ran. Above 5 passes a progress bar replaces the silence.

**What can fail.** *"would not fit in the current directory"* — the step refuses to start
rather than half-convert. Free space first. A pass that fails mid-way leaves partial files:
delete them before re-running, or the step will think that pass is done.

### Step 8 — `polconvert`: linear → circular polarization (only if needed)

EVN observations are always in circular polarization. Occasionally a station records linear
instead, and those data have to be converted. **PolConvert** (Martí-Vidal et al. 2016, A&A
587, A143) derives the conversion from a bright source and applies it to the whole dataset.
It works on the FITS-IDI files, not on the MS — which is why this step comes after step 7.

This step runs **only** if an antenna is flagged for it (`polconvert = ["Kt"]` in
`{exp}.toml`, or the decision taken at step 6).

**Automatic**

```bash
postprocess run polconvert        # or: postprocess exec polconvert
```

`postprocess` prepares the input file itself and searches a scan × time-range × parameter grid
until it finds a working solution, announcing the full inputs of every attempt:

```
PolConvert --compute [attempt 7]: scan No0018 (J1927+6117), 17:00:00 - 17:05:00,
refant=MC, linants=['EF'], exclude=['WB'], IFs=[1, 2, 3, 4], ref_idi=ez041a_1_1.IDI1,
doweight=0.01, timeavg=20s, chanavg=8.
```

**Manual**

```bash
cp /home/jops/opt/evn_support/polconvert_inputs.toml .
# edit it: the comments explain every field
polconvert.py polconvert_inputs.toml
# --compute  : only derive the solutions
# --apply    : only apply them
```

`postprocess` writes exactly this file (`polconvert_inputs.toml` in the experiment directory)
before every attempt, and records the command it used, so when the automatic search gives up
you can adjust that file and re-run `polconvert.py` on it by hand.

> `polconvert.py -h` still calls the input file `.ini`; the template it ships is
> `polconvert_inputs.toml`. Either name works — the format is TOML.

**How to read the result.** The clearest signal is the per-IF fringe-SNR table `polconvert.py`
prints when it finishes. A converted IF puts its power into RR/LL and leaves RL/LR small; a
failed one leaves all four comparable (IF5 below):

```
        PolConvert fringe SNR  ·  SCAN 0 EF-JB

  Pol   IF1   IF2   IF3   IF4   IF5   IF6   IF7   IF8
 ─────────────────────────────────────────────────────
  RR    265   307   208   285   326   395   229   236
  LL    230   273   287   365   390   209   224   316
  RL     21     6     7     8   287    26    13    21
  LR      7    18     7    11   260    12    25    14
```

If PolConvert dies before printing it, `postprocess` renders the same table itself from
whatever `FRINGE.PEAKS` files were written, so even a crash says something.

**What can fail.**

- *Only `nan` in the solutions.* It is finding no data in the parameter space you gave it:
  wrong scan, wrong time range, wrong IFs, or a reference antenna that is not in that scan.
- *Segmentation fault.* Common, and often *after* all files have been processed — check
  whether `{exp}_*_1.IDI*.PCONVERT` files exist before assuming it failed. `postprocess` runs
  PolConvert as a child process precisely because a `SIGSEGV` cannot be caught, and retries.
- *`rc=127` / `symbol lookup error … libqsvgicon.so`.* matplotlib's Qt backend aborting before
  anything was computed. Run with `MPLBACKEND=Agg`, which is what `postprocess` does.
- *Antenna-name case.* PolConvert matches the FITS-IDI `ANTENNA`/`ARRAY_GEOMETRY` tables
  literally, which hold `EF`, `JB`, `MC` — uppercase. Write them uppercase in the input file.

**Remember to mention it in the PI letter.** The conversion is transparent to the user, so a
note belongs in "Further remarks":

> Note that "ANTENNA NAME" ("XX") originally observed linear polarizations, which were
> transformed to circular ones during post-processing using the PolConvert program
> (Martí-Vidal et al. 2016, A&A 587, A143). Thanks to this correction, you can automatically
> recover the absolute EVPA value when using "XX" as reference station during fringe-fitting.

### Step 9 — `post_polconvert`: put the converted files in place

**What it does.** Backs up the original FITS-IDI files into `idi_ori/`, then renames the
`*.PCONVERT` files to the standard FITS-IDI names. For an e-EVN run, this is also where the
completion marker `{exp}.fitsidi_ready` is written, which the other experiments of the run
wait on at step 11.

**Automatic**

```bash
postprocess run post_polconvert     # or: postprocess exec postpolconvert
```

**Manual**

```bash
mkdir -p idi_ori && mv {exp}_1_1.IDI[0-9]* idi_ori/
for f in {exp}_1_1.IDI*.PCONVERT; do mv "$f" "${f%.PCONVERT}"; done
```

(There is no `rename` command on eee; `zsh` users can use
`zmv '{exp}_1_1.IDI(*).PCONVERT' '{exp}_1_1.IDI$1'`.)

To verify the conversion visually, convert back to an MS and re-plot:

```bash
idi2ms.py [-d] {exp}-pconv.ms '{exp}_1_1.IDI*'
standardplots [-scan {scan_no}] {exp}-pconv.ms {refant} {calsrcs}
```

(The quotes around the wildcard are required.) Check that the previously linear antenna now
shows its signal in RR/LL. Keep the pre-conversion files until you are satisfied.

### Step 10 — `standardplots2`: re-plot what will actually be delivered

**What it does.** Re-runs the standard plots on the post-msops / post-PolConvert data, so the
plots that get archived match the data that get archived. Same command as step 5.

```bash
postprocess run standardplots2      # or: postprocess exec standardplots
```

**What to check.** This is your last look at the data before they leave: weights flagged as
intended, polarizations right, no antenna accidentally lost.

---

## 4. ANTAB, UVFLG and the EVN Pipeline

### Step 11 — `antab`: station files, a-priori flags, and the ANTAB file

**What it does**, in order:

1. **e-EVN barrier (a)**: for a multi-experiment run, the leader (EXP1) waits until every
   sibling has written its FITS-IDI completion marker, so one `antab_editor` session can
   cover the whole run.
2. Downloads the station `.log`, `.antabfs` and `.flag` files from vlbeer into
   `antenna_files/`. Files already present are **never** overwritten, so hand-edited
   `.antabfs` files survive a re-run; genuinely new uploads are still picked up.
3. Creates the `.uvflg` files from the `.log` files.
4. Prints (and notifies) the **station summary**: who did not observe, who missed time ranges,
   who recorded reduced bandwidth.
5. Symlinks the `.vix` into `antenna_files/` so the schedule sits beside the station files.
6. Opens `antab_editor.py` for you.

Then it asks whether to continue to the pipeline or stop to fix something first.

**Automatic**

```bash
postprocess run antab
# or individually:
postprocess exec vlbeer     # download the station files
postprocess exec uvflg      # create the .uvflg
postprocess exec antab      # open antab_editor.py
```

**Manual**

```bash
cd /data/exp/{EXP}/antenna_files
sftp evn@vlbeer.ira.inaf.it          # alias on some accounts: sftpvlbeer
  cd vlbi_arch/{monthYY}             # {yy}{Mon}, e.g. 26Jun
  mget {exp}*.log {exp}*.antabfs {exp}*.uvflgfs
  bye

uvflgall.sh                          # runs uvflg.pl on every .log in the directory
cat *uvflgfs > {exp}.uvflg
ln -s ../{EXP}.vix {EXP}.vix
antab_editor.py
```

`uvflgall.sh` also patches the known `flagr,0` / `flagr,-.00` bug in the log files before
running `uvflg.pl` on each of them. To do one station only: `uvflg.pl {exp}{ant}.log`.

**`antab_editor.py`** is the tool for everything ANTAB. It shows the gain curves, lets you
edit each station's ANTAB, interpolate sparse Tsys measurements, and create nominal files for
missing stations. It reads the FITS-IDI files to verify there is a Tsys measurement for at
least every scan (slow the first time, cached afterwards).

```bash
antab_editor.py                          # in antenna_files/, infers the experiment
antab_editor.py -e {EXP} -l -a EXP2 [EXP3 …]
#  -l / --spectralline : spectral-line experiment (produces continuum + line ANTAB files)
#  -a EXP2 …           : also read the FITS-IDI of the other e-EVN experiments of the run
```

Save the per-station corrected files (they get an `.editor` extension, so the originals stay
recognisable) or, in the first tab, the combined `{exp}.antab`. That combined file, plus
`{exp}.uvflg`, are what the pipeline needs in `pipeline/in/`.

**High-frequency (K/Q band) experiments.** Some stations write `, opacity_corrected` into the
`POLY =` line of their ANTAB, which is not standard and which `antab_editor.py` cannot parse.
`postprocess` strips it automatically (leaving an `! opacity_corrected` comment as the
record); by hand, `grep opacity *.antabfs` and remove it. Either way, report it in the PI
letter under "Further remarks":

> The following antennas provided opacity-corrected gain calibration: ANTENNA1, ANTENNA2, …

**Globals (VLBA stations).** NRAO publishes the calibration at
<http://www.vlba.nrao.edu/astro/VOBS/astronomy/>; download `{exp}cal.vlba`, which holds the
flag tables, the antab and the weather information. The same files are on ccs under
`/ccs/var/log2vex/logexp_date/{EXP}_{YYYYMMDD}/`. `postprocess` fetches them automatically
when it detects VLBA/GBT antennas. The VLBA antab typically lacks the `GAIN` and `INDEX`
headers; the gains come from `/data0/tsys/gbt_gains.key` on eee. Copy both files into
`antenna_files/` and `antab_editor.py` parses them into per-station `.antabfs` files.

**If the VLA is in the array**, its individual antab should be in that same directory. If it
is not, contact the VLBA data analysts at <vlbiobs@nrao.edu>.

**What to check.** A careful look at all these files is mandatory. Pay attention to the
frequency ranges (`FREQ`) and the column indexes in the headers, and to suspicious Tsys
numbers. Check that all the `.uvflgfs` files are roughly the same size — one much smaller
than the rest usually means its log file was not interpreted correctly.

**What can fail.**

- *A station has not uploaded its `.antabfs`.* Ask them by email. In the meantime,
  `antab_editor.py` can generate a nominal file from the EVN Status Table SEFDs. Some
  stations (Arecibo, the KVN) use slightly different file names — check before concluding
  the file is missing.
- *Sparse Tsys (typically the Russian stations and Urumqi).* The measurements are taken only
  during calibrator scans or gaps, so every other scan gets flagged by the pipeline, which
  requires at least one Tsys per scan. Interpolate them in `antab_editor.py` (this is what
  the old `antabfs_interpolate.py` did) and check that outliers were ignored.
- *The step stops waiting for the other e-EVN experiments.* That is the barrier working as
  intended: process the siblings, then `postprocess run` again.

### Step 12 — `pipeinputs`: the pipeline input file

**What it does.** Copies the `.antab` and `.uvflg` files from `antenna_files/` into
`pipeline/in/` (fanning them out per pass for multi-pass experiments) and writes
`pipeline/in/{exp}.inp.txt` from the template, filling in: the AIPS user number, the reference
antenna, the bandpass (fringe-finder) sources, the phase-reference/target pairs, the solution
interval, and the primary-beam / all-sources settings for multi-phase-centre runs.

This is a separate step from running the pipeline precisely so you can read and edit the
input file first.

**Automatic**

```bash
postprocess run pipeinputs      # or: postprocess exec pyinput
```

**Manual.** Copy the template into `pipeline/in/` and edit it. The `tmask` parameter selects
which parts run:

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

**What to check.** The source lists (target / phase-reference / bandpass) and the reference
antenna. A wrong source classification at step 1 shows up here as an empty or nonsensical
list. Fix it with `postprocess edit` and re-run the step, or edit the `.inp.txt` directly.

### Step 13 — `pipeline`: run the EVN Pipeline

**Automatic**

```bash
postprocess run pipeline        # or: postprocess exec pipe
```

**Manual**

```bash
cd /data/exp/{EXP}
export PIPEFITS=$PWD            # where the FITS-IDI files are
cd pipeline/in
EVN.py {exp}.inp.txt            # or {exp}_1.inp.txt, {exp}_2.inp.txt … for multiple passes
```

Multi-pass experiments are pipelined one process per pass, in parallel. All output lands in
`pipeline/out/`.

**What can fail.** If the pipeline breaks early, look at `pipeline/out/AIPS.LOG` and the
ParselTongue output. The classic cause is an **ANTAB whose declared frequency range is
narrower than what was actually recorded**, leaving some channels/IFs without calibration.
The pipeline log names the antenna; fix the `FREQ` values in the ANTAB and re-run.

### Step 14 — `postpipe`: diagnostics, feedback page and PI letter

**What it does.** Creates the `.tasav.txt` (in `pipeline/in/`) and `.comment` (in
`pipeline/out/`) files, generates the pipeline feedback HTML page, and writes the PI letter
from its template. Then it opens the dashboard so you can review the pipeline output, and
**this is the normal review pause of the run**.

**Automatic**

```bash
postprocess run postpipe
# or individually:
postprocess exec comment_tasav
postprocess exec feedback
postprocess exec piletter
```

**Manual**

```bash
comment_tasav_file.py [-l] -oc pipeline/out -ot pipeline/in '{exp}'
feedback.pl -exp '{exp}' -jss '{your_surname}' [-source 'src1 src2 …'] [-nme=T]
```

`{exp}` is the *pass* name: `{exp}`, or `{exp}_1` / `{exp}_2` for a continuum and a spectral
line pass. It is case-insensitive. `-l` marks a spectral-line pass.

> The `-oc` / `-ot` options are **required** when running `comment_tasav_file.py` by hand:
> its defaults still point at `$IN`/`$OUT`, which no longer exist. `postprocess` uses its own
> in-tree implementation and writes to the right places. `feedback.pl` has likewise been
> ported in-tree, but the Perl script still works.

**Check the `.comment` file carefully**, especially the frequency setup — it can get it wrong
in a few particular cases.

**Reviewing the pipeline output.** Open the generated HTML page (`pipeline/out/{exp}.html`,
or through the dashboard) and go through:

- *Uncalibrated amplitude and phase vs frequency channel* — do you see fringes for all
  stations, at least on the fringe-finders and the phase calibrator?
- *Telescope sensitivities* — is there Tsys for every station, every time range, every subband?
- *Fringe-fit (delay and rate) solutions* — are they consistent across stations?
- *Telescope bandpasses* — roughly flat?
- *Calibrated amplitude and phase vs time* — do all stations have data? Did you lose one?
- *Calibrated amplitude and phase vs frequency* — flat phases? sensible amplitudes?
- *Statistical summary* (amplitude corrections from the fringe-finders) — corrections around
  1 with small median errors? A station with a large correction factor needs a closer look at
  its data and its ANTAB.

**Fixing what the pipeline reveals.**

- *A large gain correction for one antenna.* Confirm it with `gscale` in `Difmap` on a
  fringe-finder or the phase calibrator (`gscale2avg.py` reformats the Difmap output per
  antenna and gives you the average factor). Then find `GAIN {ANT}` in the `.antab` file and
  change the `FT=` value a few lines below to the **square** of the correction. Re-run the
  pipeline.
- *A significant fraction of bad data* (e.g. one polarization broken). Add the flags by hand
  to `pipeline/in/{exp}.uvflg` and re-run the pipeline with `tmask = 2`.

**The review pause.** When the run pauses here it prints the options: press *Enter* to
finalize (run the remaining steps and deliver), type a **step name** to re-run from it
(`pipeline` and `postpipe` are offered first, since those are the two you usually want), or
type `quit` to stop and resume later with `postprocess run`. In batch mode it writes
`REVIEW_REQUIRED` and exits 0 instead.

**Update the PI letter here.** The letter is regenerated from the template every time (the
previous version is kept as `.bak`), so write your remarks into `{exp}.toml`'s `[comments]`
section or the dashboard's Comments tab rather than editing the file directly. Remember the
letter goes to astronomers: keep the language non-technical.

---

## 5. Deliver the data

### Step 15 — `prearchive`: put the Tsys and gain curves into the FITS-IDI files

**What it does.** Appends the `SYSTEM_TEMPERATURE` and `GAIN_CURVE` tables from the final
`.antab` file(s) into the FITS-IDI files, and records the finalisation state (final antab and
polconvert input files, flagged-data percentage) into `{exp}.toml`.

**Automatic**

```bash
postprocess run prearchive      # or: postprocess exec append
```

**Manual**

```bash
append_antab_idi.py                                   # in the {EXP}/ directory
append_antab_idi.py --antab pipeline/in/{exp}.antab   # a specific ANTAB file
append_antab_idi.py --fits '{exp}_*_1.IDI*'           # specific FITS-IDI files (quote the wildcard!)
append_antab_idi.py --replace …                       # overwrite tables already present
```

It finds the `.antab` file(s) in `pipeline/in/` and runs `append_tsys.py` and `append_gc.py`
underneath. Only the `.IDI1` chunk of each pass carries these tables.

**What to check.** The step verifies the result itself, but you can check independently:

```bash
check_antab_idi.py [--fits '{exp}_*_1.IDI*']
```

**What can fail.** *"No ANTAB files found in pipeline/in"* — the antab step did not copy them,
or you saved only per-station `.editor` files instead of the combined `{exp}.antab`. *"The
Tsys/GC could not be imported"* — normally an ANTAB/FITS-IDI mismatch: a station in the ANTAB
that is not in the data, or the wrong pass's ANTAB against the wrong pass's FITS-IDI.

### Step 16 — `verification`: the last gate

Three independent, read-only checks on the finished FITS-IDI files. Any failure stops the run
before `distribute`, so an incomplete dataset cannot be archived. The full output goes to
`logs/verification.log`.

```bash
postprocess run verification    # or: postprocess exec verify
```

**1. The ANTAB tables really got in.**

```bash
check_antab_idi.py [--fits '{exp}_*_1.IDI*']
```

**2. No data lost between the FITS-IDI chunks.** A pass split over `.IDI1`, `.IDI2`, … is
checked by comparing the end time of each chunk with the start time of the next:

```bash
check-multipart-fits.py '{exp}_1_1.IDI*' '{exp}_2_1.IDI*'
```

It prints a line only where something is off, e.g.
`es124_1_1 loss=1.9999891519546509s gain=0.0s nZero=0`. A **loss under 10 s** is the normal
rounding of one integration at a chunk boundary and passes; more than that means visibilities
were silently dropped, and the step fails. Overlapping chunks (`gain`) and zero timestamps
(`nZero`) are logged as warnings — worth a look, but not missing data.

**3. The FITS-IDI still holds what the MS had.** Per pass, exposure time, weight and number of
visibilities are accumulated per (baseline, source) on both sides and compared:

```bash
compare-ms-idi.py --ms {exp}.ms --idi '{exp}_1_1.IDI*'
```

The tool reports every pair that differs *anywhere*, which on a perfectly good conversion is
most of them:

```
('EfEf', 'Ic2810nuc') :
      15394.0000s wgt=245480.8214   7697 times in   MS: es124-IC2810NUC.ms
      15394.0000s wgt=243547.6475   7697 times in  IDI: : es124_2_1.IDI*
Checked 2 data sets, 225 common keys with 196 problems identified
```

Those 196 "problems" are not problems: the **seconds and the visibility counts match**, and
only the weight differs, because it is recomputed during the conversion. The step applies the
real rule instead — seconds and visibility counts must be identical, weights may differ by up
to **5 %** — and fails only on a pair that breaks it, on negative weights, or on a
(baseline, source) that one of the two datasets does not have at all.

**If verification fails but you have looked at the problems and they are acceptable**,
`postprocess run distribute` deliberately continues past the failed step.

### Step 17 — `distribute`: credentials, protection, archive, PI letter

**What it does**, strictly in order, stopping at the first failure — so nothing is archived if
the credentials or the protection could not be set:

1. Read the PI/co-I contacts and the per-source protection from the `.jex` file on the archive
   machine (skipped for NMEs).
2. Set (or recover) the archive credentials.
3. Protect the sources the PI scheduled as protected.
4. Archive the standard plots, the FITS-IDI files, the PI letter, and the pipeline directories.
5. Regenerate the PI letter with the credentials and hand it to you to send.
6. Remind you about the NME report, if this is an NME.

**Automatic**

```bash
postprocess run distribute
# or individually:
postprocess exec auth            # set/recover the credentials
postprocess exec protect         # source protection in the archive
postprocess exec archive-fits    # standard plots + FITS-IDI
postprocess exec archive-pilet   # PI letter
postprocess exec nme             # NME-report reminder
```

**Manual**

```bash
# A "safe" password, and the file that records the credentials:
date | md5sum | cut -b 1-12
touch {exp}_{password}.auth

archive.pl -auth  -e {EXP}_{YYMMDD} -n {exp} -p {password}
gzip *ps
archive.pl -stnd  -e {EXP}_{YYMMDD} *ps.gz
archive.pl -stnd  -e {EXP}_{YYMMDD} {exp}.piletter
archive.pl -fits  -e {EXP}_{YYMMDD} *IDI*

(cd pipeline/in  && archive.pl -pipe -e {EXP}_{YYMMDD})
(cd pipeline/out && archive.pl -pipe -e {EXP}_{YYMMDD})

# Source protection (per source, and for the pipeline products):
auth_pipe.py -e {EXP}_{YYMMDD} -s '{SRC1} {SRC2}' -p source
auth_pipe.py -e {EXP}_{YYMMDD} -s '{SRC1} {SRC2}' -p pipe

# Only if the experiment was not submitted through NorthStar (Bob usually does this):
archive.pl -abstract {abstract.txt} -e {EXP}_{YYMMDD}
```

`archive` (no `.pl`) prints the full list of options and the file-name conventions.
`auth_pipe.py -h` lists the fields that can be protected or released, per experiment and per
source. You can also set the authentication from
<http://archive.jive.nl/scripts/pipe/admin.php>.

> **No password protection is needed** for NMEs, or for experiments where the PI has waived
> the proprietary period. `postprocess` skips the credentials for NMEs automatically.

**What to check.** Do not allow free access to sources the PI marked as protected. If the
`.jex` file could not be read, `postprocess` prints exactly the two `auth_pipe.py` commands
you need to run by hand and fails the step so it cannot be missed.

**The PI letter.** Four files are produced:

| File | What it is |
| --- | --- |
| `{exp}.piletter` | Plain text, no credentials. This is the one that goes into the archive. |
| `{exp}.piletter_auth` | The same, with the credentials. **This is what you send to the PI.** |
| `{exp}.piletter.html` | HTML version. |
| `{exp}.piletter.eml` | Open it locally and it becomes a mail draft with recipients, subject and body filled in. |

Sending the mail stays manual on purpose — `postprocess` holds no mail credentials. Send it to
the PI and CC `jops@jive.eu`.

To build the credentials letter by hand:

```bash
pipelet.py [-o OUTDIR] [-c CREDENTIALS | -u USER -p PASS] {exp} {your_surname}
```

It takes the credentials from the `{username}_{password}.auth` file in the current directory.

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
