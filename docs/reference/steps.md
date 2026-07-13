# Workflow Steps & Local Tools

The pipeline runs as a fixed, ordered list of 16 steps (`Task` objects in
`workflow._WORKFLOW_STEPS`). Each step wraps a Python function; `postprocess run`
walks the list from the last completed step, `postprocess run STEP` jumps to a
named step, and `postprocess exec NAME` runs a single underlying command in
isolation (see the table at the bottom of this page).

This page has two jobs: describe what each step does, and — for anyone who needs
to reproduce a step **by hand outside `postprocess`** (a server is unreachable, a
step needs a one-off tweak, or you're debugging) — name the exact local tool or
script each step calls, mirroring the historical [EVN Post-Correlation Checklist](https://code.jive.eu/marcote/science_support_doc).

!!! note "The mode changes what runs"
    Input acquisition (`initialize`/`lisfiles`/`antab` station files) and delivery
    (`distribute`) depend on the [operating mode](../guide/modes.md). The commands below
    describe **`supsci`** mode (the JIVE support-scientist job). In `regular` mode the
    inputs are already local and `distribute` only verifies the FITS-IDI files — no
    server is contacted.

## Step reference

### 1. `initialize` → `initialize_experiment`

Creates the experiment directory structure (`logs/`, `plots/`, `pipeline/{in,out}`,
`antenna_files/`), obtains the `.vex`/`.vix` file through the selected **retrieval**
backend, and derives all metadata from it: observing date (`$EXPER
exper_nominal_start`), e-EVN membership (`exper_description`), stations, sources,
scans. The experiment toml (`{expname}.toml`) is then applied (source types, PI,
support scientist); any source still untyped is classified heuristically (see
[Source Classification](../guide/source-classification.md)).

**Manual equivalent** (`supsci` mode):

```bash
ssh jops@ccs
cd /ccs/expr/{EXP}/
# vex is normally already there from correlation; for e-EVN EXPn copy EXP1's vex
```
```bash
# on eee, in the experiment directory
scp jops@ccs:/ccs/expr/{EXP}/{exp}.vix .
ln -s {exp}.vix {EXP}.vix
```

No `MASTER_PROJECTS.LIS`, `.jexp`, or `.expsum` lookup happens any more — everything
comes from the vex and the experiment toml.

### 2. `lisfiles` → `retrieve_lisfiles`

If no local `.lis` files exist yet, the retrieval backend creates them remotely and
copies them over.

**Manual equivalent** (on `ccs`):

```bash
showlog_new {EXP}          # GUI: mark PRODUCTION runs, ExportFile -> {exp}.lis
# or, non-interactively:
make_lis -e {exp} -p {profile} -s {exp}.lis
```
```bash
scp jops@ccs:/ccs/expr/{EXP}/{exp}*.lis .
```

### 3. `checklis` → `check_lisfiles`

Pure-Python validation (no external `checklis` binary): checks each `.lis` file for
duplicated/missing scans, and that all passes have unique `.lis`/MS/FITS-IDI names.
Also extracts the correlator passes (`lisfiles.get_passes_from_lisfiles`).

### 4. `j2ms2` → `create_msfile`

Fetches the correlator output for the jobs in the `.lis` file(s), then converts to a
Measurement Set:

```bash
getdata.pl -proj {EXP} -lis {exp}.lis
j2ms2 -v {exp}.lis
```

For e-EVN EXPn, the MS project name is fixed up afterwards (`expname.py` equivalent,
`process.update_ms_expname`). Metadata (antennas, sources, frequency setup) is then
read back from the MS (`process.get_metadata_from_ms`), and — unless `--no-lag` is
given — a lag-space MS is built to compute a per-scan antenna SNR
(`process.compute_lag_snr`), feeding the automatic MS-ops decisions in `msops`.

### 5. `standardplots` → `create_standardplots`

Runs `jplotter`/`standardplots` to produce the weight, auto-correlation,
cross-correlation, and amplitude/phase-vs-time plots:

```bash
standardplots -weight [-scan {scan_no}] {exp}.ms {refant} {calsrcs}
```

If it fails, the manual `jplotter` session from the
[old checklist](https://code.jive.eu/marcote/science_support_doc) is the fallback —
see `guide/tools.md` for the `jplotter` command cheat-sheet. Plots are converted to
PNG and become available in the web dashboard (`postprocess info --serve`).

### 6. `msops` → `msops`

Applies the MS operations. Parameters (weight threshold, polswap/polconvert/onebit
antenna lists, refant) are resolved through the precedence rule: **experiment toml
→ confident lag-MS auto-diagnostics → interactive dialog/dashboard → batch policy /
`REVIEW_REQUIRED`** (see [Experiment TOML Schema](experiment-toml.md)). Unlike the
historical standalone scripts, the operations run **in-process** via the `mstools`
subpackage — there is no `ysfocus.py`/`polswap.py`/`flag_weights.py`/`scale1bit.py`
call any more; the equivalent standalone entry point today is the `mstools` CLI:

```bash
mstools run ysfocus {exp}.ms                        # Yebes mount fix (hofocus for Hobart)
mstools run flag_weights {exp}.ms {threshold}        # flag + reports % flagged
mstools run polswap {exp}.ms {antenna}               # fix swapped polarizations
mstools run scale1bit {exp}.ms {antenna} [{antenna2} ...]  # scale 1-bit data to 2-bit
```

The resolved parameters are recorded into the experiment toml `[postprocess]`
section so a re-run applies them silently (no dialog, no dashboard).

### 7. `tconvert` → `tconvert`

Converts the MS to FITS-IDI:

```bash
tConvert {exp}.ms {exp}_1_1.IDI
```

With `--tConvert-in-eee` (default), the MS is copied to `jops@eee:/data0/temp/`, run
there, and the FITS-IDI files copied back (workaround for a broken local
tConvert/PolConvert on some machines); `--no-tConvert-in-eee` runs locally.

### 8. `polconvert` → `polconvert`

Runs **only** if any antenna is flagged for PolConvert (linear→circular
polarization conversion). Prepares `polconvert_inputs.ini` and runs:

```bash
polconvert.py polconvert_inputs.ini
```

producing `{exp}_*_1.IDI*.PCONVERT` files. See
[the manual PolConvert notes](https://code.jive.eu/marcote/science_support_doc)
(including the legacy CASA-script fallback) if the automated run needs
troubleshooting.

### 9. `post_polconvert` → `post_polconvert`

If PolConvert ran: backs up the original FITS-IDI files to `idi_ori/`, then renames
`*.PCONVERT` files to the standard FITS-IDI names (pure file management, no external
tool). Once the FITS-IDI files are final, the e-EVN completion marker
(`{exp}.fitsidi_ready`) is written here — see
[e-EVN Coordination](../guide/eevn.md).

### 10. `standardplots2` → `msops_post`

Re-runs `standardplots` (same command as step 5) on the post-MS-ops / post-PolConvert
data, so the plots that get archived reflect what is actually delivered.

### 11. `antab` → `antfiles`

The most interactive automated step. In order:

1. **e-EVN barrier (a)**: for a multi-experiment e-EVN run, the leader (EXP1) waits
   until every sibling has its FITS-IDI completion marker (pauses cleanly otherwise —
   see [e-EVN Coordination](../guide/eevn.md)).
2. Station `.log`/`.antabfs` files are fetched through the retrieval backend:
   ```bash
   sftp evn@vlbeer.ira.inaf.it
   cd vlbi_arch/{monthYY}
   mget {exp}*.log {exp}*.antabfs {exp}*.uvflgfs
   ```
   (VLBA stations additionally pull `{exp}cal.vlba` — see `retrieval.jive.get_vlba_antab`.)
3. `.uvflg` files are created from the `.log` files:
   ```bash
   uvflgall.sh
   ```
4. The **station summary** (did-not-observe, missed time ranges, reduced bandwidth)
   is printed as a terminal panel and sent via the configured notifier — see
   [Communications](../guide/comms.md).
5. `antab_editor.py` opens for manual ANTAB creation/editing:
   ```bash
   antab_editor.py [-e {EXPERIMENT}] [-l] [-a EXP2 [EXP3 ...]]
   ```
   This interaction is unchanged from the historical checklist — see the
   [ANTAB files section](https://code.jive.eu/marcote/science_support_doc) for VLBA
   handling, interpolation, and nominal-file fallbacks.

For e-EVN EXPn (n>1): **barrier (b)** instead waits for the run leader's final
`.antab`/`.uvflg` in `../EXP1/pipeline/in/`, then copies them over (renamed).

### 12. `pipeinputs` → `create_pipeline_inputs`

The selected **pipeline** backend's `prepare()`. For `aips` (default), builds the
EVN Pipeline input file from the local antab/uvflg files:

```bash
# equivalent of pipeline.create_input_file, using $IN/template.inp as a base
```

### 13. `pipeline` → `run_pipeline`

The pipeline backend's `run()`. For `aips`:

```bash
EVN.py {exp}.inp.txt
```

For `pipeline = "none"` in the experiment toml, this step is a no-op (calibration
skipped entirely — see [Operating Modes](../guide/modes.md)).

### 14. `postpipe` → `pipeline_diagnostics`

The pipeline backend's `collect()`, then `process.update_piletter`. For `aips`:

```bash
comment_tasav_file.py '{exp}'   # writes {exp}.tasav.txt and the .comment file
```

followed by the in-tree Python port of `feedback.pl`
(`evn_postprocess.feedback` / `pipeline.pipeline_feedback` — no external Perl script
needed any more). The PI letter is then auto-filled with non-observing antennas and
PolConvert remarks. **This is the usual review pause**: the terminal and the
notifier point to `postprocess info --serve` (dashboard: plots, Pipeline tab, and the
Comments tab) and to the PI letter; answering re-runs from a chosen step, or falls
through to finalisation.

### 15. `prearchive` → `pre_archive`

Appends the ANTAB Tsys/gain-curve information into the FITS-IDI files:

```bash
append_antab_idi.py [--antab {an_antab_file}] [--fits '{exp}_*_1.IDI*']
```

(internally: `append_tsys.py --replace` then `append_gc.py --replace` per FITS-IDI
file). On success, the finalisation record (final antab/polconvert-input file links,
flagged-data percentage) is written into the experiment toml `[postprocess]` section.

### 16. `distribute` → `archive`

The mode's **distribution** backend's `deliver()`. (The deprecated step name `archive`
still works as an alias for `postprocess run distribute`.) In `supsci` mode, in strict
order (stops at the first failure — nothing is archived if credentials/protection fail):

```bash
auth_pipe.py -e {EXP}_{YYMMDD} ...        # credentials / file protection
archive.pl -auth -e {exp}_{YYMMDD} -n {exp} -p {password}
archive.pl -stnd -e {exp}_{YYMMDD} {exp}.piletter *ps.gz
archive.pl -fits -e {exp}_{YYMMDD} *IDI*
archive.pl -pipe -e {exp}_{YYMMDD}        # pipeline $IN/$OUT directories
```

then the PI letter is sent, the operator is reminded to log station feedback
(`/feedback` in Mattermost + JIVE RedMine), and — for NMEs — reminded to write the
NME Report. In `regular` mode nothing is archived: `distribute` instead **verifies** the
expected `*.IDI*` files are present for every correlator pass and reports "ready" (or a
hard error naming what is missing), contacting no server.

## Steps not run automatically

A few historical checklist steps have no automated `Task` (still manual, run from
`pipe`):

```bash
ampcal.sh   # copies pipeline gain corrections into the feedback database
```

## Using step names

```bash
# Run from msops to the end:
postprocess run msops

# Run only from tconvert to polconvert (inclusive):
postprocess run tconvert polconvert

# List all steps and which have completed:
postprocess list
```

## `postprocess exec`: running a single underlying command

`exec` calls one of the functions above directly, bypassing the step order and
staleness checks — useful for a one-off retry. `postprocess exec` with no argument
lists every command; the table mirrors the step-by-step tools described above:

| Command | Underlying tool |
| --- | --- |
| `makelis` / `getlis` / `modlis` / `checklis` | `.lis` creation/copy/parsing (ccs) |
| `getdata` / `j2ms2` / `expname` / `metadata` / `lagsnr` | `getdata.pl`, `j2ms2`, MS metadata |
| `standardplots` / `gv` | `jplotter`/`standardplots`, `gv` viewer |
| `ysfocus` / `polswap` / `flag_weights` / `onebit` | `mstools` MS operations |
| `tconvert` / `polconvert` / `postpolconvert` | `tConvert`, `polconvert.py` |
| `auth` / `protect` / `archive-fits` / `archive-pilet` | `auth_pipe.py`, `archive.pl` |
| `append` | `append_antab_idi.py` (`append_tsys.py` + `append_gc.py`) |
| `issues` / `nme` | Station-feedback / NME-report reminders |
| `antab` / `uvflg` / `vlbeer` | `antab_editor.py`, `uvflgall.sh`, retrieval backend |
| `pyinput` / `pipe` / `comment_tasav` / `feedback` | EVN Pipeline input, `EVN.py`, tasav/comment, feedback page |
| `piletter` | PI letter auto-fill |
