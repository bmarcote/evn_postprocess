# Workflow Steps Reference

Complete list of all pipeline steps, their internal command names, and what they do.

## Step details

### `initialize` → `initialize_experiment`

Creates the directory structure to post-process the experiment. Checks that the necessary servers are configured. Retrieves the observing date and e-EVN run (if applicable) from the `MASTER_PROJECT.LIS` file. Retrieves the observing session information.

### `lisfile` → `get_lis_files`

Produces a `.lis` file (or multiple for multi-pass experiments) in ccs and copies it to eee. Validates the contents for known patterns.

### `checklis` → `check_lisfiles`

Verifies the existing `.lis` files for completeness and consistency with the experiment metadata.

### `ms` → `create_ms`

Runs `j2ms2` on all available `.lis` files to produce Measurement Set files. Extracts metadata (antennas, sources, frequency setup, scans) from the resulting MS.

### `plots` → `standardplots`

Generates standard plots using `jplotter`: weight distribution, auto-correlations, cross-correlations, amplitude vs phase, amplitude vs time. Converts PostScript output to PNG and opens the web dashboard.

### `msops` → `msops`

Applies MS operations. In interactive mode, opens the dashboard and asks:

- Weight flagging threshold.
- Antennas requiring polswap.
- Antennas that recorded 1-bit data.
- Antennas requiring PolConvert.

Then applies: `flag_weights`, `ysfocus`, `polswap`, `onebit`, `print_exp`, `tconvert`.

### `tconvert` → `tconvert`

Runs `tConvert` on all available MS files. If PolConvert antennas are flagged, also runs PolConvert.

### `post_polconvert` → `post_polconvert`

If PolConvert ran: renames `*.PCONVERT` files and re-runs standard plots on the new data.

### `antab` → `get_antab`

Retrieves the `.antab` amplitude calibration file. If it doesn't exist, invokes `antab_editor.py` for manual creation.

### `pipeinputs` → `prepare_pipeline_inputs`

Prepares a draft input file for the EVN Pipeline and recovers all necessary calibration files.

### `pipeline` → `run_pipeline`

Runs the EVN Pipeline (`EVN.py`) for all correlator passes.

### `postpipe` → `post_pipeline`

Creates TASAV files, comment files, and the `feedback.pl` script. This step typically triggers a pause for human review.

### `prearchive` → `prearchive`

Appends Tsys/GC to FITS-IDI, re-archives, and prompts for final verification of the PI letter.

### `archive` → `archive`

Sets archive credentials, creates the pipe letter, and archives all data products.

## Using step names

```bash
# Run from msops to end:
postprocess run msops

# Run only pipeline and postpipe:
postprocess run pipeline postpipe

# Re-run a single step's function:
postprocess exec standardplots
```
