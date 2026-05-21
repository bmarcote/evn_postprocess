# Quick Start

This guide walks you through running a basic EVN post-processing session.

## Prerequisites

1. SSH access to the JIVE processing servers (eee, ccs, pipe).
2. A correlated experiment with FITS-IDI files available.
3. `computers.toml` configured (see [Configuration](configuration.md)).

## Basic usage

Navigate to (or specify) the experiment directory and run:

```bash
cd /data0/marcote/N24AB1
postprocess run
```

The pipeline will:

1. **Initialize** — Create directory structure, retrieve metadata from MASTER_PROJECTS.LIS.
2. **Lis files** — Generate or verify `.lis` file(s) for j2ms2.
3. **MS creation** — Run `j2ms2` to produce Measurement Set(s).
4. **Standard plots** — Generate weight, auto-correlation, cross-correlation, and amp-vs-time plots. A web dashboard opens for review.
5. **MS operations** — Interactively ask about weight threshold, polswap, 1-bit, and PolConvert antennas, then apply the operations.
6. **tConvert / PolConvert** — Convert data and apply polarisation corrections.
7. **ANTAB** — Retrieve or create the amplitude calibration table.
8. **Pipeline** — Run the EVN Pipeline for all correlator passes.
9. **Post-pipeline** — Create TASAV, comment files, feedback scripts.
10. **Archive** — Set credentials, create the PI letter, archive data.

## Resuming after interruption

The pipeline saves state to `{expname}.json` after each step. Simply run `postprocess run` again — it resumes from the last successful step.

## Specifying a step range

```bash
# Run from 'pipeline' to the end:
postprocess run pipeline

# Run only from 'msops' to 'tconvert':
postprocess run msops tconvert
```

## Viewing experiment information

```bash
# Terminal summary:
postprocess info

# Web dashboard (plots + summary):
postprocess info --serve
```

## Running a single command

```bash
postprocess exec standardplots
```

Use `postprocess exec` (no argument) to list all available commands.
