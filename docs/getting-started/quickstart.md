# Quick Start

This guide walks you through running a basic EVN post-processing session.

## Prerequisites

### At JIVE (default `retrieval`/`pipeline`/`distribution` = `jive`/`aips`/`jive`)

1. SSH access to the JIVE processing servers (eee, ccs, pipe, vlbeer).
2. A correlated experiment on the correlator server.
3. `computers.toml` configured (see [Configuration](configuration.md)).

### Standalone / external use

1. The observation `.vex` file and the `.lis` file(s) of the correlated passes,
   already on disk.
2. No server access needed at all with `--mode regular`.

## Basic usage

Navigate to (or specify) the experiment directory and run:

```bash
cd /data0/marcote/N24AB1
postprocess run
```

The pipeline will:

1. **initialize** — Create the directory structure; obtain the `.vex` (via the
   retrieval backend); derive observing date, e-EVN membership, stations, sources,
   and scans directly from it; apply the experiment toml.
2. **lisfiles** / **checklis** — Create/verify the `.lis` file(s) and extract the
   correlator passes.
3. **j2ms2** — Run `j2ms2` to produce Measurement Set(s).
4. **standardplots** — Generate the standard plots. A web dashboard is available
   for review (`postprocess info --serve`).
5. **msops** — Resolve and apply weight threshold, polswap, 1-bit, and PolConvert
   flags (from the experiment toml if set, otherwise interactively or from a
   batch policy).
6. **tconvert** / **polconvert** / **post_polconvert** / **standardplots2** —
   Convert to FITS-IDI, apply polarisation corrections if needed, re-plot.
7. **antab** — Fetch station files, show the pre-ANTAB station summary, open
   `antab_editor.py`.
8. **pipeinputs** / **pipeline** / **postpipe** — Run the calibration pipeline and
   its diagnostics. **Pauses here for review** (dashboard + PI letter).
9. **prearchive** / **archive** — Attach Tsys/GC, then deliver (credentials,
   PI letter, archive upload).

See [Workflow Overview](../guide/workflow.md) for the full step list and
[Operating Modes](../guide/modes.md) for how `retrieval`/`pipeline`/
`distribution` change what actually runs at each stage.

## Resuming after interruption

The pipeline saves state to `{expname}.json` after each step. Simply run
`postprocess run` again — it resumes from the last successful step.

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

# Web dashboard (plots + summary + Comments tab):
postprocess info --serve
```

## Running a single command

```bash
postprocess exec standardplots
```

Use `postprocess exec` (no argument) to list all available commands.
