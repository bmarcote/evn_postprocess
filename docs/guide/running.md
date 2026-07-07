# Running the Pipeline

## Full run

```bash
postprocess run
```

Runs (or resumes) the entire post-processing from the last successful step. If the
experiment has never been processed, it starts from `initialize`.

## Partial runs

```bash
# From a specific step to the end:
postprocess run pipeline

# Between two steps (inclusive):
postprocess run msops tconvert
```

See [Workflow Steps & Local Tools](../reference/steps.md) for the full list of step
names.

## Single-command execution

For debugging or re-running an individual underlying command, bypassing the step
order and staleness checks:

```bash
postprocess exec standardplots
postprocess exec flag_weights
```

Use `postprocess exec` without arguments to list all available commands.

## Selecting backends

```bash
# Run fully offline: no server contacted, calibration and archiving skipped:
postprocess --retrieval none --pipeline none --distribution none run

# Override just the delivery target for a test run:
postprocess --distribution none run
```

See [Plugin Backends](backends.md). CLI flags override whatever the experiment
toml's `[retrieval]`/`[pipeline]`/`[distribution]` sections say; an unknown backend
name is rejected before anything runs.

## Overriding the reference antenna

```bash
postprocess --refant Ef Wb run
```

The override is applied right after loading the experiment and is stored
immediately (JSON state), before any step runs.

## Other useful flags

| Flag | Effect |
| --- | --- |
| `--no-lag` | Skip building the auxiliary lag-space MS and its per-scan antenna SNR (scan overview then only reports presence/absence, no SNR comparison). Sticky once set. |
| `--no-archive` / `-a` | Skip the final `archive` step. |
| `--policy FILE` | Batch-mode decisions (see [Batch Mode](batch-mode.md)). |
| `--comms FILE` | Notification settings (see [Communications](comms.md)). |
| `--tConvert-in-eee` / `--no-tConvert-in-eee` | Run `tConvert`/PolConvert on `eee` (default; workaround for a broken local install) or locally. |
| `--debug` | Verbose logging. |

## Directory resolution

By default, `postprocess` expects to run from (or creates) a directory at:

```text
/data0/{supsci}/{EXPNAME}
```

Override with:

```bash
postprocess -d /custom/path -e EXPNAME run
```

## Logging

- **Terminal output** — Controlled by `--debug` (verbose) or default (INFO level).
- **Log file** — `post_processing.log` (on `eee` when a `computers.toml` server is
  configured, otherwise the current working directory).
- **Debug log** — `logs/post_process.log` with full DEBUG output.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success, or a clean pause (batch mode, or an e-EVN barrier). |
| 1 | Error (configuration, missing files, failed step). |
