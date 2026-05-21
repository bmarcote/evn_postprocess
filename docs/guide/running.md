# Running the Pipeline

## Full run

```bash
postprocess run
```

Runs (or resumes) the entire post-processing from the last successful step. If the experiment has never been processed, it starts from `initialize`.

## Partial runs

```bash
# From a specific step to the end:
postprocess run pipeline

# Between two steps (inclusive):
postprocess run msops tconvert
```

## Single-command execution

For debugging or re-running individual operations:

```bash
postprocess exec standardplots
postprocess exec flag_weights
```

Use `postprocess exec` without arguments to list all available commands.

## Overriding the reference antenna

```bash
postprocess --refant Ef Wb run
```

The override is applied after loading the experiment state and persists to the JSON file.

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
- **Log file** — `post_processing.log` in the experiment root.
- **Debug log** — `logs/post_process.log` with full DEBUG output.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success (or clean pause in batch mode). |
| 1 | Error (configuration, missing files, failed step). |
