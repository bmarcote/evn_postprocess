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

## Selecting the mode

```bash
# Run fully local: no server contacted, nothing archived (auto-detected for a
# non-jops user, or forced explicitly):
postprocess --mode regular run

# Force the JIVE support-scientist job:
postprocess --mode supsci run
```

See [Operating Modes](modes.md). The mode is auto-detected from the OS when `--mode`
is omitted, persisted on the experiment, and reused on resume; an unknown `--mode`
value is rejected before anything runs.

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
| `--no-archive` / `-a` | Skip the final `distribute` step. |
| `--mode {supsci,regular,sweeps}` | Operating mode; auto-detected from the OS when omitted (see [Operating Modes](modes.md)). |
| `--policy FILE` | Batch-mode decisions (see [Batch Mode](batch-mode.md)). |
| `--comms FILE` | Notification settings (see [Communications](comms.md)). |
| `--tConvert-in-eee` / `--no-tConvert-in-eee` | Run `tConvert`/PolConvert on `eee` (default; workaround for a broken local install) or locally. |
| `--debug` | Verbose logging. |

## Directory resolution

By default, `postprocess` runs in the JIVE `eee` location for the experiment when a
`computers.toml` is configured, otherwise in the **current directory** (so a standalone
user needs no server configuration). Override with:

```bash
postprocess -d /custom/path -e EXPNAME run
```

## Logging (three channels)

Each step speaks on three independent channels (see [reporting](../api/review.md) —
implemented in `evn_postprocess.reporting`):

- **Terminal** — a concise, colourful per-step status for the operator (`--debug` makes
  it verbose).
- **`logs/logging_messages.log`** — the verbose loguru debug record, kept out of the
  terminal.
- **`logs/commands.sh`** — a replayable log of the exact local command(s) each step ran
  (one shell-runnable line per command, with a per-step header), so any step can be
  reproduced by hand.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success, or a clean pause (batch mode, or an e-EVN barrier). |
| 1 | Error (configuration, missing files, failed step). |
