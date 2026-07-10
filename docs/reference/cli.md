# CLI Reference

## `postprocess`

```text
postprocess [-h] [-e EXPNAME] [-jss SUPSCI] [-d DIR] [-a] [--no-lag] [--debug]
            [--refant REFANT [REFANT ...]]
            [--retrieval MODE] [--pipeline MODE] [--distribution MODE]
            [--policy FILE] [--tConvert-in-eee | --no-tConvert-in-eee]
            [--batch] [--comms FILE] [-v]
            {info,dashboard,list,last,run,exec,edit} ...
```

### Global options

| Option | Description |
| --- | --- |
| `-e`, `--expname` | Experiment name (case-insensitive). Default: from cwd. |
| `-jss`, `--supsci` | Support Scientist surname. Default: current user. |
| `-d`, `--dir` | Working directory. Default: `/data/exp/<EXPNAME>`. |
| `-a`, `--no-archive` | Skip the final `archive` step. |
| `--no-lag` | Skip building the lag-space MS / per-scan antenna SNR. Sticky across re-runs once set. |
| `--debug` | Enable verbose debug logging. |
| `--refant` | Override reference antenna(s) (space-separated). |
| `--retrieval MODE` | How input files are obtained: `jive` (default) or `none`. Overrides `[retrieval] mode` in the experiment toml. |
| `--pipeline MODE` | Calibration pipeline: `aips` (default), `none`, or `vpipe` (not implemented). Overrides `[pipeline] mode`. |
| `--distribution MODE` | Delivery/archiving: `jive` (default), `none`, or `sweeps` (not implemented). Overrides `[distribution] mode`. |
| `--policy FILE` | Path to `policy.toml` for unattended decisions. |
| `--tConvert-in-eee` / `--no-tConvert-in-eee` | Run `tConvert`/PolConvert on `eee` (default) or locally. |
| `--batch` | Run unattended; write `REVIEW_REQUIRED` instead of blocking. |
| `--comms FILE` | Path to `comms.toml` for notifications. Auto-searches if not given. |
| `-v`, `--version` | Print version and exit. |

An unknown `--retrieval`/`--pipeline`/`--distribution` name is rejected
immediately, before any step runs.

### Subcommands

#### `run`

```text
postprocess run [STEP1 [STEP2]]
```

Runs the pipeline. Without arguments: from last successful step. With one step:
from that step to the end. With two steps: from STEP1 to STEP2 (inclusive). See
[Workflow Steps & Local Tools](steps.md) for the 16 step names.

#### `info`

```text
postprocess info [--serve]
```

Shows experiment metadata (including values sourced from the experiment toml,
marked with their origin). With `--serve`: launches the web dashboard (see
[Dashboard](../guide/dashboard.md)).

#### `dashboard`

```text
postprocess dashboard
```

Launches the web dashboard with the experiment info and plots — the same
dashboard that regular operations open during the `msops` step. Equivalent to
`postprocess info --serve`. The server blocks until you press Ctrl+C and prints
the SSH tunnel command needed to open it from your local browser (see
[Dashboard](../guide/dashboard.md)).

#### `list` / `last`

```text
postprocess list
postprocess last
```

Shows all steps and which have been completed (identical output; `last` is a
historical alias).

#### `exec`

```text
postprocess exec [TASK_NAME]
```

Runs a single underlying command directly, bypassing step order and staleness
checks. Without argument: lists all available commands (see the table in
[Workflow Steps & Local Tools](steps.md#postprocess-exec-running-a-single-underlying-command)).

#### `edit`

```text
postprocess edit {refant,target,phasecal,fringefinder} [VALUES...]
```

Edit experiment metadata fields (see [Editing Metadata](../guide/editing.md)).
Without values: lists options.

---

## `mstools`

```text
mstools view <MS_FILE> [--stats]
mstools run <TOOL> <MS_FILE> [args...]
```

Standalone CLI for Measurement Set operations. `view` prints an overview;
`run <TOOL>` executes one of: `polswap`, `copypol`, `scale1bit`, `invert_subband`,
`flag_weights`, `expname`, `srcname`, `print_mounts`, `modify_mounts`, `ysfocus`,
`hofocus`. Run `mstools -h` or `mstools run -h` for the full argument list per
tool.
