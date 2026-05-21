# CLI Reference

## `postprocess`

```text
postprocess [-h] [-e EXPNAME] [-jss SUPSCI] [-d DIR] [-a] [--debug]
            [--refant REFANT [REFANT ...]] [--policy FILE] [--batch]
            [--comms FILE] [-v]
            {info,list,last,run,exec,edit} ...
```

### Global options

| Option | Description |
| --- | --- |
| `-e`, `--expname` | Experiment name (case-insensitive). Default: from cwd. |
| `-jss`, `--supsci` | Support Scientist surname. Default: current user. |
| `-d`, `--dir` | Working directory. Default: `/data/exp/<EXPNAME>`. |
| `-a`, `--no-archive` | Skip archiving step. |
| `--debug` | Enable verbose debug logging. |
| `--refant` | Override reference antenna(s). |
| `--policy FILE` | Path to `policy.toml` for unattended decisions. |
| `--batch` | Run unattended; write `REVIEW_REQUIRED` instead of blocking. |
| `--comms FILE` | Path to `comms.toml` for notifications. Auto-searches if not given. |
| `-v`, `--version` | Print version and exit. |

### Subcommands

#### `run`

```text
postprocess run [STEP1 [STEP2]]
```

Runs the pipeline. Without arguments: from last successful step. With one step: from that step to end. With two steps: from STEP1 to STEP2 (inclusive).

#### `info`

```text
postprocess info [--serve]
```

Shows experiment metadata. With `--serve`: launches the web dashboard.

#### `list` / `last`

```text
postprocess list
postprocess last
```

Shows all steps and which have been completed.

#### `exec`

```text
postprocess exec [TASK_NAME]
```

Runs a single command from the workflow. Without argument: lists available commands.

#### `edit`

```text
postprocess edit {refant,target,phasecal,fringefinder} [VALUES...]
```

Edit experiment metadata fields. Without values: lists options.

---

## `mstools`

```text
mstools [-h] <MS_FILE> [options]
```

Standalone CLI for Measurement Set operations (polswap, 1-bit scaling, weight flagging, metadata inspection).
