# Architecture

## Package structure

```text
src/evn_postprocess/
├── __init__.py
├── main.py              # CLI entry point (argparse)
├── workflow.py          # Step orchestration (Task list, run_workflow)
├── experiment.py        # Core data model (Experiment, Antennas, Sources, etc.)
├── process.py           # MS operations, standardplots, tConvert
├── pipeline.py          # EVN Pipeline execution
├── plotting.py          # Jplot wrapper, PS→PNG, web dashboard
├── dialog.py            # User interaction (Terminal / PolicyDriven)
├── comms.py             # Notifications (email / Mattermost)
├── policy.py            # Batch-mode policy dataclass
├── io.py                # File retrieval (SCP from servers)
├── lisfiles.py          # .lis file generation and validation
├── vex.py               # VEX file parser
├── tools.py             # External binary resolution
├── utils.py             # Shell commands, SSH, notifications, formatting
├── comment_tasav.py     # .comment and .tasav.txt generation
├── mstools/             # Measurement Set subpackage
│   ├── __init__.py
│   ├── main.py          # mstools CLI
│   ├── msdata.py        # MS metadata classes (Ms, Antenna, Source, FreqSetup)
│   ├── operations.py    # Data manipulation (polswap, scale1bit, flag_weights)
│   ├── mounts.py        # Antenna mount corrections
│   └── misc.py          # Stokes enum, utilities
├── scripts/             # Standalone utility scripts
│   ├── ampcal-db.py
│   ├── append_antab_idi.py
│   ├── check_antab_idi.py
│   ├── comment_tasav_file.py
│   ├── find_idi_with_time.py
│   ├── gscale2avg.py
│   ├── idi2ms.py
│   └── polconvert.py
└── templates/           # File templates (pipeline input, .comment, .tasav)
```

## Key design patterns

### State machine via Task list

The workflow is a linear sequence of `Task` objects. Each task wraps a function with signature `(Experiment) -> bool`. The runner iterates through tasks, calling each function and storing progress.

### Experiment as central state

The `Experiment` dataclass is the single source of truth. It holds:

- Metadata (name, date, PI, supsci, antennas, sources).
- Correlator pass information (frequency, MS paths, flag thresholds).
- Directory layout (`Dirs` dataclass).
- Policy for batch mode.

State is serialised to `{expname}.json` after each step.

### Dialog abstraction

User interaction is mediated by the `Dialog` ABC:

- `Terminal` — Interactive prompts via stdin.
- `PolicyDriven` — Reads answers from `exp.policy` (batch mode).
- Comms feedback — Mattermost replies parsed in `comms.py` bypass the dialog entirely.

### Server abstraction

Remote operations use `Server` objects (`hostname`, `user`, `path`) and utility functions in `utils.py` (`shell_command`, `scp_file`, `ssh_command`).

## Data flow

```text
FITS-IDI (correlator output)
    ↓ j2ms2
Measurement Set (.ms)
    ↓ metadata extraction
Experiment JSON (state)
    ↓ process operations
Modified MS + plots
    ↓ tConvert
FITS-IDI (for pipeline)
    ↓ EVN.py
Pipeline products
    ↓ post-pipeline
Archive-ready data
```

## Module dependencies (simplified)

```text
main → workflow → {process, pipeline, dialog, comms, plotting}
                → experiment (data model)
                → io, lisfiles (file operations)
                → utils (shell, SSH)
                → tools (binary resolution)

process → plotting (Jplot, dashboard)
        → mstools (MS operations)

dialog → experiment (reads/writes policy, antenna flags)
comms → experiment (builds summaries)
```

## Extension points

1. **New workflow steps** — Add a function + `Task` entry.
2. **New notification backends** — Subclass `comms.Notifier`.
3. **New dialog modes** — Subclass `dialog.Dialog`.
4. **New plot types** — Add to `Jplot.create_plot()` and the dashboard JS.
