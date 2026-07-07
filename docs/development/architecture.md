# Architecture

## Package structure

```text
src/evn_postprocess/
├── __init__.py
├── main.py              # CLI entry point (argparse, backend CLI-mode validation)
├── workflow.py          # Step orchestration (Task list, run_workflow, e-EVN barriers)
├── experiment.py         # Core data model (Experiment, Antennas, Sources, Scans, etc.)
├── experiment_state.py   # {expname}.toml load/resolve/write-back, precedence rule
├── inputs.py             # vex/lis/toml -> Experiment (no server contact)
├── source_classify.py    # Heuristic target/calibrator/fringefinder classification
├── review.py             # Station summary, dashboard Comments defaults, feedback-DB lookup
├── eevn.py               # e-EVN sibling conventions and synchronisation barriers
├── registry.py           # Shared name -> factory registry used by the 3 plugin families
├── retrieval/            # Input-file acquisition backends
│   ├── __init__.py       #   Retriever ABC, registry, selected_mode (CLI > toml > default)
│   ├── jive.py            #   copy vex from ccs, create .lis remotely, fetch from vlbeer
│   └── local.py           #   'none': validates everything is already on disk
├── pipelines/             # Calibration-pipeline backends
│   ├── __init__.py       #   PipelineBackend ABC, registry, NonePipeline
│   └── aips.py             #   wraps the historical EVN.py flow (pipeline.py)
├── distribution/          # Delivery backends
│   ├── __init__.py       #   Distributor ABC, registry, NoneDistributor
│   └── jive.py             #   credentials, PI letter (+ review comments), archive, upload_feedback stub
├── process.py            # MS operations, standardplots, tConvert, archiving glue
├── pipeline.py            # Historical EVN.py / antab / feedback glue (wrapped by pipelines.aips)
├── plotting.py            # Jplot wrapper, PS->PNG, web dashboard (incl. Comments tab)
├── dialog.py               # User interaction (Terminal / PolicyDriven)
├── comms.py                # Notifications (email / Mattermost)
├── policy.py               # Batch-mode policy dataclass
├── feedback.py             # In-tree Python port of feedback.pl (pipeline feedback page)
├── io.py                   # File retrieval (SCP from servers)
├── lisfiles.py             # .lis file creation, retrieval, and validation
├── vex.py                  # VEX file parser
├── tools.py                # External binary resolution (env var / computers.toml / $PATH)
├── utils.py                # Shell commands, SSH, notifications, formatting
├── comment_tasav.py        # .comment and .tasav.txt generation
├── mstools/                # Measurement Set subpackage
│   ├── __init__.py
│   ├── main.py             # mstools CLI (view / run <tool>)
│   ├── msdata.py           # MS metadata classes (Ms, Antenna, Source, FreqSetup)
│   ├── operations.py       # Data manipulation (polswap, scale1bit, flag_weights)
│   ├── mounts.py           # Antenna mount corrections
│   └── misc.py             # Stokes enum, utilities
├── scripts/                # Standalone utility scripts (append_antab_idi.py, polconvert.py, ...)
└── templates/              # File templates (pipeline input, .comment, .tasav)
```

## Key design patterns

### Three inputs, everything site-specific behind plugins

The core consumes exactly one `.vex` file, N `.lis` files, and one optional
experiment `.toml`. Everything JIVE-specific (server access, AIPS, archive
delivery) lives behind three ABCs — `retrieval.Retriever`,
`pipelines.PipelineBackend`, `distribution.Distributor` — each with a `jive`
implementation reproducing historical behaviour and a `none` no-op. See
[Plugin Backends](../guide/backends.md) for the user-facing view.

### Shared backend registry

All three families use `registry.BackendRegistry(kind, error_cls)`: a
name → zero-argument-factory map with a uniform "unknown backend" error message.
Factories are lazy (`_make_jive()` imports `retrieval.jive` only when selected),
so choosing `none` never pulls in ssh/AIPS/MySQL dependencies. Each package keeps
its own module-level `register`/`available_backends`/`get_*`/`selected_mode`
functions as thin delegates, so the public API per family is unchanged; only
`retrieval` and `pipelines`/`distribution`'s CLI-mode variables differ in whether
a `--<family>` flag exists (all three do, as of the CLI-validation fix — see the
[CLI reference](../reference/cli.md)).

### Experiment TOML as the single source of resolved parameters

`experiment_state.ExperimentToml` owns `{expname}.toml`: parsing into typed
sections, the precedence rule (`resolve_parameters`), and write-back
(`record_parameters`/`record_comments`/`record_sources`/`record_pi`) that never
touches user-owned sections except recording heuristic source guesses. All reads
happen through plain-Python copies of a `tomlkit` document, so untouched sections
keep byte-identical formatting/comments across saves (atomic: temp file + rename).

The single "attach or reload the toml for this experiment" implementation is
`experiment_state.attached_toml(exp, fresh=)`, used identically by `main`, the
workflow steps, the dashboard's save handler, and the distribution backend — the
`fresh=True` reload immediately before every write is what prevents a lost update
between two processes touching the same experiment (e.g. a paused `postprocess
run` and a dashboard server saving Comments-tab edits concurrently).

### Experiment as the central runtime state

The `Experiment` dataclass is the in-memory source of truth: metadata (name,
date, PI, supsci, antennas, sources, scans), correlator-pass information,
directory layout (`Dirs`), and the attached `ExperimentToml`/`Policy`. Internal
checkpoint state (step completion, file inventories, timestamps) is serialised to
`{expname}.json` after each step (`Experiment.from_dict`/`.store()`), independent
of the toml.

### State machine via Task list

The workflow is a linear sequence of 16 `Task` objects (`workflow._WORKFLOW_STEPS`).
Each task wraps a function with signature `(Experiment) -> bool`. The runner
iterates through tasks, calling each function, storing progress, and — for
`postpipe` — handling the review pause/re-run-from-step/finalise flow. A step can
also raise `workflow.StepPaused` (not a failure) to signal a clean e-EVN-barrier
wait: logged and notified as "paused", not "crashed", and the scheduler sees exit
code 0.

### Dialog abstraction

User interaction for `msops` is mediated by the `Dialog` ABC:

- `Terminal` — Interactive prompts via stdin.
- `PolicyDriven` — Reads answers from `exp.policy` (batch mode); raises
  `BatchInteractionError` on a missing required field.
- Toml-driven — a complete `[postprocess]` section in the experiment toml bypasses
  the dialog entirely (`workflow._toml_msops_available`/`_apply_toml_msops`).
- Comms feedback — Mattermost replies parsed in `comms.py` also bypass the dialog.

### e-EVN coordination without a daemon

`eevn.py` implements both synchronisation barriers as filesystem checks: an
explicit `{expname}.fitsidi_ready` marker (written at `post_polconvert`) gates the
`antab` step across sibling directories; a glob for `../EXP1/pipeline/in/*.antab`
gates each `EXPn`'s `pipeline` step. See [e-EVN Coordination](../guide/eevn.md).

### Server / tool abstraction

Remote operations use `Server` objects (`hostname`, `user`, `path`) and utility
functions in `utils.py` (`shell_command`, `scp_file`, `ssh_command`); external
binaries are resolved via `tools.resolve`/`tools.run` (env var → computers.toml →
`$PATH`) — see [External Tools](../guide/tools.md).

## Data flow

```text
.vex + .lis (+ .toml)             (inputs.py, no server contact)
    ↓ retrieval backend (jive: fetch from ccs/vlbeer | none: validate local)
Experiment (in-memory) + {expname}.toml (resolved parameters)
    ↓ j2ms2
Measurement Set (.ms)
    ↓ process operations (msops, resolved via toml/auto/dialog/policy)
Modified MS + plots
    ↓ tConvert (+ PolConvert if needed)
FITS-IDI
    ↓ pipeline backend (aips: EVN.py | none: skip)
Pipeline products
    ↓ postpipe review (dashboard incl. Comments tab) → prearchive
FITS-IDI + Tsys/GC, {expname}.toml [postprocess] complete
    ↓ distribution backend (jive: archive + PI letter | none: leave in place)
Archive-ready / delivered data
```

## Extension points

1. **New workflow steps** — Add a function + `Task` entry to `_WORKFLOW_STEPS`.
2. **New retrieval/pipeline/distribution backends** — Implement the ABC and
   `register('name', factory)`; see [Plugin Backends](../guide/backends.md).
3. **New notification backends** — Subclass `comms.Notifier`.
4. **New dialog modes** — Subclass `dialog.Dialog`.
5. **New plot types** — Add to the plotting module and the dashboard JS.
