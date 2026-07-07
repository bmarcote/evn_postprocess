# EVN Post-Processing Pipeline

**Semi-automatic post-processing of EVN (European VLBI Network) observations.**

The `evn_postprocess` package runs all steps required from post-correlation to user
delivery of the data, following the _SFXC Post-Correlation Checklist_. It operates
in a semi-interactive manner, guiding the Support Scientist through the required
quality checks while automating the repetitive work — and, since the modular
refactor, it runs equally well outside JIVE from just a `.vex` file, `.lis` files,
and correlated data on disk.

---

## Features

- **Semi-automatic workflow** — Runs the full post-correlation pipeline
  (16 steps, `initialize` → `archive`) with minimal user intervention.
- **Plugin backends** — Retrieval, calibration pipeline, and delivery are each
  swappable (`jive`/`none`/…) via CLI flag or the experiment toml, so the same
  core runs at JIVE or fully standalone. See [Plugin Backends](guide/backends.md).
- **Experiment TOML** — One `{expname}.toml` per experiment records source types,
  PI contacts, backend choices, and every resolved processing parameter, so a
  re-run is silent and fully reproducible. See [Experiment TOML Schema](reference/experiment-toml.md).
- **Heuristic source classification** — Target/calibrator/fringe-finder guessed
  from the schedule and an optional RFC-catalogue lookup when not declared. See
  [Source Classification](guide/source-classification.md).
- **e-EVN coordination** — Sibling-directory conventions and pause/resume
  synchronisation barriers, no daemon required. See [e-EVN Coordination](guide/eevn.md).
- **Batch / unattended mode** — Drive the pipeline from a TOML policy file for
  queue-based execution; every interaction point resolves silently or pauses
  cleanly with a `REVIEW_REQUIRED` marker.
- **Communications** — Receive notifications (email or Mattermost) at key review
  points; reply to Mattermost to continue without logging in.
- **Web dashboard** — Interactive summary with plots, pipeline diagnostics, and a
  per-station Comments tab, served over an SSH-tunnelled HTTP connection.
- **External tool resolution** — Environment-variable or config-file based binary
  lookup (`j2ms2`, `tConvert`, `EVN.py`, etc.), so JIVE-specific dependencies are
  only ever imported when the corresponding backend is selected.
- **Persistent state** — Experiment metadata serialised to JSON so the pipeline
  can resume after interruption.

---

## Quick Example

```bash
# From the experiment directory on eee:
postprocess run

# Show current experiment metadata:
postprocess info

# Resume from a specific step:
postprocess run pipeline

# Run unattended with a policy file:
postprocess --batch --policy policy.toml run

# Run fully standalone, no JIVE server ever contacted:
postprocess --retrieval none --pipeline none --distribution none run
```

---

## Architecture at a Glance

```text
postprocess (CLI)
  └── main.py            → argument parsing, backend selection, experiment loading
       └── workflow.py    → step orchestration (16-step Task list, e-EVN barriers)
            ├── inputs.py            → vex/lis/toml → Experiment (no server contact)
            ├── experiment_state.py  → {expname}.toml load/resolve/write-back
            ├── source_classify.py  → heuristic target/calibrator/fringefinder
            ├── retrieval/           → jive | none  (input-file acquisition)
            ├── pipelines/           → aips | none | vpipe  (calibration)
            ├── distribution/        → jive | none | sweeps  (delivery)
            ├── review.py            → station summary, dashboard Comments defaults
            ├── eevn.py              → sibling conventions, sync barriers
            ├── process.py           → MS operations, standardplots, tConvert
            ├── pipeline.py          → EVN.py pipeline glue (used by pipelines.aips)
            ├── dialog.py            → user interaction (Terminal / PolicyDriven)
            ├── comms.py             → notifications (email / Mattermost)
            └── plotting.py          → plots, web dashboard
```

See [Architecture](development/architecture.md) for the full package layout and
design patterns (registry, toml precedence, e-EVN barriers).

---

## Navigation

<div class="grid cards" markdown>

- :material-download: **[Installation](getting-started/installation.md)** — Set up the package
- :material-rocket-launch: **[Quick Start](getting-started/quickstart.md)** — Run your first experiment
- :material-book-open-variant: **[User Guide](guide/workflow.md)** — In-depth usage
- :material-puzzle: **[Plugin Backends](guide/backends.md)** — Retrieval, pipeline, distribution
- :material-api: **[API Reference](api/experiment.md)** — Module documentation
- :material-source-branch: **[Development](development/architecture.md)** — Internals & contributing

</div>
