# EVN Post-Processing Pipeline

**Semi-automatic post-processing of EVN (European VLBI Network) observations.**

The `evn_postprocess` package runs all steps required from post-correlation at JIVE to user delivery of the data, following the _SFXC Post-Correlation Checklist_. It operates in a semi-interactive manner, guiding the Support Scientist through the required quality checks while automating the repetitive work.

---

## Features

- **Semi-automatic workflow** — Runs the full post-correlation pipeline with minimal user intervention.
- **Batch / unattended mode** — Drive the pipeline from a TOML policy file for queue-based execution.
- **Communications** — Receive notifications (email or Mattermost) at key review points; reply to Mattermost to continue without logging in.
- **Web dashboard** — Interactive summary with plots served over SSH-tunnelled HTTP.
- **External tool resolution** — Environment-variable or config-file based binary lookup (j2ms2, tConvert, EVN.py, etc.).
- **Persistent state** — Experiment metadata serialised to JSON so the pipeline can resume after interruption.

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
```

---

## Architecture at a Glance

```
postprocess (CLI)
  └── main.py          → argument parsing, experiment loading
       └── workflow.py  → step orchestration (Task list)
            ├── process.py   → MS operations, standardplots, tConvert
            ├── pipeline.py  → EVN Pipeline execution
            ├── dialog.py    → user interaction (Terminal / PolicyDriven)
            ├── comms.py     → notifications (email / Mattermost)
            └── plotting.py  → plots, web dashboard
```

---

## Navigation

<div class="grid cards" markdown>

- :material-download: **[Installation](getting-started/installation.md)** — Set up the package
- :material-rocket-launch: **[Quick Start](getting-started/quickstart.md)** — Run your first experiment
- :material-book-open-variant: **[User Guide](guide/workflow.md)** — In-depth usage
- :material-api: **[API Reference](api/experiment.md)** — Module documentation
- :material-source-branch: **[Development](development/architecture.md)** — Internals & contributing

</div>
