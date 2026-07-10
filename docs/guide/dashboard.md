# Dashboard

The web dashboard provides a visual overview of the experiment, its standard
plots, the pipeline output, and — new — a station-comments review tab that feeds
the PI letter.

## Launching the dashboard

### During post-processing

The dashboard is how you're asked to review results after `postpipe`: the
terminal and the configured notifier both print the exact command to open it. It
serves until you close it with `Ctrl+C`.

### Standalone

```bash
postprocess dashboard
```

Launches the same dashboard on the given experiment, outside the workflow.
`postprocess info --serve` is an equivalent alias. Useful for reviewing a
previously processed experiment without re-running steps. Prints the SSH tunnel
command needed to open it from your local browser.

## Accessing the dashboard remotely

The dashboard binds to `127.0.0.1` on the processing server (not reachable
directly over the network). Use an SSH tunnel:

```bash
ssh -L 8050:localhost:8050 user@eee.jive.nl
```

Then open `http://localhost:8050` in your browser. The actual port is printed to
the terminal when the dashboard starts.

## Dashboard tabs

### Pipeline

Shows the EVN Pipeline feedback HTML page(s) once they exist (produced at the
`postpipe` step).

### Standard Plots

Interactive plot browser with selectors for:

- **Plot type**: weight, auto-correlation, cross-correlation, amp-phase, amp-time.
- **Scan number**: navigate through individual scans.

All standard-plot PNGs (converted from PostScript) are displayed at full
resolution.

### Comments

Persists into the experiment toml `[comments]` section (see the
[Experiment TOML Schema](../reference/experiment-toml.md)):

- **General experiment note** — free text, folded into the PI letter's "Further
  remarks" section.
- **Per-station status** — a traffic-light selector: 🟢 no problem / 🟠 issues
  reported / 🔴 could not observe.
- **Per-station note** — free text, pre-filled automatically from:
    - the station summary (did-not-observe, missed time ranges, reduced
      bandwidth — the same data shown in the terminal panel before
      `antab_editor` opens, see [Workflow Steps](../reference/steps.md#11-antab-antfiles)),
    - the EVN feedback database, when `~/.config/evn_postprocess/feedbackdb.toml`
      is configured (silently skipped otherwise — see the
      [review API](../api/review.md)).

Click **Save comments** to persist. Saved entries always win over the
auto-generated defaults on reload, and survive a re-run of earlier steps. The
dashboard reloads the toml from disk immediately before saving, so edits made by
a separate paused `postprocess run` process are never lost.

## Experiment summary (`postprocess info`, no `--serve`)

The terminal form (or the `notes.md` file it writes) shows the same summary data
without a browser: experiment name, date, PI/support scientist, reference antenna,
source list with types, antenna participation, and the values sourced from the
experiment toml (marked with their origin).
