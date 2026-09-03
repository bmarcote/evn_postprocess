# Dashboard

The web dashboard provides a visual overview of the experiment, its standard
plots, the pipeline output, a station-comments review tab that feeds the PI letter,
and the progress of the post-processing itself.

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

- **General experiment note** — free text, the first bullet of the
  [PI letter](pi-letter.md)'s "General remarks" section.
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

### Progress

The full list of post-processing steps, in execution order, and which of them have
already run — the same information as `postprocess list`, from the same source
(`workflow.step_progress`), so the terminal and the browser can never disagree.

Each step shows a marker (🟢 done, 🔵 the next one to run, ⚫ still pending), its name,
and what it does. A bar at the top summarises how far the post-processing has got.

The done flags are re-read from the `{expname}.json` checkpoint on every request and the
tab polls every 15 s while it is visible, so a dashboard left open in one terminal
follows a run advancing in another.

## Experiment summary (`postprocess info`, no `--serve`)

The terminal form (or the `notes.md` file it writes) shows the same summary data
without a browser: experiment name, date, PI/support scientist, reference antenna,
source list with types, antenna participation, and the values sourced from the
experiment toml (marked with their origin).
