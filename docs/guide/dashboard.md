# Dashboard

The web dashboard provides a visual overview of the experiment and its standard plots.

## Launching the dashboard

### During post-processing

The dashboard opens automatically when the `plots` step completes (in interactive mode). It serves until the user closes it with `Ctrl+C`.

### Standalone

```bash
postprocess info --serve
```

This is useful for reviewing a previously processed experiment without re-running steps.

## Accessing the dashboard remotely

The dashboard runs on the processing server. To access it from your local machine, use an SSH tunnel:

```bash
ssh -L 8050:localhost:8050 user@eee.jive.nl
```

Then open `http://localhost:8050` in your browser. The actual port is printed to the terminal when the dashboard starts.

## Dashboard sections

### Experiment summary

Displays:

- Experiment name, observation date, time range.
- PI and Support Scientist.
- Reference antenna.
- Source list (fringe-finders, targets, phase-cals).
- Antenna participation (observed / not observed).
- Polswap, PolConvert, 1-bit flags.
- Correlator pass details (frequency, bandwidth, subbands, channels).

### Scan overview table

A colour-coded table showing antenna participation per scan:

- **Green** — Antenna has data.
- **Red** — Antenna was scheduled but has no data.
- **No colour** — Antenna was not scheduled.

Scan rows are coloured by source type (orange = fringe-finder, cyan = target, yellow = phase-cal).

### Plot viewer

Interactive plot browser with selectors for:

- **Plot type**: weight, auto-correlation, cross-correlation, amp-phase, amp-time.
- **Scan number**: Navigate through individual scans.

All standard-plot PNGs (converted from PostScript) are displayed at full resolution.
