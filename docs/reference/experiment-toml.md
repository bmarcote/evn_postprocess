# Experiment TOML Schema

`{expname}.toml` complements the `.vex`/`.lis` files with information they cannot
express (source types, PI contacts, backend selection), and records every decision
taken during post-processing so a re-run is fully reproducible and silent. It is
loaded/written by the `experiment_state` module — see the
[full example file](https://github.com/bmarcote/evn_postprocess/blob/master/docs/experiment.toml.example)
in the repository.

Every section and key is **optional**. A missing value is inferred (with a logged
warning), asked interactively, or resolved from `policy.toml` in batch mode — see
[Parameter precedence](#parameter-precedence) below.

## Section ownership

| Section | Owner | Rewritten by the program? |
| --- | --- | --- |
| `[observation]`, `[[pi]]`, `[sources]`, `[retrieval]`, `[pipeline]`, `[distribution]` | You (or the retrieval module) | No — except heuristic source guesses (see below) |
| `[postprocess]` | Program | Yes, via `record_parameters` |
| `[comments]` | Program (dashboard) | Yes, via `record_comments` |

The one exception: when a source type is missing, the heuristic classifier writes
its guess into `[sources.NAME]` marked `guessed = true`. A source you set
explicitly (or a previously-guessed one you corrected) is **never** overwritten.

## `[observation]`

| Key | Type | Description |
| --- | --- | --- |
| `expname` | string | Experiment name (informational). |
| `supsci` | string | Support-scientist username. Toml value wins over the OS user. |
| `scans` | string / list | Scans to process: a single scan (`"4"`), a range (`"3-10"`), or a comma-separated/list combination (`"1-5,20-30,45"`, or `["1-5", "20-30", 45]`). Default: all scans. **Currently recorded but not yet applied** — a warning is logged when this key is set; no step filters by it yet. |

## `[[pi]]`

One array-of-tables entry per PI/contact, in order of preference:

```toml
[[pi]]
name = "Jane Doe"
email = "jane.doe@institute.edu"
```

Used by the `jive` distribution backend when preparing the PI letter; if none are
present at distribution time, the operator is prompted interactively (answers are
appended here) or the run fails clearly in `--batch`.

## `[sources.NAME]`

Source names containing `+`, `-`, or `.` must be quoted (TOML bare-key rule), e.g.
`[sources."J1848+3244"]`.

| Key | Type | Description |
| --- | --- | --- |
| `type` | string | One of `target`, `calibrator`, `fringefinder`, `other`. |
| `protected` | bool | Archive credentials required to download this source's data. |
| `guessed` | bool | `true` marks a program-made heuristic classification (see [Source Classification](../guide/source-classification.md)) — freely editable. |

## Backend selection: `[retrieval]` / `[pipeline]` / `[distribution]`

Each section has a single `mode` key; see [Plugin Backends](../guide/backends.md)
for the available values and defaults, and note the CLI flags (`--retrieval`,
`--pipeline`, `--distribution`) override whatever is set here.

```toml
[retrieval]
mode = "jive"
[pipeline]
mode = "aips"
[distribution]
mode = "jive"
```

## `[postprocess]` (program-written)

Filled in during the `msops` and `prearchive` steps; a complete section is what
makes a re-run silent (no dialog, no dashboard).

| Key | Type | Description |
| --- | --- | --- |
| `weight_threshold` | float | Threshold applied by the weight-flagging operation. |
| `flagged_percent` | float | Resulting flagged-data fraction (%), for the PI letter. |
| `polswap` | list[str] | Antennas requiring a polarization swap. An **explicit empty list** means "none needed" and is meaningfully different from the key being absent. |
| `polconvert` | list[str] | Antennas requiring linear→circular PolConvert. |
| `onebit` | list[str] | Antennas that recorded 1-bit data. |
| `refant` | list[str] | Reference antenna(s), in priority order. |
| `antab_files` | list[str] | Final `.antab` file(s) used, for reproducibility. |
| `polconvert_input_files` | list[str] | PolConvert input file(s) used. |
| `gain_corrections` | table (station = factor) | Per-station gain corrections applied to the ANTAB information. |

## `[comments]` (program-written, dashboard Comments tab)

| Key | Type | Description |
| --- | --- | --- |
| `general` | string | Free-text note about the experiment as a whole (folded into the PI letter). |
| `stations.XX.status` | string | One of `success` (green), `minor` (orange, issues reported), `major` (red, could not observe). |
| `stations.XX.note` | string | Free-text note, pre-filled from the station summary (did-not-observe / missed time / reduced bandwidth) and the feedback database, editable in the dashboard. |

See [Dashboard](../guide/dashboard.md) for the Comments tab UI, and
[Communications](../guide/comms.md) for how the general/station notes reach the PI
letter.

## Parameter precedence

For every decision parameter (weight threshold, polswap/polconvert/onebit lists,
refant, retrieval/pipeline/distribution mode), the resolution order is:

1. **Experiment toml value** → applied silently, no question asked.
2. **Absent, interactive mode** → the usual dialog/dashboard/auto-diagnostics runs,
   and the answer is written back into `[postprocess]`.
3. **Absent, batch mode (`--batch`)** → the `policy.toml` value is used if present,
   otherwise a `REVIEW_REQUIRED` marker is written and the run pauses cleanly.

`policy.toml` (see the [Policy File reference](policy.md)) supplies batch-mode
defaults and sits *beneath* the experiment toml: the experiment toml always wins
when both define the same key.

## Loading & writing

The toml is attached to the running experiment once and reloaded fresh
(`experiment_state.attached_toml(exp, fresh=True)`) immediately before every write
— from the CLI, the workflow steps, the dashboard's save handler, and the
distribution backend — so a value saved by one process (e.g. the dashboard) is
never silently clobbered by another (e.g. a paused `postprocess run`).
