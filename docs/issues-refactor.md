# Issues: Standalone, modular re-design of evn_postprocess

Derived from `docs/PRD-refactor.md`. 17 tracer-bullet issues in dependency order.
Decision recorded during breakdown (supersedes the PRD's Pipeline-interface wording):
**`.log`/`.antabfs` fetching belongs to the retrieval module** (a `fetch_station_files` method
called at the antab step), so all JIVE-server (vlbeer) knowledge lives in retrieval;
`PipelineBackend.prepare()` only consumes local files.

---

## Issue 1: Experiment toml round-trip (`experiment_state` module)

**Type**: AFK
**Blocked by**: None — can start immediately

### Parent PRD

`docs/PRD-refactor.md`

### What to build

The `experiment_state` module: load, merge, and write back the `EXPNAME.toml` with the section
layout defined in "Experiment toml layout" (`[observation]`, `[sources]`, `[pi]`, `[pipeline]`,
`[distribution]`, `[postprocess]`, `[comments]`). Implement the precedence rule (experiment toml
wins over `policy.toml`), the write-back API (`record_parameters`, `record_comments`), and the
guarantee that user sections are never rewritten except for recorded heuristic classifications.
Scans-to-process must accept ranges, individual scans, and lists of ranges. The existing JSON
checkpoint file stays untouched and separate. Wire a minimal end-to-end path: `postprocess info`
on a directory containing only a toml shows values sourced from it.

### How to verify

- **Automated**: round-trip test — load a toml, call `record_parameters` with sample values,
  reload, assert `[postprocess]` filled and user sections byte-identical; precedence test with
  conflicting policy.toml values; scan-selection parsing test for `"3-10"`, `"4"`, `"1-5,20-30"`.
- **Manual**: edit a value in `[sources]`, run a write-back, confirm the edit survives.

### Acceptance criteria

- [ ] Given a toml with all sections, when the program records parameters, then only
  program-owned sections change.
- [ ] Given a policy.toml and an experiment toml defining the same key, then the experiment toml
  value is used.
- [ ] Given a malformed toml, then loading fails with an error naming file and key.

### User stories addressed

- User story 6: toml as the home for sources/scans/pipeline/PI info
- User story 22: parameters written back (groundwork)
- User story 23: silent re-run from completed toml (groundwork)

---

## Issue 2: Vex-only initialize (drop MASTER_PROJECTS, .jexp, .expsum)

**Type**: AFK
**Blocked by**: Issue 1

### Parent PRD

`docs/PRD-refactor.md`

### What to build

The `inputs` module: `load_experiment(vexfile, lisfiles, tomlfile|None) -> Experiment`.
Observing date from the vex `$EXPER` block, e-EVN membership from `exper_description`
(`e-EVN: EXP1, ...`), stations/scans/sources/frequency setup from the vex, pass definitions from
the `.lis` files. Re-implement the `initialize` workflow step on top of it. Remove every core
read of `MASTER_PROJECTS.LIS`, `.jexp`, `.expsum`. JSON state migration (`Experiment.from_dict`)
keeps loading old files. Correct the stale Snakemake docstring in the engine.

### How to verify

- **Automated**: fixture vex+lis set (regular and e-EVN variants) → assert Experiment fields
  (obsdate, eEVNname, stations, scans, passes); old JSON state file loads unchanged.
- **Manual**: in a directory with only `EXP.vex` + `EXP.lis`, run `postprocess info` and see
  correct metadata with no server contact.

### Acceptance criteria

- [ ] Given a vex with `exper_description = "e-EVN: EA100, EB200"`, when initialized as EB200,
  then eEVNname is EA100 and siblings list is [EA100, EB200].
- [ ] Given no MASTER_PROJECTS/.jexp/.expsum anywhere, then initialize succeeds.
- [ ] Given an unparseable vex, then a clear parse error names the file and line.

### User stories addressed

- User story 5: metadata from vex/lis only
- User story 30: e-EVN detection from exper_description
- User story 39: JSON state migration preserved

---

## Issue 3: Heuristic source classification

**Type**: HITL — final heuristics must be validated by Benito against past experiments
**Blocked by**: Issue 2

### Parent PRD

`docs/PRD-refactor.md`

### What to build

When `[sources]` is absent/incomplete: take observed sources (vex list cross-checked against the
MS when available), match names/positions against the RFC/astrogeo catalogues (reuse the Source
component approach from `~/Programming/vlbiplanobs`), classify: unknown → target; known and
observed most of the time, bracketing targets → phase calibrator; known with only a handful of
scans → fringe finder. Log warnings, write results into the toml `[sources]` as the record of
assumptions, never block. Guesses remain editable via `postprocess edit` and the toml before the
pipeline runs. If the experiment name starts with a “N” or “F”, then all sources are targets.

### How to verify

- **Automated**: fixture scan tables (phase-referencing pattern, fringe-finder scans) → expected
  classification; missing-catalogue/network failure → classification still completes with
  warnings.
- **Manual (HITL)**: run against 3–5 past real experiments and confirm classifications with the
  support scientist before merging.

### Acceptance criteria

- [ ] Given a toml without `[sources]`, when initializing, then every observed source gets a
  type, warnings are logged, and the toml records the guesses.
- [ ] Given a toml with `[sources]` complete, then no heuristic runs and no toml rewrite occurs.
- [ ] Given no network access to catalogues, then classification degrades (scan statistics only)
  and the run continues.

### User stories addressed

- User story 7: heuristic classification, warn and continue
- User story 8: guesses visible and editable before the pipeline

---

## Issue 4: Retrieval plugin registry + NoneRetriever

**Type**: AFK
**Blocked by**: Issue 2

### Parent PRD

`docs/PRD-refactor.md`

### What to build

The `retrieval` sub-package: `Retriever` interface with `fetch(workdir, expname) -> InputSet`
and `fetch_station_files(exp) -> bool` (the `.log`/`.antabfs` acquisition, per the decision at
the top of this file), a name-keyed registry (`get_retriever(name)`), selection from toml
(`[distribution]`-style key) and CLI, defaulting to `jive`. `NoneRetriever`: validates the vex,
lis (and optional toml) already exist locally and that station files are present when requested;
raises `RetrievalError` naming exactly what is missing. Unknown backend name → explicit error at
selection time.

### How to verify

- **Automated**: registry lookup tests; NoneRetriever on a complete fixture dir → InputSet;
  on an incomplete dir → RetrievalError naming the file; unknown name → clear error.
- **Manual**: `postprocess --retrieval none run initialize` in a prepared directory completes
  without any ssh call.

### Acceptance criteria

- [ ] Given retrieval mode `none` and all files local, when initializing, then no remote host is
  ever contacted.
- [ ] Given a missing .lis file in mode `none`, then the error names the file and states that
  local mode does not create files.
- [ ] Given an unregistered backend name, then the run aborts before any step executes.

### User stories addressed

- User story 2: run from local vex/lis/data only
- User story 3: no JIVE server contacted in none/none mode
- User story 37: retrieval errors name file and source
- User story 40: backend selectable from toml/CLI

---

## Issue 5: JiveRetriever extraction

**Type**: AFK
**Blocked by**: Issue 4

### Parent PRD

`docs/PRD-refactor.md`

### What to build

`JiveRetriever` implementing the interface from Issue 4 with today's behaviour: copy
`.vex`/`.vix`/`.vax` from the correlator server, create `.lis` files remotely when missing and
fetch them, fetch `.log`/`.antabfs` from vlbeer in `fetch_station_files`, and pre-fill the toml
(PI info) when a JIVE-internal source is trivially available (PRD Open Q4 decision). All ssh/scp
behind the existing timeout-configured helpers so tests can mock them. No `MASTER_PROJECTS.LIS`
or `.jexp` reads anywhere.

### How to verify

- **Automated**: with mocked ssh/scp calls, assert the exact remote commands/paths for vex copy,
  lis creation, and vlbeer fetches match current behaviour (use existing tests as the oracle);
  failure of any remote call surfaces as RetrievalError with host and file.
- **Manual**: on eee, run initialize for a fresh experiment and diff the retrieved files against
  a run of the old version.

### Acceptance criteria

- [ ] Given a fresh experiment on JIVE infrastructure, when retrieval runs, then vex and lis
  files appear in the workdir exactly as with the current version.
- [ ] Given the correlator host is unreachable, then the error names the host and the attempted
  file.
- [ ] Given PI info available from the internal source, then `[pi]` is pre-filled in the toml.

### User stories addressed

- User story 1: unattended retrieval at JIVE
- User story 4: vex/lis retrieval without MASTER_PROJECTS/.jexp/.expsum

---

## Issue 6: Parameter precedence and the silent autonomous phase

**Type**: AFK
**Blocked by**: Issue 1

### Parent PRD

`docs/PRD-refactor.md`

### What to build

Apply the "Parameter precedence and silence rule" to every decision in steps `j2ms2` through
`standardplots2`: weight threshold, polswap/polconvert/onebit antenna lists, refant. Toml value →
apply silently; absent + interactive → current dialog/auto behaviour, answer written to
`[postprocess]`; absent + batch → policy value or `REVIEW_REQUIRED` pause. Interaction happens
only on failure or abnormal conditions. A second run with the completed toml reproduces the
first run with zero prompts.

### How to verify

- **Automated**: extend `tests/test_policy.py`/`test_workflow_batch.py`: run the (mocked-binary)
  autonomous phase with a complete toml → assert no dialog function is ever invoked and applied
  values match the toml; with an incomplete toml in batch → REVIEW_REQUIRED written, exit 0.
- **Manual**: re-run a finished fixture experiment; observe no questions and identical msops.

### Acceptance criteria

- [ ] Given a toml defining all msops parameters, when running j2ms2→standardplots2, then no
  prompt or dialog is raised and the values are applied.
- [ ] Given a missing parameter in interactive mode, when the operator answers, then the answer
  is persisted to `[postprocess]`.
- [ ] Given a step failure, then the workflow stops with an explicit error and remains resumable
  with `postprocess run STEP`.

### User stories addressed

- User story 9: autonomous sweep to FITS-IDI, interaction only on failure
- User story 22: parameters written back
- User story 23: deterministic silent re-run

---

## Issue 7: Antab station summary (terminal + notifier)

**Type**: AFK
**Blocked by**: Issue 2

### Parent PRD

`docs/PRD-refactor.md`

### What to build

`station_summary(exp) -> StationSummary` in the `review` module: per station, did-not-observe,
missed time ranges (scheduled vs observed scans), reduced bandwidth (subband comparison).
Immediately before `antab_editor` launches: render it as a rich terminal panel and send the same
text through the configured notifier. `antab_editor` invocation itself unchanged.

### How to verify

- **Automated**: fixture experiment with one absent station, one partial station, one
  narrow-band station → StationSummary contents asserted; message builder output contains all
  three findings.
- **Manual**: run the antab step on a fixture; see the panel, receive the Mattermost message,
  then antab_editor opens as before.

### Acceptance criteria

- [ ] Given a station that never appears in the MS, then the summary lists it as did-not-observe.
- [ ] Given a station observing half the scans, then its missed time ranges are listed.
- [ ] Given no notifier configured, then the panel still prints and the step proceeds.

### User stories addressed

- User story 10: pre-antab_editor terminal summary
- User story 11: summary via notifier
- User story 12: antab_editor untouched

---

## Issue 8: Pipeline backend registry (aips / none / vpipe)

**Type**: AFK
**Blocked by**: Issues 2, 6

### Parent PRD

`docs/PRD-refactor.md`

### What to build

The `pipelines` sub-package: `PipelineBackend` with `prepare(exp)` (build uvflg and pipeline
input files from **local** files — station-file fetching lives in retrieval, Issue 4),
`run(exp)`, `collect(exp)`. `AipsPipeline` wraps the current EVN.py flow unchanged;
`NonePipeline` no-op that still satisfies downstream steps; `VpipeBackend` registered, raising
"not implemented" at selection. Backend chosen from toml `[pipeline]`/CLI, default `aips`.

### How to verify

- **Automated**: registry tests; NonePipeline run → downstream steps (prearchive) still
  executable; AipsPipeline `prepare` produces the same input files as today on a fixture
  (existing `test_pipeline_antab_handling.py` as oracle); vpipe selection → explicit error.
- **Manual**: run pipeline step with `pipeline = "none"` and confirm the workflow continues to
  prearchive.

### Acceptance criteria

- [ ] Given pipeline mode `aips`, then generated input files are identical to the current
  version's.
- [ ] Given pipeline mode `none`, then the step succeeds without calling any external binary.
- [ ] Given pipeline mode `vpipe`, then selection fails with "not implemented" before any step
  runs.

### User stories addressed

- User story 27: named unimplemented backends
- User story 40: backend selectable from toml/CLI

---

## Issue 9: Dashboard Comments tab

**Type**: HITL — UI layout/behaviour reviewed by Benito before merge
**Blocked by**: Issues 1, 7

### Parent PRD

`docs/PRD-refactor.md`

### What to build

Extend the existing single-page dashboard with a Comments tab: one general experiment note
textarea; per-station editable note; per-station green/orange/red selector (no problem / issues
reported / could not observe). Notes and statuses pre-filled from the Issue-7 StationSummary
(did-not-observe → red + auto-note, missed time/reduced bandwidth → orange + auto-note).
Saving POSTs to the dashboard API, which persists into the toml `[comments]` section via
`record_comments`.

### How to verify

- **Automated**: dashboard JSON API tests — GET returns pre-filled comments derived from a
  fixture summary; POST round-trips into the toml `[comments]`; reload returns the edited text.
- **Manual (HITL)**: open the dashboard on a fixture experiment, edit notes and statuses,
  confirm the toml content and the look-and-feel.

### Acceptance criteria

- [ ] Given a station that could not observe, when the dashboard opens, then its note is
  pre-written and its status is red.
- [ ] Given an operator edits a note and saves, then the toml `[comments]` reflects it and a
  re-opened dashboard shows it.
- [ ] Given a re-run of earlier steps, then previously saved comments survive.

### User stories addressed

- User story 14: comments tab
- User story 15: auto-filled station notes
- User story 16: traffic-light station status
- User story 19: comments persisted to toml

---

## Issue 10: Station feedback MySQL lookup

**Type**: HITL — DB schema/credentials (PRD Open Q1) must be supplied by Benito
**Blocked by**: Issue 9

### Parent PRD

`docs/PRD-refactor.md`

### What to build

`station_feedback(exp) -> dict[station, comment]` backed by the EVN feedback MySQL database.
Connection settings from `~/.config/evn_postprocess/feedbackdb.toml`. Missing config, connection
error, or query error → return `{}` and log at debug level only. Returned comments become the
default per-station notes in the Comments tab (operator-editable, auto-notes appended).

### How to verify

- **Automated**: with a stubbed DB layer — comments merge into dashboard defaults; no config
  file → empty dict, no warning above debug; connection raising → empty dict, run continues.
- **Manual (HITL)**: with real credentials at JIVE, open the dashboard for a past experiment and
  confirm station comments match the archive feedback pages.

### Acceptance criteria

- [ ] Given a valid config and reachable DB, then station notes default to the DB comments.
- [ ] Given no config file, then the dashboard behaves identically to a non-JIVE machine with no
  visible warning.
- [ ] Given a DB error mid-query, then the run continues and the tab shows only auto-notes.

### User stories addressed

- User story 17: DB-sourced default notes
- User story 18: silent skip without config/connection

---

## Issue 11: Review request flow (announce + confirm + re-run-from-step)

**Type**: AFK
**Blocked by**: Issue 9

### Parent PRD

`docs/PRD-refactor.md`

### What to build

After `postpipe` (pipeline diagnostics): announce the review in the terminal and via the
notifier with the exact URL/command to open the dashboard. Confirmation accepts "approve" or
"re-run from step X" (validated against step names); re-run resets the workflow to that step. In
batch mode: no dashboard served, `REVIEW_REQUIRED` marker + notifier message, exit 0, resume on
next invocation (as today).

### How to verify

- **Automated**: interactive confirm with "re-run from tconvert" → workflow state reset to
  tconvert and steps re-executed; invalid step name → re-prompt; batch mode → marker written,
  exit 0, no dialog invoked.
- **Manual**: full fixture run; receive the Mattermost message, open the dashboard, answer
  re-run, watch it resume.

### Acceptance criteria

- [ ] Given postpipe completes, then terminal and notifier both carry the dashboard pointer.
- [ ] Given the operator requests re-run from a step, then processing restarts there and returns
  to the review point.
- [ ] Given batch mode, then no prompt is raised and REVIEW_REQUIRED is written.

### User stories addressed

- User story 13: reactive review request
- User story 20: optional re-run from a chosen step

---

## Issue 12: Finalisation — ANTAB attach + derived-info persistence

**Type**: AFK
**Blocked by**: Issues 6, 9

### Parent PRD

`docs/PRD-refactor.md`

### What to build

On review confirmation, run the ANTAB/Tsys attachment into the FITS-IDI files (current
`prearchive`) unattended, then complete the toml `[postprocess]` record: flag weight threshold,
polswap/polconvert/2-bit antenna lists, refant, amount of flagged data, antennas with problems,
gain corrections required for the ANTAB files, and links to final antab and polconvert-input
files. This record is what Issue 14's PI letter and the future feedback upload consume. It must verify that the Tsys has been correctly appended.

### How to verify

- **Automated**: fixture run with mocked binaries → after finalisation the toml contains every
  field with expected values; a fresh run seeded with that toml reproduces the same msops with
  no prompts (ties to Issue 6).
- **Manual**: inspect the toml of a completed fixture experiment for completeness.

### Acceptance criteria

- [ ] Given review confirmation, then prearchive runs without further interaction.
- [ ] Given finalisation completes, then the toml links resolve to existing antab/polconvert
  files.
- [ ] Given the derived-info record, then flagged-data fraction and gain corrections are present
  per pass/station as available.

### User stories addressed

- User story 21: automatic final steps after confirmation
- User story 22: full parameter write-back
- User story 34: derived experiment info persisted for letter/Grafana

---

## Issue 13: Distribution plugin registry + NoneDistributor + sweeps stub

**Type**: AFK
**Blocked by**: Issue 2

### Parent PRD

`docs/PRD-refactor.md`

### What to build

The `distribution` sub-package: `Distributor` interface (`deliver(exp) -> bool`), name-keyed
registry, selection from toml/CLI with default `jive`. `NoneDistributor`: no-op that marks the
workflow complete without archiving or contacting any server. `SweepsDistributor`: registered,
raising "not implemented" at selection time.

### How to verify

- **Automated**: registry tests; none mode → workflow reaches completed state, no external
  calls; sweeps selection → explicit not-implemented error.
- **Manual**: full fixture run with `distribution = "none"` finishes cleanly with data left in
  place.

### Acceptance criteria

- [ ] Given distribution mode `none`, then nothing is archived and no JIVE server is contacted.
- [ ] Given distribution mode `sweeps`, then selection fails with a "not implemented" message.
- [ ] Given no distribution key anywhere, then `jive` is selected by default.

### User stories addressed

- User story 26: none mode leaves no trace
- User story 27: named unimplemented backends
- User story 40: backend selectable from toml/CLI

---

## Issue 14: JiveDistributor extraction

**Type**: AFK
**Blocked by**: Issues 12, 13

### Parent PRD

`docs/PRD-refactor.md`

### What to build

`JiveDistributor.deliver(exp)`: build the observation summary, create PI credentials, generate
the PI letter from `[pi]`, `[comments]`, and the Issue-12 derived record, archive all data
(current archive flow), then ask the operator to send the letter. Missing PI/support-scientist
info → interactive prompt (answers written to `[pi]`); in batch mode → clear failure naming the
missing fields. Include `upload_feedback(exp)` as a defined, documented stub for the future
Grafana-visible feedback upload.

### How to verify

- **Automated**: letter generation from fixture toml → station notes and statuses appear in the
  letter; missing `[pi]` in batch → failure message lists the fields; archive commands asserted
  via mocked ssh against current behaviour.
- **Manual**: dry-run at JIVE on a test experiment; compare letter and archive layout with the
  old version.

### Acceptance criteria

- [ ] Given complete toml sections, then summary, credentials, letter, and archive complete and
  the operator is asked to send the letter.
- [ ] Given missing PI email interactively, then the operator is prompted and the answer is
  persisted.
- [ ] Given missing PI email in batch, then distribution fails with a message naming the field.

### User stories addressed

- User story 24: jive delivery preserved
- User story 25: PI-info prompt / batch failure
- User story 41: upload_feedback stub

---

## Issue 15: e-EVN synchronisation barriers

**Type**: AFK
**Blocked by**: Issues 7, 8

### Parent PRD

`docs/PRD-refactor.md`

### What to build

The `eevn` module: `siblings(exp)` from `exper_description` + `../EXPm` convention;
explicit FITS-IDI completion markers written by the engine at tconvert/post_polconvert
completion (PRD Open Q3 decision); barrier (a) `antab` step requires completion markers in every
sibling directory; barrier (b) `pipeline` step for EXPn requires the final antab in `../EXP1/`.
Unmet barrier → pause marker, exit 0, resume on next invocation. EXPn uses the vex named after
EXP1.

### How to verify

- **Automated**: tmp-dir fixtures with two sibling experiment dirs — antab step pauses until
  both markers exist, then proceeds; EXPn pipeline pauses until `../EXP1/*.antab` appears;
  resume after file creation continues from the paused step.
- **Manual**: simulate a two-experiment e-EVN run in sibling folders and drive both to
  completion with repeated invocations.

### Acceptance criteria

- [ ] Given a sibling without its completion marker, when antab is reached, then the run pauses
  cleanly with exit 0 and a marker explaining what it waits for.
- [ ] Given the final antab appears in ../EXP1/, when EXPn is re-invoked, then its pipeline
  starts.
- [ ] Given a non-e-EVN experiment, then no barrier check ever triggers.

### User stories addressed

- User story 31: EXP1-named vex + sibling convention
- User story 32: antab gate on all FITS-IDI
- User story 33: pipeline gate on ../EXP1 antab, pause/resume

---

## Issue 16: Multiple-phase-centre passes

**Type**: AFK
**Blocked by**: Issues 2, 6

### Parent PRD

`docs/PRD-refactor.md`

### What to build

Detect multiple phase centres from the vex scan section (more than one source in some scans).
Represent each phase centre's correlation as its own pass (own `.lis`/MS/FITS-IDI), integrate
with the existing multi-lis pass handling so the autonomous phase, antab step, and pipeline
treat them consistently with spectral-line passes. Record the phase-centre → pass mapping in the
experiment state and toml. Note that unless specified in the toml file, the pipeline will only run in the first correlator pass.
Only if `pipeline_all_passes` is defined and is True (assume False otherwise), all passes should be pipelined.

### How to verify

- **Automated**: fixture vex with multi-source scans → passes enumerated with correct
  source/pass association; single-phase-centre vex → behaviour identical to before (regression).
- **Manual**: run a past multi-phase-centre experiment's vex/lis fixture through checklis and
  confirm the reported passes.

### Acceptance criteria

- [ ] Given a vex where some scans list multiple sources, then the experiment reports one pass
  per phase centre with the correct sources.
- [ ] Given a standard single-source vex, then pass detection is unchanged.
- [ ] Given multiple phase-centre passes, then the pipeline runs over all of them like other
  multi-lis passes.

### User stories addressed

- User story 28: single-pass case stays simple (regression guarantee)
- User story 29: multi-pass observations keep working, extended to phase centres

---

## Issue 17: Batch-mode end-to-end sweep + CLI regression

**Type**: AFK
**Blocked by**: Issues 6, 11, 14, 15, 16

### Parent PRD

`docs/PRD-refactor.md`

### What to build

The closing verification slice: end-to-end batch runs (mocked external binaries) over three
fixture types — standard single-pass, multi-lis (continuum + line), and a two-sibling e-EVN run —
asserting the full contract: no prompt ever raised in `--batch`, every interaction point resolves
from toml/policy or writes `REVIEW_REQUIRED` and exits 0, failures leave resumable state, and the
public CLI (`postprocess`, `run`, `exec`, `edit`, `info`, `last`, `--batch`) behaves as
documented. Fix anything the sweep uncovers.

### How to verify

- **Automated**: the three end-to-end tests above added to CI; full existing suite green; a
  scripted `--batch` loop (invoke → create awaited file → re-invoke) drives each fixture to the
  completed state.
- **Manual**: run the batch loop on eee for one real experiment with distribution `none`.

### Acceptance criteria

- [ ] Given any fixture in batch mode, then no interactive prompt is ever raised.
- [ ] Given an induced step failure, then state remains intact and `postprocess run STEP`
  resumes.
- [ ] Given the pre-refactor CLI invocations, then all still work unmodified.

### User stories addressed

- User story 28: single-pass end-to-end
- User story 29: multi-lis end-to-end
- User story 35: batch mode never prompts
- User story 36: failures resumable
- User story 38: CLI preserved

---
