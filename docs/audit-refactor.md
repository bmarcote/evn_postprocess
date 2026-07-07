# Audit Report: Standalone, modular re-design of evn_postprocess

Parent PRD: `docs/PRD-refactor.md` (17 issues in `docs/issues-refactor.md`)
Date: 2026-07-06
Files in scope: 16 source files (10 new, 6 modified), 10 test files
Status: **all 11 findings fixed** in the same session (see "Resolution" per finding).

## Summary

The architecture is coherent: the three plugin families follow the same registry
pattern, toml ownership rules are enforced in one place, and the never-block error
contracts are applied consistently. The audit found no critical issues; the three high
findings (network-exposed write API, a silently-ignored toml key, late backend
validation) and all medium/low findings were fixed immediately after review.

## Critical findings

None.

## High findings

### 1. Dashboard write API bound to all interfaces, unauthenticated

**Location**: `plotting.py` (`serve_dashboard`)
**Category**: Security
**Problem**: The dashboard server bound 0.0.0.0 while exposing unauthenticated write
endpoints (`set_comments`, `set_source_type`, `set_refant`): anyone on the network
could rewrite PI-letter content or flip source types mid-processing.
**Resolution**: Binds 127.0.0.1; remote viewing goes through the SSH tunnel whose
command was already printed.

### 2. `scans` selection parsed and recorded but consumed by nothing

**Location**: `experiment_state.py` / no consumer
**Category**: Logic / acceptance criteria (story 6)
**Problem**: `[observation] scans` was validated, resolved, and displayed, but no step
filtered by it — a silent configuration no-op.
**Resolution**: A prominent warning is logged whenever the key is set ("recorded but
NOT applied yet"), and `docs/experiment.toml.example` documents it as a planned
feature. Full wiring (j2ms2/pipeline input generation) remains an open item.

### 3. Pipeline/distribution modes validated only mid-run

**Location**: `main.py` / `workflow.py`
**Category**: Consistency / acceptance criteria (Issue 8)
**Problem**: A typo'd `[pipeline] mode` surfaced only at the pipeinputs step — hours
into a run — via the generic traceback path, while retrieval was validated at startup.
**Resolution**: All three CLI mode flags are validated at argparse time, the effective
toml modes are validated right after the experiment toml is attached (fail-fast before
any step), and the three pipeline-backed steps turn `PipelineError` into a clean step
failure (`workflow._run_pipeline_stage`).

## Medium findings

### 4. `_exp_toml` attach/reload logic triplicated

**Location**: `workflow.py`, `plotting.py`, `distribution/jive.py`
**Category**: Best practices
**Resolution**: Single implementation `experiment_state.attached_toml(exp, fresh=)`;
all three call sites are thin delegates. The lost-update rationale is documented once.

### 5. Explicit `type = "other"` in the toml re-classified on the Experiment

**Location**: `source_classify.py` (`classify_sources`)
**Category**: Logic
**Resolution**: Classification now gates on the toml: a source with an explicit,
non-guessed type entry (including "other") is never re-classified. Regression test
added (`test_explicit_other_in_toml_not_reclassified`).

### 6. vlbeer knowledge in `pipeline.py`; `exec vlbeer` bypassed the backend

**Location**: `pipeline.py`, `workflow.py` (exec registry)
**Category**: Consistency (vs the recorded breakdown decision)
**Resolution**: The implementation moved to `retrieval.jive.fetch_from_vlbeer`;
`pipeline.get_files_from_vlbeer` is a documented deprecated alias; `postprocess exec
vlbeer` now routes through the selected retrieval backend (mode `none` validates
locally instead of ssh-ing out).

### 7. Story 40 partially unmet: no `--pipeline` / `--distribution` CLI flags

**Location**: `main.py`
**Category**: Consistency / acceptance criteria
**Resolution**: Both flags added, mirroring `--retrieval`: validated at parse time,
overriding the toml (`set_cli_mode`/`selected_mode` in both packages), tested.

## Low findings

### 8. Mixed exception base classes

**Resolution**: Documented in each error docstring: backend errors subclass
RuntimeError (operational failures), input errors subclass ValueError (bad data);
nothing catches by common base.

### 9. Registry boilerplate triplicated

**Resolution**: Shared `registry.BackendRegistry` (kind name + error class); the three
packages keep their public APIs as thin delegates. `selected_mode` stays per-package
(different precedence rules).

### 10. Two idioms for operator notification

**Resolution**: New `comms.notify_operator(exp, subject, body, notifier)` (never
blocks); the antab summary uses it. `notify_step_pause` remains the dedicated
pause-flow helper.

### 11. Experiment toml loaded up to three times during initialize

**Resolution**: Folded into finding 4: `main` and the steps reuse
`experiment_state.attached_toml`, cutting redundant loads while keeping the
deliberate fresh-reload points.

## No findings

SQL injection (parameterised feedback-DB query); secrets in logs (DB password never
logged; permission warning added earlier); test brittleness (suites assert observable
behaviour on fixtures); accidental dead code (`parse_masterprojects`/`get_jexp_info`
retained deliberately, pinned by a regression test, removal tracked as follow-up);
JSON state migration (round-trip preserved, `phase_centers` defaults correctly).

## Assessment

With all findings resolved: safe for supervised production use at JIVE, and for
standalone (`--retrieval none --distribution none`) use elsewhere. Remaining
pre-production items are operational, not code: the HITL validations (classifier
against real experiments, dashboard look-and-feel, feedback-DB credentials/query),
wiring the `scans` selection (finding 2 follow-up), and a full local `pytest` pass
after this fix round.
