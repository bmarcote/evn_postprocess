# Tasks for Issue 1: Mode resolution, `--mode`/`--config`, and persistence

Parent issue: Issue 1 of `docs/issues-cleanup.md`
Parent PRD: `docs/PRD-cleanup.md`

Context for the executor: this is the foundational slice of Phase 2. It introduces a single operating **mode** (`supsci` | `regular` | `sweeps`) that replaces the three Phase-1 selection flags (`--retrieval`, `--pipeline`, `--distribution`). The mode is auto-detected from the OS user/group, overridable with `--mode`, and persisted on the experiment state so resumes never silently switch modes. The existing name-keyed backend registries (`retrieval.get_retriever`, `pipelines.get_pipeline`, `distribution.get_distributor`) are kept; only how the backend *name* is chosen changes — it now comes from the resolved mode via a mapping, not from per-family CLI globals. Per-mode initialization/distribution *bodies* are later issues (3 and 8); this slice keeps everything working by mapping mode → existing backends.

Reconciliations already decided (see `docs/issues-cleanup.md` preamble): `sweeps` is auto-detected from the OS `sweeps` group and `--config PATH` is optional (falls back to the conventional `{expname}.toml`). Schema handling for Task 3: bump `Experiment.SCHEMA_VERSION` and tolerate an absent `mode` on load by re-detecting.

## Tasks

### 1. Create the `mode` module (enum, detection, resolution, backend mapping)

**Type**: WRITE  
**Output**: `src/evn_postprocess/mode.py` exists with a `Mode` enum, `detect()`, `resolve()`, and `backends_for()`, importable with no side effects.  
**Depends on**: none

Create a new deep module `mode` that owns everything about the operating mode. Define a `Mode` enum with members `supsci`, `regular`, `sweeps`. Implement `detect()` returning a `Mode` from the OS: the running user's login/effective name being `jops`, or membership of the OS group named `supsci`, selects `supsci`; membership of the OS group `sweeps` selects `sweeps`; otherwise `regular`. Use the standard library for this (`getpass`/`os` for the username, `os.getgroups()` plus `grp` to resolve the configured group names to gids); treat a group that does not exist on the machine as "not a member" (do not raise), falling back accordingly. Implement `resolve(cli_mode, stored_mode)` applying precedence CLI > stored > detected, and emit a loguru warning (not an exception) when an explicit `cli_mode` overrides a different `stored_mode`. Implement `backends_for(mode)` returning the retrieval/pipeline/distribution backend names for a mode: `supsci` → (`jive`, `aips`, `jive`), `regular` → (`none`, `aips`, `none`), `sweeps` → (`sweeps`, `aips`, `sweeps`). Follow the naming/docstring conventions and the loguru usage already established in the package (see `retrieval/__init__.py` and `experiment.py`). Do not touch the CLI or the registries yet.

---

### 2. Unit-test the `mode` module

**Type**: TEST  
**Output**: `tests/test_mode.py` passes, covering detection, resolution, and the backend mapping.  
**Depends on**: 1

Write `tests/test_mode.py` following the fixture/mock style of the existing pure-logic suites (e.g. `tests/test_backends.py`, `tests/test_experiment_state.py`). With the OS username and group calls mocked, assert `detect()` returns `supsci` for user `jops`, `supsci` for a member of the `supsci` group, `sweeps` for a member of the `sweeps` group, and `regular` otherwise — including the case where the named group does not exist (no exception, returns `regular`). Assert `resolve()` implements CLI > stored > detected and that an override emits a warning (capture the loguru message). Assert `backends_for()` returns the exact mapping for all three modes. Keep the tests casacore-free so they run locally per-process.

---

### 3. Persist `mode` on the Experiment state

**Type**: WRITE  
**Output**: an `Experiment` carries a `mode`, and it round-trips through `store()`/`load()`.  
**Depends on**: 1

Add a `mode` attribute to `Experiment` (typed as the `Mode` enum, stored by its value string). Serialize it in `Experiment.to_dict` and read it back in `Experiment.from_dict`, mirroring how other scalar fields there are handled. Bump `Experiment.SCHEMA_VERSION` (2 → 3) and, in the loader/migration path (`_migrate_experiment_dict` and/or `from_dict`), tolerate an absent `mode` (older/Phase-1 files) by leaving it unset so callers re-detect it — do not fail to load. Do not wire the CLI here; this task only makes the field exist and persist.

---

### 4. Test mode persistence and resume

**Type**: TEST  
**Output**: a test asserting a stored mode survives store→load and that an absent mode loads cleanly.  
**Depends on**: 3

Extend the persistence tests (follow `tests/test_experiment_persistence.py`, or add to `tests/test_mode.py`) to build an `Experiment` with a set `mode`, `store()` it, `load()` it, and assert the mode is preserved. Add a case where a state dict lacks `mode` (simulating a pre-Phase-2 file) and assert it loads without error and leaves the mode unset/re-detectable. Keep it casacore-free.

---

### 5. Drive backend selection from the mode; remove per-family CLI-mode plumbing

**Type**: WRITE  
**Output**: `retrieval`/`pipelines`/`distribution` no longer expose `_CLI_MODE`/`set_cli_mode`; backend names come from the resolved mode; a retrieval `sweeps` stub is registered.  
**Depends on**: 1

Remove the `_CLI_MODE` module global and the `set_cli_mode` function from `retrieval/__init__.py`, `pipelines/__init__.py`, and `distribution/__init__.py`. Change how the backend name is chosen: instead of each family's `selected_mode(exp_toml)` consulting a CLI global, the caller resolves the mode and derives the three backend names via `mode.backends_for(...)`. Update the selection call sites in `workflow.py` (the `retrieval.selected_mode(...)`, `pipelines.selected_mode(...)`, and `distribution.selected_mode(...)` uses) to select the backend from `exp.mode` via `backends_for`, keeping `get_retriever`/`get_pipeline`/`get_distributor` unchanged. Keep each family's `selected_mode` only if it still reads the experiment toml as a secondary source; otherwise simplify it to take an explicit backend name. Register a minimal retrieval `sweeps` backend stub (factory raising the family's not-implemented error) so selecting `sweeps` mode fails explicitly and symmetrically with the existing `distribution` `sweeps` stub. Do not change the CLI surface in this task.

---

### 6. Update backend tests for mode-driven selection

**Type**: TEST  
**Output**: `tests/test_backends.py` and `tests/test_retrieval.py` pass against the removed `set_cli_mode` and the new mode→backend mapping.  
**Depends on**: 5

Update the existing backend suites that exercise `set_cli_mode`/`_CLI_MODE` (notably `tests/test_backends.py`, and any assertion in `tests/test_retrieval.py`/`tests/test_distribution_jive.py`) to the new model: assert that `mode.backends_for(mode)` selects the correct registered backend for each mode, that the retrieval/distribution `sweeps` stubs raise the explicit not-implemented error, and remove or replace the now-deleted `set_cli_mode` round-trip assertions. Keep the registry-lookup and unknown-name-error assertions intact.

---

### 7. Wire the CLI to `--mode`/`--config`; remove the three old flags

**Type**: WRITE  
**Output**: `postprocess` accepts a single `--mode` (+ optional `--config`), no longer accepts `--retrieval`/`--pipeline`/`--distribution`, resolves+persists the mode at init, and reports it in `postprocess info`.  
**Depends on**: 3, 5

In `main.py`, remove the `--retrieval`, `--pipeline`, and `--distribution` arguments and the `set_cli_mode` validation block. Add `--mode` with choices `supsci|regular|sweeps` (an invalid value must error at argparse time) and an optional `--config PATH` used by `sweeps` mode, falling back to the conventional `{expname}.toml` when omitted. At initialization, call `mode.resolve(cli_mode=args.mode, stored_mode=<persisted or None>)` after `detect()`, persist the resolved mode on the experiment (via Task 3's field) and `store()` it, and warn when `--mode` overrides a different stored value. On resume (loading an existing experiment), reuse the stored mode unless `--mode` is given. Keep the existing fail-fast validation that the resolved backends are registered/implemented (now derived from the mode), erroring before any step runs. Surface the resolved mode in `postprocess info` output alongside the other metadata. Do not change the per-mode initialization/distribution bodies (later issues).

---

### 8. CLI/integration tests for mode selection and flag removal

**Type**: TEST  
**Output**: tests asserting override-warns-and-repersists, unknown `--mode` fails at parse time, resume keeps the stored mode, and the old flags are gone.  
**Depends on**: 7

Add CLI-level tests (extend `tests/test_mode.py` or add a small `tests/test_cli_mode.py`, following the batch/CLI-driving style of `tests/test_workflow_batch.py` and `tests/test_batch_e2e.py` with external binaries mocked). Assert: an unknown `--mode` value aborts at parse time; a `--mode` that differs from the persisted value overrides, re-persists, and logs the change; a plain resume with no `--mode` reuses the stored mode without re-detecting; and that `--retrieval`/`--pipeline`/`--distribution` are no longer accepted. Keep everything casacore-free where possible; where a test must drive the full CLI, isolate it so it can run per-process on Linux/JIVE.

---
