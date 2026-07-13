# Issues: Cleaner, mode-driven evn_postprocess (Phase 2)

Derived from `docs/PRD-cleanup.md`. 10 tracer-bullet issues in dependency order. Each issue is a thin end-to-end slice (schema/logic/CLI/tests), not a horizontal layer.

Decisions recorded during breakdown (they resolve wording in the PRD; the PRD file is not modified):

- **Sweeps entry**: `sweeps` mode is auto-detected from membership of the OS group `sweeps` (like `supsci`), and `--config PATH` is optional — when omitted, the conventional `{expname}.toml` in the working directory is used. This supersedes the "sweeps is never auto-detected / requires --config" wording in Implementation Decisions.
- **Concurrency scope**: this phase does NOT add a new explicit per-MS lock. It keeps today's `ThreadPoolExecutor` parallelism and adds a benchmark/comparison test harness (open question 5) for a later decision. The "per-MS lock" language in the PRD is treated as a documented property of current behaviour, not new code in this phase. (PRD user story 38 was intentionally removed.)
- **Log file names** (PRD open questions 1–2, resolved): the three channels are the Rich terminal; `logs/logging_messages.log` (loguru DEBUG/INFO); and `logs/commands.sh` (replayable shell-runnable commands, one per line, optional per-step comment headers). The historical `post_processing.log` debug sink is renamed/relocated into `logs/`.

---

## Issue 1: Mode resolution, `--mode`/`--config`, and persistence

**Type**: AFK
**Blocked by**: None — can start immediately

### Parent PRD

`docs/PRD-cleanup.md`

### What to build

The `mode` module and its wiring into the CLI, making an operating mode (`supsci` | `regular` | `sweeps`) the single organising concept. Auto-detect from the OS: username `jops` or membership of the OS group `supsci` selects `supsci`; membership of the OS group `sweeps` selects `sweeps`; otherwise `regular`. A missing group is treated as non-membership (fall back to `regular`). `--mode supsci|regular|sweeps` forces the mode. For `sweeps`, `--config PATH` is optional (falls back to the conventional `{expname}.toml`). The resolved mode is persisted in the experiment state at initialization and reused by every later invocation (`run`, `exec`, `dashboard`); a `--mode` on a later invocation overrides and re-persists, warning when it differs from the stored value.

Remove the three Phase-1 selection flags (`--retrieval`, `--pipeline`, `--distribution`) and their per-family `set_cli_mode`/`selected_mode` CLI plumbing, mapping the chosen mode to the internal backends instead (the calibration pipeline remains the current AipsPipeline for all modes — see PRD "Pipeline and distribution abstractions"). This slice keeps behaviour working by routing mode → existing backends; the per-mode initialization and distribution bodies are Issues 3 and 8.

### How to verify

- **Automated**: with the OS user/group calls mocked, assert `detect()` returns `supsci` for user `jops` and for a `supsci`-group member, `sweeps` for a `sweeps`-group member, `regular` otherwise (including when the group does not exist); `resolve()` applies CLI-over-stored-over-detected and warns on override; the resolved mode round-trips through the experiment state (init then reload). Assert an invalid `--mode` value errors at parse time. Assert `--retrieval`/`--pipeline`/`--distribution` are gone.
- **Manual**: as a non-jops user in a prepared directory, `postprocess info` reports mode `regular`; `postprocess --mode supsci info` reports `supsci` with an override warning; re-running without `--mode` keeps `supsci`.

### Acceptance criteria

- [ ] Given the running user is `jops` or in the `supsci` group, when the mode is resolved with no `--mode`, then it is `supsci`.
- [ ] Given the running user is in the `sweeps` group, when the mode is resolved with no `--mode`, then it is `sweeps`.
- [ ] Given no matching user/group, when the mode is resolved with no `--mode`, then it is `regular` and no server is implied.
- [ ] Given a mode was persisted at init, when the experiment is re-run without `--mode`, then the same mode is used without re-detection.
- [ ] Given `--mode` differs from the persisted mode, then the new mode is used, re-persisted, and a warning names the change.
- [ ] Given an unknown `--mode` value, then the program aborts at parse time before any step runs.

### User stories addressed

- User story 1: supsci auto-detected
- User story 2: regular auto-detected
- User story 3: `--mode` override
- User story 4: mode persisted and reused
- User story 5: override re-persists with warning
- User story 9: `sweeps` config supplied via `--config` (optional)
- User story 27: pipeline behind the ABC (retained)
- User story 28: first pipeline impl reproduces current behaviour

---

## Issue 2: Reporting — three separate per-step output channels

**Type**: AFK
**Blocked by**: None — can start immediately

### Parent PRD

`docs/PRD-cleanup.md`

### What to build

The `reporting` module giving every step three independent sinks behind one small interface: a concise, colourful Rich terminal message for the operator; a verbose loguru debug record written to `logs/logging_messages.log`; and a replayable command log at `logs/commands.sh` that records the exact local command(s) each step ran, one shell-runnable line per command (optionally preceded by a per-step comment header) so the file reads top-to-bottom as a manual runbook. Relocate/rename the historical `post_processing.log` debug sink into `logs/logging_messages.log` so nothing collides with the command log. Adopt the interface across the existing steps so each emits on all three channels as appropriate.

### How to verify

- **Automated**: drive a representative step (e.g. flag-weights) through the reporting interface on a fixture and assert (a) a terminal message was emitted, (b) the loguru detail landed in `logs/logging_messages.log`, and (c) `logs/commands.sh` contains the expected replayable line (e.g. `mstools run flag_weights exp.ms 0.9`). Assert no file named `post_process.log`/`post_processing.log` is written at the experiment root.
- **Manual**: run a step and confirm the terminal shows a clean coloured status while `logs/logging_messages.log` holds the detail and `logs/commands.sh` can be read as a runbook.

### Acceptance criteria

- [ ] Given a step runs, then it prints a concise Rich terminal message and writes verbose detail only to `logs/logging_messages.log`.
- [ ] Given a step runs a local tool, then `logs/commands.sh` gains a shell-runnable line reproducing that command.
- [ ] Given the three sinks, then changing one (e.g. terminal verbosity) does not alter the contents of the others.
- [ ] Given a completed run, then `logs/commands.sh` executed top-to-bottom reproduces the manual steps.

### User stories addressed

- User story 40: concise colourful terminal message
- User story 41: verbose loguru debug kept out of the way
- User story 42: replayable command log
- User story 43: three genuinely separate sinks
- User story 44: debug log renamed off the command-log name

---

## Issue 3: Initialization abstraction with one implementation per mode

**Type**: AFK
**Blocked by**: Issue 1

### Parent PRD

`docs/PRD-cleanup.md`

### What to build

The initialization abstraction (`Initializer`) replacing the Phase-1 `retrieval/` package, with one implementation per mode selected from the resolved mode (Issue 1). `Supsci` reproduces today's retrieval exactly (`.vix` from the correlator server following the current rules, remote creation and copy of the `.lis` files); it is one of the only two modules permitted outbound server access. `Regular` validates that the `.vex`, `.lis`, and other required inputs already exist locally and never contacts a server. `Sweeps` is registered but not-yet-implemented (fetch-from-SWEEPS), failing explicitly at selection. A shared final phase (all modes) creates the directory structure and verifies every required input file is present. In `sweeps` mode the experiment toml is the config, required to be fully populated up-front: no heuristic classification runs and no value is guessed, and a value a step needs but the config lacks is a hard, immediate, field-named error (never a pause).

### How to verify

- **Automated**: registry/selection tests per mode. `Regular` on a complete fixture dir → success; on an incomplete dir → error naming the missing file and where it was expected, and a non-zero exit path. `Supsci` with mocked ssh/scp → the expected remote commands/paths (existing tests as the oracle). `Sweeps` selection → explicit not-implemented error. `Sweeps` with an incomplete toml → hard error naming the missing field (no pause).
- **Manual**: in a prepared local directory as a regular user, `postprocess run initialize` completes with no ssh; removing a required `.lis` produces a clear named error and non-zero exit.

### Acceptance criteria

- [ ] Given `regular` mode and all inputs local, when initializing, then no remote host is contacted and the directory structure is created.
- [ ] Given `regular`/any mode with a required file missing, when initializing, then the run reports the missing file (Rich + comms if configured) and exits non-zero.
- [ ] Given `supsci` mode, when initializing, then the `.vix` and `.lis` files are obtained exactly as the current version does (mocked ssh oracle).
- [ ] Given `sweeps` mode, when initializing, then it fails with an explicit not-implemented message at selection time.
- [ ] Given `sweeps` mode with an incomplete config, when a step needs a missing value, then the run fails immediately naming the field, without pausing.

### User stories addressed

- User story 6: supsci retrieves the same inputs, repackaged
- User story 7: regular assumes local inputs, no server
- User story 8: sweeps retrieves from SWEEPS (stub)
- User story 10: sweeps config is the experiment toml, fully populated
- User story 12: create structure + verify required files
- User story 13: missing file → notify + error exit
- User story 14: sweeps incomplete config → immediate field-named failure
- User story 33: sweeps retrieval registered but not implemented

---

## Issue 4: Server-agnostic core — legacy removal and clean-break state

**Type**: AFK
**Blocked by**: Issue 3

### Parent PRD

`docs/PRD-cleanup.md`

### What to build

Remove the legacy server-coupled code from the core now that initialization owns all supsci-mode retrieval (Issue 3): delete the `Server`/`Servers` classes, `retrieve_servers`, `parse_masterprojects`, `get_jexp_info`, and the `.jexp`/`.expsum` handling; replace the `comment_tasav.py` `ssh jops@ccs … MASTER_PROJECTS.LIS` observing-date lookup with the vex-derived date already available; and remove the compatibility shims (e.g. the re-exported `parse_masterprojects`) together with the regression test that pinned them. Enforce the server-agnostic invariant mechanically: no module outside the supsci-mode initialization module and the supsci-mode distribution module performs outbound server access (ssh/scp/sftp/remote subprocess). The `--tConvert-in-eee` remote workaround is the one sanctioned, walled-off exception, recorded as tech debt. The standalone `scripts/` utilities are left untouched. Take the clean break on persisted state: drop legacy fields (Servers, etc.) from the state schema with no migration; pre-phase `{expname}.json`/`{expname}.toml` need not load.

### How to verify

- **Automated**: a guard test asserting that no module except the two sanctioned supsci modules (and the tConvert-in-eee exception) imports the ssh/scp helpers or references the removed symbols. `comment_tasav` produces the same observing date from a fixture vex with no network. The full suite stays green after the deletions; the old pinning test is gone.
- **Manual**: grep confirms `Server`/`Servers`/`parse_masterprojects`/`get_jexp_info`/`.expsum` are absent from the core (present only, if at all, under untouched `scripts/`).

### Acceptance criteria

- [ ] Given the cleaned core, then `Server`/`Servers`, `retrieve_servers`, `parse_masterprojects`, `get_jexp_info`, and `.jexp`/`.expsum` handling no longer exist in the core package.
- [ ] Given `comment_tasav`, when it needs the observing date, then it derives it from the vex with no ssh call.
- [ ] Given any core module outside the two supsci modules (excepting tConvert-in-eee), then it performs no outbound server access, enforced by a test.
- [ ] Given the `scripts/` directory, then its utilities are unchanged.
- [ ] Given a pre-phase state file, then it is acceptable for it not to load (clean break); a fresh init works.

### User stories addressed

- User story 15: remove Server(s)/masterprojects/jexp/expsum from core
- User story 16: comment_tasav uses the vex date, no ssh
- User story 17: server access only inside the two supsci modules (checkable invariant)
- User story 18: scripts/ left untouched
- User story 19: remove compat shims and their pinning test
- User story 45: clean break on persisted state, no migration

---

## Issue 5: Antab step — reposition and mode gating

**Type**: AFK
**Blocked by**: Issue 3

### Parent PRD

`docs/PRD-cleanup.md`

### What to build

Position the antab step immediately after `post_polconvert` and the standard plots, and before the pipeline steps, so a single ANTAB session covers the final data. Gate it by mode: in `supsci` mode it retrieves `.log`/`.antabfs` from vlbeer (inside the sanctioned supsci path) and produces the ANTAB and UVFLG files; in `regular` and `sweeps` modes it never contacts vlbeer and instead verifies those files are already present, failing immediately with an error naming exactly which files are missing and where they were expected when they are not.

### How to verify

- **Automated**: assert the step order (`… post_polconvert → standardplots → antab → pipeline …`). `supsci` with mocked vlbeer → ANTAB/UVFLG produced from fixture inputs. `regular`/`sweeps` with the files present → success and no server contact; with files missing → hard error naming the missing files.
- **Manual**: a supsci fixture run reaches antab after the plots and opens the ANTAB flow; a regular fixture run with ANTAB/UVFLG in place proceeds, and without them fails with a clear named error.

### Acceptance criteria

- [ ] Given any mode, then the antab step runs after post_polconvert and the plots and before the pipeline.
- [ ] Given `supsci` mode, when antab runs, then `.log`/`.antabfs` are fetched from vlbeer and ANTAB/UVFLG are produced.
- [ ] Given `regular`/`sweeps` mode with the ANTAB/UVFLG files present, then antab verifies them and contacts no server.
- [ ] Given `regular`/`sweeps` mode with required ANTAB/UVFLG files missing, then the run fails immediately naming the missing files and their expected location.

### User stories addressed

- User story 24: supsci antab after post_polconvert/plots, before pipeline
- User story 25: non-supsci antab expects files present, no vlbeer
- User story 26: non-supsci missing ANTAB/UVFLG → immediate named error

---

## Issue 6: Standard plots twice, with a conditional review pause

**Type**: AFK
**Blocked by**: Issue 1

### Parent PRD

`docs/PRD-cleanup.md`

### What to build

Produce the standard plots twice — once after `j2ms2` (MS creation) and once after `post_polconvert` — always generating them at each point (they are archival). The workflow pauses for review only when a human decision is genuinely required: the flag-weight threshold was not set automatically, or the code cannot determine whether any antenna recorded linear polarization. When it does pause, reuse the single existing review mechanism (the dashboard/dialog in interactive mode; a clean review marker in batch/sweeps). Unambiguous runs generate the plots and proceed without stopping.

### How to verify

- **Automated**: on a fixture where both the threshold and linear-pol determination are resolved (from toml/auto), assert plots are generated at both points and the workflow does not pause. On a fixture where the threshold is unset or linear-pol is undetermined, assert the run pauses via the existing review mechanism (dashboard/dialog interactive; marker in batch/sweeps).
- **Manual**: run an unambiguous fixture and observe two plot generations and no pause; run an ambiguous one and observe the single review pause.

### Acceptance criteria

- [ ] Given any run, then standard plots are generated after j2ms2 and again after post_polconvert.
- [ ] Given the threshold was set automatically and linear-pol is determined, then the run does not pause for plot review.
- [ ] Given the threshold is unset or linear-pol is undetermined, then the run pauses using the existing review mechanism (marker + exit 0 in batch/sweeps).
- [ ] Given a pause, then the same dashboard/dialog gate is used — no second, separate review gate is introduced.

### User stories addressed

- User story 20: identical processing sequence across modes
- User story 22: pause only when a human decision is genuinely required
- User story 23: reuse the single existing review mechanism

---

## Issue 7: j2ms2 lag-MS output to a log file

**Type**: AFK
**Blocked by**: None — can start immediately

### Parent PRD

`docs/PRD-cleanup.md` (Solution paragraph on the lag MS)

### What to build

Make the `j2ms2` run that produces the lag MS write its output directly to a log file rather than to standard output, so that the main `j2ms2` run's real-time console output is no longer delayed or interleaved by the lag-MS run's printing.

### How to verify

- **Automated**: on a fixture (mocked/observed j2ms2), assert the lag-MS creation directs its output to the expected log file and does not write to the process stdout; assert the main j2ms2 run's stdout is unaffected.
- **Manual**: create the lag MS and confirm its output lands in the log file while the main j2ms2 console output streams in real time.

### Acceptance criteria

- [ ] Given the lag-MS j2ms2 run, then its output goes to a log file, not stdout.
- [ ] Given the main j2ms2 run is active, then its console output is not delayed or interleaved by the lag-MS run.

### User stories addressed

- (No numbered user story; Solution paragraph on the lag MS.)

---

## Issue 8: Distribution — rename to `distribute` and mode-gated distributors

**Type**: AFK
**Blocked by**: Issue 1

### Parent PRD

`docs/PRD-cleanup.md`

### What to build

Rename the final step `archive` → `distribute` and the concept to `distribution` throughout, keeping `archive` as a deprecated CLI/exec alias so existing scripts and habits keep working. Implement the mode-gated distributors behind the established ABC + registry. The `supsci` distributor, on approval of the single existing post-`postpipe` review, runs unattended to completion: credentials, auth PI letter, archive of all data, and the final PI letter template for delivery. The non-supsci (`regular`) distributor performs no archiving and no PI-letter work; it verifies the final FITS-IDI files are in order — the expected `*.IDI*` files present for every correlator pass and the ANTAB Tsys/gain-curve information appended (from the existing prearchive/append step) — reporting a clear "ready" on success or a hard error naming what is missing. The `sweeps` distributor is registered but not-yet-implemented (fails explicitly at selection).

### How to verify

- **Automated**: `postprocess run distribute` and the deprecated `postprocess run archive` both resolve to the distribute step. `supsci` distributor with fixture data → credentials/letter/archive stages run in order on approval. `regular` distributor → FITS-IDI + Tsys/GC verification passes on a complete fixture and fails with a named error on a missing/incomplete one, contacting no server. `sweeps` selection → explicit not-implemented error.
- **Manual**: a supsci fixture, after approving the review, completes distribution unattended; a regular fixture ends with a "ready" message and no server contact.

### Acceptance criteria

- [ ] Given the CLI, then `distribute` is the step name and `archive` still works as a deprecated alias.
- [ ] Given `supsci` mode and review approval, then credentials, auth PI letter, archive, and the final PI letter template run unattended to completion.
- [ ] Given `regular`/`sweeps` mode, then no archiving or PI-letter action occurs and no server is contacted.
- [ ] Given `regular`/`sweeps` mode with complete FITS-IDI (files per pass + Tsys/GC appended), then distribute reports ready.
- [ ] Given `regular`/`sweeps` mode with FITS-IDI missing or Tsys/GC not appended, then distribute fails with a named error.
- [ ] Given `sweeps` distribution selected, then it fails with an explicit not-implemented message.

### User stories addressed

- User story 29: archive → distribute rename with deprecated alias
- User story 30: supsci distribution runs unattended on review approval
- User story 31: non-supsci distribution verifies FITS-IDI in order, no archive
- User story 32: sweeps distribution registered but not implemented

---

## Issue 9: Execution and error model — hard-fail resume, skip_steps, parallel-comparison test

**Type**: AFK
**Blocked by**: Issues 1, 2

### Parent PRD

`docs/PRD-cleanup.md`

### What to build

Evolve the engine so all steps run automatically from init to distribute, and a step failure is a hard stop distinct from the clean review-pause: on error the engine notifies (Rich terminal + the configured `comms.toml` notifier, unchanged), leaves the state so re-launching resumes from the failed step, and exits non-zero — whereas a review-required pause still writes its marker and exits zero. Honour a `skip_steps` list (from the experiment toml, used in `sweeps` mode) so the runner bypasses the named steps. Keep today's intra-step parallelism (`ThreadPoolExecutor`) as-is — this phase does not add a new explicit per-MS lock — and add a benchmark/comparison test harness that can compare parallelism approaches (ThreadPool vs asyncio vs …) for a later decision (PRD open question 5).

### How to verify

- **Automated**: on a mocked-binary fixture, induce a step failure → assert the configured notifier fired, the process exit is non-zero, and a subsequent `postprocess run` resumes from the failed step. Assert a review-required pause still exits zero with its marker (distinct outcome). Assert `skip_steps` in the toml causes the runner to skip exactly those steps. Add the parallel-comparison test harness and assert it runs and reports comparative timings on a synthetic workload.
- **Manual**: force a step to fail and confirm the terminal + comms notification and non-zero exit, then re-run and watch it resume; set `skip_steps` and confirm those steps are skipped.

### Acceptance criteria

- [ ] Given a step fails, then the operator is notified (terminal + comms) and the process exits non-zero with the failed step as the resume point.
- [ ] Given a re-launch after a failure, then the workflow resumes from the failed step.
- [ ] Given a review-required pause, then it remains a distinct outcome (marker + exit 0), never confused with a failure.
- [ ] Given `skip_steps` in the toml, then the runner bypasses exactly those steps.
- [ ] Given multiple correlator passes, then independent per-pass work within a step runs concurrently (current behaviour retained).
- [ ] Given the comparison test harness, then it runs and produces comparative timings for the parallelism approaches.

### User stories addressed

- User story 11: sweeps `skip_steps`
- User story 34: steps run automatically
- User story 35: failure → notify + exit non-zero + resumable
- User story 36: failure distinct from clean review-pause
- User story 37: independent per-pass work runs concurrently
- User story 39: failure notification reuses existing comms config

---

## Issue 10: Documentation update to the mode-oriented model

**Type**: AFK
**Blocked by**: Issues 1, 4, 5, 6, 8

### Parent PRD

`docs/PRD-cleanup.md` (Further Notes)

### What to build

Update the reference and guide docs to describe the mode-oriented model rather than the three-backend model: rewrite `docs/reference/cli.md` (the single `--mode`/`--config`, removal of `--retrieval`/`--pipeline`/`--distribution`, the `distribute` step and `archive` alias), `docs/guide/workflow.md` and `docs/reference/steps.md` (plots twice + conditional pause, antab reposition, distribute), the three-channel logging (`logs/logging_messages.log`, `logs/commands.sh`), and replace the backend-oriented `docs/guide/backends.md` with a mode-oriented guide. Ensure the docs build cleanly.

### How to verify

- **Automated**: `mkdocs build --strict` succeeds with no broken internal links; a grep confirms the removed flags and the old `archive`-only step name are no longer described as current.
- **Manual**: read the CLI, workflow, steps, and mode guides and confirm they match the shipped behaviour of Issues 1–9.

### Acceptance criteria

- [ ] Given the docs, then they describe `--mode`/`--config` and no longer present `--retrieval`/`--pipeline`/`--distribution` as current.
- [ ] Given the docs, then the workflow/steps pages reflect plots-twice + conditional pause, the antab reposition, and the `distribute` step (with the `archive` alias noted).
- [ ] Given the docs, then the three logging channels and their filenames are documented.
- [ ] Given `mkdocs build --strict`, then it succeeds with no broken links.

### User stories addressed

- (No numbered user story; PRD Further Notes on keeping the reference docs in lockstep.)

---
