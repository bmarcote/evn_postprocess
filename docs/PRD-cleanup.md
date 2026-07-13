# PRD: Cleaner, mode-driven evn_postprocess (Phase 2)

Status: draft for implementation. Date: 2026-07-13. Owner: Benito (marcote@jive.eu).

This is the **second phase** of the evn_postprocess redesign. It assumes the architecture delivered by the first phase (`docs/PRD-refactor.md`, `docs/issues-refactor.md`, and the audit in `docs/audit-refactor.md`) as its starting point: vex/lis/toml ingestion (`inputs`, `experiment_state`), heuristic source classification (`source_classify`), the station summary and dashboard Comments tab (`review`, dashboard in `plotting`), e-EVN coordination (`eevn`), the comms/notifier abstraction, and the step engine in `workflow`. All of those remain. What this phase changes is the **backend-selection mechanism**, the **legacy server-coupled code**, the **step ordering around plots and antab**, the **execution/error model**, and the **per-step messaging**.

## Problem Statement

The last iteration of evn_postprocess added a large amount of new capability quickly: three plugin families (retrieval, pipeline, distribution), the experiment toml, source classification, the dashboard Comments tab, and e-EVN barriers. It works, but it now carries the cost of that speed. As a maintainer I face three concrete problems.

First, there is a layer of legacy code that no longer does anything useful but still exists and still couples the package to specific JIVE servers: the `Server`/`Servers` classes, `retrieve_servers`, `parse_masterprojects`, `get_jexp_info`, the `.jexp`/`.expsum` handling, and modules that still `ssh jops@ccs` to grep `MASTER_PROJECTS.LIS` for an observing date the vex file already provides. This code is dead weight, it makes the package look server-dependent when the core no longer is, and it is a trap for the next person who reads it.

Second, the mental model for "how does this run for a given user" is muddled. There are three independent CLI flags (`--retrieval`, `--pipeline`, `--distribution`) each selecting a backend by name, when in practice the relevant question is simply *who is running this and in what context*: a JIVE support scientist doing the full archive-bound job, a regular user with the files already on disk, or the future SWEEPS system running blind from a prepared config. The current design forces the operator to assemble that context out of three orthogonal switches.

Third, the per-step experience is noisy and hard to follow. Terminal output, internal logging, and "here is the command you could run by hand" are all tangled together, so the operator cannot get a clean colourful status while a support scientist who wants to reproduce a step manually cannot easily find the exact command that was run.

The goal of this phase is a smaller, clearer, genuinely server-agnostic evn_postprocess whose behaviour is organised around three explicit operating **modes**, whose steps run automatically and fail loudly, and whose output is cleanly separated into what the operator sees, what is logged for debugging, and what can be replayed by hand.

## Solution

When this phase is complete, the following will be true.

evn_postprocess runs the same experiment start-to-finish (init through distribute) on a single server, with no outbound server access anywhere except inside the two support-scientist modules that genuinely need it (fetching inputs, and archiving outputs). The `Server`/`Servers` classes, the masterprojects/jexp/expsum lookups, and every incidental `ssh` in the core are gone.

The operator chooses one **mode** rather than three backends. There are three modes: `supsci` (the JIVE support-scientist job: retrieve inputs from the correlator, produce ANTAB/UVFLG from vlbeer, and archive/deliver the data), `regular` (a normal user: all inputs already local, no server contact, no archiving), and `sweeps` (the future automated system: retrieve inputs from and deliver back to SWEEPS, running blind from a fully prepared config). The mode is auto-detected from the running user (username `jops` or membership of the OS group `supsci` selects `supsci`; membership of the OS group `sweeps` selects `sweeps`; otherwise `regular`), can be forced with `--mode`, and is remembered for the life of the experiment so a resume never silently switches modes.

Initialization is a single small abstraction with one implementation per mode; after initialization, every mode runs the identical processing sequence and expects the same files to be in place. Missing files are reported through the configured notification and the run exits with an error.

Steps run automatically. A step failure is a hard stop: the operator is notified (terminal + configured comms), the state is left so that re-launching resumes from the failed step, and the process exits non-zero. This is distinct from the existing clean review-pause. Within a step, independent work (multiple correlator passes, per-file fetches, per-pass conversions) runs concurrently, with any access to a given Measurement Set serialized by a per-MS lock.

The `j2ms2` run to produce the lag MS should not write into the standard output but directly to a log file. This should avoid that the main j2ms2 run, if running, will show in real-time the printing lines, which currently it is not real-time or have lags.

Standard plots run always twice, one after creating the MS and another time after `post_polconvert`, and the antab step runs immediately after them and before the pipeline. The pipeline and the final delivery step (renamed from `archive` to `distribute`) are each behind a clean abstract interface so a different pipeline or a different delivery target can be dropped in later without touching the engine.

Every step speaks on three clearly separated channels: a concise, colourful Rich message for the operator; a verbose loguru record for debugging; and a `post_process.log` file that records the exact local command(s) the step ran, so a support scientist can replay any step by hand.

## User Stories

### Modes and initialization

1. As a JIVE support scientist logged in as `jops` (or a member of the `supsci` group), I want evn_postprocess to select `supsci` mode automatically, so that I get today's full behaviour without passing any flag.
2. As a regular user (not `jops`, not in the `supsci` group), I want evn_postprocess to select `regular` mode automatically, so that it never contacts a server and works from the files already in my directory.
3. As any user, I want to force the mode with `--mode supsci|regular|sweeps`, so that I can override auto-detection when my environment does not match my intent.
4. As an operator, I want the chosen mode persisted in the experiment state at initialization, so that a later `postprocess run` (or `exec`, or `dashboard`) resumes in the same mode without re-detecting.
5. As an operator resuming a run, if I pass a `--mode` that differs from the persisted one, I want the tool to override and re-persist it and warn me that it changed, so that an intentional switch is possible but never silent.
6. As a support scientist in `supsci` mode, I want initialization to retrieve exactly what it retrieves today — the `.vix` file from the correlator server (same rules as now) and the remotely-created `.lis` files — so that nothing about which inputs are fetched changes, only how the code is organised.
7. As a regular user in `regular` mode, I want initialization to assume the `.vex`, `.lis`, and any other required inputs already exist in the working directory, so that no server is ever contacted.
8. As the future SWEEPS system in `sweeps` mode, I want initialization to retrieve the required files from wherever SWEEPS keeps them, driven entirely by a prepared config, so that a whole experiment can be processed blind with no human interaction.
9. As an operator in `sweeps` mode, I want to supply the prepared config via `--config PATH` (a required argument for that mode), so that all the choices a support scientist would otherwise make (weight threshold, polswap/polconvert/onebit antennas, sources, refant, etc.) are already present.
10. As an operator in `sweeps` mode, I want the config to be the existing experiment toml schema (fully populated up-front, with no heuristics or guessing permitted), so that there is one config format to understand and maintain rather than a separate JSON schema.
11. As an operator in `sweeps` mode, I want to declare a list of steps to skip via a `skip_steps` key in the toml, so that the automated run can bypass steps that do not apply.
12. As an operator in any mode, I want initialization to create the directory structure and verify that every required input file is present, so that the run does not start only to fail three steps later.
13. As an operator in any mode, when a required file is missing at initialization, I want the tool to report it through the configured notification (Rich terminal message plus comms if configured) and exit with an error, so that I know exactly what to provide before re-launching.
14. As an operator in `sweeps` mode, if the prepared config turns out to be incomplete when a step needs a value it should have supplied, I want the run to fail immediately with a clear error naming the missing field, so that the upstream support scientist knows precisely what to fix — sweeps mode never pauses-and-waits.

### Server-agnostic core

15. As a maintainer, I want the `Server` and `Servers` classes, `retrieve_servers`, `parse_masterprojects`, `get_jexp_info`, and the `.jexp`/`.expsum` handling removed from the core package, so that the code no longer pretends to depend on JIVE servers that it no longer needs.
16. As a maintainer, I want the `ssh jops@ccs ... MASTER_PROJECTS.LIS` observing-date lookup in `comment_tasav.py` removed and replaced by the vex-derived date already available, so that a mid-workflow module makes no outbound server call.
17. As a maintainer, I want all remaining ssh/scp/sftp/remote-subprocess access to live exclusively inside the `supsci`-mode initialization module and the `supsci`-mode distribution module, so that every other module is purely local and the server-agnostic claim is a checkable invariant rather than an aspiration.
18. As a maintainer, I want the standalone utilities under `scripts/` left untouched by this cleanup, so that independently-invoked legacy tools keep working while the core is purged.
19. As a maintainer, I want the compatibility shims retained by the first phase purely to keep legacy call sites alive (for example the re-exported `parse_masterprojects`) and their pinning regression test removed along with the code they pinned, so that no test forces dead code to stay.

### Processing steps and ordering

20. As an operator, I want the processing sequence after initialization to be identical across all three modes (the same steps expecting the same files in place), so that mode only affects how inputs arrive and how outputs leave, never the science in between.
22. As a support scientist, I want the workflow to pause for my review after those plots only when a human decision is genuinely required — specifically when the flag-weight threshold was not set automatically, or when the code cannot determine whether any antenna recorded linear polarization — so that unambiguous runs proceed without stopping while ambiguous ones get my eyes.
23. As a support scientist, when the run does pause for that review, I want the same review mechanism that already exists (the dashboard/dialog in interactive mode, a clean review marker in batch/sweeps), so that there is one gate to learn, not two.
24. As a support scientist in `supsci` mode, I want the antab step (retrieve `.log`/`.antabfs` from vlbeer and produce the ANTAB and UVFLG files) to run after `post_polconvert` and the plots, and before the pipeline steps, so that a single ANTAB session covers the final data.
25. As a regular or sweeps-mode user, I want the antab step to expect the ANTAB and UVFLG files to already be present (never contacting vlbeer), so that non-supsci modes stay server-agnostic.
26. As a regular or sweeps-mode user, if the required ANTAB/UVFLG files are missing when the pipeline needs them, I want the run to fail immediately with an error naming exactly which files are missing and where they were expected, so that I can produce or place them and re-launch.

### Pipeline and distribution

27. As a maintainer, I want the pipeline encapsulated behind an abstract interface (using the established ABC + registry pattern), so that the current EVN AIPS pipeline can later be replaced by another pipeline without touching the engine.
28. As a maintainer, I want the first implementation of that interface to simply wrap and reproduce the current EVN pipeline behaviour, so that this phase changes structure, not calibration results.
29. As a maintainer, I want the final step renamed from `archive` to `distribute` and the concept called `distribution` throughout, with `archive` kept as a deprecated CLI/exec alias, so that the naming reflects that not every mode archives while existing scripts and habits keep working.
30. As a support scientist in `supsci` mode, I want the distribute step, on approval of the single existing post-`postpipe` review, to run unattended to completion — produce the credentials, the auth PI letter, archive all data, and finalise the PI letter template for delivery — so that after I approve I do not have to babysit a second prompt.
31. As a regular or sweeps-mode user, I want the distribute step to skip every archive- and PI-letter-related action and instead verify that the final FITS-IDI files are in order — the expected `*.IDI*` files present for every correlator pass and the ANTAB Tsys/gain-curve information appended — reporting a clear "ready" on success or a hard error naming what is missing, so that a non-supsci run ends in a known-good state without contacting any server.
32. As a maintainer, I want the distribution interface to have a `sweeps` implementation registered as a defined but not-yet-implemented backend, so that SWEEPS delivery has a fixed extension point that fails explicitly until it is built.
33. As a maintainer, I want `sweeps`-mode retrieval likewise registered as a defined but not-yet-implemented backend, so that the mode exists and has an interface even though its concrete file-fetching from SWEEPS is future work.

### Execution model and errors

34. As an operator, I want all steps to run automatically from init to distribute, so that I only intervene when something needs me.
35. As an operator, when any step fails, I want to be notified (Rich terminal message plus the configured comms notifier) and the process to exit non-zero, leaving the state so that re-launching `postprocess run` resumes from the failed step, so that no failure loses progress and no failure is mistaken for a clean pause.
36. As an operator, I want a step failure to be clearly distinct from the clean review-pause (which still writes its marker and exits zero), so that a scheduler or a human can tell "this stopped because it needs a decision" apart from "this stopped because it broke".
37. As an operator processing multiple correlator passes, I want the independent per-pass within a step to run concurrently, so that a multi-pass experiment is not needlessly serial.
39. As an operator, I want the failure notification to reuse the existing comms configuration (`comms.toml`: none/email/mattermost) unchanged, so that this phase adds no new notification configuration surface.

### Per-step messaging and logging

40. As an operator, I want each step to print a concise, colourful Rich message telling me what it is doing and how it went, so that I can follow the run at a glance without wading through internal detail.
41. As a maintainer debugging a problem, I want each step to emit verbose loguru detail to a debug log kept out of the operator's way, so that I can diagnose failures without cluttering the terminal.
42. As a support scientist who wants to reproduce a step by hand, I want a `post_process.log` file at the experiment root that records, per step, the exact local command(s) that were run (for example `mstools run flag_weights exp.ms 0.9`), so that I can replay any step manually.
43. As a maintainer, I want the three channels — Rich terminal, loguru debug file, and the replayable `post_process.log` — to be genuinely separate sinks with separate responsibilities, so that changing one does not disturb the others.
44. As a maintainer, I want the existing debug log renamed off the `post_process.log` name (which currently collides with the intended command log) so that the replayable command log can own that clean name and the two files are never confused.

### Compatibility

45. As a maintainer, I want a clean break on persisted state: existing `{expname}.json` and `{expname}.toml` from before this phase need not keep loading, so that legacy fields (Servers, etc.) can be removed from the state schema without migration machinery. In-flight experiments are re-initialized.

## Implementation Decisions

**Mode as the primary organising concept.** Selection is replaced by a single `--mode supsci|regular|sweeps` flag plus auto-detection. Auto-detection selects `supsci` when the running user is `jops` or belongs to the OS group named `supsci` (checked via the standard OS user/group facilities), and `regular` otherwise. `sweeps` is never auto-detected; it is chosen explicitly and requires `--config PATH`. The three separate Phase-1 selection flags (`--retrieval`, `--pipeline`, `--distribution`) and their per-family `set_cli_mode`/`selected_mode` plumbing are removed in favour of this one mode. Mode is resolved once at initialization and persisted in the experiment state; subsequent invocations reuse it. A `--mode` on a later invocation overrides and re-persists, with a warning if it differs from the stored value.

**Modes bind both ends together.** A mode determines both how inputs are retrieved and how outputs are distributed (and, for `supsci`, whether the antab/vlbeer step runs). `supsci` = retrieve-from-correlator + antab-from-vlbeer + archive-to-EVN-archive. `regular` = inputs-already-local + antab-already-local + verify-FITS-IDI-only. `sweeps` = retrieve-from-SWEEPS + antab-already-local + deliver-to-SWEEPS. Retrieval and distribution are not independently selectable; the mode picks both.

**Server-agnostic invariant.** After this phase, no module outside the `supsci`-mode initialization module and the `supsci`-mode distribution module performs any outbound server access (ssh/scp/sftp/remote subprocess). The `Server`/`Servers` classes, `retrieve_servers`, `parse_masterprojects`, `get_jexp_info`, and `.jexp`/`.expsum` handling are deleted from the core. The `comment_tasav` observing-date ssh lookup is replaced by the vex-derived date. The one explicitly sanctioned, temporary exception is the `--tConvert-in-eee` remote-execution workaround, which is retained but walled off and recorded as known tech debt to be removed once local tConvert/PolConvert works. The standalone `scripts/` utilities are out of scope and left untouched.

**Initialization abstraction.** One abstract initializer with an implementation per mode. `Supsci` reproduces today's retrieval exactly (`.vix` from the correlator server following the current rules, remote creation and copy of the `.lis` files); vlbeer `.log`/`.antabfs` retrieval is not part of init and lives in the antab step. `Regular` validates that the `.vex`, `.lis`, and other required inputs already exist locally. `Sweeps` is a registered but not-yet-implemented backend that will fetch from SWEEPS driven by the config. All three, via a shared final phase, create the directory structure and verify required files are present, reporting-and-exiting on any miss.

**Sweeps config.** The prepared config is the existing experiment toml schema (`experiment_state`), required to be fully populated up-front. In `sweeps` mode no heuristic classification runs and no value is guessed; a value that a step needs and does not find in the config is a hard, immediate, field-named error (never a pause). `skip_steps` is a toml key listing steps to bypass; it applies in `sweeps` mode.

**Step ordering and conditional plots.** Standard plots are produced twice, after `j2ms2` and  after `post_polconvert`. . The plots are always generated once at each step, but the workflow only *pauses* for review when a human decision is genuinely required: the flag-weight threshold was not set automatically, or the presence of linear-polarization antennas could not be determined. The review pause reuses the single existing post-`postpipe` mechanism (dashboard/dialog interactive, clean marker in batch/sweeps). The antab step runs immediately after the plots and before the pipeline; it produces ANTAB/UVFLG from vlbeer in `supsci` mode only, and in the other modes it verifies those files are already present, failing with a named-file error if not.

**Pipeline and distribution abstractions.** Both use the established ABC + name-keyed registry pattern (as already used for `Dialog`, `Notifier`, and the Phase-1 backends). The pipeline's first implementation wraps the current EVN AIPS pipeline unchanged. The final step is renamed `archive` → `distribute` and the concept `distribution`, with `archive` retained as a deprecated CLI/exec alias. The `supsci` distributor, on approval of the existing review, runs unattended: credentials, auth PI letter, archive of all data, final PI letter template. The non-supsci distributor performs no archiving and no PI-letter work; it verifies the expected `*.IDI*` files exist for every correlator pass and that the ANTAB Tsys/gain-curve information was appended (from the existing prearchive/append step), reporting ready or a named-miss error. `sweeps` distribution is a registered not-yet-implemented backend.

**Execution and error model.** Steps run automatically in order. Within a step, independent work items run concurrently; any access to a given MS is serialized by a per-MS lock. This formalizes and extends the current `ThreadPoolExecutor` usage with an explicit MS-exclusivity guarantee; steps themselves are not run concurrently with each other. On a step error the engine notifies (Rich terminal + configured comms), keeps state so the failed step is the resume point, and exits non-zero. This is distinct from the clean review-pause (marker + exit zero). Error notifications reuse the existing `comms.toml` configuration unchanged.

**Per-step messaging.** A reporting abstraction gives every step three separate sinks: a concise Rich terminal message for the operator; a verbose loguru debug file (renamed off the `post_process.log` name to avoid the current collision); and a `post_process.log` at the experiment root that records the exact replayable local command(s) each step ran. The three are independent concerns with independent sinks.

**Compatibility.** Clean break on persisted state: pre-phase `{expname}.json`/`{expname}.toml` need not keep loading, so legacy fields can be removed from the schema without migration. In-flight experiments are re-initialized.

## Module Design

- **Name**: `mode`
  - **Responsibility**: resolve the operating mode (supsci | regular | sweeps) from OS user/group and the `--mode` flag, and own its persistence on the experiment state.
  - **Interface**: `detect() -> Mode` (OS user/group heuristic); `resolve(cli_mode, stored_mode) -> Mode` (CLI over stored over detected, with a warning on override); a `Mode` enum. Failure modes: an invalid `--mode` value errors at parse time; `sweeps` without `--config` errors at parse time.
  - **Tested**: yes (mockable OS calls; override/persistence/resume behaviour).

- **Name**: initialization / retrieval (`Initializer` abstraction, replaces the Phase-1 `retrieval/` package)
  - **Responsibility**: acquire or validate the input files for the chosen mode, then (shared) create the directory structure and verify required files.
  - **Interface**: abstract `initialize(workdir, expname) -> InputSet`-equivalent; `SupsciInitializer` (correlator `.vix` + remote `.lis`), `RegularInitializer` (validate local), `SweepsInitializer` (stub, fetch-from-SWEEPS). Registry by mode name. Failure modes: missing required file → notify + error exit, naming the file and where it was expected.
  - **Tested**: yes (registry + per-mode behaviour; Regular missing-file errors; Supsci with mocked ssh; Sweeps stub error).

- **Name**: antab step
  - **Responsibility**: in `supsci` mode, retrieve `.log`/`.antabfs` from vlbeer and produce the ANTAB and UVFLG files; in other modes, verify those files are present. Positioned after `post_polconvert` + plots, before the pipeline.
  - **Interface**: `run(exp) -> bool`; supsci-only vlbeer access. Failure modes: non-supsci with missing files → hard error naming the files.
  - **Tested**: yes (supsci with mocked vlbeer; non-supsci missing-file error).

- **Name**: pipeline (abstract backend, repackaged)
  - **Responsibility**: encapsulate the calibration pipeline behind a stable interface.
  - **Interface**: abstract `prepare`/`run`/`collect(exp) -> bool`; `AipsPipeline` wraps the current EVN pipeline unchanged; registry by name. Failure modes: unimplemented backend → explicit error at selection.
  - **Tested**: yes (registry; AipsPipeline prepare against a fixture as oracle).

- **Name**: distribution (abstract backend, renamed from archive)
  - **Responsibility**: deliver the finished experiment per mode.
  - **Interface**: abstract `distribute(exp) -> bool`; `SupsciDistributor` (credentials, auth PI letter, archive, final template — runs on review approval); non-supsci distributor (verify FITS-IDI present per pass + Tsys/GC appended, else named error); `sweeps` distributor (stub). `archive` kept as deprecated alias. Failure modes: supsci missing PI info handled by existing prompt/batch rules; non-supsci missing/incomplete FITS-IDI → named error.
  - **Tested**: yes (registry; non-supsci FITS-IDI verification pass/fail; sweeps stub error; letter generation with fixture data).

- **Name**: `reporting` (deep module)
  - **Responsibility**: give every step three separate output channels behind one simple interface.
  - **Interface**: per-step `announce(message)` (Rich terminal), `logger` (loguru debug file, renamed off `post_process.log`), `record_command(cmd)` (appends the replayable command to `post_process.log`). Stable, small surface; the three sinks are independent.
  - **Tested**: yes (terminal message emitted; loguru detail captured; `post_process.log` records the expected replayable command per step).

- **Name**: engine (the `workflow` runner, evolved)
  - **Responsibility**: ordered step execution with automatic progression, intra-step concurrency, per-MS locking, and the error→notify→exit-nonzero→resume contract distinct from the clean review-pause.
  - **Interface**: `run_workflow(exp, from_step, to_step)`; per-MS lock guarantee; error path returns non-zero and leaves the failed step as the resume point. Failure modes: step error (hard stop) vs review-required (clean pause) are separate outcomes.
  - **Tested**: yes (per-MS lock exclusivity; error→exit-nonzero→resume-from-failed-step on a mocked-binary fixture).

- **Name**: legacy-removal (cross-cutting workstream, not a module)
  - **Responsibility**: delete `Server`/`Servers`, `retrieve_servers`, `parse_masterprojects`, `get_jexp_info`, `.jexp`/`.expsum` handling, the `comment_tasav` masterprojects ssh lookup, and the shims/pinning test that only exist to keep them alive — from the core, leaving `scripts/` untouched.
  - **Tested**: via the invariant that the core has no outbound server access outside the two supsci modules; existing suite stays green.

## Testing Decisions

Good tests here assert observable behaviour on synthetic experiment directories and mocked externals, following the Phase-1 prior art: `tests/test_backends.py` (registry behaviour), `tests/test_retrieval.py` / `tests/test_distribution_jive.py` (per-backend behaviour with mocked ssh and fixture files), `tests/test_experiment_state.py` (toml round-trip and precedence), `tests/test_workflow_batch.py` (pause/resume and batch semantics), and `tests/test_eevn.py` (tmp-dir barrier fixtures). External binaries (j2ms2, tConvert, EVN.py, antab_editor) and remote servers are always mocked; casacore-touching tests run on Linux/JIVE (they cannot be collected on macOS — a known environment constraint).

Tests are written for: (1) **mode** detection and selection — OS user/group heuristic, `--mode` override, and persistence/resume behaviour, with the OS calls mocked; (2) **init/retrieval and distribution backends** — registry lookup, Regular validate-local missing-file errors, Supsci with mocked ssh, the non-supsci distribute FITS-IDI/Tsys verification (pass and named-fail), and the Sweeps stub errors; (3) **reporting/logging** — that the terminal message is emitted, the loguru detail is captured in the debug file, and `post_process.log` records the expected replayable command per step; (4) **engine** — per-MS lock exclusivity, and the error→exit-nonzero→resume-from-failed-step semantics on a mocked-binary fixture experiment. The dashboard HTML/JS and the antab_editor GUI remain excluded; their data-producing functions are tested instead.

## Out of Scope

- Any change to what `supsci` initialization retrieves (the retrieved file set is unchanged; only its packaging changes).
- Concrete SWEEPS integration: both `sweeps` retrieval and `sweeps` distribution are registered but not-yet-implemented backends; where SWEEPS stores files and how it is contacted is future work.
- The standalone `scripts/` utilities (e.g. `comment_tasav_file.py`): left untouched by the legacy removal.
- Removal of the `--tConvert-in-eee` remote workaround: retained as a sanctioned temporary exception and recorded as tech debt, not removed in this phase.
- State migration: this phase makes a clean break; pre-phase `{expname}.json`/`{expname}.toml` are not required to load.
- Cross-step concurrency: only intra-step parallelism with per-MS locking is in scope; independent steps are not run concurrently with each other.
- Any change to the calibration science, the pipeline results, or the dashboard/review UI beyond the plots-run-once and antab-reordering changes.
- OS-level containerisation of the pipeline or distribution backends.
- New notification configuration: the error path reuses the existing `comms.toml` unchanged.

## Open Questions - Solved

1. Exact filename for the renamed debug/loguru log (freeing `post_process.log` for the replayable command log). Implementation: store all these files under `log/`.  The repayable command log should be stored as `commands.sh`;  `logging_mesages.log` to store all DEBUG INFO messages from loguru.
2. The precise `post_process.log` format for replayable commands (to be renamed to `logs/commands.sh` and be saved under `logs/`) — one shell-runnable command per line versus an annotated log with step headers. Implementation: shell-runnable lines (optionally with a comment header per step) so the file can be read top-to-bottom as a manual runbook.
3. The exact OS mechanism and edge cases for `supsci` detection (login name vs effective user; supplementary group membership; behaviour when the `supsci` group does not exist on a machine). Implementation: Treat a missing group as "not supsci" and fall back to `regular`, and document the exact functions used.
4. Whether `sweeps` retrieval and `sweeps` distribution should share a single SWEEPS transport module when implemented, or remain two independent stubs now. Keep two stubs now behind the mode; unify later if a shared transport emerges.
5. The exact concurrency mechanism for intra-step parallelism (retaining `ThreadPoolExecutor` vs moving to `asyncio`) and the lock primitive for per-MS exclusivity (in-process lock vs an on-disk lock file that also guards against two separate `postprocess` processes on the same MS). Implementation: keep it as it is now. Create a test that will be able to compare the different parallel tools in a later interaction.

## Further Notes

- This phase deliberately narrows the Phase-1 design: the three orthogonal backend flags proved to be more mechanism than users needed, and the mode concept re-expresses the same capability in the terms operators actually think in (who am I, and is this the archive-bound job). The Phase-1 ABC + registry machinery is reused as the engine underneath the modes; it is the user-facing selection surface that collapses from three flags to one mode.
- The `distribute` rename and the plots-once / antab-reorder changes are the only workflow-visible step changes; everything else is internal simplification, so the reference docs (`docs/reference/steps.md`, `docs/guide/workflow.md`, `docs/guide/backends.md`, `docs/reference/cli.md`) will need updating in lockstep, and the `backends` guide in particular is superseded by a mode-oriented guide.
- The server-agnostic invariant (story 17) is worth enforcing mechanically — for example a test or a lint that asserts no module outside the two supsci modules imports the ssh/scp helpers — so that it does not silently regress as the code evolves.
- The error-vs-pause distinction (stories 35–36) matters most to schedulers: `sweeps` and `--batch` runs are driven by automation that must tell "needs a human decision" (clean marker, exit 0) apart from "broke" (notify, exit non-zero). Keeping these two outcomes visibly different in both exit code and marker/notification is a hard requirement, not a nicety.
