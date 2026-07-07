# PRD: Standalone, modular re-design of evn_postprocess

Status: draft for implementation. Date: 2026-07-02. Owner: Benito (marcote@jive.eu).

## Problem Statement

The current `evn_postprocess` automates the EVN post-correlation checklist, but it can only run
inside the JIVE infrastructure: it bootstraps itself from `MASTER_PROJECTS.LIS`, `.jexp` and
`.expsum` files, hardcoded knowledge of the `ccs`/`eee`/`vlbeer`/`archive` servers, and
JIVE-installed tools. A normal user (on an external institute, or a future JIVE setup on
different machines) cannot run the post-processing from a `.vex` file and correlated data alone.
JIVE-specific concerns (file retrieval, the AIPS EVN pipeline, archiving/distribution) are
interleaved with the generic processing logic, so replacing any one of them requires touching the
whole package. Upcoming changes (new pipelines such as VPIPE, new distribution targets such as
SWEEPS, a station-feedback database feeding Grafana) cannot be added without this separation.

## Solution

`evn_postprocess` becomes a single package whose core needs exactly three inputs, all local files:
the observation `.vex` file, one or more `.lis` files describing the correlated passes, and an
optional experiment `.toml` complementing what the `.vex`/`.lis` cannot express. Everything
site-specific lives behind three plugin interfaces, each with a JIVE implementation that
reproduces today's behaviour:

- **Retrieval** obtains the input files (JIVE: copy `.vex`/`.vix`/`.vax` from the correlator
  server, create `.lis` files when missing, including gathering `.log`/`.antabfs` files and creating its inputs).
  Users with the files already on disk skip it.
- **Pipeline** encapsulates calibration completely (`none`, `aips` = current EVN.py AIPS
  pipeline, `vpipe` later).
- **Distribution** encapsulates delivery (`jive` = summary + PI letter + credentials + archive +
  feedback upload, `none`, `sweeps` later).

Between retrieval and pipeline the core runs autonomously — no operator interaction — until the
final FITS-IDI files exist. Operator interaction is reactive and concentrated at three points:
the (still manual) `antab_editor` step preceded by an automatic station summary, the dashboard
review with a new comments tab, and the final "send the PI letter" confirmation. Every parameter
chosen along the way is written back into the experiment `.toml`, so a re-run with a completed
toml executes end-to-end with no questions asked.

## User Stories

1. As a JIVE support scientist, I want to run `postprocess` in an experiment directory and have
   retrieval, conversion, and diagnostics happen without my intervention until the FITS-IDI files
   are ready, so that I only act where human judgement is needed.
2. As an external (non-JIVE) user, I want to post-process an observation given only a `.vex`
   file, `.lis` files, and correlated data on disk, so that the package is usable outside JIVE
   servers and accounts.
3. As an external user, I want the program to run with retrieval mode `none` (files already
   local) and distribution mode `none`, so that no JIVE server is ever contacted.
4. As a support scientist, I want the JIVE retrieval module to fetch the `.vex`/`.vix`/`.vax`
   file from the correlator server and create the `.lis` files if absent, so that current
   behaviour is preserved without `MASTER_PROJECTS.LIS`, `.jexp`, or `.expsum`.
5. As a user, I want observation metadata (stations, scans, sources, frequency setup, mode,
   observing date, e-EVN membership) parsed from the `.vex` and `.lis` files, so that no external
   catalogue is needed.
6. As a user, I want to declare in the `.toml` which sources are targets, calibrators, and fringe
   finders, which scans to process (it can be a range, individual scans, or several ranges of scans),
   which pipeline to run, and PI/support-scientist contact information, so that information not
   present in `.vex`/`.lis` has one well-defined home.
7. As a user, when source types are missing from the `.toml`, I want the program to classify
   sources heuristically, log clear warnings, record the guesses in the toml, and continue, so
   that the run never blocks on classification.
8. As a user, I want the heuristic classification to be visible and editable (via `postprocess
   edit` or the toml) before the pipeline runs, so that a wrong guess cannot silently ruin the
   calibration.
9. As a support scientist, I want the autonomous phase to cover MS creation (j2ms2), standard
   plots, MS operations (weight flagging, polswap, 1-bit correction), tConvert, PolConvert, and
   post-PolConvert plots, so that the FITS-IDI files are produced in one unattended sweep.
   Only when something fails or is out of normal, I want user interaction.
10. As a support scientist, just before `antab_editor` launches, I want a terminal summary of
    stations that did not observe, missed time ranges, and reduced bandwidths, so that I know
    what to fix in the ANTAB without hunting through logs.
11. As a support scientist, I want that same antab summary sent to me via the configured
    notifier (Mattermost), so that I am pulled back to the terminal when the run needs me.
12. As a support scientist, I want the `antab_editor` interaction itself left unchanged in this
    iteration, so that the refactor does not destabilise a manual, safety-critical step.
13. As a support scientist, after pipeline diagnostics I want to be asked — in the terminal and
    via Mattermost, with instructions on what to open — to review the dashboard, so that review
    is reactive rather than something I must remember to do.
14. As a support scientist, I want a dashboard comments tab with one general experiment note and
    one editable note per station, so that observation feedback is captured where I review plots.
15. As a support scientist, I want per-station notes pre-filled automatically when a station did
    not observe, missed part of the observation, or observed with reduced bandwidth, so that
    routine remarks cost no typing.
16. As a support scientist, I want each station to carry a green/orange/red status selector (no
    problem / issues reported / could not observe), pre-set consistently with the auto-notes, so
    that station health is recorded in a structured, machine-readable way.
17. As a support scientist, I want station feedback comments fetched from the EVN feedback
    database (MySQL) and inserted as default per-station notes, so that PI-visible notes start
    from what stations reported.
18. As a user without database access, I want the feedback lookup silently skipped when no
    configuration exists in the user config directory or when the connection fails, so that the
    dashboard works identically outside JIVE.
19. As a support scientist, I want dashboard comments and station statuses persisted into the
    experiment toml, so that they survive re-runs and flow into the PI letter and the future
    feedback/Grafana upload.
20. As a support scientist, when confirming the dashboard review I want to optionally request a
    re-run from a chosen step, so that fixing a problem does not require restarting from scratch.
21. As a support scientist, after confirmation I want the final steps (attach ANTAB/Tsys to the
    FITS-IDI files) to run automatically, so that the data become distribution-ready unattended.
22. As a user, I want every selected parameter — flag weight threshold, polswap/polconvert/
    2-bit-sampling antenna lists, reference antennas, and links to the final antab and
    polconvert input files — written into the experiment toml at the end of post-processing, so
    that the run is fully reproducible.
23. As a user re-running with a toml that already defines those parameters, I want the program to
    apply them without asking or re-checking, so that re-processing is deterministic and silent.
24. As a support scientist, I want distribution mode `jive` to create the observation summary,
    generate PI credentials and the PI letter, archive all data, and then ask me to send the
    letter, so that current delivery behaviour is preserved.
25. As a support scientist, when PI name/email or support-scientist info is missing at
    distribution time, I want to be prompted for it interactively (and the run to fail with a
    clear message in batch mode), so that a letter is never sent half-filled.
26. As a user, I want distribution mode `none` to stop after post-processing with nothing
    archived, so that test runs and external users leave no trace on JIVE systems.
27. As a maintainer, I want distribution mode `sweeps` and pipeline mode `vpipe` to exist as
    named, registered-but-unimplemented backends that fail with an explicit "not implemented"
    message, so that future work has a fixed extension point.
28. As a support scientist processing a standard continuum single-pass observation, I want one
    `.lis` file to produce one MS processed start-to-finish, so that the common case stays simple.
29. As a support scientist processing an observation with multiple `.lis` files (continuum +
    spectral line passes), I want each pass converted and the pipeline run over all correlated
    passes as today, so that multi-pass experiments keep working through the refactor.
30. As a support scientist processing an e-EVN experiment, I want the e-EVN run detected from the
    `exper_description` field of the `.vex` (`e-EVN: EXP1, ...`), so that no
    `MASTER_PROJECTS.LIS` lookup remains.
31. As a support scientist processing e-EVN experiment EXPn (n>1), I want the program to use the
    `.vex` file named after EXP1 and to locate sibling experiments in `../EXPm` by convention, so
    that the current directory layout keeps working.
32. As a support scientist, I want `antab_editor` to launch only when all experiments of the
    e-EVN run have their FITS-IDI files produced, so that a single ANTAB session covers the run.
33. As a support scientist, I want an EXPn pipeline to start only when the final antab file
    exists in `../EXP1/`, and otherwise the run to pause cleanly (marker file, exit code 0) and
    resume on the next invocation, so that e-EVN coordination needs no daemon.
34. As a support scientist, I want the information retrieved from the experiment (amount of flagged data, antennas to be polconverted, polswapped, the ones with problems, and the gain corrections required to correct for the ANTAB files) persisted into the experiment toml, so that they survive re-runs and flow into the PI letter and the future
    feedback/Grafana upload.
35. As an operator running in `--batch` mode, I want no GUI or terminal prompt ever raised: every
    interaction point either resolves from the toml/policy or writes a `REVIEW_REQUIRED` marker
    and exits cleanly, so that schedulers (HTCondor, cron) can drive the workflow.
36. As an operator, I want failures at any step to leave an explicit error message, an intact
    state file, and a resumable workflow (`postprocess run STEP`), so that no failure forces a
    restart from zero.
37. As an operator, when retrieval fails (server unreachable, files missing on the correlator
    host), I want a clear error naming the missing file and the attempted source, so that I can
    fix the cause without reading code.
38. As a maintainer, I want the existing CLI (`postprocess`, `run`, `exec`, `edit`, `info`,
    `last`, `--batch`) preserved, so that operators and wrapper scripts keep working.
39. As a maintainer, I want existing experiment JSON state files to keep loading through schema
    migration, so that in-flight experiments survive the upgrade.
40. As a maintainer, I want each plugin family (retrieval, pipeline, distribution) selectable
    from the toml and the CLI, defaulting to today's JIVE behaviour, so that the refactor is
    invisible to current users.
41. As a future maintainer, I want the results upload to the feedback database (Grafana-visible)
    defined as a stub inside the jive distribution backend, so that it can be implemented later
    without redesign.

## Implementation Decisions

**Packaging.** One package, plugin interfaces. `evn_postprocess` remains a single distribution
and repository; retrieval, pipeline, and distribution are abstract interfaces with concrete
implementations living in sub-packages (`retrieval/`, `pipelines/`, `distribution/`). JIVE
implementations ship inside the package but are imported only when selected, so their
dependencies (ssh access, MySQL client, AIPS/ParselTongue) are never required for core runs.
Backends are looked up in a registry keyed by name (`"jive"`, `"none"`, `"aips"`, `"vpipe"`,
`"sweeps"`), so third parties can register new backends without touching core code.

**Inputs.** The core consumes exactly: one `.vex` file, N `.lis` files, one optional experiment
`.toml`. `MASTER_PROJECTS.LIS`, `.jexp`, and `.expsum` are eliminated from the core; the
observing date comes from the vex `$EXPER` block, e-EVN membership from `exper_description`,
stations/scans/sources/setup from the vex, and pass definitions from the `.lis` files. The toml
complements with: scans to process, source types, pipeline selection, PI/support-scientist info,
and (after a first run) all resolved processing parameters.

**Experiment toml layout.** A single `EXPNAME.toml` with distinct sections: user/retrieval-
provided input (`[observation]`, `[sources]`, `[pi]`, `[pipeline]`, `[distribution]`), program-
written results (`[postprocess]`: weight threshold, polswap/polconvert/onebit antennas, refant,
antab and polconvert-input file links), and review output (`[comments]`: general note, and per-
station note + status green/orange/red). The program appends/updates its sections; it never
rewrites user sections except to record heuristic source classifications. Internal checkpoint
state (step completion, file inventories, timestamps) stays in the existing machine-only JSON
state file, which remains loadable via `Experiment.from_dict` migration.

**Parameter precedence and silence rule.** For each decision parameter: toml value → use without
asking; absent + interactive mode → current dialog/auto behaviour, then write the answer to the
toml; absent + batch mode → policy value or `REVIEW_REQUIRED` pause. A completed toml therefore
implies a fully unattended run. The existing `policy.toml` mechanism is retained for batch
defaults and merges beneath the experiment toml (experiment toml wins).

**Source classification heuristics.** When `[sources]` is absent: from the experiment, retrieve all sources available. Note that in many cases there may be more sources in the .vex file than what was observed, and hence they should be cross-checked with the MS file. From the list of observed sources: the names/positions can be compared with the RFC/astrogeo catalogs; if there are sources not known there: they are likely targets. If there are sources in there: the ones observed most of the time will be the phase calibrator (to confirm that by position: there may be phase calibrators close to the target sources). If a source has been observed way less time (only a handful of scans), then it is a fringe finder. Results logged as warnings and written
to the toml as the record of what was assumed. The run never blocks on classification.

**Retrieval interface.** `Retriever.fetch(workdir, expname) -> InputSet` where `InputSet` names
the vex, lis, and toml paths. `JiveRetriever` replicates current behaviour: copy
`.vex`/`.vix`/`.vax` from the correlator server, create `.lis` files remotely when missing and
fetch them, and may pre-fill the toml (PI info) from JIVE-internal sources when available.
`NoneRetriever` validates that the files already exist locally. Retrieval is the only module
allowed to know about JIVE servers for input acquisition.

**Core engine.** The existing step runner (Task list, `run_workflow`, `validate_steps`, output
staleness checks, `REVIEW_REQUIRED` markers, loguru logging, notifier hooks) is retained as the
execution substrate. Steps from `j2ms2` through `post_polconvert`/`standardplots2` run with zero
interaction (today's `msops` questions resolve via the precedence rule above). `initialize` is
re-implemented on top of Retriever + vex/lis/toml parsing.

**ANTAB step.** Unchanged mechanically (`antab_editor` manual GUI). New: immediately before
launching, print a rich terminal panel summarising per station: did-not-observe, missed time
ranges (from scheduled vs observed scans), and reduced bandwidth (subband comparison), and send
the same summary through the configured notifier.

**Pipeline interface.** Code encapsulation only (no OS containers in this iteration).
`PipelineBackend.prepare(exp)` (fetch `.log`/`.antabfs`, build uvflg and input files),
`.run(exp)`, `.collect(exp)` (diagnostics/outputs). `AipsPipeline` wraps the current EVN.py
flow; `NonePipeline` is a no-op that still satisfies downstream expectations; `VpipeBackend`
registered but not implemented. The backend owns every pipeline-specific file format.

**Dashboard review.** The existing single-page dashboard gains a Comments tab: general note
textarea, per-station editable notes, per-station green/orange/red selector. Auto-filled notes
and statuses derive from the same data as the antab summary. Default station notes are fetched
via a new `station_feedback` function backed by the EVN feedback MySQL database; connection
settings read from the user config directory (`~/.config/evn_postprocess/`); absence of the
config or any DB error results in silent skip (log at debug level). Comments persist to the toml
`[comments]` section via the dashboard's POST API. The review request is announced in the
terminal and via Mattermost with the exact URL/command to open. Confirmation accepts an optional
"re-run from step X" answer. In batch mode no dashboard is served; the pause is a
`REVIEW_REQUIRED` marker plus notifier message, as today.

**Finalisation.** On confirmation, run the ANTAB/Tsys attachment into the FITS-IDI files
(current `prearchive`) and write all resolved parameters into the toml `[postprocess]` section.

**Distribution interface.** `Distributor.deliver(exp)`. `JiveDistributor`: build observation
summary, create credentials, generate PI letter (using `[pi]` and `[comments]`), archive data,
then ask the operator to send the letter; prompts for missing PI info interactively, fails
clearly in batch. Contains a stub `upload_feedback(exp)` for the future Grafana-visible feedback
database upload. `NoneDistributor`: no-op. `SweepsDistributor`: registered, not implemented.

**e-EVN coordination.** Detection from `exper_description`. Convention: sibling experiments live
in `../EXPm`. Two synchronisation barriers, both implemented as filesystem checks with
pause-and-resume semantics (marker file, exit 0, resume on next invocation): (a) `antab` step
requires FITS-IDI completion markers in every sibling directory; (b) `pipeline` step for EXPn
requires the final antab in `../EXP1/`. No daemon, no cross-process communication.

**Compatibility.** Refactor in place: same repo, same `postprocess` entry point, same step names
where possible, JSON state migration preserved, existing tests kept green throughout. Stale
references (e.g. the Snakemake mention in the workflow module docstring) are corrected.

**Known constraint flagged during design.** Standard plots depend on jiveplot/jplotter, which is
itself a JIVE-flavoured dependency; it remains a required tool for the plotting steps but must
keep failing with an explicit "tool not found" message (via the existing `tools.resolve`
mechanism) rather than blocking package import, so the core remains importable anywhere.

## Module Design

- **Name**: `inputs` (vex/lis/toml ingestion)
  - **Responsibility**: turn `.vex` + `.lis` + `.toml` into the populated `Experiment` object,
    including heuristic source classification and e-EVN detection.
  - **Interface**: `load_experiment(vexfile, lisfiles, tomlfile|None) -> Experiment`; raises
    on unparseable vex/lis; warns (never raises) on missing toml fields.
  - **Tested**: yes.
- **Name**: `retrieval`
  - **Responsibility**: acquire the three input file types into the work directory.
  - **Interface**: `get_retriever(name) -> Retriever`; `Retriever.fetch(workdir, expname) ->
    InputSet`; raises `RetrievalError` naming the missing file and attempted source.
  - **Tested**: yes (interface + NoneRetriever; JiveRetriever logic behind mockable ssh calls).
- **Name**: `engine` (existing `workflow.py`, reorganised)
  - **Responsibility**: ordered step execution, checkpointing, pause/resume, staleness checks.
  - **Interface**: `run_workflow(exp, from_step, to_step)`; step functions returning bool;
    pause via `REVIEW_REQUIRED` marker.
  - **Tested**: yes (existing batch/workflow tests extended).
- **Name**: `experiment_state`
  - **Responsibility**: toml read/merge/write-back (sections, precedence, comments) and JSON
    checkpoint persistence with migration.
  - **Interface**: `load/save_toml(exp)`, `record_parameters(exp)`, `record_comments(exp)`.
  - **Tested**: yes.
- **Name**: `pipelines`
  - **Responsibility**: everything calibration-pipeline-specific, per backend.
  - **Interface**: `get_pipeline(name) -> PipelineBackend`; `prepare/run/collect(exp) -> bool`;
    unknown or unimplemented backend → explicit error at selection time.
  - **Tested**: yes (registry + NonePipeline; Aips backend smoke-tested with mocked EVN.py).
- **Name**: `review` (dashboard + antab summary)
  - **Responsibility**: station summary computation, dashboard comments tab, station feedback DB
    lookup, review confirmation flow.
  - **Interface**: `station_summary(exp) -> StationSummary`; `station_feedback(exp) ->
    dict[station, str]` (empty on any error); dashboard POST persists into `[comments]`.
  - **Tested**: yes (summary computation and feedback-lookup error paths; not the GUI itself).
- **Name**: `distribution`
  - **Responsibility**: everything delivery-specific, per backend.
  - **Interface**: `get_distributor(name) -> Distributor`; `deliver(exp) -> bool`; missing PI
    info → interactive prompt or batch failure.
  - **Tested**: yes (registry, none mode, letter generation with fake data).
- **Name**: `eevn`
  - **Responsibility**: sibling-directory conventions and the two synchronisation barriers.
  - **Interface**: `siblings(exp) -> list[Path]`; `fitsidi_ready_everywhere(exp) -> bool`;
    `final_antab_available(exp) -> bool`.
  - **Tested**: yes (tmp-dir fixtures).
- **Name**: `comms` (existing)
  - **Responsibility**: notifier abstraction (Mattermost/Email/None); unchanged interface,
    gains the antab-summary and dashboard-review messages.
  - **Tested**: existing tests kept.

## Testing Decisions

Good tests here exercise observable behaviour on synthetic experiment directories: given a small
`.vex` + `.lis` + `.toml` fixture set, assert what the module produces (parsed passes, toml
sections written, markers created, messages built) — never internal call sequences. External
binaries (j2ms2, tConvert, EVN.py, antab_editor) and remote servers are always mocked or replaced
by recorded fakes; the MySQL feedback lookup is tested only for its silent-failure contract.
Prior art to follow: `tests/test_workflow_batch.py` (pause/resume and batch semantics),
`tests/test_lisfile_consistency.py` and `test_pipeline_antab_handling.py` (fixture-driven file
handling), `tests/test_experiment_persistence.py` (state migration), `tests/test_policy.py`
(parameter precedence). All modules listed above get tests; the dashboard's HTML/JS and the
antab_editor GUI are excluded (their data-producing functions are tested instead). The full
existing suite must stay green after every phase of the refactor.

## Out of Scope

- OS-level containerisation (Docker/Apptainer) of pipeline backends.
- Implementation of the `vpipe` pipeline backend and the `sweeps` distribution backend (names
  and registry entries only).
- Implementation of the feedback-database/Grafana results upload (stub with defined signature
  only).
- Any change to `antab_editor` itself or replacement of the manual ANTAB interaction.
- Replacing jiveplot/jplotter for standard plots.
- Changes to the correlator side, SFXC, or how `.lis` content is defined.
- A daemon/orchestrator execution model; scheduling remains external (cron/HTCondor + resume).

## Open Questions

1. Exact schema/credentials format for the EVN feedback MySQL database and the config file in
   `~/.config/evn_postprocess/`. Owner: Benito. Path: copy the connection parameters used by the
   existing archive feedback pages; define a minimal `feedbackdb.toml` (host, db, user, password
   or socket) before implementing `station_feedback`.
2. Heuristic rules for source classification: which fringe-finder catalogue to bundle and the
   precise scan-statistics rule for target vs calibrator. Owner: Benito. Path: check the `vlbiplanobs` repository at `~/Programming/vlbiplanobs`, it has a Source component that already does a RFC, astroquery search for sources
   and that can be used for checking which sources are known. Base on observing time and scan arrangements for the final decisions (a phase calibrator will always bracket the target source(s)).
3. FITS-IDI "completion marker" definition for the e-EVN barrier (presence of expected
   `*_1_1.IDI*` files vs an explicit `.done` marker written by the engine). Owner: implementation.
   Decision: explicit marker written at `tconvert`/`post_polconvert` completion, since file
   presence alone cannot distinguish partial output.
4. Whether the JIVE retrieval module should pre-fill PI info automatically (from which internal
   source, given `.expsum` is dropped) or rely fully on the distribution-time prompt. Owner:
   Benito. Decision: retrieval fills it when a source is trivially available; the prompt remains
   the safety net.
5. Mattermost targeting: current notifier posts to a configured channel; "send to the user"
   may require per-supsci DM configuration in `comms.toml`. Owner: implementation; extend
   `CommsConfig` if needed.

## Further Notes

- The refactor removes three JIVE bootstrap dependencies with in-band replacements: observing
  date (`MASTER_PROJECTS.LIS` → vex `$EXPER`), e-EVN detection (`MASTER_PROJECTS.LIS` →
  `exper_description`), source types and PI info (`.expsum` → toml + heuristics + prompts).
- Recommended implementation order (each phase leaves the suite green): (1) `inputs` +
  `experiment_state` (toml round-trip, vex-only initialize); (2) `retrieval` extraction;
  (3) parameter precedence + silent re-run; (4) `pipelines` extraction; (5) antab summary +
  review/dashboard comments tab + feedback lookup; (6) `distribution` extraction + PI prompt;
  (7) e-EVN barriers.
- The module docstring of the engine still claims Snakemake drives it; correct during phase 1.
- `comms.toml.example` and `computers.toml` remain the model for site configuration; tool
  resolution via `EVN_<TOOLNAME>` env vars / `computers.toml` / `$PATH` is unchanged and is what
  makes the standalone claim realistic for external users.
