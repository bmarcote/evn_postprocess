# Changelog

## v2.0.0a7 (in development) — Mode-driven cleanup (Phase 2)

Cleanup pass on top of the modular refactor (see `docs/PRD-cleanup.md`): a single
operating **mode** replaces the three backend flags, the core is genuinely
server-agnostic, and per-step output is split into three channels.

### Added

- **Operating modes** (`mode.py`) — `--mode supsci|regular|sweeps`, auto-detected from
  the OS user/group (`jops`/`supsci` group → supsci; `sweeps` group → sweeps; else
  regular), persisted on the experiment and reused on resume. See
  [Operating Modes](../guide/modes.md).
- **Three-channel reporting** (`reporting.py`) — a concise Rich terminal line,
  `logs/logging_messages.log` (loguru debug), and `logs/commands.sh` (replayable local
  commands, one per line with per-step headers).
- **`skip_steps`** top-level toml key (bypass steps; used by `sweeps` mode).
- **`servers.py`** — `Server`/`Servers`/`retrieve_servers` moved out of the core data
  model into a dedicated leaf module, imported only by the JIVE backends and `tools`.
- A server-access **invariant test** (`tests/test_server_boundary.py`): outbound
  ssh/scp may live only in `retrieval/jive.py` and the sanctioned `process.py`
  tConvert-in-eee workaround.

### Changed

- The three `--retrieval`/`--pipeline`/`--distribution` flags are removed in favour of
  `--mode`; the backend registries stay (mode → backend names).
- The final step `archive` is renamed **`distribute`** (`archive` kept as a deprecated
  alias). In non-`supsci` modes it archives nothing and instead verifies the FITS-IDI
  files are in order.
- A step failure now notifies (terminal + comms) and exits non-zero, distinct from the
  clean review pause (marker + exit 0); the failed step stays the resume point.
- The debug log moved to `logs/logging_messages.log` (freeing `logs/commands.sh`).
- `main`'s default working directory degrades to the current directory when no
  `computers.toml` exists, so a standalone user needs no server configuration.

### Removed

- The core server-coupled legacy: `io.py` (its transport moved into the JIVE retrieval
  backend), `parse_masterprojects`, `get_jexp_info`, `expsumfile`, and the
  `comment_tasav` MASTER_PROJECTS ssh lookup (the observing date now comes from the
  local vex). The ccs `.lis` transport moved from `lisfiles.py` (now local-only) into
  `retrieval/jive.py`.

## v2.0.0a6 (in development) — Modular refactor

Standalone, modular re-design (see `docs/PRD-refactor.md`): the core now consumes
exactly a `.vex` file, N `.lis` files, and an optional experiment `.toml`, with
every JIVE-specific concern moved behind three plugin interfaces.

### Added

- **Retrieval, pipeline, and distribution backends** (`retrieval/`, `pipelines/`,
  `distribution/`) — each with a `jive` implementation and a `none` no-op;
  `pipelines` additionally registers `vpipe` and `distribution` registers
  `sweeps` as not-yet-implemented placeholders. Selectable via
  `--retrieval`/`--pipeline`/`--distribution` or the experiment toml, sharing a
  `registry.BackendRegistry`.
- **Experiment TOML** (`experiment_state.py`, `{expname}.toml`) — source types, PI
  contacts, backend selection, and the program-written `[postprocess]`/
  `[comments]` sections, with a parameter-precedence rule (toml → policy →
  ask/pause) that makes a completed toml reproduce a run silently.
- **Vex-only initialisation** (`inputs.py`) — observing date, e-EVN membership,
  stations/sources/scans all derived from the `.vex` file; `MASTER_PROJECTS.LIS`,
  `.jexp`, and `.expsum` are no longer read anywhere in the core.
- **Heuristic source classification** (`source_classify.py`) — target/calibrator/
  fringe-finder guessed from the schedule and an optional RFC-catalogue lookup
  (`vlbiplanobs`, optional `catalogs` extra) when not declared in the toml.
- **Station summary + dashboard Comments tab** (`review.py`, `plotting.py`) — a
  pre-`antab_editor` terminal panel (did-not-observe, missed time, reduced
  bandwidth), and a dashboard tab persisting per-station status/notes into the
  toml, pre-filled from the summary and (optionally) the EVN feedback database.
- **e-EVN coordination** (`eevn.py`) — sibling-directory convention and two
  filesystem synchronisation barriers (shared ANTAB session, leader→EXPn antab
  hand-off), pause-and-resume, no daemon.
- New `polconvert` workflow step, split out from `tconvert`.
- `--pipeline`/`--distribution` CLI flags (alongside the existing `--retrieval`),
  all three validated at parse time and, again, right after the toml attaches.

### Changed

- Dashboard binds `127.0.0.1` only (was `0.0.0.0`): the write API
  (`set_comments`/`set_source_type`/`set_refant`) is unauthenticated, so it must
  not be reachable off-box; use the printed SSH tunnel command.
- MS operations (weight flagging, polswap, 1-bit, Yebes/Hobart mount fix) run
  in-process via the `mstools` subpackage; the standalone `mstools` CLI
  (`mstools run <tool> <msfile> ...`) replaces the historical individual scripts.
- `feedback.pl` ported in-tree to `feedback.py` for the `aips` pipeline backend.

### Fixed

- `distribution.jive.deliver` stops at the first failing stage instead of
  evaluating the whole delivery chain eagerly, so a failed credentials/protection
  step can no longer be followed by an archive upload.

## v2.0.0a5

### Added

- **Communications module** (`comms.py`) — Send notifications via email or Mattermost at pipeline review points. Mattermost supports interactive feedback for msops decisions.
- **`--comms` CLI flag** — Point to a `comms.toml` configuration file.
- **Web dashboard** — Interactive experiment summary and plot viewer served over HTTP.
- **Batch mode** (`--batch`, `--policy`) — Fully unattended pipeline execution.
- **External tool resolution** (`tools.py`) — Environment-variable and config-based binary lookup.
- **Policy file** (`policy.toml`) — Machine-readable decisions for unattended runs.
- **`REVIEW_REQUIRED` marker** — Written in batch mode when human review is needed.
- **`postprocess edit`** — Edit experiment metadata (refant, source types).
- **`postprocess exec`** — Run individual pipeline commands.
- **Full documentation** — Zensical/MkDocs-based docs with ReadTheDocs deployment.

### Changed

- Standardplots now convert PS → PNG and open a web dashboard instead of printing file paths.
- `open_standardplot_files()` replaced with `serve_dashboard()`.
- MS metadata extraction now aggregates across all passes and marks missing antennas.
- Logging uses `post_processing.log` in experiment root (human-readable) + `logs/post_process.log` (debug).

### Fixed

- Shell commands no longer use string interpolation (safe filenames with spaces/metacharacters).
- SSH operations have configurable timeouts and retries.
