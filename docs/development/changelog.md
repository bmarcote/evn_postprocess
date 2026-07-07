# Changelog

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
