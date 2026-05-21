# Changelog

## v2.0.0a5 (in development)

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
