# Environment Variables

All environment variables recognised by `evn_postprocess`.

## SSH / SCP

| Variable | Default | Description |
| --- | --- | --- |
| `EVN_SSH_TIMEOUT_S` | `60` | Connect timeout (seconds) for every SSH/SCP call. |
| `EVN_SCP_TIMEOUT_S` | `600` | Wall-clock timeout (seconds) for an SCP transfer. |
| `EVN_SSH_RETRIES` | `2` | Number of retries with backoff on transient SSH failures. |
| `EVN_SSH_BACKOFF_S` | `3.0` | Base backoff (seconds) between SSH retries. |

## Concurrency

How many correlator passes are worked on at once. They only matter for
multi-phase-centre experiments (a normal run has a handful of passes, always below the
ceiling); see [Workflow Overview](../guide/workflow.md#per-pass-concurrency).

| Variable | Default | Description |
| --- | --- | --- |
| `EVN_MAX_PASS_WORKERS` | `min(cpu_count, 16)` | Passes handled at once for in-process (casacore) work: `ysfocus`, `polswap`, `flag_weights`, `onebit`, MS metadata. |
| `EVN_MAX_PASS_IO_WORKERS` | `10` | Passes handled at once for the steps that spawn one subprocess per pass (`j2ms2`, `getdata.pl`), where disk throughput is the ceiling, not cores. |

## External tool overrides

Derived automatically by `tools.resolve` as `EVN_<NAME>`, where `<NAME>` is the
tool's canonical name uppercased with `.`/`-` replaced by `_` — see
[External Tools](../guide/tools.md) for the full resolution order.

| Variable | Overrides |
| --- | --- |
| `EVN_J2MS2` | `j2ms2` |
| `EVN_TCONVERT` | `tConvert` |
| `EVN_EVN_PY` | `EVN.py` |
| `EVN_FEEDBACK_PL` | `feedback.pl` |
| `EVN_ARCHIVE_PL` | `archive.pl` |
| `EVN_ANTAB_EDITOR_PY` | `antab_editor.py` |
| `EVN_CHECK_MULTIPART_FITS_PY` | `check-multipart-fits.py` |
| `EVN_COMPARE_MS_IDI_PY` | `compare-ms-idi.py` |

## Communications

| Variable | Description |
| --- | --- |
| `POSTPROCESS_SMTP_PASSWORD` | SMTP password for email notifications. |
| `POSTPROCESS_MM_TOKEN` | Mattermost personal access token. |

## Configuration paths

| Variable | Default | Description |
| --- | --- | --- |
| `EVN_COMPUTERS_TOML` | (search order) | Explicit path to `computers.toml`. |
| `XDG_CONFIG_HOME` | `~/.config` | Base directory for user-level config files (also used to locate `~/.config/evn_postprocess/feedbackdb.toml`, see the [review API](../api/review.md)). |
