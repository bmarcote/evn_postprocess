# Environment Variables

All environment variables recognised by `evn_postprocess`.

## SSH / SCP

| Variable | Default | Description |
| --- | --- | --- |
| `EVN_SSH_TIMEOUT_S` | `60` | Connect timeout (seconds) for every SSH/SCP call. |
| `EVN_SCP_TIMEOUT_S` | `600` | Wall-clock timeout (seconds) for an SCP transfer. |
| `EVN_SSH_RETRIES` | `2` | Number of retries with backoff on transient SSH failures. |
| `EVN_SSH_BACKOFF_S` | `3.0` | Base backoff (seconds) between SSH retries. |

## External tool overrides

| Variable | Description |
| --- | --- |
| `EVN_J2MS2` | Path to the `j2ms2` binary. |
| `EVN_TCONVERT` | Path to the `tConvert` binary. |
| `EVN_EVNPY` | Path to the `EVN.py` pipeline script. |
| `EVN_FEEDBACKPL` | Path to `feedback.pl`. |
| `EVN_ARCHIVEPL` | Path to `archive.pl`. |
| `EVN_ANTAB_EDITOR` | Path to `antab_editor.py`. |

## Communications

| Variable | Description |
| --- | --- |
| `POSTPROCESS_SMTP_PASSWORD` | SMTP password for email notifications. |
| `POSTPROCESS_MM_TOKEN` | Mattermost personal access token. |

## Configuration paths

| Variable | Default | Description |
| --- | --- | --- |
| `EVN_COMPUTERS_TOML` | (search order) | Explicit path to `computers.toml`. |
| `XDG_CONFIG_HOME` | `~/.config` | Base directory for user-level config files. |
