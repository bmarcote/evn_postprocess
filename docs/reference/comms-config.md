# Communications Config Reference

The `comms.toml` file configures how the pipeline sends notifications.

## File locations (search order)

1. Explicit path via `--comms /path/to/comms.toml`.
2. `./comms.toml` in the experiment directory.
3. `$XDG_CONFIG_HOME/evn/comms.toml` (default: `~/.config/evn/comms.toml`).
4. `~jops/.config/evn/comms.toml` (shared account).

## Full schema

```toml
# Communication mode: "none", "email", or "mattermost"
mode = "none"

# Target: email address (email mode) or Mattermost username (mattermost mode).
# Leave empty on a shared account: the support scientist of each experiment is then
# looked up in [[people]] below.
username = ""

# The people directory: one entry per support scientist.
[[people]]
username = "marcote"        # login / -jss / the .jex `support` field (case-insensitive)
email = "marcote@jive.eu"   # used in mode = "email"
mattermost = "marcote"      # used in mode = "mattermost"

[email]
smtp_host = "smtp.example.com"
smtp_port = 587
from_address = ""   # Defaults to 'username' if empty
password = ""       # Or use POSTPROCESS_SMTP_PASSWORD env var

[mattermost]
server_url = "https://mattermost.example.com"
token = ""          # Or use POSTPROCESS_MM_TOKEN env var
channel_id = ""     # Optional: if empty, a DM channel is created
```

## Field descriptions

### Top-level

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `mode` | str | `"none"` | Communication back-end: `none`, `email`, or `mattermost`. |
| `username` | str | `""` | Recipient identifier. When empty, the recipient is resolved per experiment from `[[people]]` (see below). |

### `[[people]]` entries

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `username` | str | — | The name the program knows the support scientist by: their login on `eee`, the `-jss` argument, and the `support` field of the experiment `.jex` file. Matched case-insensitively. Entries without it are ignored. |
| `email` | str | `""` | Address used in `mode = "email"`. |
| `mattermost` | str | `""` | Mattermost username (no `@`) used in `mode = "mattermost"`. |

### `[email]` section

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `smtp_host` | str | `""` | SMTP server hostname. |
| `smtp_port` | int | `587` | SMTP port (587 for STARTTLS). |
| `from_address` | str | `username` | Sender email address. |
| `password` | str | `""` | SMTP password. Prefer env var `POSTPROCESS_SMTP_PASSWORD`. |

### `[mattermost]` section

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `server_url` | str | `""` | Mattermost instance URL (no trailing slash). |
| `token` | str | `""` | Personal access token. Prefer env var `POSTPROCESS_MM_TOKEN`. |
| `channel_id` | str | `""` | Fixed channel ID. If empty, a DM with `username` is created. |

## Environment variables

| Variable | Description |
| --- | --- |
| `POSTPROCESS_SMTP_PASSWORD` | SMTP password (overrides `[email].password`). |
| `POSTPROCESS_MM_TOKEN` | Mattermost token (overrides `[mattermost].token`). |

## Mattermost interactive feedback format

When the pipeline sends a dashboard review message via Mattermost, the user can reply with:

```text
weight_threshold: 0.85
polswap: Wb, Jb
onebit: none
polconvert: Kt
```

Rules:

- One `key: value` per line.
- Antenna names are validated against the experiment's antenna list.
- `none` or empty means no antennas for that operation.
- If parsing fails, the pipeline sends an error message and falls back to manual login.
