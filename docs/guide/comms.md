# Communications

The communications module allows the pipeline to send notifications at key interaction points instead of (or in addition to) blocking in the terminal.

## Supported modes

| Mode | Description |
| --- | --- |
| `none` | Default — no notifications, same behaviour as before. |
| `email` | Sends an email with experiment summary and plots at review points. |
| `mattermost` | Sends a Mattermost DM with summary, plots, and (for `msops`) waits for interactive feedback. |

## Configuration

Create a `comms.toml` file in one of these locations (searched in order):

1. Explicit path via `--comms /path/to/comms.toml`.
2. `./comms.toml` (per-experiment).
3. `$XDG_CONFIG_HOME/evn/comms.toml` (user-level).
4. `~jops/.config/evn/comms.toml` (shared account).

### Example: Email

```toml
mode = "email"
username = "scientist@example.com"

[email]
smtp_host = "smtp.example.com"
smtp_port = 587
from_address = "postprocess@jive.eu"
# Or set POSTPROCESS_SMTP_PASSWORD env var
password = ""
```

### Example: Mattermost

```toml
mode = "mattermost"
username = "bmarcote"

[mattermost]
server_url = "https://mattermost.jive.eu"
# Or set POSTPROCESS_MM_TOKEN env var
token = "your-personal-access-token"
# Optional: fixed channel. If empty, a DM is created.
channel_id = ""
```

## When notifications are sent

### Dashboard review (msops step)

When the pipeline reaches the `msops` step:

1. A message is sent with the full experiment summary (antennas, sources, frequency, etc.) and all standard-plot PNGs attached.
2. **Email mode**: Informational only — log in to review.
3. **Mattermost mode**: The message includes a reply template. The user can reply directly with the msops parameters:

    ```text
    weight_threshold: 0.85
    polswap: Wb, Jb
    onebit: none
    polconvert: Kt
    ```

    The pipeline parses the reply and continues automatically without requiring terminal access.

### Pipeline pause (_signal_pause)

After steps like `postpipe`, an informational notification is sent with the experiment summary and pause reason. No reply is expected — the user must log in for further review.

## Security

- Passwords and tokens can be stored in environment variables (`POSTPROCESS_SMTP_PASSWORD`, `POSTPROCESS_MM_TOKEN`) to avoid putting secrets in config files.
- The Mattermost notifier uses **Personal Access Tokens** — create one in Mattermost → Account Settings → Security.

## Combining with batch mode

```bash
postprocess --batch --policy policy.toml --comms comms.toml run
```

In this configuration:

- The policy provides all decisions (no interactive dialog needed).
- Comms sends a notification when the pipeline pauses so the operator knows to review.
