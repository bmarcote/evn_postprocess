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

## Who is notified

`username` names the recipient directly, which is what a support scientist wants in
their own `~/.config/evn/comms.toml`.

On the **shared `jops` account** there is no single recipient: each experiment belongs to
a different support scientist. Leave `username` empty there and list everybody once, in
`~jops/.config/evn/comms.toml`:

```toml
mode = "mattermost"
username = ""          # empty: resolve per experiment

[[people]]
username = "marcote"        # login / -jss / the .jex `support` field
email = "marcote@jive.eu"
mattermost = "marcote"

[[people]]
username = "nair"
email = "nair@jive.eu"
mattermost = "dnair"
```

`postprocess` then works out, for each run, who to talk to:

1. `username` in `comms.toml`, when set — it always wins.
2. Otherwise the support scientist of the experiment (`exp.supsci`), looked up in
   `[[people]]` case-insensitively; their `email` or `mattermost` address is used
   according to `mode`.

`exp.supsci` itself is resolved when the experiment is loaded, not only for the
notifications: `-jss` when given, the login name otherwise, and — when that is the shared
`jops` account — the `support` field of the experiment's `.jex` file (the later name when
it lists two). It is stored in the checkpoint, so the lookup happens once, and the same
name signs the pipeline feedback page and picks the AIPS user number for the pipeline
input file.

If nobody can be resolved, the run says so in the log and continues with notifications
off; a missing directory entry never stops the post-processing.

## What a message looks like

Every message the operator gets has the same shape, built by `comms.operator_message()`,
so a chat holding several experiments at once stays readable:

```markdown
**Processing of EB101**

The post-processing **stopped at the `antab` step** after 12 s:

`antab_editor.py` needs to be run by hand in `/data/EB101/pipeline/tmp` (check the
station summary above for what to fix), and then run the step again.

**What is needed:**
Fix the cause and re-run `postprocess run` in /data/EB101 to resume from 'antab'.
```

The header names the experiment, the situation says what happened, and *What is needed*
lists what the operator has to do — the same words the terminal prints, written once and
sent to both (the review pause builds its instructions once and renders them in the
terminal panel through `rich.markdown.Markdown`). In Mattermost the body is posted as it
is; in email it is the body and `Processing of EB101 — <headline>` the subject line.

## When notifications are sent

The rule: **whenever the run stops and waits for a human**, a message goes out — plus
one at the end, when it finishes and nobody is waiting any more.

| Situation | Sent from | What it says |
| --- | --- | --- |
| A step failed | `workflow._notify_step_failure()` | Which step, why, and the command to resume from it. |
| A step needs manual work (`antab_editor` by hand, missing station files, a uvflg that cannot be built, msops in batch mode) | `workflow.StepFailed` → same | What has to be done by hand, and where. |
| The MS operations must be decided | `comms.notify_dashboard_review()` | The plots, the summary, and the reply template (Mattermost) or the dashboard command. |
| e-EVN barrier: waiting for sibling experiments | `comms.notify_step_pause()` | Which experiments it waits for, and to re-run once they are done. |
| Review pause after `postpipe` | `workflow._review_pause()` | Open the dashboard, review the PI letter, then answer. |
| `antab_editor` about to start | `review.announce_antab_summary()` | The stations to check in the ANTAB (informational). |
| The post-processing finished | `workflow._announce_completion()` | Everything the run spotted (informational). |

A step that fails is *not* marked done, so `postprocess run` resumes from it. Sending can
never break a run: no notifier, `mode = "none"`, or a server error only logs a warning.

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

### Review pause (`workflow._review_pause`)

After a pause step (`postpipe` by default, configurable through `Policy.pause_after`) the
operator gets the same numbered instructions the terminal panel shows: open the dashboard
(with the exact command and directory), review the PI letter, then answer in the terminal
— or run `postprocess run` again if that terminal is gone. No reply to the message itself
is expected.

### What was spotted (end of the run)

A run that goes well leaves nobody watching the terminal, so the message that says it
finished carries everything the post-processing found, in four blocks (built by
`review.final_summary()`, and shown in the same Markdown in the terminal panel):

```markdown
**What was applied to the data:**
- Weights below 0.7 flagged (2.31% of the non-zero visibilities).
- Wb: polarizations swapped from 10/04/2026 14:22:07 UTC to the end.

**Did not observe:**
- Tr

**Spotted per antenna:**
- Jb: missed 10 10:00-10:20 UT.
- Ir: reduced bandwidth (2/4 subbands).
- Unexpectedly low weights, worth a look at the weight plots: Ys.

**Station files:**
- both `.log` and `.antabfs`: Ef, Mc, Nt.
- only `.log` (**no ANTAB**): Jb.
- only `.antabfs` (**no log**): Wb.
- **neither**: Ys.
- Tsys corrected for opacity: Ys.
```

Only the blocks with something to say are written. When every scheduled station observed
the whole experiment the summary opens with a line saying so, and an experiment with no
antenna information at all sends nothing. The antennas that did not observe are left out
of *Station files*: without data they have nothing to deliver.

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
