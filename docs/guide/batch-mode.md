# Batch Mode

Batch mode allows running the pipeline unattended — useful for queue-based systems
(HTCondor, Slurm, cron) or the standalone (`--retrieval none --distribution none`)
use case. Every interaction point either resolves silently from the experiment toml
/ policy, or writes a `REVIEW_REQUIRED` marker and exits cleanly (code 0) instead of
blocking on a prompt.

## Usage

```bash
postprocess --batch --policy /path/to/policy.toml run
```

## How it works

1. Every decision parameter (weight threshold, polswap/polconvert/onebit antennas,
   refant, and the three backend modes) is resolved through the
   [precedence rule](../reference/experiment-toml.md#parameter-precedence):
   experiment toml value first, then the **policy file**, then a pause.
2. When the workflow needs human review (the `msops` decision if unresolved, the
   `postpipe` review pause, or an e-EVN barrier), it writes a `REVIEW_REQUIRED`
   marker in the experiment root directory and exits.
3. A scheduler or wrapper script detects that file, notifies the operator (see
   below), and resumes later with another `postprocess run`.

## The REVIEW_REQUIRED marker

```text
step: postpipe
experiment: N24AB1
reason: Step 'postpipe' finished successfully. Review n24ab1.piletter and the
pipeline output, then run `postprocess run` to continue.
```

The marker is purely informational for a human or a script watching the directory;
the only supported way to resume is re-invoking `postprocess run` (which clears the
marker once the pause condition is satisfied).

## Combining with communications

```bash
postprocess --batch --policy policy.toml --comms comms.toml run
```

When a pause occurs, the comms module additionally sends a notification (email or
Mattermost) with the experiment summary and pause reason — see
[Communications](comms.md).

## Policy file

See [Policy File reference](../reference/policy.md) for the full schema.

### Minimal example

```toml
weight_threshold = 0.85
polswap          = ["Wb"]
polconvert       = ["Kt"]
onebit           = []
refant           = ["Ef"]
pause_after      = ["postpipe"]
skip_archive     = false
batch            = true
```

## Behaviour differences in batch mode

| Feature | Interactive | Batch |
| --- | --- | --- |
| Standard plots dashboard | Opens web server (via `postprocess info --serve`) | Not opened automatically; plots stay on disk |
| MS operations decision | Terminal prompt / auto-diagnostics | Toml → policy → `REVIEW_REQUIRED` |
| `postpipe` review pause | Rich panel + terminal prompt (approve / re-run from step / quit) | `REVIEW_REQUIRED` marker + notifier, exit 0 |
| e-EVN barriers | Same pause/resume either way | Same pause/resume either way |
| Missing PI info at `archive` | Interactive prompt | Fails clearly, naming the missing field |
| Pipeline errors | Rich traceback | Logged, non-zero exit |
