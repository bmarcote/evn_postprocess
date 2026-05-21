# Batch Mode

Batch mode allows running the pipeline unattended — useful for queue-based systems (HTCondor, Slurm, cron).

## Usage

```bash
postprocess --batch --policy /path/to/policy.toml run
```

## How it works

1. All interactive decisions (weight threshold, polswap, polconvert, onebit, refant) are read from the **policy file**.
2. When the workflow needs human review (e.g. after `postpipe`), it writes a `REVIEW_REQUIRED` text file and exits cleanly (code 0).
3. A scheduler or wrapper script can detect that file, notify the operator, and resume later with another `postprocess run`.

## The REVIEW_REQUIRED marker

The marker file is written to the experiment root directory and contains:

```text
step: postpipe
experiment: N24AB1
reason: Step 'postpipe' finished successfully. Review n24ab1.piletter and the pipeline output, then run `postprocess run` or `postprocess review ok` to continue.
```

## Combining with communications

Batch mode works well with the [communications module](comms.md):

```bash
postprocess --batch --policy policy.toml --comms comms.toml run
```

When a pause occurs, the comms module will additionally send a notification (email or Mattermost DM) with the experiment summary and pause reason.

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
| Standard plots dashboard | Opens web server | Skipped (plots on disk) |
| MS operations dialog | Terminal prompts | Read from policy |
| Pause notification | Rich panel + desktop notify | `REVIEW_REQUIRED` file + comms |
| Pipeline errors | Rich traceback | Logged, non-zero exit |
