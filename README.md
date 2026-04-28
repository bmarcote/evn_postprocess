# EVN Postprocess Pipeline

**This is a program meant for internal use. It is expected to not run in other environments**

Pipeline to run all post-processing steps of EVN data in a semi-interactive and semi-smart way. This code will run all steps required from post-correlation at JIVE to user delivery of the data. That is, it runs the steps defined in the _SFXC Post-Correlation Checklist_.


## Usage

Being in `eee` at a `/data0/ {supsci} / {EXPNAME}` folder, you can call it as simple as:

```
postprocess
``` 
to run the full process semi-automatically.
Or use the `--expname` and `--supsci` options if you are in a different location.


The program has the following extra command options:

```
postprocess info
```
to show the info related to the experiment if it already run though the required steps that recover it (when copying expsum, lis, and after creating the MS file).

```
postprocess last
```
to show the last successfully-run step.

```
postprocess run STEP1  [STEP2]
```
to run the post-process from `STEP1`  until `STEP2` (or until the end if the later is not specified).

```
postprocess exec COMMAND
```
to run only a single command by using the existing metadata, so you don't care on parameters. do `exec -h` to see all of them.

```
postprocess edit PARAM VALUE
```
to edit something that was wrong from the metadata (e.g. which sources are targets, or change the reference antenna, etc).

As always, `postprocess -h` for the full help, or inside each command as `postprocess command -h`.

## Unattended / batch mode

Since v2.0 the program can also run unattended, which is useful when calling it
from a queueing system (HTCondor, Slurm, cron, …):

```
postprocess --batch --policy /path/to/policy.toml run
```

Every decision that used to be asked interactively in `msops` is read from the
TOML file. When the workflow needs human input (or finishes a step that the
policy lists as a review point — by default `postpipe`), it does **not** block
waiting on a TTY: it writes a `REVIEW_REQUIRED` text file in the experiment
directory, returns from the runner, and exits with status code 0. A scheduler
or wrapper script can detect that file, ping a human, and resume the run later
with another `postprocess run` invocation.

A minimal `policy.toml` looks like:

```toml
weight_threshold = 0.85          # required in batch mode (0.0..1.0)
polswap          = ["Wb"]
polconvert       = ["Kt"]
onebit           = []
refant           = ["Ef"]
pause_after      = ["postpipe"]   # which step names should pause for review
skip_archive     = false
batch            = true
```

The full schema (with field semantics) lives in the docstring of
`evn_postprocess.policy.Policy`. Existing experiment JSON state files keep
loading without intervention thanks to schema-migration logic in
`Experiment.from_dict`.

## Configuring external binaries

External tools (`tConvert`, `j2ms2`, `EVN.py`, `feedback.pl`, `archive.pl`,
`antab_editor.py`) are resolved by `evn_postprocess.tools.resolve` in the
following order:

  1. `EVN_<TOOLNAME>` environment variable (e.g. `EVN_TCONVERT=/opt/.../tConvert`).
  2. A matching entry in `computers.toml` (entry name == tool name, `path` is used).
  3. The system `$PATH`.

This replaces the previous hardcoded paths and silent `FileNotFoundError`s.

## Tunables (environment variables)

| Variable | Default | Purpose |
| --- | --- | --- |
| `EVN_SSH_TIMEOUT_S` | 60 | Connect timeout for every SSH/SCP call. |
| `EVN_SCP_TIMEOUT_S` | 600 | Wall-clock timeout for an SCP transfer. |
| `EVN_SSH_RETRIES` | 2 | Retries with backoff on transient SSH failures. |
| `EVN_SSH_BACKOFF_S` | 3.0 | Base backoff between SSH retries. |
