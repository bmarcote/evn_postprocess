# Operating Modes

`evn_postprocess` runs in one of three **modes**. The mode answers a single question —
*who is running this, and in what context* — and from that decides how the input files
are obtained and how the finished experiment is delivered. Everything **between**
initialization and delivery is identical in every mode.

A single `--mode` flag replaces the earlier three `--retrieval`/`--pipeline`/`--distribution`
backend flags. When omitted, the mode is auto-detected from the OS.

| Mode | Who | Input files | Delivery | Server access |
| --- | --- | --- | --- | --- |
| `supsci` | JIVE support scientist (`jops`, or the `supsci` OS group) | fetched from the correlator (`ccs`) + station files from `vlbeer` | credentials, PI letter, EVN-archive upload | yes (correlator, vlbeer, archive) |
| `regular` | anyone else | already local | none — verifies the FITS-IDI files are in order | **none** |
| `sweeps` | the automated SWEEPS system (`sweeps` OS group) | from SWEEPS (not implemented yet) | to SWEEPS (not implemented yet) | yes (future) |

## Selecting the mode

Auto-detection (no flag): login name `jops` or membership of the OS group `supsci`
selects `supsci`; membership of the OS group `sweeps` selects `sweeps`; anything else
is `regular`. A group that does not exist on the machine simply counts as "not a member".

Force it explicitly with `--mode`:

```bash
postprocess --mode regular run          # never contacts a server
postprocess --mode supsci run           # full JIVE support-scientist job
postprocess --mode sweeps --config eb101.toml run   # automated (not implemented yet)
```

The resolved mode is **persisted on the experiment** at initialization and reused by
every later invocation (`run`, `exec`, `dashboard`), so a resume never silently switches
mode. Passing a different `--mode` on a later run overrides and re-persists it, with a
warning. An unknown `--mode` value is rejected at parse time.

## `supsci` — the JIVE support-scientist job

Reproduces the historical behaviour. Initialization fetches the `.vix` file from the
correlator server and creates/copies the `.lis` files; the `antab` step fetches the
`.log`/`.antabfs` files from `vlbeer`; the `distribute` step creates credentials and the
PI letter and uploads everything to the EVN archive. Requires a `computers.toml` server
configuration (see [Configuration](../getting-started/configuration.md)). All outbound
`ssh`/`scp` for input/output lives inside the JIVE backend modules — no other part of
the package contacts a server.

## `regular` — a normal, standalone user

Everything the run needs is already in the working directory: the `.vex`, the `.lis`
file(s), the correlated data, and (by the `antab` step) the `.antab`/`.uvflg` files.
No server is ever contacted, and **no `computers.toml` is required**. The `distribute`
step archives nothing; instead it **verifies the run ended in a known-good state** — the
expected `*.IDI*` files are present for every correlator pass — reporting "ready" or a
hard error naming exactly what is missing.

```bash
# in the experiment directory, with all inputs already present:
postprocess run                 # auto-detects 'regular' for a non-jops user
```

If a required file is missing at initialization (or the `.antab`/`.uvflg` files are
missing when the pipeline needs them), the run stops immediately with a message naming
the file and where it was expected — `regular` mode never tries to fetch it.

## `sweeps` — the automated SWEEPS system (future)

Registered but **not implemented yet**: selecting it fails with an explicit
"not implemented" message. When built, it will retrieve inputs from and deliver back to
the SWEEPS system, driven entirely by a fully-prepared experiment toml (`--config`, or
the conventional `{expname}.toml`). In this mode nothing is guessed: heuristic source
classification is disabled, and any value a step needs but the config does not supply is
a hard, field-named error rather than a pause. A `skip_steps` list in the toml lets the
automated run bypass steps that do not apply (see
[Experiment TOML Schema](../reference/experiment-toml.md)).

## Under the hood

Each mode maps to a small set of pluggable backends (retrieval / pipeline / distribution)
built on an ABC + name-keyed registry; the calibration pipeline is the EVN AIPS pipeline
in every mode. Backends are looked up lazily, so a `regular` run never imports the
JIVE-specific machinery. See the API reference for
[retrieval](../api/retrieval.md), [pipelines](../api/pipelines.md), and
[distribution](../api/distribution.md).
