# Plugin Backends

Everything site-specific (where files come from, which calibration pipeline runs,
how the finished experiment is delivered) lives behind three plugin interfaces.
Each has a `jive` implementation reproducing the historical JIVE behaviour, plus a
`none`/no-op for external users and, for two of them, a registered-but-unimplemented
placeholder for future work.

| Family | Module | Backends | Default | CLI flag | Toml key |
| --- | --- | --- | --- | --- | --- |
| Retrieval | `retrieval` | `jive`, `none` | `jive` | `--retrieval MODE` | `[retrieval] mode` |
| Pipeline | `pipelines` | `aips`, `none`, `vpipe` (stub) | `aips` | `--pipeline MODE` | `[pipeline] mode` |
| Distribution | `distribution` | `jive`, `none`, `sweeps` (stub) | `jive` | `--distribution MODE` | `[distribution] mode` |

**Precedence** (same for all three): `--<family>` CLI flag > `[<family>] mode` in the
experiment toml > the default above. An unknown backend name aborts at startup,
before any step runs — a typo never surfaces hours into a run.

## Retrieval: how input files are obtained

`Retriever.fetch(workdir, expname) -> InputSet` locates/obtains the `.vex`, `.lis`,
and optional toml; `fetch_lisfiles(exp)` creates `.lis` files when none exist yet;
`fetch_station_files(exp)` obtains the `.log`/`.antabfs` files needed at the `antab`
step.

- **`jive`** (default): copies the vex from the correlator server (`ccs`), creates
  `.lis` files there when missing, and fetches station files from `vlbeer` — the
  historical behaviour, requiring a `computers.toml` server configuration.
- **`none`**: validates that everything is already on disk; never contacts a server.
  Missing files raise a clear error naming exactly what to place where. This is
  what makes the package usable outside JIVE:

  ```bash
  # everything needed is already in the current directory:
  postprocess --retrieval none --pipeline none --distribution none run
  ```

## Pipeline: the calibration step

`PipelineBackend.prepare(exp)` builds the pipeline input files from local files
only (station-file fetching belongs to retrieval, not here); `.run(exp)` executes
the pipeline over all correlator passes; `.collect(exp)` gathers diagnostics.

- **`aips`** (default): wraps the historical EVN.py AIPS pipeline unchanged.
- **`none`**: skips calibration entirely; downstream steps (archiving) still run,
  useful for testing the rest of the workflow without AIPS.
- **`vpipe`**: registered name for a future pipeline; selecting it fails immediately
  with an explicit "not implemented" message.

## Distribution: delivery

`Distributor.deliver(exp) -> bool` runs the delivery stages in order, stopping at
the first failure (so nothing is archived if credentials/protection fail).

- **`jive`** (default): observation summary, PI credentials, PI letter (folding in
  the review `[comments]`), archive upload, then a prompt to send the letter.
  Missing PI contact info is asked for interactively (persisted to `[[pi]]`) or
  fails clearly in `--batch`. Contains a documented `upload_feedback(exp)` stub for
  the future Grafana-visible feedback-database upload.
- **`none`**: nothing is archived and no server is ever contacted; the workflow
  completes with the data left in the experiment directory. This is the mode for
  test runs and for external (non-JIVE) users.
- **`sweeps`**: registered name for a future delivery target; selecting it fails
  immediately with an explicit "not implemented" message.

## Writing a new backend

Each family is a small name-keyed registry (`retrieval.register`,
`pipelines.register`, `distribution.register`) built on the shared
`registry.BackendRegistry`. A third party registers a zero-argument factory under a
new name — factories are lazy, so an unselected backend's dependencies (ssh, MySQL,
AIPS/ParselTongue) are never imported. See the [API reference](../api/retrieval.md)
for the exact interfaces.
