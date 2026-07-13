# Workflow Overview

The post-processing pipeline is structured as a sequential list of 16 **steps**
(internally `Task` objects). Each step wraps a Python function and can be
individually re-run (`postprocess run STEP`) or executed in isolation
(`postprocess exec NAME`). Three groups of steps delegate to a selectable
**plugin backend** (retrieval, pipeline, distribution — see
[Operating Modes](modes.md)), so the same step list runs unattended at JIVE or
fully offline for an external user.

## Pipeline steps

| Step name | Description |
| --- | --- |
| `initialize` | Directory structure; obtains the `.vex` via the retrieval backend; derives date, e-EVN membership, stations/sources/scans from it; applies the experiment toml; classifies untyped sources. |
| `lisfiles` | Creates `.lis` file(s) via the retrieval backend when none exist locally. |
| `checklis` | Validates the `.lis` files; extracts the correlator passes. |
| `j2ms2` | Runs `j2ms2` to produce Measurement Set(s); extracts MS metadata; computes the lag-space SNR (unless `--no-lag`). |
| `standardplots` | Generates the standard plots (weight, auto, cross, amp-phase, amp-time). |
| `msops` | Applies MS operations: weight flagging, polswap, 1-bit scaling — parameters resolved via the [toml precedence rule](../reference/experiment-toml.md#parameter-precedence). |
| `tconvert` | Converts the MS to FITS-IDI via `tConvert`. |
| `polconvert` | Runs PolConvert on antennas that observed with linear polarization (skipped if none need it). |
| `post_polconvert` | Backs up and renames the `*.PCONVERT` files; writes the e-EVN FITS-IDI completion marker. |
| `standardplots2` | Re-runs the standard plots after all MS operations / PolConvert. |
| `antab` | Fetches station files, prints the station summary, opens `antab_editor.py` (manual step, unchanged). |
| `pipeinputs` | The pipeline backend's `prepare()`: builds the calibration-pipeline input file(s). |
| `pipeline` | The pipeline backend's `run()`: runs the calibration pipeline over all correlator passes. |
| `postpipe` | The pipeline backend's `collect()`: diagnostics, TASAV/comment files, feedback page; PI letter auto-fill. **Review pause.** |
| `prearchive` | Appends Tsys/gain-curve info to the FITS-IDI files; records the finalisation parameters into the toml. |
| `distribute` | The distribution backend's `deliver()`: credentials, protection, archive upload, PI letter. |

See [Workflow Steps & Local Tools](../reference/steps.md) for what each step calls
under the hood — useful if you ever need to reproduce a step by hand.

## Flow diagram

```text
initialize → lisfiles → checklis → j2ms2 → standardplots → msops
    → tconvert → polconvert → post_polconvert → standardplots2 → antab
    → pipeinputs → pipeline → postpipe → ⏸ (review) → prearchive → archive
```

Steps that require a decision (`msops`) or a review pause (`postpipe`, plus the
e-EVN barriers inside `antab`) either:

- **Interactive mode**: present a terminal dialog or the web dashboard.
- **Batch mode** (`--batch`): write a `REVIEW_REQUIRED` marker file and exit
  cleanly (code 0).
- **Comms mode**: additionally send a notification via email/Mattermost, and (for
  `msops` over Mattermost) accept a structured reply to continue without logging
  in.

## The review pause (`postpipe`)

After `postpipe`, the terminal and the configured notifier both point to the
dashboard (`postprocess info --serve`, including the new **Comments** tab — see
[Dashboard](dashboard.md)) and the PI letter. Answering:

- **Enter** — finalises: runs `prearchive` and `distribute` in the same invocation.
- **a step name** — re-runs the workflow from that step, returning to this same
  review point afterwards.
- **quit** — stops here; resume any time with `postprocess run`.

In batch mode there is no prompt: a `REVIEW_REQUIRED` marker is written and the
notifier is informed; resume with `postprocess run` once you've reviewed.

## e-EVN coordination

For an e-EVN run, the `antab` and `pipeline` steps additionally wait on
cross-experiment barriers (sibling FITS-IDI completion, and the run leader's final
ANTAB) — see [e-EVN Coordination](eevn.md).

## State persistence

After each successful step, the experiment state is saved to `{expname}.json`
(internal checkpoint: step completion, file inventories, timestamps). Separately,
resolved parameters and review comments are written into `{expname}.toml` (see the
[Experiment TOML Schema](../reference/experiment-toml.md)). This split allows:

- Resuming after interruption (`postprocess run`).
- Inspecting current state (`postprocess info`).
- Editing metadata (`postprocess edit`).
- Reproducing a run end-to-end from just the toml, with zero questions asked.
