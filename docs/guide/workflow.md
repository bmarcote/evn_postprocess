# Workflow Overview

The post-processing pipeline is structured as a sequential list of **steps** (internally called `Task` objects). Each step wraps a Python function and can be individually re-run or skipped.

## Pipeline steps

| Step name | Description |
| --- | --- |
| `initialize` | Creates the directory structure; retrieves obs date and e-EVN run. |
| `lisfile` | Produces `.lis` file(s) in ccs and copies them to eee. |
| `checklis` | Validates the `.lis` files for completeness. |
| `ms` | Runs `j2ms2` to produce Measurement Set files. |
| `plots` | Generates standard plots (weight, auto, cross, amp-phase, amp-time). |
| `msops` | Applies MS operations: weight flagging, polswap, 1-bit scaling. |
| `tconvert` | Runs `tConvert` on all MS files; runs PolConvert if required. |
| `post_polconvert` | Renames `*.PCONVERT` files and reruns standard plots. |
| `antab` | Retrieves or creates the `.antab` amplitude calibration file. |
| `pipeinputs` | Prepares input files and recovers data for the EVN Pipeline. |
| `pipeline` | Runs the EVN Pipeline for all correlator passes. |
| `postpipe` | Post-pipeline: TASAV, comment files, feedback.pl. |
| `prearchive` | Appends Tsys/GC, re-archives FITS-IDI, asks for final checks. |
| `archive` | Sets credentials, creates pipe letter, archives all data. |

## Flow diagram

```text
initialize → lisfile → checklis → ms → plots → msops
    → tconvert → post_polconvert → antab → pipeinputs
    → pipeline → postpipe → ⏸ (review) → prearchive → archive
```

Steps that require user interaction (like `msops` and `postpipe`) will either:

- **Interactive mode**: Present a terminal dialog or web dashboard.
- **Batch mode**: Write a `REVIEW_REQUIRED` marker file and exit cleanly.
- **Comms mode**: Send a notification via email/Mattermost and optionally wait for a reply.

## Pause points

After certain steps (configurable via `pause_after` in the policy file), the pipeline pauses and signals the user. The default pause point is after `postpipe`, where the PI letter and pipeline output require review before archiving.

## State persistence

After each successful step, the experiment state is saved to `{expname}.json`. This allows:

- Resuming after interruption (`postprocess run`).
- Inspecting current state (`postprocess info`).
- Editing metadata (`postprocess edit`).
