# e-EVN Coordination

An e-EVN run correlates several experiments (EXP1, EXP2, …) from a single
observation, sharing one `.vex` file (named after EXP1). `postprocess` detects this
from the vex `$EXPER` block's `exper_description` field
(`e-EVN: EXP1, EXP2, ...`) — no `MASTER_PROJECTS.LIS` lookup is needed any more.

## Directory convention

Each experiment of the run is processed in its own directory, following the
sibling convention `../EXPm` relative to any one of them:

```text
/data0/you/EXP1/   <- run leader; shared .vex lives here
/data0/you/EXP2/
/data0/you/EXP3/
```

EXPn (n>1) locates EXP1's vex automatically (or, in `--mode regular` mode, you
copy it there yourself — see [Operating Modes](modes.md)).

## The two synchronisation barriers

Both are plain filesystem checks with pause-and-resume semantics — **no daemon,
no cross-process communication**. A barrier that isn't satisfied writes a
`REVIEW_REQUIRED` marker, sends a notification, and exits cleanly (code 0); simply
re-running `postprocess run` later re-checks it.

### Barrier (a): `antab` waits for every sibling's FITS-IDI

A single ANTAB-editing session should cover the whole e-EVN run, so the leader's
`antab` step waits until **every** sibling experiment has produced its FITS-IDI
files. This is tracked by an explicit completion marker
(`{expname}.fitsidi_ready`), written by the engine once `post_polconvert` finishes
— file presence alone can't distinguish partial output, hence the explicit marker
rather than just globbing for `*.IDI*`.

```text
Waiting for the FITS-IDI completion of the other e-EVN experiments: EXP2, EXP3
(markers in ../EXPn). Re-run `postprocess run` once they are processed.
```

### Barrier (b): EXPn's `pipeline` waits for the leader's final ANTAB

Once the shared ANTAB session (run from EXP1) has produced the final `.antab`
files in `../EXP1/pipeline/in/`, each EXPn copies them over (renamed to its own
experiment code) and proceeds to its own pipeline run.

```text
Waiting for the final .antab files of the e-EVN run leader EXP1 (expected in
../EXP1/pipeline/in/). Re-run `postprocess run` once they exist.
```

## Practical workflow

1. Run `postprocess run` independently in each `EXPn` directory — through
   `standardplots2`, every experiment proceeds on its own.
2. All experiments converge at `antab`: the leader (EXP1) pauses until every
   sibling reaches this point too, then opens `antab_editor.py` once for the whole
   run.
3. Once EXP1's ANTAB is done, re-run `postprocess run` in each `EXPn` (n>1)
   directory; barrier (b) is now satisfied and each proceeds through its own
   pipeline, postpipe, and archive independently.

A non-e-EVN experiment never triggers either barrier. See the
[eevn API reference](../api/eevn.md) for the exact functions.
