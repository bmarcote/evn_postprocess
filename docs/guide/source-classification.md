# Source Classification

The core needs to know which observed source is a **target**, a **phase
calibrator**, or a **fringe finder** — information that used to come from the
JIVE-internal `.expsum` file. Since `.expsum` is gone, this comes from the
experiment toml `[sources]` section; when it is absent or incomplete, the missing
sources are classified heuristically at `initialize` time.

**This never blocks the run.** Every degradation path (no network, no catalogue,
ambiguous schedule) still produces a best-effort guess plus a logged warning, and
the guess is written back into the toml marked `guessed = true` so it stays visible
and editable (via `postprocess edit` or the toml file) before the pipeline runs.

## The rules, in order

1. **NME / fringe-test experiments** — if the experiment name starts with `N` or
   `F`, every unclassified source is a **target**.
2. **Bundled fringe finders** — a small list of standard EVN fringe finders /
   bandpass calibrators (3C84, 3C273, 3C345, OJ287, DA193, …) is matched by name
   directly: **fringe finder**.
3. **RFC catalogue lookup** (via the optional [`vlbiplanobs`](https://github.com/bmarcote/vlbiplanobs)
   package, matched by name or position within 1 arcsec):
    - **not** in the catalogue → **target** (an unknown source is assumed new).
    - in the catalogue, observed in only a handful of scans (≤ 3 by default) →
      **fringe finder**.
    - in the catalogue, and the schedule shows it bracketing another source
      (`X, Y, X` scan pattern — the classic phase-referencing cycle) → **phase
      calibrator**. When both sources of a pair bracket each other, the one
      spending *less* total time on source wins the tie (phase-cal scans are the
      short ones).
    - in the catalogue, heavily observed, not on the calibrator side of an
      alternation → **phase calibrator** (or **target**, if it lost the
      alternation tie-break, i.e. it is itself being phase-referenced).
4. **Degraded mode** (`vlbiplanobs` not installed, or the catalogue failed to
   load): scan statistics only — a handful of scans → fringe finder; the
   less-observed side of an alternating pair → calibrator; everything else →
   target.

## Installing the catalogue lookup

```bash
pip install evn-postprocess[catalogs]
```

Without it, classification still runs — it just skips step 3 and falls straight
to the degraded scan-statistics rule.

## Overriding a guess

Declare the source explicitly in the experiment toml (this always wins and is
never touched by the heuristic again):

```toml
[sources."J1848+3244"]
type = "target"
```

or fix it after the fact:

```bash
postprocess edit target J1848+3244
postprocess edit phasecal J0900+1234
postprocess edit fringefinder 3C345
```

See the [Experiment TOML Schema](../reference/experiment-toml.md) for the full
`[sources]` section, and the [source_classify API](../api/source_classify.md) for
the implementation.
