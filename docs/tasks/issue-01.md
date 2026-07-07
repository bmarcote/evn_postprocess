# Tasks for #1: Experiment toml round-trip (`experiment_state` module)

Parent issue: #1 in `docs/issues-refactor.md`
Parent PRD: `docs/PRD-refactor.md` (sections "Experiment toml layout", "Parameter precedence
and silence rule", Module Design → `experiment_state`)

## Tasks

### 1. Approve the concrete EXPNAME.toml schema

**Type**: REVIEW
**Output**: `docs/experiment.toml.example` committed and approved by Benito
**Depends on**: none

Draft a fully-commented example experiment toml containing every section and key the PRD
defines: `[observation]` (scans to process as ranges/individual/lists of ranges, expname
override if needed), `[sources]` (per-source type: target/calibrator/fringefinder, plus a flag
marking entries as heuristic guesses), `[pi]` (name(s), email(s), support scientist),
`[pipeline]` (backend name), `[distribution]` (backend name), `[postprocess]` (weight
threshold, polswap/polconvert/onebit antenna lists, refant, antab and polconvert-input file
links — all optional, program-written), `[comments]` (general note, per-station note + status
green/orange/red). Reuse existing field vocabulary from `policy.py` (weight_threshold, polswap,
polconvert, onebit, refant) and `experiment.py` (SourceType, PI) so names stay consistent.
Present the file to Benito for approval before any code depends on it. Do not proceed to task 2
until approved.

---

### 2. Add tomlkit dependency and the experiment_state skeleton

**Type**: CONFIG
**Output**: `tomlkit` in `pyproject.toml`; `src/evn_postprocess/experiment_state.py` imports
cleanly and declares section ownership
**Depends on**: 1

Add `tomlkit` (format/comment-preserving TOML read-write) to the project dependencies —
stdlib `tomllib` stays for read-only uses like `Policy.load`. Create
`src/evn_postprocess/experiment_state.py` with module docstring, constants for the section
names, and an explicit ownership map: user-owned sections (`observation`, `sources`, `pi`,
`pipeline`, `distribution`) vs program-owned (`postprocess`, `comments`), noting the single
exception that heuristic source classifications may be recorded into `[sources]`. Follow the
flat-module style of `policy.py` (dataclasses, no deep hierarchies, docstrings per function).

---

### 3. Implement toml loading and validation

**Type**: WRITE
**Output**: `load_toml(path) -> ExperimentToml` returning structured section data
**Depends on**: 2

In `experiment_state.py`, implement loading of an experiment toml into a typed container
(dataclass per section, mirroring `Policy`'s style, with `from_dict` ignoring unknown keys
defensively). Validation errors must name the file, the section, and the offending key with the
expected type. A missing file or missing sections are not errors — every section is optional
(the PRD's "warn, never block" rule); return empty defaults. Keep the raw `tomlkit` document
accessible on the container so later write-backs preserve formatting and comments.

---

### 4. Tests for toml loading

**Type**: TEST
**Output**: `tests/test_experiment_state.py` passing
**Depends on**: 3

New test module following the fixture style of `tests/test_policy.py`. Cases: full toml (all
sections, values land in the right fields), partial toml (missing sections → defaults), empty
file, missing file (→ empty container, no exception), malformed toml (→ error naming file),
wrong-typed value (→ error naming section and key), unknown keys ignored. Use `tmp_path`
fixtures writing literal toml strings.

---

### 5. Implement the scan-selection parser

**Type**: WRITE
**Output**: scans-to-process key accepts `"4"`, `"3-10"`, `"1-5,20-30"` and lists thereof
**Depends on**: 3

Implement a parser that expands a scan selection expression into an explicit ordered set of
scan numbers: single scans, inclusive ranges, comma-separated mixes, and TOML lists of such
strings/ints. Reject malformed input (reversed ranges, negatives, non-numeric tokens) with an
error quoting the offending token. Place the pure parsing function in `experiment_state.py`
(it is toml-schema logic, not a generic utility) and call it during `[observation]` loading so
the container exposes resolved scan numbers.

---

### 6. Tests for scan-selection parsing

**Type**: TEST
**Output**: parser tests in `tests/test_experiment_state.py` passing
**Depends on**: 5

Cover: single scan, single range, multiple ranges with individual scans mixed, TOML list of
entries, overlapping ranges (deduplicated, ordered), and the failure cases (reversed range,
garbage token, negative number) asserting the offending token appears in the error message.

---

### 7. Implement the write-back API

**Type**: WRITE
**Output**: `record_parameters(...)`, `record_comments(...)`, `record_sources(...)` writing
only program-owned content
**Depends on**: 3

Implement the three write-back entry points on the loaded container: `record_parameters`
fills/updates `[postprocess]`; `record_comments` fills/updates `[comments]`; `record_sources`
records heuristic classifications into `[sources]` only for keys not already set by the user.
All three mutate the preserved `tomlkit` document and save atomically (write temp file, rename)
so a crash never truncates the toml. Untouched sections — including their comments and
formatting — must survive byte-identical. If no toml existed, create one containing only the
recorded sections plus a generated-by header comment.

---

### 8. Tests for write-back round-trip

**Type**: TEST
**Output**: round-trip tests passing
**Depends on**: 7

Cases: load a toml with user comments and unusual-but-valid formatting, `record_parameters`,
reload → `[postprocess]` correct AND the bytes of all user sections unchanged (compare section
text, not just values); `record_comments` twice → second call updates, does not duplicate;
`record_sources` does not override an explicit user-set source type; write-back with no
pre-existing toml creates a valid file; simulated crash (temp file left behind) does not corrupt
the original.

---

### 9. Implement precedence: experiment toml over policy.toml

**Type**: WRITE
**Output**: a resolver returning effective decision values from toml + policy + defaults
**Depends on**: 3

Implement the precedence rule from the PRD: for each decision parameter (weight_threshold,
polswap, polconvert, onebit, refant, pipeline/distribution backend, scans), the experiment toml
value wins; else the `Policy` value; else None (meaning "ask in interactive mode / pause in
batch"). Build this as a small function combining the `ExperimentToml` container with the
existing `Policy` object (reuse `Policy.merge` semantics rather than duplicating them). The
resolver also reports which parameters remain unresolved, superseding `Policy.requires_input`
for callers that have a toml.

---

### 10. Tests for precedence

**Type**: TEST
**Output**: precedence tests passing
**Depends on**: 9

Extend the patterns of `tests/test_policy.py`: same key in both files → toml wins; key only in
policy → policy used; key in neither → reported unresolved; empty-list vs absent distinction
(an explicit empty polswap list in the toml means "none", not "unset"). Guard the existing
policy-only behaviour: all current `test_policy.py` tests keep passing unchanged.

---

### 11. Wire into Experiment and `postprocess info`

**Type**: WRITE
**Output**: `postprocess info` in a directory with only `EXPNAME.toml` shows toml-sourced values
**Depends on**: 7, 9

Connect `experiment_state` to the existing objects without changing the JSON checkpoint: when
an `EXPNAME.toml` exists in the experiment directory, load it during experiment
setup/`postprocess info` and surface its values (sources, PI, scans, resolved parameters) in
the info output, marking their origin. `Experiment.store`/`load` (JSON) remain untouched; the
toml container is attached to the Experiment at runtime, not serialized into the JSON. Follow
the existing `main.py` subcommand structure; keep changes to `main.py` minimal and delegate
formatting to `experiment_state`.

---

### 12. End-to-end fixture test and full-suite check

**Type**: TEST
**Output**: e2e test passing; entire existing test suite green
**Depends on**: 11

Add an end-to-end test: `tmp_path` experiment directory containing only a complete
`EXPNAME.toml` (copy of the approved example) → build the experiment context → assert sources,
PI, scan selection, and resolved parameters match the file; then `record_parameters` and reload
to prove the full cycle. Finish by running the whole test suite and fixing any regression this
issue introduced (acceptance gate from the PRD: suite stays green after every phase).

---
