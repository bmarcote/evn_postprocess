# External Tools

The pipeline relies on several external binaries (AIPS/ParselTongue-based, JIVE
in-house scripts, casacore tools). The `tools` module resolves their locations
flexibly so hardcoded paths are never needed, and runs them without a shell (no
argument-quoting bugs).

## Tool resolution order

For each external binary, `tools.resolve(name)` checks (in order):

1. An explicit `env_var` argument, if the call site provides one.
2. **`EVN_<NAME>`** environment variable, where `<NAME>` is the tool name
   uppercased with `.`/`-` replaced by `_` (e.g. `tConvert` → `EVN_TCONVERT`,
   `EVN.py` → `EVN_EVN_PY`, `antab_editor.py` → `EVN_ANTAB_EDITOR_PY`).
3. A **`computers.toml`** server-style entry whose name equals the tool name (its
   `path` is used directly).
4. **`$PATH`** (`shutil.which`).
5. A caller-provided default, if any — otherwise `ToolMissingError` is raised,
   naming everywhere that was searched.

## Tools wrapped by `tools.py`

| Tool | Auto env var | Purpose |
| --- | --- | --- |
| `tConvert` | `EVN_TCONVERT` | FITS-IDI ↔ MS conversion |
| `j2ms2` | `EVN_J2MS2` | Correlator output → Measurement Set |
| `EVN.py` | `EVN_EVN_PY` | EVN Pipeline execution |
| `feedback.pl` | `EVN_FEEDBACK_PL` | Legacy feedback-page script (superseded by the in-tree `feedback` module for the `aips` backend, kept as a resolvable name) |
| `archive.pl` | `EVN_ARCHIVE_PL` | Archive data to the EVN archive |
| `antab_editor.py` | `EVN_ANTAB_EDITOR_PY` | ANTAB file editing (manual GUI step) |

Other tools invoked during the workflow (`getdata.pl`, `uvflgall.sh`,
`polconvert.py`, `append_antab_idi.py`, `auth_pipe.py`, `ampcal.sh`, `jplotter`)
are called through `$PATH` via `utils.shell_command`/`subprocess`; see
[Workflow Steps & Local Tools](../reference/steps.md) for the exact command run at
each step.

## Example: overriding tConvert

```bash
export EVN_TCONVERT=/home/user/bin/tConvert-3.0
postprocess run tconvert
```

## computers.toml tool entries

```toml
[tConvert]
path = "/opt/evn/bin/tConvert"

[j2ms2]
path = "/opt/evn/bin/j2ms2"
```

## Error handling

When a tool cannot be found:

```text
ToolMissingError: Could not find 'tConvert' (looked at env EVN_TCONVERT,
computers.toml, and $PATH). Set the env var or add an entry to your
computers.toml.
```

## The in-tree `mstools` CLI

MS operations that used to be standalone scripts (`ysfocus.py`, `polswap.py`,
`flag_weights.py`, `scale1bit.py`) are now implemented in-process in the
`mstools` subpackage and run directly by the `msops` step — no external binary
call. The same operations are available as a standalone command for manual use:

```bash
mstools view {exp}.ms [--stats]
mstools run ysfocus {exp}.ms
mstools run polswap {exp}.ms {antenna} [-t1 START] [-t2 END]
mstools run flag_weights {exp}.ms {threshold} [--no-apply]
mstools run scale1bit {exp}.ms {antenna} [{antenna2} ...] [--undo]
```

Run `mstools -h` or `mstools run -h` for the full list of tools.
