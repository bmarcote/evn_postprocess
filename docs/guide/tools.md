# External Tools

The pipeline relies on several external binaries. The `tools` module resolves their locations flexibly so hardcoded paths are not needed.

## Tool resolution order

For each external binary, the resolver checks (in order):

1. **Environment variable** — `EVN_<TOOLNAME>` (e.g. `EVN_TCONVERT=/opt/bin/tConvert`).
2. **computers.toml entry** — A server-style entry with a `path` key matching the tool name.
3. **System $PATH** — Standard `which`-style lookup.

If none of these succeed, a `ToolMissingError` is raised with a clear message indicating what was searched.

## Supported tools

| Tool | Env var | Purpose |
| --- | --- | --- |
| `j2ms2` | `EVN_J2MS2` | JIVE correlator output → Measurement Set |
| `tConvert` | `EVN_TCONVERT` | FITS-IDI → MS conversion |
| `EVN.py` | `EVN_EVNPY` | EVN Pipeline execution |
| `feedback.pl` | `EVN_FEEDBACKPL` | Generate feedback page content |
| `archive.pl` | `EVN_ARCHIVEPL` | Archive data to the EVN archive |
| `antab_editor.py` | `EVN_ANTAB_EDITOR` | ANTAB file editing tool |

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
ToolMissingError: Could not locate 'tConvert'. Searched:
  1. EVN_TCONVERT environment variable (not set)
  2. computers.toml entry 'tConvert' (not found)
  3. System PATH (not found)
Please install tConvert or set EVN_TCONVERT to its path.
```
