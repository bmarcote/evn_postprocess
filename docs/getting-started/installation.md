# Installation

## Requirements

- **Python ≥ 3.13**
- **python-casacore** — Required for Measurement Set access (linked against casacore libraries).
- **Ghostscript (`gs`)** — Used to convert PostScript plots to PNG.
- External binaries (only needed for the `jive`/`aips` backends; not required for a
  fully local `--mode regular` run): `j2ms2`, `tConvert`,
  `EVN.py`, `archive.pl`, `antab_editor.py` (resolved at runtime, see
  [External Tools](../guide/tools.md)).

!!! note "Standalone / non-JIVE use"
    The core package (`.vex`/`.lis` parsing, MS operations via `mstools`,
    experiment-toml handling) only needs `python-casacore` and pure-Python
    dependencies. JIVE-specific tooling (ssh access, AIPS/ParselTongue, MySQL
    client) is only imported when the corresponding backend (`jive`, `aips`) is
    actually selected — see [Operating Modes](../guide/modes.md).

## Install with pip

```bash
pip install evn-postprocess
```

Or install from the repository in development mode:

```bash
git clone https://github.com/bmarcote/evn_postprocess.git
cd evn_postprocess
pip install -e .
```

## Install with uv

```bash
uv add evn-postprocess
```

Or for development:

```bash
git clone https://github.com/bmarcote/evn_postprocess.git
cd evn_postprocess
uv sync
```

## Optional dependencies

### Documentation

```bash
pip install evn-postprocess[docs]
# or
uv add --dev zensical mkdocstrings-python
```

### Testing

```bash
pip install evn-postprocess[test]
# or
uv add --dev pytest pytest-mock
```

### Source-classification catalogue lookup

```bash
pip install evn-postprocess[catalogs]
```

Optional; without it, [heuristic source classification](../guide/source-classification.md)
degrades gracefully to scan-statistics-only rules.

## Verifying the installation

```bash
postprocess --version
postprocess --help
```

## Server configuration

The pipeline connects to several servers (eee, ccs, pipe) via SSH. Server definitions live in a `computers.toml` file. See [Configuration](configuration.md) for details.
