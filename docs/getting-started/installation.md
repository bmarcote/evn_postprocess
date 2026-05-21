# Installation

## Requirements

- **Python ≥ 3.13**
- **python-casacore** — Required for Measurement Set access (linked against casacore libraries).
- **Ghostscript (`gs`)** — Used to convert PostScript plots to PNG.
- External binaries: `j2ms2`, `tConvert`, `EVN.py`, `feedback.pl`, `archive.pl` (resolved at runtime, see [External Tools](../guide/tools.md)).

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

## Verifying the installation

```bash
postprocess --version
postprocess --help
```

## Server configuration

The pipeline connects to several servers (eee, ccs, pipe) via SSH. Server definitions live in a `computers.toml` file. See [Configuration](configuration.md) for details.
