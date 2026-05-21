# Editing Metadata

The `postprocess edit` subcommand lets you modify experiment metadata after initialization.

## Usage

```bash
postprocess edit <field> [values...]
```

If `values` is omitted, the command lists available options for the field.

## Available fields

### refant

Override the reference antenna(s).

```bash
# List available antennas:
postprocess edit refant

# Set reference antenna:
postprocess edit refant Ef

# Multiple reference antennas (space-separated):
postprocess edit refant Ef Wb
```

### target

Mark a source as a target.

```bash
# List sources and their types:
postprocess edit target

# Set a source as target:
postprocess edit target J1234+5678
```

### phasecal

Mark a source as a phase calibrator.

```bash
postprocess edit phasecal J0900+1234
```

### fringefinder

Mark a source as a fringe-finder.

```bash
postprocess edit fringefinder 3C345
```

## Notes

- Changes are saved immediately to the experiment JSON.
- If you assign values before they are read during normal processing, they may be overwritten by the automated metadata retrieval.
- Source type changes propagate to all correlator passes.
