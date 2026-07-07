# Policy File Reference

A `policy.toml` file provides all decisions for unattended (batch) operation. It
sits *beneath* the [experiment toml](experiment-toml.md): when both define the same
parameter, the experiment toml always wins — see
[Parameter precedence](experiment-toml.md#parameter-precedence).

## Schema

```toml
# Required in batch mode: weight flagging threshold (0.0 to 1.0)
weight_threshold = 0.85

# Antenna lists (two-letter codes). Use [] for empty.
polswap    = ["Wb"]
polconvert = ["Kt"]
onebit     = []

# Reference antenna override (optional).
refant = ["Ef"]

# Steps after which the pipeline should pause for review.
# Default: ["postpipe"]
pause_after = ["postpipe"]

# Whether to skip the archive step entirely.
skip_archive = false

# Mark this as a batch run (enables PolicyDriven dialog).
batch = true
```

## Field descriptions

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `weight_threshold` | float | Yes (batch) | Threshold for `flag_weights`. Visibilities with weight below this value are flagged. |
| `polswap` | list[str] | Yes | Antennas requiring polarisation swap. |
| `polconvert` | list[str] | Yes | Linear-pol antennas requiring PolConvert. |
| `onebit` | list[str] | Yes | Antennas that recorded with 1-bit sampling. |
| `refant` | list[str] | No | Override reference antenna selection. |
| `pause_after` | list[str] | No | Step names that trigger a review pause. |
| `skip_archive` | bool | No | If true, skip the final archive step. |
| `batch` | bool | No | Marks this as a batch-mode policy. |

## Loading

The policy is loaded via the `--policy` CLI flag:

```bash
postprocess --policy /path/to/policy.toml --batch run
```

It is stored in the experiment state (`exp.policy`) and persists across resumes.

## Interaction with PolicyDriven dialog

When batch mode is enabled, the `dialog.PolicyDriven` class reads values from `exp.policy` instead of prompting the user. If a required field is missing, a `BatchInteractionError` is raised and the pipeline pauses with a `REVIEW_REQUIRED` marker.
