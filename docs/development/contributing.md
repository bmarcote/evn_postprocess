# Contributing

## Development setup

```bash
git clone https://github.com/bmarcote/evn_postprocess.git
cd evn_postprocess
uv sync --all-extras
```

## Running tests

```bash
uv run pytest tests/ -x -q
```

!!! note
    Tests that import `casacore` require `python-casacore` linked against working casacore libraries. On macOS, this may segfault if the libraries are not properly built for your architecture.

## Building documentation

```bash
uv run zensical serve
```

This starts a local preview server with live reload.

## Code style

- Python 3.13+ features are used (match statements, `X | Y` union types).
- Docstrings follow **Google style**.
- Imports at top of file, grouped: stdlib → third-party → local.
- Line length: 120 characters max.
- No global mutable state except explicit module-level flags with setter functions.

## Adding a new workflow step

1. Define the step function in `workflow.py` (signature: `def step_name(exp: Experiment) -> bool`).
2. Add a `Task(...)` entry to `_WORKFLOW_STEPS` in the appropriate position.
3. If the step requires user interaction, use `dialog.make_dialog(batch=is_batch_mode())`.
4. If the step should trigger a notification, call the comms helpers.
5. Add documentation in `docs/reference/steps.md`.

## Adding a new external tool

1. Add a wrapper function in `tools.py` following the existing pattern.
2. Document the `EVN_<TOOLNAME>` environment variable in `docs/reference/env-vars.md`.
3. Add it to the table in `docs/guide/tools.md`.

## Pull request checklist

- [ ] All existing tests pass.
- [ ] New functionality has tests (where casacore is not required).
- [ ] Docstrings added/updated.
- [ ] Documentation pages updated if user-facing behaviour changed.
- [ ] No hardcoded paths or credentials.
