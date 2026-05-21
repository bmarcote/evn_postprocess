
# Changelog of evn_postprocess

This is the change log for the different production (master) versions of the program.


## Version 2.0 -- batch-ready refactor

Major refactor that turns the program into something a queue/scheduler can run
unattended without losing the existing interactive supsci flow. **Existing
`*.json` state files keep loading without intervention** thanks to a small
schema-migration step.

Added:
  - `evn_postprocess.policy.Policy` dataclass capturing every decision that
    used to be asked interactively (`weight_threshold`, `polswap`, `polconvert`,
    `onebit`, `refant`, `pause_after`, `skip_archive`, `batch`).
  - `evn_postprocess.tools` adapter that resolves external binaries via env
    vars, `computers.toml`, or `$PATH` and runs them with explicit `cwd` and
    list-form arguments (no more shell quoting bugs).
  - `dialog.PolicyDriven` and `dialog.make_dialog(batch=...)` factory: in batch
    mode every dialog is replaced by direct reads from `exp.policy`.
  - `dialog.BatchInteractionError` raised when batch mode hits a missing
    decision; the runner catches it, marks the step as needs-review, and
    writes a `REVIEW_REQUIRED` marker file so a queue system can detect the
    pause without parsing logs.
  - `--batch` and `--policy FILE` CLI flags; `workflow.set_batch_mode()`,
    `workflow._signal_pause()`, `workflow._review_flag_path/_write_review_flag/
    _clear_review_flag()` helpers.
  - JSON schema versioning (`Experiment.SCHEMA_VERSION = 2`) plus a tiny
    `_migrate_experiment_dict()` helper that converts unversioned (v1) state
    files to v2 on load.
  - `utils.format_remote_path()` replacing `eval(f"f'…'")` for the `vlbeer`
    and `ccs` server paths in `computers.toml`.
  - SSH/SCP timeouts, retries with backoff, and `BatchMode=yes` on every
    OpenSSH invocation (`utils.scp/ssh/remote_file_exists/grep_remote_file`).
    Tunable via the `EVN_SSH_TIMEOUT_S`, `EVN_SCP_TIMEOUT_S`,
    `EVN_SSH_RETRIES`, `EVN_SSH_BACKOFF_S` env vars.
  - 53 new tests covering Policy, atomic store, schema migration, batch
    dialog, batch workflow helpers, format_remote_path, tools resolution,
    plus regression tests for every Stage-B bug.

Changed:
  - `Experiment.store()` is now atomic (`*.tmp` + `os.replace`).
  - `notify()` is a no-op when stderr is not a TTY (avoids polluting batch logs).
  - Dependencies: replaced the obsolete `pyrap` PyPI package with
    `python-casacore`; dropped the `evn_support` external dependency in
    favour of the existing local `find_idi_with_time` script.
  - `plotting.py` imports `jiveplot` lazily so the rest of the package can
    import in environments where the plotting stack is missing.
  - Workflow steps short-circuit on failure (`if not op(): return False`) instead
    of running the bitwise `&` chain that always evaluated every operand.
  - `pipeline.run_antab_editor()` now returns `bool` (was `Optional[bool]`).
  - Log file lives at `exp.dirs.logs / "post_processing.log"` (was hardcoded
    to the `eee` server root).
  - File and terminal log sinks are added in separate try/excepts so a file
    permission error no longer silences terminal logging.
  - `pipeline.create_input_file()` reads the input template once and reuses
    the contents for every pass; the unguarded `.uvflg` copy is now
    conditional on the unnumbered file actually existing.
  - PI letter generation: the polconvert / bandwidth-limitation / opacity
    paragraphs are independent again (the latter two were nested inside the
    polconvert block and silently dropped when no antenna needed PolConvert).
  - `lisfiles.get_passes_from_lisfiles()` and `get_lis_files()` now sort the
    `glob.glob` output so multi-pass numbering is deterministic.
  - `process.tconvert()` resolves the binary via `tools.resolve()` (was the
    hardcoded developer-only path).
  - `process._du_kbytes()` replaces the unsafe `subprocess.run("du -s …")`
    one-liner with a typed helper that returns 0 on parse failure.
  - `Path.glob(...)` results are materialised before truthiness checks
    (`workflow.antfiles`).

Fixed:
  - `pipeline.run_antab_editor` returned `None`, which the workflow treated as a
    failure even though the editor exited successfully.
  - Log warnings used plain strings with `{name}`-style placeholders (no `f`
    prefix) in `io.get_vlbeer_sched_files`, `pipeline.get_files_from_vlbeer`.
  - `logger.debug('# Running the pipeline...', True)` typo (loguru rejects
    the second positional).
  - Duplicate `parse_masterprojects` implementations in `io.py` and
    `experiment.py`; `io.parse_masterprojects` is now a re-export of the
    canonical `experiment.parse_masterprojects`.
  - Several `try`/`finally` blocks (`pipeline.run_pipeline`, `pipeline_feedback`,
    `pipeline.archive`, `pipeline.ampcal`) referenced `original_cwd` from a
    variable defined inside the `try` — `os.chdir(original_cwd)` could raise
    `NameError` and mask the real exception.
  - Removed the dead `Experiment.get_setup_from_ms()` method that called a
    non-existent `Antennas.add()`.
  - Test environment now collects: replaced the wrong `pyrap` PyPI package
    (which fails on Python 3.13) with `python-casacore`.

Removed:
  - The hardcoded `tConvert` path
    `/home/verkout/src/jive-casa/build-reftime_assert_fail/...` is gone.
  - `eval()` of TOML server-path strings.

Migration notes for operators:
  - Running the new build against an existing experiment directory works
    unchanged. The first `store()` call rewrites the JSON with
    `_schema_version = 2`.
  - Add a `tconvert` entry to your `computers.toml` (or set the
    `EVN_TCONVERT` env var) so the new resolver can find your binary.
  - To run unattended in a queue, write a `policy.toml` (see
    `evn_postprocess.policy.Policy` docstring for the schema) and call
    `postprocess --batch --policy /path/to/policy.toml run`. The run will
    stop with exit code 0 and a `REVIEW_REQUIRED` marker any time human
    review is needed (default: after `postpipe`).


## Version 0.4 -- 14 November 2019

First version that fully works for all steps than in the eee computer.

Fixed:
    - Several bugs across the entire program.
Known errors:
    - Log files show multiple repeated lines.


## Version 0.3 -- 13 November 2019

Changed:
    - Checklis is done after the manual modification of the .lis file. It repeats the check if user not happy.
Fixed:
    - Output line 'j2sm2' -> 'j2ms2'.
    - Output from 'r' command during standardplots in the default log file.
    - Construction of the touch credential auth file.
    - archive command was not recognized in the session. Changed to archive.pl.
    - Wrong experiment name use when getting lis/vix files and getdata for e-EVN that are not the master name.
    - Bad parsing of the experiment names from ccs MASTER_PROJECTS.LIS in e-EVN experiments.
Added:
Deprecated:
Removed:
Known errors:
    - Log files show multiple repeated lines.

## Version 0.2 -- 7 November 2019

First real test for the eee machine related part.
