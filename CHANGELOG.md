
# Changelog of evn_postprocess

This is the change log for the different production (master) versions of the program.

## Version 3.1 -- robustness pass and the dashboard Progress tab

Fixed:
  - `process.tconvert()` crashed with `AttributeError: tconvert_in_eee` on every
    experiment: the attribute was removed with the eee workaround but the branch
    that reads it stayed. tConvert now always runs locally, one pass at a time,
    and the dead commented-out `_tconvert_pass_in_eee` helper is gone.
  - `retrieval.jive.get_vlba_antab()` raised `NotImplementedError` unconditionally
    (with unreachable code after it), so the `antab` step crashed on every global
    (EVN+VLBA) experiment. It now retrieves `{exp}cal.vlba` from ccs and the
    `/data/tsys/gbt_gains.key` gain curves into `antenna_files/`, and a missing file
    is a warning with the manual instructions, never a step failure. The decision to
    fetch them moved into `JiveRetriever.fetch_station_files`, where all retrieval
    knowledge belongs (the workflow no longer hard-codes VLBA station codes).
  - `Experiment.mode` defaulted to `regular` instead of `None` when a checkpoint had
    no stored mode, so a pre-Phase-2 state file silently pinned a support scientist
    to the `none` retrieval/distribution backends (no vlbeer, no archiving). `None`
    again means "unresolved" and triggers auto-detection. The `Mode.default` alias
    (indistinguishable from `Mode.regular` in a `str` enum) is removed.
  - `workflow._plan_steps()` (formerly inline in `run_workflow`) no longer mutates
    the module-level `_WORKFLOW_STEPS` template, so the done flags of one run cannot
    leak into the next one in the same process.
  - `Jplot._get_observed_subbands()` compared integer antenna indices against antenna
    *names*, so every comparison was false and every antenna was credited with every
    subband; the subband chosen for the amplitude/phase-vs-time plots was effectively
    always the first one. It now reads the autocorrelations per antenna index, and
    `_find_best_subband()` picks the subband observed by the most antennas.
  - The jplotter error hook called `sys.exit(-1)`, tearing down the whole run on any
    plotting error; it raises now, so the step fails cleanly and can be retried.
  - `postprocess -d DIR` derived the experiment name from the *calling* directory
    instead of `DIR`; the working directory is entered before the name is resolved.
  - `postprocess info` (and `dashboard`) went through the initialization path in a
    directory with no stored experiment: they retrieved the vex file and built the
    directory structure before failing. They now load the checkpoint through the single
    `_stored_experiment()` helper (shared with `edit`) and stop with a clear message —
    only `postprocess run` ever starts the post-processing of an experiment.
  - `Experiment.supsci` stayed `'jops'` for every run made under the shared account, and
    it is used well beyond the notifications: it picks the AIPS user number for the
    pipeline input file (`aips_userno.py`), signs the pipeline feedback page, and
    addresses the review messages. It is now resolved to a person when the experiment is
    loaded — `-jss` wins, otherwise the `support` field of the `.jex` file through the new
    `Retriever.fetch_support_scientist()` hook — and kept in the checkpoint, so an
    experiment already stored as `jops` heals itself on the next run. `-jss` also overrides
    the stored value again, which it did not on a recovered experiment.
  - `Experiment.print_blessed()` raised `IndexError` on an experiment with no correlator
    passes yet (the reduced-bandwidth block indexed `correlator_passes[0]` inside an
    `except AttributeError` that could not catch it), and tried to read the MS metadata
    before the MS existed. Both are guarded, and the block's two duplicated loops are now
    one. The `info` help no longer claims to write `notes.md` (the msops step does).
  - One shared definition of a Network Monitoring Experiment (`experiment.is_nme`:
    starts with N or F, but not FT) replaces the four divergent checks in
    `set_credentials`, `nme_report`, `pipeline_feedback`, `source_classify` and the
    JIVE distributor. FT fringe tests are no longer treated as NMEs, so they get
    archive credentials and source protection like any other experiment.

Added:
  - **A `verification` step** (new `verification` module), between `prearchive` and
    `distribute`: the last gate before anything leaves the working directory. Three
    read-only checks on the FITS-IDI files that `prearchive` has just written into —
    (1) the ANTAB `SYSTEM_TEMPERATURE`/`GAIN_CURVE` tables really are in the first file of
    every correlator pass; (2) `check-multipart-fits.py` over all FITS-IDI sets, failing
    when more than `MAX_LOSS_SECONDS` (10 s) went missing between consecutive chunks of a
    multi-part file (a smaller loss is one integration's rounding at a chunk boundary, and
    `gain`/`nZero` are warnings); (3) `compare-ms-idi.py` per pass, which reports every
    (baseline, source) that differs *anywhere* — on a healthy conversion most of them,
    because the weights are recomputed. The step applies the rule that actually matters:
    exposure seconds and visibility counts must be identical, a weight may differ by up to
    `MAX_WEIGHT_DIFF` (5 %), and it fails only on a pair that breaks that, on negative
    weights, or on a pair one of the two data sets does not have at all. A failure stops
    the run before `distribute`, so an incomplete data set cannot be archived; the full
    tool output is kept in `logs/verification.log`, and `postprocess run distribute` is the
    deliberate way past a failure whose findings have been checked. Also available on its
    own as `postprocess exec verify`.
  - **The experiment `.vix` is symlinked into `antenna_files/`** before `antab_editor.py`
    opens (`pipeline._link_vixfile`), so the vex schedule sits beside the `.log`/`.antabfs`
    files the editor works on and can be read without stepping back out of the directory.
    The link is relative, so it survives the experiment directory being moved; a missing
    vex, or the name already taken in `antenna_files/`, is a warning and never fails the
    antab step.
  - **`process.polswap_check()`**: a station that swapped its polarizations often fixes it
    partway through the run, and swapping the whole observation would then corrupt the half
    that was already correct. For every antenna marked for polswap, the per-scan lag SNRs
    already in `exp.lag_snr` say whether it was swapped in each scan; one change of state
    sets the start (or end) time of the swap, several changes or none fall back to the whole
    observation with a warning. The range is stored in `exp.pol_diagnostics` (no new state
    field) and applied through the `starttime`/`endtime` arguments `mstools.polswap` already
    accepted but nobody passed.
  - **A summary of what was spotted closes every finished run** (`review.final_summary()`,
    announced by `workflow._announce_completion()`): what was applied to the data
    (`review.msops_summary()`: weight threshold and flagged fraction, polswap with its time
    range, PolConvert, 1-bit scaling), the antennas that did not observe, what was found on
    the ones that did (missed time ranges, reduced bandwidth, unexpectedly low weights), and
    which station files arrived — the antennas with both `.log` and `.antabfs`, with only one
    of them, and with neither. A successful run left nobody watching the terminal, and this
    used to end with "Completed successfully"; the same Markdown now goes to the terminal
    panel and to the chat.
  - **`make_lis -m SRC`** is added automatically when the vex shows more than
    `MAX_PHASE_CENTERS` (5) phase centres in a scan, so the calibrator data stay in a single
    pass instead of being duplicated in every one of them.
  - **One shape for every operator message** (`comms.operator_message()`): they all start
    with `**Processing of EXPNAME**`, then what happened, then a `**What is needed:**`
    section with the actions expected — the same words the terminal prints (the review
    pause now writes its instructions once and renders them in the panel through
    `rich.markdown.Markdown`, so the two channels cannot drift). Mattermost posts the body
    as it is, instead of repeating the experiment name in a `###` heading on top of it.
  - **`workflow.StepFailed`**: a step that cannot continue without the operator says so in
    its own words, which reach the terminal, the desktop notification and the chat. The
    manual-intervention stops of the `antab` step (station files that could not be
    retrieved, a uvflg needing hand work, `antab_editor.py` to be run manually) and msops
    in batch mode used to `return False` and were announced as "the step reported a
    failure", saying nothing about what to do.
  - **A people directory in `comms.toml`**: `[[people]]` entries mapping each support
    scientist's username to their email address and Mattermost username. With `username`
    left empty (the shared `jops` account), `comms.recipient_for()` resolves the recipient
    per experiment from the support scientist assigned to it. Notifications are now wired
    only for `postprocess run`, so `info`/`dashboard` no longer load the comms config nor
    touch the servers.
  - **Progress tab** in the web dashboard: every workflow step in execution order,
    which of them have run, the next one to run, and a completion bar. Served by
    `/api/progress`, re-read from the checkpoint on each request and polled while
    visible, so a dashboard open in one terminal follows a run advancing in another.
  - `workflow.step_progress()` — the single source of the step list and its done
    flags, shared by `postprocess list` and the Progress tab.
  - Every step is now traced in `logs/logging_messages.log`: `_run_step()` logs the
    start, the outcome and the elapsed time of each one, and `run_workflow` opens the
    run with an identifying header (experiment, date, support scientist, version,
    mode, working directory). The `Jplot` progress messages and the previously
    stdout-only tracebacks (`traceback.print_exc()`) go through loguru too, so they
    reach the file as well as the terminal.
  - `Policy.pause_after` and `Policy.skip_archive` are honoured (they were declared
    but never read): the review pause after `postpipe` is now the default value of
    `pause_after`, not a hard-coded step name.

Changed:
  - **The per-correlator-pass concurrency is one shared, tunable pair of knobs**
    (`utils.MAX_PASS_WORKERS` / `utils.MAX_PASS_IO_WORKERS`, applied through
    `utils.pass_workers()`) instead of a worker count hard-coded separately at every call
    site — four `min(passes, 4)` in `msops`, `min(passes, 10)` and a flat `10` around
    `j2ms2`, and an unbounded `ProcessPoolExecutor()` for the pipeline. The default is
    `min(cpu_count, 16)` for the in-process casacore steps (`ysfocus`, `polswap`,
    `flag_weights`, `onebit`, MS metadata) and `10` for the ones that spawn a subprocess
    per pass (`j2ms2`, `getdata.pl`), where disk throughput is the ceiling rather than
    cores; `EVN_MAX_PASS_WORKERS` / `EVN_MAX_PASS_IO_WORKERS` override both. The old
    limits were written for far smaller machines and only ever bound on multi-phase-centre
    runs — a normal experiment has two to five passes, but such a run can have several
    hundred, and `msops` crawled through them four at a time. `pass_workers()` also never
    returns 0, which `min(len(passes) - 1, 10)` in `get_metadata_from_ms` could have done.
  - **A PolConvert `--compute` now says what it is trying, and what came of it.** Each
    attempt logs its full inputs before it runs (scan and source, the time range in a
    readable `17:00:00 - 17:05:00` instead of the raw AIPS 8-element list, refant, linants,
    excluded antennas, IFs, ref_idi, and the doweight/timeavg/chanavg being tried), and a
    run that completes has the per-IF fringe-SNR table `polconvert.py` prints at the end
    visible as it runs. `_run_polconvert_cli` no longer captures the child's output, it
    streams it, so the progress and that table appear live and in colour instead of being
    swallowed. When PolConvert dies before printing it, `_log_fringe_snr_table` renders the
    same table in-process from the `FRINGE.PEAKS` files it did write, by importing
    `evn_support.polconvert.print_fringe_snr_table` — cheap and safe, since that module
    imports PolConvert (and matplotlib, and Qt) only inside its own `main()`. A search that
    never converges used to leave a single failure line with no record of the combinations it
    went through; the rejected-attempt line also moved from DEBUG to INFO.
  - **PolConvert runs headless (`MPLBACKEND=Agg`), which fixes an `rc=127` that looked like a
    failed solution.** matplotlib's default backend is `qtagg`; loading it pulls in a PyQt5
    Qt5 plugin that aborts with `symbol lookup error: ... libqsvgicon.so: undefined symbol:
    _ZdlPvm` and takes the interpreter with it the moment PolConvert plots — before any
    solving happens, so every attempt failed with no FRINGE.PEAKS and no table to explain it.
    Reproduced exactly (rc=127 on the default backend, rc=0 under `Agg`), and `Agg` still
    writes the PNGs PolConvert produces. PolConvert stays in a **child process** deliberately:
    it segfaults often and a `SIGSEGV` cannot be caught by `try`/`except`, so running it
    in-process would end the whole post-processing rather than one attempt.
  - **Every antenna handed to PolConvert goes through `_pc_ants` (upper case).** PolConvert
    matches names against the FITS-IDI `ANTENNA`/`ARRAY_GEOMETRY` tables literally, and
    those hold `EF`/`JB`/`MC` while `exp.antennas` carries the mixed-case vex spelling. The
    input file was already upper-casing `linants`/`refant`/`exclude_ants`; the shared helper
    makes that the single place it happens, and the log lines now report the same upper-case
    names so what is reported cannot drift from what was passed.
  - **getdata.pl's "Ignoring"/"Skipping" notes are shown yellow instead of red.** getdata.pl
    reports through perl's `warn()`, so all of it lands on stderr and was coloured as an
    error; the "Ignoring `<job-list line>`" note alone reaches ~250k lines on a
    multi-phase-centre run (EM164B), and a screen of red reads like a failed step. The
    `stderr_warn_re` hook the getdata call already used was only matching "warning", so
    `process._GETDATA_WARN_RE` now also covers the `Ignoring`/`Skipping` prefixes. Genuine
    failures ("Could not open ...", perl `die` messages, scp errors) match none of them and
    stay red. No change was needed for j2ms2 or tConvert: their informational output
    (including j2ms2's "**** WARNING: SYSTEM IS IN BYPASS MODE!" banner and tConvert's
    `UV_TABLE:` lines) goes to stdout and was never red, and their stderr carries only real
    failures.
  - `utils.shell_command(..., echo=False)` now also drops the `> command` banner and the
    "Logging output to ..." line from INFO to DEBUG. "Quietly" has to cover the banner too:
    the callers that ask for it run several commands at once behind a progress bar, and a
    banner per command would scroll it away. The command is still recorded in
    `logs/commands.sh` and heads its own log file, so nothing is lost.
  - **`tconvert` now converts the correlator passes concurrently**, under the same ceiling
    as `j2ms2` (`MAX_PASS_IO_WORKERS`); it was the last step still running them one after
    the other. The reason it was serial was readability, not resources — tConvert is
    verbose about its progress and several of those streams interleaved on one terminal are
    unreadable — so the live output is now kept only while a single pass is converting, and
    from two upwards each pass writes to its own `logs/tconvert.log` sibling through the
    existing `utils.open_unique_log` (whose docstring already described `process.tconvert`
    as a parallel caller). The chunk sizes, which is where the "does this still fit on
    disk" check lives, are resolved for every pass before any conversion starts, so a pass
    that does not fit stops the step outright instead of raising out of a worker with the
    other conversions already running. Past `_TCONVERT_PROGRESS_MIN_PASSES` (5) passes a
    Rich progress bar replaces the silence the muted output would otherwise leave, counting
    the passes done with an elapsed/remaining estimate; and one pass failing no longer
    abandons the rest — every failure is collected and they are all named together at the
    end, the step then reporting a clean failure instead of a traceback from the first
    future that raised.
  - `run_workflow()` split into `_plan_steps()`, `_run_step()` and `_review_pause()`;
    `main()` split into `_build_parser()`, `_load_experiment()`, `_apply_cli_options()`,
    `_print_info()` and `_run()`.
  - `workflow.msops()` runs its MS operations sequentially and short-circuits (it used
    the eager bitwise `&`, which kept operating on a half-corrected MS after a failure),
    in the order of the post-processing guide: ysfocus, flag_weights, polswap, onebit.
    `pipelines.aips.collect()` short-circuits likewise.
  - The `postprocess run` step list and the `postprocess edit` field list are generated
    from the workflow tables, so the help can no longer name steps that do not exist
    (it listed `init` and `archive`, and omitted `lisfiles`).
  - `-a/--no-archive` stores its value as `args.archive` instead of an inverted
    `args.no_archive`.
  - Removed: `workflow._signal_pause()` (never called), `workflow.create_folder_structure()`
    (a thin alias of `inputs.create_folder_structure`), `workflow._log_file_path()`,
    `Experiment.write_log_file()` (it wrote a stale jplotter cheat-sheet into the loguru
    file; replaced by `Experiment.log_header()`), and the `plotting` demo `__main__` block.
  - Fixed the stale comment in `process.protect_experiment_files` claiming both `-p source`
    and `-p pipe` are applied; only `-p source` is (commit 4f47809).
  - `comms.toml`'s `from_address = ""` (present but empty) left the From header empty
    instead of falling back to the recipient, as its own comment documented.
  - `casatasks` is imported lazily, inside the one function that uses it. It only loads
    after casacore does, so the module-level import made the whole package unimportable
    whenever that ordering did not hold (it does not, outside pytest, in the current
    environment). The unreachable `if casatasks is None` guard is gone with it.
  - Removed the unused `_POLCONVERT_CHANAVG`/`_POLCONVERT_TIMEAVG_S`/`_POLCONVERT_SOLVE_WEIGHT`
    constants (the search tuples are what the code reads) and the `NoWgt`/`ScanNo`/`Version`
    leftovers of the original standardplots script.



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
