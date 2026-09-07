
# Changelog of evn_postprocess

This is the change log for the different production (master) versions of the program.

## Unreleased

Fixed:
  - `Ms.get_msmetadata()` read the SPECTRAL_WINDOW `TOTAL_BANDWIDTH` column, which is
    the bandwidth of a *single* subband, and stored it as `FreqSetup.bandwidth`, the
    *total* one. Everything downstream then divided it by the number of subbands, so
    EY054 (8 x 32 MHz) was reported as `8 x 4-MHz subbands (32 MHz in total)` instead
    of `8 x 32-MHz subbands (256 MHz in total)`, and the frequency range was eight
    times too narrow. The per-subband value is now multiplied by the number of
    subbands when the setup is read. Note that experiments whose state was stored by
    an earlier version keep the wrong value until their MS metadata is read again.
  - `Ms.overview()` printed the bandwidth as `nspw x (bandwidth * nspw) subbands
    (total bandwidth of bandwidth)`: the two numbers were swapped and the per-subband
    one was multiplied instead of divided.
  - `verification.check_multipart()` failed the whole `verification` step on any
    experiment whose FITS-IDI was not split. `tConvert` only appends a sequence number
    when it has to write several chunks, so a pass below `chunk_size` becomes a single
    `{exp}_1_1.IDI`; `check-multipart-fits.py` identifies a chunk by that number
    (`([^/]+)\.IDI([0-9]+)(\.(.+))?$`), so an unnumbered name makes its regex return None
    and the tool dies with `AttributeError: 'NoneType' object has no attribute 'group'`
    and exit code 1. Passes with fewer than two chunks are now skipped, which is also the
    right answer on its own terms: a single file has no boundary to lose data across.
  - A placeholder the template carries but `piletter.build()` does not fill was only
    warned about and then left in the rendered letter, so a template one step ahead of the
    module (a half-applied upgrade) sent the PI a letter containing a literal
    `{extra_acknowledgments}`. Unfilled placeholders are now removed from the template
    before anything is substituted into it — before, so a dashboard comment that happens to
    contain braces is still passed through untouched.
  - The vex symlink placed in `antenna_files/` for `antab_editor.py` carried the name
    of `exp.vixfile`, which is the *uppercase* `{EXP}.vix` of the experiment root. It is
    now named after the lowercase experiment (`{exp}.vix`), which is what the editor
    looks for. The target is unchanged: still relative, still resolved through the root
    link to the real file.
  - `piletter._pass_line()` crashed with `KeyError` on every letter: the polarization
    label was looked up by the *length of the joined string* (`len('RR, LL')` -> 6)
    rather than by the number of polarizations. An unexpected count now falls back to
    'mixed' instead of raising.

Removed:
  - The "Station Feedback" panel printed at the end of a run ("Update the database with
    observed issues: /feedback in Mattermost, JIVE RedMine"). With it go
    `process.antenna_feedback()`, the `station-feedback` stage of the `jive` delivery and
    the `issues` workflow step, whose only content it was.

Changed:
  - The `checklis` step now says what is actually wrong with the .lis files, and stops the
    run through `StepFailed` (so the summary reaches the terminal, the desktop notification
    and the chat) instead of the bare "Issues found in .lis files. Please check the files.".
    The raw `checklis.py` output of every correlator pass is classified into skipped scans,
    duplicated data, and anything else, and reported as one summary per issue — "N .lis
    file(s): a.lis, b.lis, ..." (truncated after 8 names, since a multi-phase-center run can
    have dozens of passes) — saying that skipped scans may well be right but have to be
    double checked manually, that duplicated data MUST be fixed by hand, and asking the
    operator to verify the .lis file(s). Repeated .lis/MS/FITS-IDI names across passes are
    reported in the same summary instead of failing on the first one found. A `checklis.py`
    that cannot run at all is now an explicit error rather than an unexpected exception, and
    the per-pass output no longer interleaves in the terminal (the passes run in parallel):
    the full lines go to the debug log.
  - Once antab_editor.py is closed, the run asks in the terminal — behind its own rule and
    panel, like the review pause — whether to go on and run the EVN Pipeline or to stop
    there ("Enter" / "stop"). Stopping raises `StepPaused`, so the run ends cleanly (exit
    code 0) and `postprocess run` resumes it. Nothing is asked when the editor was never
    opened (the .antab files already existed: `antfiles` returns before reaching it) nor in
    batch mode, where the run simply continues.
  - The review pause (by default after `postpipe`) is far harder to miss and says what can
    be answered. The announcement is framed by rules and a "THE RUN IS WAITING FOR YOU"
    banner, and the terse `Review answer [Enter=finalize / STEP=re-run from STEP / quit]`
    prompt is preceded by a "How to answer" box listing every option with its reason —
    built from the step the run paused after, so after `postpipe` it names `postpipe`
    (redo the diagnostics, e.g. after running the pipeline by hand) and `pipeline` (redo
    the pipeline, e.g. after editing its input file). A wrong answer reprints the box.
    The chat notification carries the same options; the terminal shows them once.
    Deprecated step aliases are now accepted as an answer, like everywhere else.
  - The dashboard is much faster to load over the SSH tunnel it is normally viewed
    through. It served HTTP/1.0 without keep-alive, closing the connection after every
    response, and ran on a single-threaded `HTTPServer`: one page load (the HTML, five
    `/api/` calls and the plot images) meant ~9 fresh TCP connections through the tunnel,
    each paying a handshake plus an SSH channel setup, and all of them queued behind one
    another. It now sets `protocol_version = "HTTP/1.1"` (safe: every response already
    sends an accurate Content-Length) and uses `ThreadingHTTPServer`. Rendering itself was
    never the problem — every endpoint answers in under 10 ms on localhost.
  - The review pause no longer announces itself outside the terminal — neither the
    Mattermost message nor the desktop notification — when it comes straight after the
    pipeline dashboard (the `postpipe` step): that dashboard only returns once the operator
    closes it themselves, so they are sitting in front of the prompt and both are noise. The
    terminal announcement is unchanged, and both still go out in batch mode and when the
    pause follows any other step.
  - The `-auto-` and `-cross-` standardplots plot against frequency (`pt ampfreq` /
    `pt anpfreq`) instead of channel number. `Jplot.amp_chan_auto_plot()` and
    `Jplot.anp_chan_cross_plot()` are renamed to `amp_freq_auto_plot()` and
    `anp_freq_cross_plot()`.
  - The PI letter no longer mentions the weight-flagging threshold or the percentage
    of visibilities it removed: that is post-processing done at JIVE, not a
    correlation parameter the PI needs to be told about.
  - The 'Remarks on individual stations' section of the PI letter opens with a bullet
    listing, comma-separated, the antennas that observed.
  - The per-station lines of the PI letter no longer carry the dashboard's traffic-light
    status as a suffix: neither ' (minor issues)' nor ' (could not observe)'. The note the
    support scientist wrote is the whole message — the status is ours to triage with, and
    restating it in jargon only repeats what the note already says in words. The
    `piletter.STATUS_LABELS` mapping is gone with it.
  - The acknowledgment section of the PI letter is dynamic: an array containing any
    e-MERLIN out-station (De, Da, Pi, Kn) now carries e-MERLIN's own acknowledgment as a
    second quote, after the EVN one and introduced by its own sentence. Only the
    out-stations that observed count, and Jb alone does not trigger it (Jodrell Bank
    observes with the EVN in its own right). See `piletter.EMERLIN_ANTENNAS`.
  - The correlation-parameters bullets only name the FITS-IDI files when the
    experiment has more than one correlator pass: with a single pass there is nothing
    to tell apart, and the PI sees the file names in the archive anyway.
  - The plain-text letter (`.piletter`, `.piletter_auth` and the text part of the
    `.eml`) is no longer hard-wrapped at 78 columns: each paragraph, bullet and quote
    is one long line, left for the reader's mail client or editor to reflow. The
    `piletter.TEXT_WIDTH` constant and the `_fill()` helper are gone with it. (The
    `.eml` still carries quoted-printable soft breaks on the wire, as the mail format
    requires, but they decode back to the unwrapped paragraph.)

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
  - A station that observed with reduced bandwidth was classified as a `minor` finding
    ("issues reported" in the dashboard, "(minor issues)" in the PI letter). Observing
    fewer subbands than the experiment setup is a scheduling choice, not a fault: the
    status now stays `success` ("no problem") and only the informational note is kept.
    Reduced bandwidth still shows up in the station summary, the final summary and the
    dashboard Comments tab, and a station that also missed time is still `minor`.
  - `pipeline.run_pipeline()` ran the per-pass `EVN.py` in an **unbounded**
    `ProcessPoolExecutor`: on a multi-phase-centre run with many pipelined passes, that is
    one AIPS session per pass, all at once. Each pass still gets its own process (an
    EVN.py run drags a whole AIPS/ParselTongue session behind it, which must not be shared),
    but the pool is now bounded by `utils.pass_workers()` with the I/O ceiling, like every
    other per-pass pool. The single-pass case returns early instead of falling through the
    parallel branch.
  - `pytest` now runs against the working tree (`pythonpath = ["src"]` in
    `[tool.pytest.ini_options]`). Without it the suite imported whatever
    `evn_postprocess` the environment had installed — in the `pyjops` conda env that is a
    non-editable copy of another checkout — so the tests could silently pass against code
    that is not the one being edited.
  - Exceptions re-raised inside an `except` block now chain (`raise ... from e`) in
    `experiment.py` (VEX parsing, checkpoint loading), `utils.remote_file_exists` and
    `mstools.operations`, so the original traceback survives in the report. The `ssh`
    retry loop no longer keeps the timeout in a variable to re-raise it by hand.
  - The antenna_files vex-link regression test still required the link target to be
    absolute, while the link has deliberately been relative since it started being
    recomputed against `antenna_files/` (so the experiment directory can be moved). It now
    asserts what the regression is actually about — the link resolves from that directory
    and is not the root link's target — plus the move it was made relative for.

Added:
  - The **PI letter is generated** from `templates/piletter.md.template` for every
    experiment, instead of being retrieved and patched in place. It belongs to the JIVE
    delivery: it lives in `distribution/piletter.py` and is reached through the new
    `Distributor.prepare_letter()` / `send_letter()` interface, so the modes that archive
    nowhere (`regular`, `sweeps`) write no letter at all and their review pause does not
    mention one. `process.update_piletter` / `create_piletter_auth` / `send_letters` are
    gone from `process.py`. The letter is filled with the experiment metadata (correlation
    parameters per pass, weight flagging, PolConvert,
    bandwidth limitations, opacity) and with what the support scientist writes in the
    dashboard Comments tab, and it is written again before the delivery so the letter
    that is sent is the reviewed one (the previous version is kept as `.bak`).
    Four products, all from the same content (`evn_postprocess.distribution.piletter`): the plain-text
    `{exp}.piletter` (archived, no credentials), `{exp}.piletter_auth` (with them), a
    styled `{exp}.piletter.html` (hyperlinks everywhere, the EVN acknowledgment in grey
    italics), and `{exp}.piletter.eml` — a draft that a local mail client opens with the
    recipients, the subject and the formatting already in place. The archived copy carries
    no `To:`/`Cc:` header, so the PI's email address is not published with the data.
  - The finished letter is posted to the operator's chat as Markdown, with the `.eml` and
    `.html` attached: no mail credentials live in this program, so sending stays a manual
    step, but nothing has to be retyped. Any file type can now be attached to a comms
    message (it was limited to `.png` plots).

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
