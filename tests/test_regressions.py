"""Regression tests for individually-discovered Stage-B bugs.

Each test in this module pins a single small fix so a future refactor cannot
silently re-introduce the bug. They are deliberately separate from the
behavioural suites in the other ``test_*.py`` files.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

from concurrent.futures import ThreadPoolExecutor

from evn_postprocess import lisfiles, pipeline, experiment, io, workflow, utils


class TestLisFileOrderingDeterministic:
    """`get_passes_from_lisfiles` and `get_lis_files` must sort the glob output.

    Otherwise the assignment of ``IDI`` filenames and the ``pipeline`` flag depends
    on the OS-dependent order of ``glob.glob`` which can flip between machines.
    """

    def test_get_passes_from_lisfiles_uses_sorted_globs(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        # Three .lis files written in non-alphabetical order.
        for name in ("testexp3.lis", "testexp1.lis", "testexp2.lis"):
            Path(name).write_text("dummy line that is not a header\n")

        exp = Mock(spec=experiment.Experiment)
        exp.expname = "testexp"
        exp.correlator_passes = []

        # We capture the args_list as it is submitted to ThreadPoolExecutor.map.
        # That is the deterministic, pre-thread-pool ordering we care about \u2014
        # the per-thread call order would be racy and is not what the bug fix
        # was about.
        captured: list = []

        class _FakeExecutor:
            def __enter__(self_):
                return self_

            def __exit__(self_, *a):
                return False

            def map(self_, fn, args_list):
                captured.extend(args_list)
                return [None] * len(captured)

        # `lisfiles.get_passes_from_lisfiles` does ``from . import process`` lazily,
        # so we patch the canonical attribute on the process module rather than
        # trying to patch a non-existent ``lisfiles.process``.
        with patch("evn_postprocess.lisfiles.ThreadPoolExecutor", return_value=_FakeExecutor()), \
             patch("evn_postprocess.process.aggregate_sources_from_passes"), \
             patch.object(exp, "store"):
            lisfiles.get_passes_from_lisfiles(exp)

        # args_list tuples are (i, filename, expname, thereis_line, i_lines_done).
        seen_filenames = [tup[1] for tup in captured]
        assert seen_filenames == sorted(seen_filenames), (
            f"glob output was not sorted: {seen_filenames}"
        )


class TestRunAntabEditorReturnsTrueOnSuccess:
    """Regression for the bug where antab_editor returned None and the workflow
    treated successful runs as failures.
    """

    def test_returns_true(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        # Build the minimum directory shape the function expects.
        pipe_temp = tmp_path / "temp"
        pipe_temp.mkdir()
        # An empty .lis (without "_line") so the non-line branch runs.
        (pipe_temp / "testexp.lis").write_text("")

        exp = Mock(spec=experiment.Experiment)
        exp.expname = "testexp"
        exp.eEVNname = None
        exp.dirs = Mock()
        exp.dirs.pipe_temp = pipe_temp
        exp.antennas = []  # no missing-antab warning

        with patch("evn_postprocess.pipeline.utils.shell_command") as mock_shell:
            mock_shell.return_value = ""
            assert pipeline.run_antab_editor(exp) is True


class TestRunAntabEditoreEVNAssociatesOtherExperiments:
    """The antab step of an e-EVN run must invoke antab_editor.py once from the main
    experiment, passing the other experiments of the session via ``-a`` together with
    the path to their FITS-IDI files, and only once those FITS-IDI files exist.
    """

    def _make_exp(self, tmp_path: Path, others: list[str]):
        # Mirror the real layout: <base>/EZ041A/antenna_files with siblings at <base>/<EXP>,
        # so ../../<EXP>/ (relative to antenna_files) points at the sibling experiment dir.
        pipe_temp = tmp_path / "EZ041A" / "antenna_files"
        pipe_temp.mkdir(parents=True)
        (pipe_temp / "ez041a.lis").write_text("")  # non-line branch

        exp = Mock(spec=experiment.Experiment)
        exp.expname = "EZ041A"
        exp.eEVNname = "EZ041A"  # main experiment of the e-EVN run
        exp.eEVN_experiments = Mock(return_value=["EZ041A", *others])
        exp.dirs = Mock()
        exp.dirs.pipe_temp = pipe_temp
        exp.antennas = []
        return exp

    def test_builds_associated_command_when_idi_present(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = self._make_exp(tmp_path, ["ER057C"])
        # FITS-IDI for the sibling must exist (../../ER057C/ relative to antenna_files).
        sibling = tmp_path / "ER057C"
        sibling.mkdir()
        (sibling / "er057c_1_1.IDI1").write_text("")

        with patch("evn_postprocess.pipeline.utils.shell_command") as mock_shell:
            mock_shell.return_value = ""
            assert pipeline.run_antab_editor(exp) is True

        args = mock_shell.call_args[0][1]
        assert args == ["-e", "ez041a", "-a", "ER057C", "-p", "1", "-f", "..", "../../ER057C/"]

    def test_returns_false_when_sibling_idi_missing(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = self._make_exp(tmp_path, ["ER057C"])
        # No FITS-IDI created for the sibling.

        with patch("evn_postprocess.pipeline.utils.shell_command") as mock_shell:
            assert pipeline.run_antab_editor(exp) is False
            mock_shell.assert_not_called()


class TestParseMasterprojectsReExportedFromIO:
    """`io.parse_masterprojects` must be the same callable as
    `experiment.parse_masterprojects` so legacy code paths continue to work
    after the de-duplication.
    """

    def test_reexport_is_canonical(self):
        assert io.parse_masterprojects is experiment.parse_masterprojects


class TestPipelineUvflgUnnumberedGuarded:
    """`pipeline.create_input_file` must NOT crash when only numbered ``.uvflg``
    files exist. Originally an unguarded ``shutil.copy(testexp.uvflg, …)`` ran
    even when the unnumbered file was absent.
    """

    def test_numbered_uvflg_only_does_not_crash(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        pipe_temp = tmp_path / "temp"
        pipe_in = tmp_path / "in"
        pipe_temp.mkdir()
        pipe_in.mkdir()

        # Numbered files only, simulating a multi-pass setup that is already split.
        (pipe_in / "testexp_1.antab").write_text("a")
        (pipe_in / "testexp_2.antab").write_text("a")
        (pipe_in / "testexp_1.uvflg").write_text("u")
        (pipe_in / "testexp_2.uvflg").write_text("u")

        exp = Mock(spec=experiment.Experiment)
        exp.expname = "testexp"
        exp.supsci = "tester"
        exp.refant = ["Ef"]
        exp.multi_phase_center = False
        exp.dirs = Mock()
        exp.dirs.pipe_temp = pipe_temp
        exp.dirs.pipe_in = pipe_in

        # Two pipeline passes.
        passes = []
        for i in (1, 2):
            p = Mock(spec=experiment.CorrelatorPass)
            p.pipeline = True
            p.sources = Mock()
            p.sources.fringefinder = ["S"]
            p.sources.target = ["T"]
            p.sources.calibrator = ["C"]
            p.sources.calibrator_for_target = Mock(return_value="C")
            passes.append(p)
        exp.correlator_passes = passes

        with patch("subprocess.run") as mock_run, \
             patch("evn_postprocess.pipeline.resources.files") as mock_resources:
            mock_run.return_value.stdout = "100"
            mock_resources.return_value.joinpath.return_value.read_text.return_value = (
                "{expname} {userno} {refant}"
            )
            # The fix means no FileNotFoundError, even though testexp.uvflg does not exist.
            assert pipeline.create_input_file(exp) is True


class TestPipelinePrimaryBeamCommented:
    """``doprimarybeam`` / ``setup_station`` must be commented out (``#``-prefixed) for a
    single-pass experiment and only left active for a multi-phase-center (multi-pass) one.
    Renders the *real* template so a template/replacement drift is caught.
    """

    def _exp(self, tmp_path: Path, n_passes: int, multi: bool):
        pipe_in = tmp_path / "in"
        pipe_temp = tmp_path / "temp"
        pipe_in.mkdir()
        pipe_temp.mkdir()
        exp = Mock(spec=experiment.Experiment)
        exp.expname = "testexp"
        exp.supsci = "tester"
        exp.refant = ["Ef"]
        exp.multi_phase_center = multi
        exp.dirs = Mock()
        exp.dirs.pipe_in = pipe_in
        exp.dirs.pipe_temp = pipe_temp
        passes = []
        for _ in range(n_passes):
            p = Mock(spec=experiment.CorrelatorPass)
            p.pipeline = True
            p.sources = Mock()
            p.sources.fringefinder = ["3C84"]
            p.sources.target = ["T"]
            p.sources.calibrator = ["C"]
            p.sources.calibrator_for_target = Mock(return_value="C")
            passes.append(p)
        exp.correlator_passes = passes
        return exp, pipe_in

    def test_single_pass_comments_out_primarybeam(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp, pipe_in = self._exp(tmp_path, n_passes=1, multi=False)
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.stdout = "100"
            assert pipeline.create_input_file(exp) is True
        text = (pipe_in / "testexp.inp.txt").read_text()
        assert "#doprimarybeam =" in text
        assert "#setup_station =" in text
        # The active (uncommented) forms must be absent.
        assert "\ndoprimarybeam =" not in text
        assert "\nsetup_station =" not in text

    def test_multi_phase_center_keeps_primarybeam_active(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp, pipe_in = self._exp(tmp_path, n_passes=2, multi=True)
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.stdout = "100"
            assert pipeline.create_input_file(exp) is True
        text = (pipe_in / "testexp_1.inp.txt").read_text()
        assert "\ndoprimarybeam = 1" in text
        assert "\nsetup_station = Ef" in text
        assert "#doprimarybeam =" not in text


class TestAntennasLowWeightsProperty:
    """`Antennas.low_weights` centralises the heuristic that used to be copy-pasted in
    the msops dialog and the automatic-threshold helper. It flags antennas with less
    than 95% of the data in the first/last weight bin, or with nothing in the last bin,
    while skipping antennas without enough weight data to judge.
    """

    def test_flags_only_genuinely_low_antennas(self):
        normal = experiment.Antenna(name="Ef", weights=(95, 0, 0, 0, 0, 0, 5))
        low_middle = experiment.Antenna(name="Wb", weights=(0, 0, 0, 7, 0, 0, 0))
        empty_last_bin = experiment.Antenna(name="Tr", weights=(90, 0, 0, 0, 0, 0, 0))
        no_data = experiment.Antenna(name="Jb", weights=(0, 0, 0, 0, 0, 0, 0))
        too_short = experiment.Antenna(name="Mc", weights=(1, 2, 3))
        ants = experiment.Antennas([normal, low_middle, empty_last_bin, no_data, too_short])
        assert ants.low_weights == ["Wb", "Tr"]


class TestLowWeightWarningBeforeDashboard:
    """The unexpectedly-low-weight warning must be emitted *before* the standardplot
    dashboard is opened, so the operator knows to inspect the weight plots while the
    dashboard is up rather than only learning about it afterwards in the msops dialog.
    """

    def test_warning_logged_before_dashboard_opens(self, monkeypatch):
        from loguru import logger

        # One healthy antenna and one with unexpectedly low weights.
        normal = experiment.Antenna(name="Ef", weights=(95, 0, 0, 0, 0, 0, 5))
        low = experiment.Antenna(name="Wb", weights=(0, 0, 0, 7, 0, 0, 0))
        exp = Mock()
        exp.antennas = experiment.Antennas([normal, low])
        # A pass whose FITS-IDI file does not exist, so msops does not short-circuit.
        exp.correlator_passes = [Mock(fitsidifile="no_such_idi_prefix_xyz")]

        events: list[str] = []

        def _fake_dashboard(_exp):
            events.append("dashboard")
            return True

        def _sink(message):
            rec = message.record
            if rec["level"].name == "WARNING" and "low weights" in rec["message"]:
                events.append("warning")

        sink_id = logger.add(_sink, level="WARNING")

        monkeypatch.setattr(workflow, "_BATCH_MODE", False)
        monkeypatch.setattr(workflow, "_NOTIFIER", None)
        monkeypatch.setattr(workflow, "_auto_msops_available", lambda _e: False)
        monkeypatch.setattr(workflow.process, "open_standardplot_files", _fake_dashboard)
        # Interactive dialog: accept defaults and report success.
        fake_gui = Mock()
        fake_gui.askMSoperations.return_value = True
        monkeypatch.setattr(workflow.dialog, "make_dialog", lambda batch=False: fake_gui)
        # The downstream MS operations are not under test here.
        for fn in ("flag_weights", "ysfocus", "polswap", "onebit", "tconvert"):
            monkeypatch.setattr(workflow.process, fn, lambda _e: True)
        monkeypatch.setattr(workflow.process, "print_exp", lambda _e, _d: True)

        try:
            assert workflow.msops(exp) is True
        finally:
            logger.remove(sink_id)

        assert events == ["warning", "dashboard"], (
            f"expected the low-weight warning before the dashboard, got {events}"
        )


class TestShellCommandLogging:
    """`utils.shell_command(logfile=...)` must tee the tool output (getdata, j2ms2,
    tConvert) into ``logs/`` without ever overwriting an existing file, so each run
    is preserved as its own file (``tconvert.log``, ``tconvert-2.log``, ...).
    """

    def test_open_unique_log_never_overwrites(self, tmp_path: Path):
        base = tmp_path / "logs" / "tconvert.log"
        opened = []
        for _ in range(3):
            fh, path = utils.open_unique_log(base)
            fh.close()
            opened.append(path.name)
        assert opened == ["tconvert.log", "tconvert-2.log", "tconvert-3.log"]

    def test_logfile_captures_both_stdout_and_stderr(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        utils.shell_command("echo OUTLINE; echo ERRLINE 1>&2",
                            logfile=Path("logs") / "j2ms2.log")
        body = (tmp_path / "logs" / "j2ms2.log").read_text()
        # Combined output, plus the header/footer bookkeeping.
        assert "OUTLINE" in body and "ERRLINE" in body
        assert body.startswith("# command:")
        assert "exit code 0" in body
        # No ANSI colour codes leak into the file (those are terminal-only).
        assert "\033[" not in body

    def test_failure_records_exit_code_and_still_raises(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import pytest
        with pytest.raises(ValueError):
            utils.shell_command("exit 3", logfile=Path("logs") / "getdata.log")
        body = (tmp_path / "logs" / "getdata.log").read_text()
        assert "exit code 3" in body

    def test_parallel_passes_get_distinct_files(self, tmp_path: Path, monkeypatch):
        # j2ms2/tConvert run correlator passes concurrently; each must get its own
        # file even when they share the same base name (O_EXCL, no race/clobber).
        monkeypatch.chdir(tmp_path)

        def _one(_i):
            utils.shell_command("echo hi", logfile=Path("logs") / "tconvert.log")

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(_one, range(8)))

        files = sorted(p.name for p in (tmp_path / "logs").iterdir())
        assert len(files) == 8, files
        assert "tconvert.log" in files
