"""Regression tests for individually-discovered Stage-B bugs.

Each test in this module pins a single small fix so a future refactor cannot
silently re-introduce the bug. They are deliberately separate from the
behavioural suites in the other ``test_*.py`` files.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

from evn_postprocess import lisfiles, pipeline, experiment, io


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
