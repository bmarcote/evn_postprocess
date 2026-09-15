"""Focused unit tests for source-list fallback behavior.

Covers the new helpers that choose which sources to use for standardplots
and pipeline bandpass calibration when fringe finders are not present.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from evn_postprocess import experiment, pipeline, workflow
from evn_postprocess.pipeline import _select_bandpass_sources
from evn_postprocess.process import _select_plot_sources


class TestSelectPlotSources:
    """Tests for process._select_plot_sources precedence."""

    def _make_sources(self, fringefinder=None, calibrator=None, target=None):
        sources = Mock()
        sources.fringefinder = fringefinder or []
        sources.calibrator = calibrator or []
        sources.target = target or []
        return sources

    def test_returns_fringe_finders_when_present(self):
        sources = self._make_sources(
            fringefinder=["FF1", "FF2"],
            calibrator=["C1"],
            target=["T1"],
        )
        assert _select_plot_sources(sources) == ["FF1", "FF2"]

    def test_falls_back_to_calibrators(self):
        sources = self._make_sources(
            fringefinder=[],
            calibrator=["C1", "C2"],
            target=["T1"],
        )
        assert _select_plot_sources(sources) == ["C1", "C2"]

    def test_falls_back_to_targets(self):
        sources = self._make_sources(
            fringefinder=[],
            calibrator=[],
            target=["T1"],
        )
        assert _select_plot_sources(sources) == ["T1"]

    def test_returns_empty_when_sources_is_none(self):
        assert _select_plot_sources(None) == []

    def test_returns_empty_when_no_sources(self):
        sources = self._make_sources(fringefinder=[], calibrator=[], target=[])
        assert _select_plot_sources(sources) == []


class TestWorkflowCreateStandardplots:
    """Tests for workflow.create_standardplots source-type acceptance."""

    def _make_pass(self, fringefinder=None, calibrator=None, target=None):
        a_pass = Mock(spec=experiment.CorrelatorPass)
        a_pass.pipeline = True
        a_pass.sources = Mock()
        a_pass.sources.fringefinder = fringefinder or []
        a_pass.sources.calibrator = calibrator or []
        a_pass.sources.target = target or []
        return a_pass

    def test_target_only_pass_returns_true(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = Mock(spec=experiment.Experiment)
        exp.refant = ["Ef"]
        exp.correlator_passes = [self._make_pass(target=["SRC"])]
        exp.sources = Mock()
        exp.sources.fringefinder = []
        exp.sources.calibrator = []
        exp.sources.target = []

        with patch("evn_postprocess.workflow.process.standardplots", return_value=True) as mock_sp:
            assert workflow.create_standardplots(exp) is True
            mock_sp.assert_called_once_with(exp, do_weights=True)

    def test_returns_false_when_no_source_exists(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = Mock(spec=experiment.Experiment)
        exp.refant = ["Ef"]
        exp.correlator_passes = [self._make_pass()]
        exp.sources = Mock()
        exp.sources.fringefinder = []
        exp.sources.calibrator = []
        exp.sources.target = []

        with patch("evn_postprocess.workflow.process.standardplots", return_value=True) as mock_sp:
            assert workflow.create_standardplots(exp) is False
            mock_sp.assert_not_called()


class TestSelectBandpassSources:
    """Tests for pipeline._select_bandpass_sources precedence."""

    def _make_sources(self, fringefinder=None, calibrator=None, target=None, names=None):
        sources = Mock()
        sources.fringefinder = fringefinder or []
        sources.calibrator = calibrator or []
        sources.target = target or []
        sources.names = names or []
        return sources

    def test_returns_fringe_finders_when_present(self):
        sources = self._make_sources(
            fringefinder=["FF1"],
            calibrator=["C1"],
            target=["T1"],
            names=["FF1", "C1", "T1"],
        )
        assert _select_bandpass_sources(sources) == ["FF1"]

    def test_falls_back_to_calibrators(self):
        sources = self._make_sources(
            fringefinder=[],
            calibrator=["C1"],
            target=["T1"],
            names=["C1", "T1"],
        )
        assert _select_bandpass_sources(sources) == ["C1"]

    def test_falls_back_to_targets(self):
        sources = self._make_sources(
            fringefinder=[],
            calibrator=[],
            target=["T1"],
            names=["T1"],
        )
        assert _select_bandpass_sources(sources) == ["T1"]

    def test_falls_back_to_all_names(self):
        sources = self._make_sources(
            fringefinder=[],
            calibrator=[],
            target=[],
            names=["ALL"],
        )
        assert _select_bandpass_sources(sources) == ["ALL"]

    def test_returns_empty_when_nothing(self):
        sources = self._make_sources(
            fringefinder=[],
            calibrator=[],
            target=[],
            names=[],
        )
        assert _select_bandpass_sources(sources) == []


class TestPipelineInputBandpassFallback:
    """Tests that pipeline.create_input_file fills {bpass} from fallback sources."""

    def test_bpass_filled_with_target_when_no_calibrator(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        pipe_in = tmp_path / "pipe_in"
        pipe_temp = tmp_path / "pipe_temp"
        pipe_in.mkdir()
        pipe_temp.mkdir()

        sources = Mock()
        sources.fringefinder = []
        sources.calibrator = []
        sources.target = ["NME_TGT"]
        sources.names = ["NME_TGT"]
        sources.calibrator_for_target = Mock(return_value=None)

        a_pass = Mock(spec=experiment.CorrelatorPass)
        a_pass.pipeline = True
        a_pass.sources = sources

        exp = Mock(spec=experiment.Experiment)
        exp.expname = "testexp"
        exp.supsci = "testjss"
        exp.refant = ["Ef"]
        exp.multi_phase_center = False
        exp.dirs = Mock()
        exp.dirs.pipe_in = pipe_in
        exp.dirs.pipe_temp = pipe_temp
        exp.correlator_passes = [a_pass]

        template = "{expname} {userno} {refant}\nbpass = {bpass}"
        with patch("subprocess.run") as mock_run, patch(
            "evn_postprocess.pipeline.resources.files"
        ) as mock_resources:
            mock_run.return_value.stdout = "100"
            mock_resources.return_value.joinpath.return_value.read_text.return_value = template
            assert pipeline.create_input_file(exp) is True

        generated = pipe_in / "testexp.inp.txt"
        assert generated.exists()
        text = generated.read_text()
        assert "bpass = NME_TGT" in text
