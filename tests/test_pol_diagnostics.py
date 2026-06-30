"""Tests for the automatic lag-MS polarization diagnostics and their wiring into msops.

Covers:
  * the parallel/cross-hand decision logic (process._derive_pol_diagnostics),
  * persistence of the new Experiment fields (no_lag, pol_diagnostics),
  * the workflow helpers that apply the findings automatically (_auto_msops_available,
    _apply_auto_msops).
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np

from evn_postprocess import experiment, process, workflow


POLS = ["RR", "RL", "LR", "LL"]  # parallel = RR, LL ; cross = RL, LR


def _diag(amp_by_ant, snr_by_ant, ant_names, pols=POLS):
    """Helper: run _derive_pol_diagnostics from {idx: amp-array} and {idx: snr}."""
    amp_sum = {i: np.asarray(a, dtype=float) for i, a in amp_by_ant.items()}
    cnt = {i: 4 for i in amp_by_ant}
    return process._derive_pol_diagnostics(amp_sum, cnt, snr_by_ant, ant_names, pols)


class TestPolDecisionLogic:
    def test_normal_antenna_not_flagged(self):
        d = _diag({0: [10, 0.5, 0.5, 10]}, {0: 30.0}, ["Tr"])
        assert d["analyzed"] is True
        assert d["polswap"] == [] and d["polconvert"] == []
        assert d["antennas"]["Tr"]["decision"] == "normal"

    def test_polswap_detected(self):
        # cross-hand (RL, LR) dominate -> R/L swapped
        d = _diag({0: [0.5, 10, 10, 0.5]}, {0: 25.0}, ["Mc"])
        assert d["polswap"] == ["Mc"]
        assert d["antennas"]["Mc"]["decision"] == "polswap"

    def test_polconvert_only_for_candidates(self):
        # All four products comparable -> linear pol. Ef is a candidate, Jb is not.
        d = _diag({0: [5, 5, 5, 5], 1: [5, 5, 5, 5]}, {0: 40.0, 1: 40.0}, ["Ef", "Jb"])
        assert d["polconvert"] == ["Ef"]
        assert d["antennas"]["Ef"]["decision"] == "polconvert"
        assert d["antennas"]["Jb"]["decision"] == "normal"

    def test_low_snr_is_undetermined(self):
        # Swap-like amplitudes but SNR below the threshold -> cannot decide.
        d = _diag({0: [0.5, 10, 10, 0.5]}, {0: 3.0}, ["Hh"])
        assert d["polswap"] == []
        assert d["antennas"]["Hh"]["decision"] == "undetermined"

    def test_dual_pol_only_not_analyzed(self):
        # No cross-hand products available -> analysis cannot run.
        d = process._derive_pol_diagnostics({0: np.array([10.0, 10.0])}, {0: 4}, {0: 30.0},
                                            ["Tr"], ["RR", "LL"])
        assert d["analyzed"] is False


class TestPersistence:
    def _dirs(self, tmp_path: Path) -> experiment.Dirs:
        return experiment.Dirs(logs=tmp_path, plots=tmp_path, pipeline=tmp_path,
                               pipe_in=tmp_path, pipe_out=tmp_path, pipe_temp=tmp_path)

    def test_no_lag_and_pol_diagnostics_round_trip(self, tmp_path: Path):
        exp = experiment.Experiment("TEST01", dt.date(2026, 5, 29), "marcote", self._dirs(tmp_path))
        exp.no_lag = True
        exp.pol_diagnostics = {"analyzed": True, "polswap": ["Mc"], "polconvert": ["Ef"],
                               "antennas": {"Mc": {"decision": "polswap", "ratio": 20.0, "snr": 25.0}}}
        exp2 = experiment.Experiment.from_dict(exp.to_dict())
        assert exp2.no_lag is True
        assert exp2.pol_diagnostics["polswap"] == ["Mc"]
        assert exp2.pol_diagnostics["polconvert"] == ["Ef"]

    def test_defaults_for_old_json(self, tmp_path: Path):
        exp = experiment.Experiment("TEST01", dt.date(2026, 5, 29), "marcote", self._dirs(tmp_path))
        data = exp.to_dict()
        # Simulate an older JSON without the new keys.
        data.pop("no_lag", None)
        data.pop("pol_diagnostics", None)
        exp2 = experiment.Experiment.from_dict(data)
        assert exp2.no_lag is False
        assert exp2.pol_diagnostics == {}


class TestAutoMsopsWiring:
    def _exp(self, tmp_path: Path) -> experiment.Experiment:
        dirs = experiment.Dirs(logs=tmp_path, plots=tmp_path, pipeline=tmp_path,
                               pipe_in=tmp_path, pipe_out=tmp_path, pipe_temp=tmp_path)
        exp = experiment.Experiment("TEST01", dt.date(2026, 5, 29), "marcote", dirs)
        exp.antennas = experiment.Antennas([experiment.Antenna("Ef"), experiment.Antenna("Mc"),
                                            experiment.Antenna("Tr")])
        exp.correlator_passes = [experiment.CorrelatorPass(Path("test01.lis"), Path("test01.ms"),
                                                           "test01_1_1.IDI", True)]
        return exp

    def test_available_when_analyzed_and_no_1bit(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(workflow.utils, "station_1bit_in_vix", lambda *_a, **_k: False)
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {"analyzed": True, "polswap": ["Mc"], "polconvert": ["Ef"],
                               "antennas": {"Mc": {"decision": "polswap"},
                                            "Ef": {"decision": "polconvert"}}}
        assert workflow._auto_msops_available(exp) is True

    def test_not_available_when_all_undetermined(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(workflow.utils, "station_1bit_in_vix", lambda *_a, **_k: False)
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {"analyzed": True, "polswap": [], "polconvert": [],
                               "antennas": {"Mc": {"decision": "undetermined"}}}
        assert workflow._auto_msops_available(exp) is False

    def test_not_available_with_1bit(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(workflow.utils, "station_1bit_in_vix", lambda *_a, **_k: True)
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {"analyzed": True, "polswap": ["Mc"], "polconvert": [],
                               "antennas": {"Mc": {"decision": "polswap"}}}
        assert workflow._auto_msops_available(exp) is False

    def test_not_available_when_not_analyzed(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(workflow.utils, "station_1bit_in_vix", lambda *_a, **_k: False)
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {}
        assert workflow._auto_msops_available(exp) is False

    def test_apply_sets_flags_and_threshold(self, tmp_path: Path):
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {"analyzed": True, "polswap": ["Mc"], "polconvert": ["Ef"],
                               "antennas": {}}
        workflow._apply_auto_msops(exp)
        assert exp.antennas["Mc"].polswap is True
        assert exp.antennas["Ef"].polconvert is True
        assert exp.antennas["Tr"].polswap is False
        assert exp.correlator_passes[0].flagged_weights.threshold == 0.9
