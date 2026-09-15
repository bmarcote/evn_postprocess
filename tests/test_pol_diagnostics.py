"""Tests for the automatic lag-MS polarization diagnostics and their wiring into msops.

Covers:
  * the parallel/cross-hand decision logic (process._derive_pol_diagnostics),
  * persistence of the new Experiment fields (no_lag, pol_diagnostics),
  * the workflow helpers that apply the findings automatically (_auto_msops_available,
    _apply_auto_msops),
  * the per-scan polswap check that decides over which time range the swap applies
    (process.polswap_check) and the end-of-run summary it feeds (review.msops_summary).
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import Mock

import numpy as np

from evn_postprocess import experiment, process, utils, workflow


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

    def test_available_when_analyzed(self, tmp_path: Path):
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {"analyzed": True, "polswap": ["Mc"], "polconvert": ["Ef"],
                               "antennas": {"Mc": {"decision": "polswap"},
                                            "Ef": {"decision": "polconvert"}}}
        assert workflow._auto_msops_available(exp) is True

    def test_not_available_when_all_undetermined(self, tmp_path: Path):
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {"analyzed": True, "polswap": [], "polconvert": [],
                               "antennas": {"Mc": {"decision": "undetermined"}}}
        assert workflow._auto_msops_available(exp) is False

    def test_available_with_1bit_trace(self, tmp_path: Path):
        """1-bit stations are derived from the VEX independently and do not disable auto-msops."""
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {"analyzed": True, "polswap": ["Mc"], "polconvert": [],
                               "antennas": {"Mc": {"decision": "polswap"}}}
        assert workflow._auto_msops_available(exp) is True

    def test_not_available_when_not_analyzed(self, tmp_path: Path):
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {}
        assert workflow._auto_msops_available(exp) is False

    def test_apply_sets_flags_and_threshold(self, tmp_path: Path, monkeypatch):
        exp = self._exp(tmp_path)
        exp.pol_diagnostics = {"analyzed": True, "polswap": ["Mc"], "polconvert": ["Ef"],
                               "antennas": {}}
        monkeypatch.setattr(workflow.utils, "onebit_stations_in_vix", lambda _path: ["Tr"])
        workflow._apply_auto_msops(exp)
        assert exp.antennas["Mc"].polswap is True
        assert exp.antennas["Ef"].polconvert is True
        assert exp.antennas["Tr"].onebit is True
        assert exp.antennas["Tr"].polswap is False
        assert exp.correlator_passes[0].flagged_weights.threshold == 0.9


class TestAutoWeightThreshold:
    """_auto_weight_threshold derives the flag cutoff from the aggregate 7-bin histogram.

    Bins: <1e-3, [1e-3,0.2), [0.2,0.4), [0.4,0.6), [0.6,0.8), [0.8,0.9), >=0.9.
    """

    @staticmethod
    def _exp(*weights):
        """Build a mock experiment with the given per-antenna weight histograms."""
        exp = Mock()
        exp.antennas = experiment.Antennas([
            experiment.Antenna(name=f"A{i}", weights=w) for i, w in enumerate(weights)
        ])
        return exp

    def test_healthy_weights_use_point_nine(self):
        """>=95% in first+last bins and last bin non-empty -> 0.9."""
        assert workflow._auto_weight_threshold(self._exp((10, 0, 0, 0, 0, 0, 990))) == 0.9

    def test_lower_cluster_uses_lower_edge(self):
        """Main cluster in [0.8,0.9) -> threshold 0.8."""
        assert workflow._auto_weight_threshold(self._exp((10, 0, 0, 0, 0, 990, 0))) == 0.8

    def test_mid_cluster_uses_point_six(self):
        """Main cluster in [0.6,0.8) -> threshold 0.6."""
        assert workflow._auto_weight_threshold(self._exp((5, 0, 0, 0, 995, 0, 0))) == 0.6

    def test_sparse_high_outliers_do_not_raise_threshold(self):
        """A few samples in the >=0.9 bin should not override a large [0.8,0.9) cluster."""
        exp = self._exp((0, 0, 0, 0, 700, 295, 5), (0, 0, 0, 0, 300, 0, 0))
        assert workflow._auto_weight_threshold(exp) == 0.8

    def test_missing_statistics_falls_back_to_point_nine(self):
        """No valid histograms (empty or all-zero) -> default 0.9."""
        assert workflow._auto_weight_threshold(self._exp((), (0, 0, 0, 0, 0, 0, 0))) == 0.9

    def test_multi_antenna_aggregation(self):
        """Histograms from multiple antennas are summed before picking the bin."""
        exp = self._exp((5, 0, 0, 0, 0, 500, 0), (5, 0, 0, 0, 0, 500, 0))
        assert workflow._auto_weight_threshold(exp) == 0.8

    def test_returns_float_in_open_interval(self):
        """The returned value must always be a float in (0, 1)."""
        for weights in [(10, 0, 0, 0, 0, 0, 990), (0, 0, 0, 0, 0, 990, 0), (0, 500, 0, 0, 0, 0, 0)]:
            t = workflow._auto_weight_threshold(self._exp(weights))
            assert isinstance(t, float)
            assert 0 < t < 1


class TestOnebitAutoDetection:
    """onebit_stations_in_vix parses the VEX to find stations recorded at 1-bit."""

    def test_qualified_station_detected(self, tmp_path: Path):
        """A station explicitly qualified with a 1-bit TRACKS def is detected."""
        vex = tmp_path / "test.vix"
        vex.write_text(
            "$TRACKS;\n"
            "def two_bit; track_frame_format = Mark5B; enddef;\n"
            "def one_bit; track_frame_format = Mark5B 1bit; enddef;\n"
            "$MODE;\n"
            "def active; ref $TRACKS = one_bit : Ef; ref $TRACKS = two_bit : Mc; enddef;\n"
            "$SCHED;\n"
            "scan no0001; mode = active; station = Ef : 0 sec : 10 sec;"
            " station = Mc : 0 sec : 10 sec; endscan;\n"
        )
        assert utils.onebit_stations_in_vix(vex) == ["Ef"]

    def test_unused_onebit_capability_ignored(self, tmp_path: Path):
        """A 1-bit def that is not referenced by any scheduled mode is ignored."""
        vex = tmp_path / "test.vix"
        vex.write_text(
            "$TRACKS;\n"
            "def capable; track_frame_format = 1bit; enddef;\n"
            "$MODE;\n"
            "def active; ref $TRACKS = normal : Ef; enddef;\n"
            "$SCHED;\n"
            "scan no0001; mode = active; station = Ef : 0 sec : 10 sec; endscan;\n"
        )
        assert utils.onebit_stations_in_vix(vex) == []

    def test_empty_list_when_no_onebit(self, tmp_path: Path):
        """A VEX with no 1-bit references returns an empty list."""
        vex = tmp_path / "test.vix"
        vex.write_text(
            "$TRACKS;\ndef normal; track_frame_format = Mark5B; enddef;\n"
            "$MODE;\ndef active; ref $TRACKS = normal : Ef; enddef;\n"
            "$SCHED;\nscan no0001; mode = active; station = Ef : 0 sec : 10 sec; endscan;\n"
        )
        assert utils.onebit_stations_in_vix(vex) == []

    def test_missing_vex_raises(self, tmp_path: Path):
        """A missing VEX file raises FileNotFoundError."""
        import pytest
        with pytest.raises(FileNotFoundError):
            utils.onebit_stations_in_vix(tmp_path / "nonexistent.vix")

    def test_apply_auto_msops_sets_onebit(self, tmp_path: Path, monkeypatch):
        """_apply_auto_msops sets onebit=True on stations returned by onebit_stations_in_vix."""
        dirs = experiment.Dirs(logs=tmp_path, plots=tmp_path, pipeline=tmp_path,
                               pipe_in=tmp_path, pipe_out=tmp_path, pipe_temp=tmp_path)
        exp = experiment.Experiment("TEST01", dt.date(2026, 5, 29), "marcote", dirs)
        exp.antennas = experiment.Antennas([experiment.Antenna("Ef"), experiment.Antenna("Mc"),
                                            experiment.Antenna("Tr")])
        exp.correlator_passes = [experiment.CorrelatorPass(Path("test01.lis"), Path("test01.ms"),
                                                           "test01_1_1.IDI", True)]
        exp.pol_diagnostics = {"analyzed": True, "polswap": [], "polconvert": [], "antennas": {}}
        monkeypatch.setattr(workflow.utils, "onebit_stations_in_vix", lambda _path: ["Tr"])
        workflow._apply_auto_msops(exp)
        assert exp.antennas["Tr"].onebit is True
        assert exp.antennas["Ef"].onebit is False
        assert exp.antennas["Mc"].onebit is False


def _swap_exp(tmp_path, swapped_scans, n_scans=6, antenna='Wb'):
    """An experiment with *n_scans* scans where *antenna* is swapped in `swapped_scans`.

    The lag SNRs are synthetic: a swapped scan puts the signal in the cross-hand products
    (RL, LR), a correct one in the parallel-hand products (RR, LL).
    """
    dirs = experiment.Dirs(logs=tmp_path / 'logs', plots=tmp_path / 'plots',
                           pipeline=tmp_path / 'p', pipe_in=tmp_path / 'p/in',
                           pipe_out=tmp_path / 'p/out', pipe_temp=tmp_path / 'p/tmp')
    exp = experiment.Experiment('EB101', dt.date(2026, 4, 10), 'tester', dirs)
    exp.antennas = experiment.Antennas([experiment.Antenna(name=antenna)])
    exp.antennas[antenna].polswap = True
    start = dt.datetime(2026, 4, 10, 10, 0)
    exp.scans = experiment.Scans([
        experiment.Scan(scanno=f'No{i:04d}', starttime=start + dt.timedelta(minutes=10 * i),
                        duration_s=300, source='3C286', stations_scheduled=(antenna,))
        for i in range(1, n_scans + 1)])
    exp.lag_snr = {str(i): {antenna: ({'RR': 2.0, 'LL': 2.0, 'RL': 30.0, 'LR': 30.0}
                                      if i in swapped_scans else
                                      {'RR': 30.0, 'LL': 30.0, 'RL': 2.0, 'LR': 2.0})}
                   for i in range(1, n_scans + 1)}
    return exp


class TestPolswapCheck:
    """`polswap_check` must find WHEN a station was swapped, not just whether it was.

    Swapping a whole observation when the station fixed itself partway through would
    corrupt the half that was already correct.
    """

    def test_swapped_throughout_covers_the_whole_observation(self, tmp_path):
        exp = _swap_exp(tmp_path, swapped_scans=range(1, 7))
        process.polswap_check(exp)
        assert process.polswap_range(exp, 'Wb') == (None, None)

    def test_fixed_partway_sets_the_end_time(self, tmp_path):
        # Swapped in scans 1-3, correct from scan 4 on: swap only until scan 4 starts.
        exp = _swap_exp(tmp_path, swapped_scans=(1, 2, 3))
        process.polswap_check(exp)
        start, end = process.polswap_range(exp, 'Wb')
        assert start is None
        assert end == exp.scans[3].starttime

    def test_broken_partway_sets_the_start_time(self, tmp_path):
        # Correct in scans 1-2, swapped from scan 3 on: swap only from scan 3 on.
        exp = _swap_exp(tmp_path, swapped_scans=(3, 4, 5, 6))
        process.polswap_check(exp)
        start, end = process.polswap_range(exp, 'Wb')
        assert end is None
        assert start == exp.scans[2].starttime

    def test_flip_flopping_falls_back_to_the_whole_observation(self, tmp_path):
        exp = _swap_exp(tmp_path, swapped_scans=(1, 2, 5, 6))
        process.polswap_check(exp)
        assert process.polswap_range(exp, 'Wb') == (None, None)

    def test_no_lag_data_falls_back_to_the_whole_observation(self, tmp_path):
        exp = _swap_exp(tmp_path, swapped_scans=())
        exp.lag_snr = {}          # e.g. --no-lag
        process.polswap_check(exp)
        assert process.polswap_range(exp, 'Wb') == (None, None)

    def test_weak_scans_are_ignored(self, tmp_path):
        exp = _swap_exp(tmp_path, swapped_scans=(1, 2, 3))
        # Scans 4-6 are too weak to decide: only the swapped ones remain -> whole obs.
        for scan in ('4', '5', '6'):
            exp.lag_snr[scan]['Wb'] = {'RR': 1.0, 'LL': 1.0, 'RL': 1.0, 'LR': 1.0}
        process.polswap_check(exp)
        assert process.polswap_range(exp, 'Wb') == (None, None)

    def test_range_survives_a_store_load_round_trip(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _swap_exp(tmp_path, swapped_scans=(1, 2, 3))
        process.polswap_check(exp)
        exp.store()
        reloaded = experiment.Experiment.load('EB101')
        assert process.polswap_range(reloaded, 'Wb') == process.polswap_range(exp, 'Wb')

    def test_polswap_applies_the_range(self, tmp_path, monkeypatch):
        exp = _swap_exp(tmp_path, swapped_scans=(1, 2, 3))
        exp.correlator_passes = [experiment.CorrelatorPass(
            lisfile=Path('eb101.lis'), msfile=Path('eb101.ms'),
            fitsidifile='eb101_1_1.IDI', pipeline=True)]
        applied = []
        monkeypatch.setattr(process.mstools, 'polswap',
                            lambda ms, ant, start=None, end=None: applied.append((ant, start, end)))
        monkeypatch.setattr(experiment.Experiment, 'store', lambda self: None)
        assert process.polswap(exp) is True
        assert applied == [('Wb', None, exp.scans[3].starttime)]


class TestMsopsSummary:
    def test_reports_the_polswap_time_range(self, tmp_path):
        from evn_postprocess import review
        exp = _swap_exp(tmp_path, swapped_scans=(1, 2, 3))
        process.polswap_check(exp)
        summary = review.msops_summary(exp)
        assert 'Wb' in summary and '10:40:00' in summary

    def test_empty_when_nothing_was_applied(self, tmp_path):
        from evn_postprocess import review
        exp = _swap_exp(tmp_path, swapped_scans=())
        exp.antennas['Wb'].polswap = False
        assert review.msops_summary(exp) == ''
