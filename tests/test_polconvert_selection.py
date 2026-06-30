"""Tests for the automatic PolConvert scan / reference-antenna selection and runner.

Covers the helpers that replaced the old "most scheduled stations" heuristic:
  * _scan_lag_score / _rank_fringefinder_scans   (scan picked by real lag SNR),
  * _refant_bandpass_scatter / _rank_polconvert_refants
                                                 (reference = non-linear, full-IF, flattest),
  * _polconvert_exclude_ants, _check_fringe_peaks, _run_polconvert_cli (segfault retry),
  * end-to-end process.polconvert() selection,
  * persistence of the new exp.lag_bandpass field.

The scenario mirrors EZ041A: Ef is linear (PolConvert), Mc/O8 are circular full-band, Wb only
covers half the IFs, Cm did not observe, scan No0003 was scheduled (13 stations) but never
correlated (absent from lag_snr) while scan No0018 is the one with real fringes.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import astropy.units as u
import pytest
from astropy import coordinates as coord

from evn_postprocess import experiment, process


IFS = list(range(8))


def _src(name, stype):
    return experiment.Source(name=name, coordinates=coord.SkyCoord(ra=0 * u.deg, dec=0 * u.deg),
                             type=stype)


def _make_exp(tmp_path: Path) -> experiment.Experiment:
    dirs = experiment.Dirs(logs=tmp_path, plots=tmp_path, pipeline=tmp_path,
                           pipe_in=tmp_path, pipe_out=tmp_path, pipe_temp=tmp_path)
    exp = experiment.Experiment("EZ041A", dt.date(2026, 6, 25), "marcote", dirs)
    exp.antennas = experiment.Antennas([
        experiment.Antenna("Ef", observed=True, subbands=tuple(IFS), polconvert=True),
        experiment.Antenna("Mc", observed=True, subbands=tuple(IFS)),   # flat bandpass
        experiment.Antenna("O8", observed=True, subbands=tuple(IFS)),   # scattered bandpass
        experiment.Antenna("Wb", observed=True, subbands=(4, 5, 6, 7)),  # half the IFs only
        experiment.Antenna("Cm", observed=False, subbands=tuple()),     # not correlated
    ])
    exp.sources = experiment.Sources([_src("4C39.25", experiment.SourceType.fringefinder),
                                      _src("J1112+07", experiment.SourceType.calibrator)])
    # Priority deliberately ranks O8/Wb above Mc, so any test that still prefers Mc proves the
    # bandpass-flatness criterion overrides the plain experiment-refant order.
    exp.refant = ["Ef", "O8", "Wb", "Mc"]
    t0 = dt.datetime(2026, 6, 25, 9, 52, 0)
    t18 = dt.datetime(2026, 6, 25, 13, 40, 0)
    exp.scans = experiment.Scans([
        # Scheduled on 13 stations but never correlated -> absent from lag_snr.
        experiment.Scan("No0003", t0, 780, "4C39.25",
                        stations_scheduled=tuple("Ef Mc O8 Wb Cm Da De Pi Kn Jb Tr Hh Ir".split()),
                        stations_observed=()),
        # The scan that actually has fringes.
        experiment.Scan("No0018", t18, 220, "4C39.25",
                        stations_scheduled=("Ef", "Mc", "O8", "Wb"),
                        stations_observed=("Ef", "Mc", "O8", "Wb")),
    ])
    # Only scan 18 has lag SNR; all four antennas detected, Mc/O8 strongest.
    exp.lag_snr = {"18": {"Ef": {"RR": 400.0, "LL": 410.0, "RL": 20.0, "LR": 20.0},
                          "Mc": {"RR": 419.0, "LL": 419.0, "RL": 10.0, "LR": 10.0},
                          "O8": {"RR": 285.0, "LL": 286.0, "RL": 10.0, "LR": 10.0},
                          "Wb": {"RR": 286.0, "LL": 286.0, "RL": 10.0, "LR": 10.0}}}
    # Per-IF parallel-hand amplitude. Mc is flat across IFs; O8 is scattered.
    exp.lag_bandpass = {"18": {
        "Mc": [1.00, 1.01, 0.99, 1.00, 1.02, 0.98, 1.00, 1.01],
        "O8": [1.00, 0.40, 1.60, 0.30, 1.70, 0.50, 1.50, 0.40],
        "Wb": [None, None, None, None, 1.0, 1.0, 1.0, 1.0],
        "Ef": [0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7],
    }}
    return exp


# --- scan selection ---------------------------------------------------------------------

class TestScanSelection:
    def test_lag_score_ignores_non_correlated_scan(self, tmp_path):
        exp = _make_exp(tmp_path)
        no3, no18 = exp.scans[0], exp.scans[1]
        assert process._scan_lag_score(exp, no3) == (0, 0.0)       # phantom scan
        n_det, snr_sum = process._scan_lag_score(exp, no18)
        assert n_det == 4 and snr_sum > 0

    def test_rank_prefers_real_fringes_over_scheduled_count(self, tmp_path):
        exp = _make_exp(tmp_path)
        ranked = process._rank_fringefinder_scans(exp)
        # No0018 (real fringes) must come before the 13-station-but-phantom No0003.
        assert [s.scanno for s in ranked] == ["No0018", "No0003"]

    def test_rank_falls_back_to_scheduled_when_no_lag(self, tmp_path):
        exp = _make_exp(tmp_path)
        exp.lag_snr = {}
        ranked = process._rank_fringefinder_scans(exp)
        # Without lag data, the old scheduled-count ordering applies (No0003 has 13).
        assert ranked[0].scanno == "No0003"


# --- reference-antenna selection --------------------------------------------------------

class TestRefantSelection:
    def test_bandpass_scatter_flatter_is_smaller(self, tmp_path):
        exp = _make_exp(tmp_path)
        flat = process._refant_bandpass_scatter(exp, "Mc", "18", IFS)
        rough = process._refant_bandpass_scatter(exp, "O8", "18", IFS)
        assert flat < rough
        # Missing data -> inf (so antennas with data are always preferred).
        assert process._refant_bandpass_scatter(exp, "Nope", "18", IFS) == float("inf")

    def test_rank_excludes_linear_and_partial_band_picks_flattest(self, tmp_path):
        exp = _make_exp(tmp_path)
        refants = process._rank_polconvert_refants(exp, ["Ef"], set(IFS), "18")
        assert "Ef" not in refants          # the linear antenna cannot reference itself
        assert "Wb" not in refants          # does not cover all IFs to convert
        assert "Cm" not in refants          # not observed
        assert refants == ["Mc", "O8"]      # flattest bandpass first, despite refant priority

    def test_snr_gate_keeps_weak_ultraflat_antenna_from_winning(self, tmp_path):
        # Jb is observed, full-band and the *flattest* of all, but far weaker (SNR 113 vs 419).
        # The SNR gate must keep it from outranking the strong, flat Mc (mirrors EZ041A).
        exp = _make_exp(tmp_path)
        exp.antennas.append(experiment.Antenna("Jb", observed=True, subbands=tuple(IFS)))
        exp.lag_snr["18"]["Jb"] = {"RR": 113.0, "LL": 113.0, "RL": 5.0, "LR": 5.0}
        exp.lag_bandpass["18"]["Jb"] = [1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000]
        refants = process._rank_polconvert_refants(exp, ["Ef"], set(IFS), "18")
        assert refants[0] == "Mc"               # strong + flat wins
        assert refants.index("Mc") < refants.index("Jb")   # weak ultra-flat ranked below

    def test_exclude_ants_drops_unobserved_and_partial_band(self, tmp_path):
        exp = _make_exp(tmp_path)
        excl = process._polconvert_exclude_ants(exp, ["Ef"], "Mc", set(IFS))
        assert excl == ["Cm", "Wb"]
        assert "Ef" not in excl and "Mc" not in excl and "O8" not in excl


# --- solution quality check -------------------------------------------------------------

def _write_peaks(logdir: Path, per_if):
    peaks = logdir / "FRINGE.PEAKS"
    peaks.mkdir(parents=True, exist_ok=True)
    for i, (rr, ll, rl, lr) in enumerate(per_if, start=1):
        (peaks / f"FRINGE.PEAKS_IF{i}_SCAN_0_EF-MC.dat").write_text(
            f"BASELINE EF TO MC\n  FOR IF #{i}.\n"
            f"     RR: {rr:.3e} ; SNR: 400.0\n     LL: {ll:.3e} ; SNR: 500.0\n"
            f"     RL: {rl:.3e} ; SNR: 100.0\n     LR: {lr:.3e} ; SNR: 100.0\n"
            f"     AMPLITUDE: 5.0e-01  RL/LR Norm.: 1.0e+00\n")


class TestFringePeaksCheck:
    def test_good_solution_passes(self, tmp_path):
        _write_peaks(tmp_path, [(0.9, 1.0, 0.05, 0.05)] * 8)   # ratio ~19
        assert process._check_fringe_peaks(str(tmp_path)) is True

    def test_one_bad_if_fails(self, tmp_path):
        peaks = [(0.9, 1.0, 0.05, 0.05)] * 7 + [(0.5, 0.5, 0.5, 0.5)]  # last IF ratio ~1
        _write_peaks(tmp_path, peaks)
        assert process._check_fringe_peaks(str(tmp_path)) is False

    def test_missing_dir_fails(self, tmp_path):
        assert process._check_fringe_peaks(str(tmp_path / "nope")) is False


# --- subprocess runner with segfault retry ----------------------------------------------

class _FakeProc:
    def __init__(self, rc):
        self.returncode = rc
        self.stderr = "boom"


class TestRunnerRetry:
    def test_retries_transient_segfault_then_succeeds(self, monkeypatch):
        seq = iter([-11, -11, 0])   # two SIGSEGVs then success
        monkeypatch.setattr(process.subprocess, "run", lambda *a, **k: _FakeProc(next(seq)))
        assert process._run_polconvert_cli(Path("in.toml"), "--compute") == 0

    def test_persistent_segfault_gives_up(self, monkeypatch):
        monkeypatch.setattr(process.subprocess, "run", lambda *a, **k: _FakeProc(-11))
        assert process._run_polconvert_cli(Path("in.toml"), "--compute") == -11

    def test_real_error_not_retried(self, monkeypatch):
        calls = []
        monkeypatch.setattr(process.subprocess, "run",
                            lambda *a, **k: calls.append(1) or _FakeProc(2))
        assert process._run_polconvert_cli(Path("in.toml"), "--compute") == 2
        assert len(calls) == 1   # a non-signal failure is not retried


# --- end-to-end selection ---------------------------------------------------------------

class TestPolconvertIntegration:
    def test_picks_scan18_and_mc_then_applies(self, tmp_path, monkeypatch):
        exp = _make_exp(tmp_path)
        captured: dict = {}

        def fake_write(exp, ref_idi, lin_ants, refant, exclude_ants, do_ifs, time_range,
                       chan_avg, time_avg, solve_weight, logdir,
                       output_file=Path('polconvert_inputs.toml')):
            captured.update(refant=refant, exclude=exclude_ants, do_ifs=do_ifs,
                            ref_idi=ref_idi, time_range=time_range)
            return Path('polconvert_inputs.toml')

        modes: list[str] = []
        monkeypatch.setattr(process, "_write_polconvert_template", fake_write)
        monkeypatch.setattr(process, "_run_polconvert_cli",
                            lambda tmpl, mode: modes.append(mode) or 0)
        monkeypatch.setattr(process, "_check_fringe_peaks", lambda logdir='polconvert_logs': True)
        monkeypatch.setattr(process.find_idi_mod, "find_idi_with_time",
                            lambda idi_files, aipstime, verbose=False: "ez041a_1_1.IDI1")
        monkeypatch.setattr(process.glob, "glob",
                            lambda pat: [] if "PCONVERT" in pat else
                            (["ez041a_1_1.IDI1"] if "IDI" in pat else []))
        monkeypatch.setattr(exp, "store", lambda: None)

        assert process.polconvert(exp) is True
        assert captured["refant"] == "Mc"                 # flattest non-linear full-band antenna
        assert captured["do_ifs"] == [1, 2, 3, 4, 5, 6, 7, 8]
        assert "Wb" in captured["exclude"] and "Cm" in captured["exclude"]
        # time_range must be scan No0018 (13:4x), not the phantom No0003 (09:xx).
        assert captured["time_range"][1] == 13
        assert modes == ["--compute", "--apply"]          # computed, accepted, then applied


# --- persistence ------------------------------------------------------------------------

def test_lag_bandpass_round_trips(tmp_path):
    exp = _make_exp(tmp_path)
    exp2 = experiment.Experiment.from_dict(exp.to_dict())
    assert exp2.lag_bandpass["18"]["Mc"] == exp.lag_bandpass["18"]["Mc"]
    assert exp2.lag_bandpass["18"]["Wb"][0] is None   # missing IFs survive as None


def test_lag_bandpass_defaults_for_old_json(tmp_path):
    exp = _make_exp(tmp_path)
    data = exp.to_dict()
    data.pop("lag_bandpass", None)                    # simulate an older JSON
    exp2 = experiment.Experiment.from_dict(data)
    assert exp2.lag_bandpass == {}
