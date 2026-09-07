"""Tests for the automatic PolConvert scan / reference-antenna selection and runner.

Covers the helpers that replaced the old "most scheduled stations" heuristic:
  * _scan_lag_score / _rank_fringefinder_scans   (scan picked by real lag SNR),
  * _polconvert_solve_scans                      (fringe-finders with a fringe on the linear
                                                 antenna, else the phase calibrators),
  * _polconvert_time_ranges                      (last / middle minute, scan minus its first),
  * _polconvert_refants                          (reference = non-linear, full-IF, strongest),
  * _polconvert_exclude_ants, _fringe_peak_ratios, _polconvert_compute / _polconvert_apply
    (judged by what PolConvert wrote, never by its exit code),
  * end-to-end process.polconvert() selection and the bounds of its parameter search,
  * persistence of the new exp.lag_bandpass field.

The scenario mirrors EZ041A: Ef is linear (PolConvert), Mc/O8 are circular full-band, Wb only
covers half the IFs, Cm did not observe, scan No0003 was scheduled (13 stations) but never
correlated (absent from lag_snr) while scan No0018 is the one with real fringes.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import astropy.units as u
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

    def test_refant_is_the_strongest_valid_fringe(self, tmp_path):
        # Mc (SNR 419) beats O8 (286) even though the experiment refant order lists O8 first.
        exp = _make_exp(tmp_path)
        assert process._polconvert_refants(exp, ["Ef"], set(IFS), "18")[0] == "Mc"

    def test_refant_offers_the_runner_up_as_a_fallback(self, tmp_path):
        # Two candidates, best first: the solutions found by hand on ES123D/ES123F used the
        # second one. Never more than two, so a hopeless search cannot sweep the whole array.
        exp = _make_exp(tmp_path)
        assert process._polconvert_refants(exp, ["Ef"], set(IFS), "18") == ["Mc", "O8"]

    def test_refant_skips_linear_partial_band_and_unobserved(self, tmp_path):
        # Ef is the strongest of all (SNR 410) but is the antenna being converted; Wb is strong
        # too (286) but only covers half the IFs; Cm never observed.
        exp = _make_exp(tmp_path)
        exp.lag_snr["18"]["Mc"] = {"RR": 5.0, "LL": 5.0}      # demote the usual winner
        exp.lag_snr["18"]["O8"] = {"RR": 6.0, "LL": 6.0}
        refants = process._polconvert_refants(exp, ["Ef"], set(IFS), "18")
        assert refants[0] == "O8"                              # strongest full-band, non-linear
        assert "Ef" not in refants and "Wb" not in refants and "Cm" not in refants

    def test_refant_is_empty_when_nothing_qualifies(self, tmp_path):
        exp = _make_exp(tmp_path)
        assert process._polconvert_refants(exp, ["Ef", "Mc", "O8"], set(IFS), "18") == []

    def test_refant_falls_back_to_experiment_order_without_lag_data(self, tmp_path):
        exp = _make_exp(tmp_path)
        exp.lag_snr = {}
        # exp.refant is ["Ef", "O8", "Wb", "Mc"]; Ef is linear and Wb partial-band, so O8 wins.
        assert process._polconvert_refants(exp, ["Ef"], set(IFS), "18")[0] == "O8"

    def test_exclude_ants_drops_unobserved_and_partial_band(self, tmp_path):
        exp = _make_exp(tmp_path)
        excl = process._polconvert_exclude_ants(exp, ["Ef"], "Mc", set(IFS), "18")
        assert excl == ["Cm", "Wb"]
        assert "Ef" not in excl and "Mc" not in excl and "O8" not in excl

    def test_exclude_ants_drops_weak_fringes(self, tmp_path):
        # O8 is full-band but below _POLCONVERT_SOLVE_MIN_SNR on this scan -> out of the solve.
        exp = _make_exp(tmp_path)
        exp.lag_snr["18"]["O8"] = {"RR": 2.0, "LL": 1.5, "RL": 0.5, "LR": 0.5}
        excl = process._polconvert_exclude_ants(exp, ["Ef"], "Mc", set(IFS), "18")
        assert excl == ["Cm", "O8", "Wb"]

    def test_exclude_ants_keeps_weak_refant_and_linear_antennas(self, tmp_path):
        # The reference and the antennas being converted are never excluded on SNR grounds.
        exp = _make_exp(tmp_path)
        exp.lag_snr["18"]["Mc"] = {"RR": 1.0, "LL": 1.0}
        exp.lag_snr["18"]["Ef"] = {"RR": 1.0, "LL": 1.0}
        excl = process._polconvert_exclude_ants(exp, ["Ef"], "Mc", set(IFS), "18")
        assert "Mc" not in excl and "Ef" not in excl

    def test_exclude_ants_skips_snr_filter_without_lag_data(self, tmp_path):
        # A --no-lag run has no SNR at all: every antenna would read 0.0, so the filter is off.
        exp = _make_exp(tmp_path)
        exp.lag_snr = {}
        excl = process._polconvert_exclude_ants(exp, ["Ef"], "Mc", set(IFS), "18")
        assert excl == ["Cm", "Wb"]


# --- solve-scan selection ----------------------------------------------------------------

class TestSolveScanSelection:
    def test_uses_fringefinder_scans_with_a_linear_fringe(self, tmp_path):
        exp = _make_exp(tmp_path)
        scans = process._polconvert_solve_scans(exp, ["Ef"])
        # Only No0018 was correlated; No0003 has no lag SNR at all.
        assert [s.scanno for s in scans] == ["No0018"]

    def test_falls_back_to_phase_calibrator_scans(self, tmp_path):
        # Ef shows no fringe on any fringe-finder scan, but does on a phase-cal scan.
        exp = _make_exp(tmp_path)
        exp.scans.append(experiment.Scan("No0025", dt.datetime(2026, 6, 25, 14, 10), 300,
                                         "J1112+07", stations_scheduled=("Ef", "Mc", "O8"),
                                         stations_observed=("Ef", "Mc", "O8")))
        exp.lag_snr["18"]["Ef"] = {"RR": 2.0, "LL": 2.0}          # no fringe on the FF scan
        exp.lag_snr["25"] = {"Ef": {"RR": 50.0, "LL": 50.0}, "Mc": {"RR": 60.0, "LL": 60.0}}
        scans = process._polconvert_solve_scans(exp, ["Ef"])
        assert [s.scanno for s in scans] == ["No0025"]

    def test_empty_when_the_linear_antenna_never_fringes(self, tmp_path):
        exp = _make_exp(tmp_path)
        exp.lag_snr["18"]["Ef"] = {"RR": 1.0, "LL": 1.0}
        assert process._polconvert_solve_scans(exp, ["Ef"]) == []

    def test_falls_back_to_plain_ranking_without_lag_data(self, tmp_path):
        exp = _make_exp(tmp_path)
        exp.lag_snr = {}
        scans = process._polconvert_solve_scans(exp, ["Ef"])
        assert [s.scanno for s in scans] == ["No0003", "No0018"]   # scheduled-station order


# --- solve time ranges -------------------------------------------------------------------

class TestSolveTimeRanges:
    def test_last_minute_then_middle_then_scan_without_its_first_minute(self, tmp_path):
        exp = _make_exp(tmp_path)
        scan = exp.scans[1]                       # No0018: 13:40:00 + 220 s -> 13:43:40
        ranges = process._polconvert_time_ranges(scan, exp.obsdate)
        assert ranges == [[0, 13, 42, 40, 0, 13, 43, 40],     # last minute
                          [0, 13, 41, 20, 0, 13, 42, 20],     # the minute around the middle
                          [0, 13, 41, 0, 0, 13, 43, 40]]      # all but the first minute

    def test_a_short_scan_does_not_repeat_the_same_range(self, tmp_path):
        # 13:40:00 + 120 s: the last minute and "all but the first" are the same 13:41-13:42.
        exp = _make_exp(tmp_path)
        scan = experiment.Scan("No0099", dt.datetime(2026, 6, 25, 13, 40), 120, "4C39.25",
                               stations_scheduled=("Ef", "Mc"))
        assert process._polconvert_time_ranges(scan, exp.obsdate) == \
            [[0, 13, 41, 0, 0, 13, 42, 0], [0, 13, 40, 30, 0, 13, 41, 30]]

    def test_short_scan_keeps_its_full_range(self, tmp_path):
        exp = _make_exp(tmp_path)
        scan = experiment.Scan("No0099", dt.datetime(2026, 6, 25, 13, 40), 45, "4C39.25",
                               stations_scheduled=("Ef", "Mc"))
        assert process._polconvert_time_ranges(scan, exp.obsdate) == \
            [[0, 13, 40, 0, 0, 13, 40, 45]]

    def test_day_counter_rolls_over_midnight(self, tmp_path):
        exp = _make_exp(tmp_path)
        scan = experiment.Scan("No0099", dt.datetime(2026, 6, 25, 23, 58), 300, "4C39.25",
                               stations_scheduled=("Ef", "Mc"))
        ranges = process._polconvert_time_ranges(scan, exp.obsdate)
        assert ranges == [[1, 0, 2, 0, 1, 0, 3, 0],
                          [1, 0, 0, 0, 1, 0, 1, 0],
                          [0, 23, 59, 0, 1, 0, 3, 0]]


# --- solution quality check -------------------------------------------------------------

def _write_peaks(logdir: Path, per_if, gains=True):
    """Writes what a finished --compute leaves behind: the gains file plus one peaks file/IF."""
    peaks = logdir / "FRINGE.PEAKS"
    peaks.mkdir(parents=True, exist_ok=True)
    if gains:
        (logdir / "polconvert.gains").write_bytes(b"pickled gains")
    for i, (rr, ll, rl, lr) in enumerate(per_if, start=1):
        (peaks / f"FRINGE.PEAKS_IF{i}_SCAN_0_EF-MC.dat").write_text(
            f"BASELINE EF TO MC\n  FOR IF #{i}.\n"
            f"     RR: {rr:.3e} ; SNR: 400.0\n     LL: {ll:.3e} ; SNR: 500.0\n"
            f"     RL: {rl:.3e} ; SNR: 100.0\n     LR: {lr:.3e} ; SNR: 100.0\n"
            f"     AMPLITUDE: 5.0e-01  RL/LR Norm.: 1.0e+00\n")


class TestFringePeakRatios:
    """A finished run is read off its files; anything short of a full set reads as unfinished."""

    def test_good_solution_gives_a_ratio_per_if(self, tmp_path):
        _write_peaks(tmp_path, [(0.9, 1.0, 0.05, 0.05)] * 8)   # ratio ~19
        ratios = process._fringe_peak_ratios(str(tmp_path), 8)
        assert len(ratios) == 8 and min(ratios) > process._POLCONVERT_MIN_RATIO

    def test_one_bad_if_is_still_a_finished_run(self, tmp_path):
        peaks = [(0.9, 1.0, 0.05, 0.05)] * 7 + [(0.5, 0.5, 0.5, 0.5)]  # last IF ratio ~1
        _write_peaks(tmp_path, peaks)
        ratios = process._fringe_peak_ratios(str(tmp_path), 8)
        assert len(ratios) == 8 and min(ratios) < process._POLCONVERT_MIN_RATIO

    def test_missing_dir_is_unfinished(self, tmp_path):
        assert process._fringe_peak_ratios(str(tmp_path / "nope"), 8) == []

    def test_a_partial_set_of_ifs_is_unfinished(self, tmp_path):
        # Crashed halfway: 5 of the 8 IFs written. Judging those 5 would accept a torn run.
        _write_peaks(tmp_path, [(0.9, 1.0, 0.05, 0.05)] * 5)
        assert process._fringe_peak_ratios(str(tmp_path), 8) == []

    def test_peaks_without_the_gains_file_are_unfinished(self, tmp_path):
        _write_peaks(tmp_path, [(0.9, 1.0, 0.05, 0.05)] * 8, gains=False)
        assert process._fringe_peak_ratios(str(tmp_path), 8) == []


# --- the runner: results on disk decide, not the exit code -------------------------------

class _FakeProc:
    def __init__(self, rc, stdout=""):
        self.returncode = rc
        self.stdout = stdout
        self.stderr = "boom"


class TestComputeIgnoresTheExitCode:
    """PolConvert dies in its teardown *after* writing the solution (verified on ES123B and
    ES123E: the solutions found by hand are reproduced, and the process still exits 134)."""

    def test_a_complete_solution_is_kept_even_when_the_process_aborts(self, tmp_path, monkeypatch):
        def fake_run(cmd, **kwargs):
            _write_peaks(tmp_path, [(0.9, 1.0, 0.05, 0.05)] * 8)
            return _FakeProc(-6)                       # SIGABRT: "double free or corruption"

        monkeypatch.setattr(process.subprocess, "run", fake_run)
        ratios = process._polconvert_compute(Path("in.toml"), str(tmp_path), 8)
        assert len(ratios) == 8                        # the crash did not lose the solution

    def test_only_a_run_that_wrote_nothing_is_retried(self, tmp_path, monkeypatch):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(1)
            if len(calls) == 3:                        # writes a full result only on the third go
                _write_peaks(tmp_path, [(0.9, 1.0, 0.05, 0.05)] * 8)
            return _FakeProc(-11)

        monkeypatch.setattr(process.subprocess, "run", fake_run)
        assert len(process._polconvert_compute(Path("in.toml"), str(tmp_path), 8)) == 8
        assert len(calls) == 3

    def test_gives_up_after_the_retries(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(process.subprocess, "run",
                            lambda *a, **k: calls.append(1) or _FakeProc(-11))
        monkeypatch.setattr(process, "_log_fringe_snr_table", lambda logdir: None)
        assert process._polconvert_compute(Path("in.toml"), str(tmp_path), 8) == []
        assert len(calls) == process._POLCONVERT_RETRIES + 1


class TestApplyIsJudgedByItsOutputFiles:
    def test_success_when_every_idi_came_out_converted(self, tmp_path, monkeypatch):
        idis = [str(tmp_path / f"ez041a_1_1.IDI{n}") for n in (1, 2)]

        def fake_run(cmd, **kwargs):
            for idi in idis:
                Path(idi + '.PCONVERT').write_text('converted')
            return _FakeProc(-6)                       # aborts on the way out, as it always does

        monkeypatch.setattr(process.subprocess, "run", fake_run)
        assert process._polconvert_apply(Path("in.toml"), idis) is True

    def test_failure_when_a_file_is_left_unconverted(self, tmp_path, monkeypatch):
        idis = [str(tmp_path / f"ez041a_1_1.IDI{n}") for n in (1, 2)]

        def fake_run(cmd, **kwargs):
            Path(idis[0] + '.PCONVERT').write_text('converted')
            return _FakeProc(0)                        # exits cleanly, but IDI2 never appears

        monkeypatch.setattr(process.subprocess, "run", fake_run)
        assert process._polconvert_apply(Path("in.toml"), idis) is False


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
        monkeypatch.setattr(process, "_polconvert_compute",
                            lambda tmpl, logdir, n_ifs: modes.append('--compute') or [19.0] * n_ifs)
        monkeypatch.setattr(process, "_polconvert_apply",
                            lambda tmpl, idis: modes.append('--apply') or True)
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

    def test_the_accepted_combination_is_recorded_once_in_the_runbook(self, tmp_path, monkeypatch):
        """logs/commands.sh gets the two lines that reproduce the accepted run, not one per
        subprocess launch: the search overwrites the same input file on every attempt."""
        exp = _make_exp(tmp_path)
        recorded: list[str] = []

        monkeypatch.setattr(process, "_write_polconvert_template",
                            lambda *a, **k: Path('polconvert_inputs.toml'))
        monkeypatch.setattr(process, "_polconvert_compute",
                            lambda tmpl, logdir, n_ifs: [19.0] * n_ifs)
        monkeypatch.setattr(process, "_polconvert_apply", lambda tmpl, idis: True)
        monkeypatch.setattr(process.reporting, "record_command",
                            lambda command, step=None: recorded.append(command))
        monkeypatch.setattr(process.find_idi_mod, "find_idi_with_time",
                            lambda idi_files, aipstime, verbose=False: "ez041a_1_1.IDI1")
        monkeypatch.setattr(process.glob, "glob",
                            lambda pat: [] if "PCONVERT" in pat else
                            (["ez041a_1_1.IDI1"] if "IDI" in pat else []))
        monkeypatch.setattr(exp, "store", lambda: None)

        assert process.polconvert(exp) is True
        assert recorded == ['polconvert.py polconvert_inputs.toml --compute',
                            'polconvert.py polconvert_inputs.toml --apply']

    def test_search_space_is_bounded_and_ordered(self, tmp_path, monkeypatch):
        """A search that never converges stays inside the declared, capped parameter space.

        The space is two reference antennas x three time ranges x doweight x timeavg x chanavg
        per scan, ordered best-first and stopped at _POLCONVERT_MAX_ATTEMPTS so that widening
        it cannot turn a hopeless run into an overnight one.
        """
        exp = _make_exp(tmp_path)
        attempts: list[tuple] = []

        def fake_write(exp, ref_idi, lin_ants, refant, exclude_ants, do_ifs, time_range,
                       chan_avg, time_avg, solve_weight, logdir,
                       output_file=Path('polconvert_inputs.toml')):
            attempts.append((refant, tuple(time_range), solve_weight, time_avg, chan_avg))
            return Path('polconvert_inputs.toml')

        monkeypatch.setattr(process, "_write_polconvert_template", fake_write)
        # Every attempt finishes and every attempt is bad, so the search runs to its bound.
        monkeypatch.setattr(process, "_polconvert_compute",
                            lambda tmpl, logdir, n_ifs: [1.0] * n_ifs)
        monkeypatch.setattr(process.find_idi_mod, "find_idi_with_time",
                            lambda idi_files, aipstime, verbose=False: "ez041a_1_1.IDI1")
        monkeypatch.setattr(process.glob, "glob",
                            lambda pat: [] if "PCONVERT" in pat else
                            (["ez041a_1_1.IDI1"] if "IDI" in pat else []))

        assert process.polconvert(exp) is False          # never converges

        # One usable scan (No0018) x 2 refants x 3 time ranges x 5 doweights x 4 timeavgs x
        # 3 chanavgs = 360, cut short by the attempt budget.
        assert len(attempts) == process._POLCONVERT_MAX_ATTEMPTS
        assert attempts[0][0] == "Mc"                                  # strongest fringe first
        assert len({a[1] for a in attempts}) == 3                      # the three time ranges
        assert {a[3] for a in attempts} == {10, 20, 30, 60}            # time averaging (s)
        assert {a[4] for a in attempts} == {8, 16, 32}                 # channel averaging
        # Cheapest-first within a time range, doweight slowest-varying and ordered by how
        # often it produced the solutions found by hand.
        assert attempts[0][2:] == (0.1, 10, 8)
        assert attempts[1][2:] == (0.1, 10, 16)
        assert attempts[3][2:] == (0.1, 20, 8)
        assert attempts[12][2:] == (0.01, 10, 8)
        assert attempts[36][2:] == (0.0001, 10, 8)
        assert attempts[48][2:] == (1.0, 10, 8)
        assert [a[2] for a in attempts[:60]] == \
            [w for w in (0.1, 0.01, 0.001, 0.0001, 1.0) for _ in range(12)]

    def test_the_second_reference_antenna_is_a_fallback_not_a_sweep(self, tmp_path, monkeypatch):
        """Mc is exhausted before O8 is tried at all, and no third antenna ever is."""
        exp = _make_exp(tmp_path)
        monkeypatch.setattr(process, "_POLCONVERT_MAX_ATTEMPTS", 1000)   # let it run to the end
        attempts: list[str] = []

        monkeypatch.setattr(process, "_write_polconvert_template",
                            lambda exp, ref_idi, lin_ants, refant, *a, **k:
                            attempts.append(refant) or Path('polconvert_inputs.toml'))
        monkeypatch.setattr(process, "_polconvert_compute",
                            lambda tmpl, logdir, n_ifs: [1.0] * n_ifs)
        monkeypatch.setattr(process.find_idi_mod, "find_idi_with_time",
                            lambda idi_files, aipstime, verbose=False: "ez041a_1_1.IDI1")
        monkeypatch.setattr(process.glob, "glob",
                            lambda pat: [] if "PCONVERT" in pat else
                            (["ez041a_1_1.IDI1"] if "IDI" in pat else []))

        assert process.polconvert(exp) is False
        assert len(attempts) == 1 * 2 * 3 * 5 * 4 * 3 == 360
        assert set(attempts) == {"Mc", "O8"}
        assert attempts[:180] == ["Mc"] * 180 and attempts[180:] == ["O8"] * 180


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


# --- what PolConvert is handed, and what the log says about it ---------------------------

def _captured_info():
    """A (messages, remove) pair capturing loguru INFO output."""
    from loguru import logger
    messages: list[str] = []
    sink = logger.add(lambda m: messages.append(m.record['message']), level='INFO')
    return messages, lambda: logger.remove(sink)


class TestAntennaCase:
    """PolConvert matches names against the FITS-IDI ANTENNA table, which is upper case."""

    def test_pc_ants_uppercases(self):
        assert process._pc_ants(['Ef', 'Jb', 'T6']) == ['EF', 'JB', 'T6']

    def test_template_gets_uppercase_antennas(self, tmp_path, monkeypatch):
        # exp.antennas carries the mixed-case vex spelling; the file PolConvert reads must not.
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        out = process._write_polconvert_template(
            exp, 'ez041a_1_1.IDI1', ['Ef'], 'Mc', ['Wb', 'O8'], [1, 2],
            [0, 17, 0, 0, 0, 17, 5, 0], chan_avg=1, time_avg=20, solve_weight=0.0,
            logdir='polconvert_logs', output_file=tmp_path / 'pc.toml')
        content = out.read_text()
        assert "linants = ['EF']" in content
        assert "refant = 'MC'" in content
        assert "exclude_ants = ['WB', 'O8']" in content
        # and no mixed-case spelling leaked through ('O8' is unchanged by upper(), so it is
        # not evidence either way and is left out).
        for name in ("'Ef'", "'Mc'", "'Wb'"):
            assert name not in content


class TestReadableTimeRange:
    def test_within_one_day(self):
        assert process._aips_timerange_str([0, 17, 0, 0, 0, 17, 5, 0]) == '17:00:00 - 17:05:00'

    def test_across_midnight_keeps_the_day(self):
        assert process._aips_timerange_str([0, 23, 58, 0, 1, 0, 3, 0]) == \
            '0/23:58:00 - 1/00:03:00'

    def test_unexpected_shape_falls_back_to_the_raw_list(self):
        assert process._aips_timerange_str([1, 2, 3]) == '[1, 2, 3]'


class TestRunnerIsolation:
    """The child process is where PolConvert's crashes have to stay, and it must be headless."""

    def test_runs_headless_so_the_qt_plugin_is_never_loaded(self, monkeypatch):
        # matplotlib's default backend here is 'qtagg'; loading it aborts the interpreter with
        # "symbol lookup error: ... libqsvgicon.so: undefined symbol: _ZdlPvm" (rc=127) before
        # PolConvert computes anything, which reads as a failed solution but is not one.
        seen: dict = {}

        def fake_run(cmd, **kwargs):
            seen.update(kwargs)
            return _FakeProc(0)

        monkeypatch.setattr(process.subprocess, "run", fake_run)
        process._run_polconvert_cli(Path("in.toml"), "--compute")
        assert seen['env']['MPLBACKEND'] == 'Agg'

    def test_child_output_is_not_captured_so_it_streams_live(self, monkeypatch):
        seen: dict = {}

        def fake_run(cmd, **kwargs):
            seen.update(kwargs)
            return _FakeProc(0)

        monkeypatch.setattr(process.subprocess, "run", fake_run)
        process._run_polconvert_cli(Path("in.toml"), "--compute")
        assert not seen.get('capture_output')
        assert 'stdout' not in seen and 'stderr' not in seen   # inherited from this process


class TestFringeSnrSummary:
    """The table is rendered in-process from the FRINGE.PEAKS files, and can never raise."""

    def _fake_polconvert_module(self, monkeypatch, func):
        """Puts a stub 'evn_support.polconvert' in sys.modules for the local import to find."""
        import sys
        import types
        pkg = types.ModuleType('evn_support')
        pkg.__path__ = []
        mod = types.ModuleType('evn_support.polconvert')
        mod.print_fringe_snr_table = func
        monkeypatch.setitem(sys.modules, 'evn_support', pkg)
        monkeypatch.setitem(sys.modules, 'evn_support.polconvert', mod)

    def test_calls_the_printer_with_the_log_directory(self, monkeypatch):
        called: list[str] = []
        self._fake_polconvert_module(monkeypatch, lambda logdir: called.append(logdir))
        process._log_fringe_snr_table('polconvert_logs')
        assert called == ['polconvert_logs']

    def test_a_missing_module_is_not_an_error(self, monkeypatch):
        import sys
        monkeypatch.setitem(sys.modules, 'evn_support.polconvert', None)   # forces ImportError
        process._log_fringe_snr_table('polconvert_logs')                   # must not raise

    def test_a_printer_that_blows_up_is_not_an_error(self, monkeypatch):
        def boom(logdir):
            raise RuntimeError('no FRINGE.PEAKS here')
        self._fake_polconvert_module(monkeypatch, boom)
        process._log_fringe_snr_table('polconvert_logs')                   # must not raise


class TestComputeAttemptIsAnnounced:
    """Each --compute says what it is about to try, so a failed search is still readable."""

    def test_inputs_are_logged_before_each_attempt(self, tmp_path, monkeypatch):
        exp = _make_exp(tmp_path)
        monkeypatch.setattr(process, "_write_polconvert_template",
                            lambda *a, **k: Path('polconvert_inputs.toml'))
        monkeypatch.setattr(process, "_polconvert_compute",
                            lambda tmpl, logdir, n_ifs: [19.0] * n_ifs)
        monkeypatch.setattr(process, "_polconvert_apply", lambda tmpl, idis: True)
        monkeypatch.setattr(process.find_idi_mod, "find_idi_with_time",
                            lambda idi_files, aipstime, verbose=False: "ez041a_1_1.IDI1")
        monkeypatch.setattr(process.glob, "glob",
                            lambda pat: [] if "PCONVERT" in pat else
                            (["ez041a_1_1.IDI1"] if "IDI" in pat else []))
        monkeypatch.setattr(exp, "store", lambda: None)

        messages, remove = _captured_info()
        try:
            assert process.polconvert(exp) is True
        finally:
            remove()

        launch = [m for m in messages if m.startswith('PolConvert --compute [attempt')]
        assert len(launch) == 1
        assert 'refant=MC' in launch[0]                  # upper case, as PolConvert gets it
        assert "linants=['EF']" in launch[0]
        assert ' - ' in launch[0]                        # the readable time range
        assert 'doweight=' in launch[0] and 'timeavg=' in launch[0] and 'chanavg=' in launch[0]
