"""Tests for where the calibrator scans go in the correlator passes (lisfiles.get_passes_from_lisfiles).

The rule: a multi-phase-centre run (vex scans with several phase centres) keeps the calibrator
scans only in the pipeline pass(es); every other run keeps them in all passes, also when the
passes are reloaded.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from evn_postprocess import lisfiles


def _job(scan: int, source: str) -> str:
    """One active .lis job line, with the source as the .cor suffix (as in a multi-phase-centre .lis)."""
    return (f"+ 40863/EXP_No{scan:04d}.cor_{source}   - No{scan:04d} No{scan:04d} 64 2.0 Xpol 44 GOOD DONE "
            f"PROD prod exp.vix \"TRUNK r2303\" -- eo:setup_ref_station=Ef\n")


def _write_lis(path: Path, target: str) -> None:
    """Writes a .lis with one calibrator scan (CAL), one scan on *target* and one on a target named CAL_FIELD."""
    path.write_text(f"EXP exp.vix Prod {path.stem}.ms\n\n" + _job(1, 'CAL') + _job(2, target) + _job(3, 'CAL_FIELD'))


def _experiment(phase_centers: tuple[str, ...]) -> SimpleNamespace:
    """A minimal experiment: one scan with the given phase centres, CAL as its only calibrator."""
    scans = [SimpleNamespace(source='FIELD', phase_centers=phase_centers)]
    mapping = {'FIELD': list(phase_centers)} if len(phase_centers) > 1 else {}
    return SimpleNamespace(expname='EXP', eEVNname=None, correlator_passes=[], phase_center_sources=mapping,
                           scans=scans, multi_phase_center=bool(mapping), store=lambda: None,
                           sources=SimpleNamespace(calibrator=['CAL'], fringefinder=[]))


def _active_sources(path: Path) -> list[str]:
    """The .cor suffixes (sources) of the active job lines of a .lis file."""
    return [line.split()[1].split('.cor_')[1] for line in path.read_text().splitlines() if line.startswith('+ ')]


def _setup(tmp_path, monkeypatch, phase_centers):
    """Writes exp_1.lis / exp_2.lis, runs get_passes_from_lisfiles twice (initial load + reload)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(lisfiles.process, 'aggregate_sources_from_passes', lambda exp: None)
    _write_lis(tmp_path / 'exp_1.lis', 'T1')
    _write_lis(tmp_path / 'exp_2.lis', 'T2')
    exp = _experiment(phase_centers)
    assert lisfiles.get_passes_from_lisfiles(exp)
    assert lisfiles.get_passes_from_lisfiles(exp)
    return tmp_path / 'exp_1.lis', tmp_path / 'exp_2.lis'


def test_multi_phase_center_keeps_calibrators_only_in_first_pass(tmp_path, monkeypatch):
    first, second = _setup(tmp_path, monkeypatch, ('FIELD', 'T1', 'T2'))
    assert _active_sources(first) == ['CAL', 'T1', 'CAL_FIELD']
    assert _active_sources(second) == ['T2', 'CAL_FIELD']  # only the exact calibrator name is dropped


def test_standard_multi_pass_keeps_calibrators_in_all_passes(tmp_path, monkeypatch):
    first, second = _setup(tmp_path, monkeypatch, ())
    assert _active_sources(first) == ['CAL', 'T1', 'CAL_FIELD']
    assert _active_sources(second) == ['CAL', 'T2', 'CAL_FIELD']
