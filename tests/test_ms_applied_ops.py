"""Tests for the record of in-place MS operations (polswap / 1-bit scaling must never be applied twice)."""
from pathlib import Path
from types import SimpleNamespace
import numpy as np
from casacore import tables as pt
from evn_postprocess import process
from evn_postprocess.mstools import misc


def make_table(tmp_path: Path) -> Path:
    path = tmp_path / "t.ms"
    desc = pt.maketabdesc([pt.makescacoldesc('X', 0)])
    with pt.table(str(path), desc, nrow=1, ack=False) as t:
        t.putcol('X', np.array([1]))
    return path


def test_record_and_read_back(tmp_path):
    ms = make_table(tmp_path)
    assert misc.applied_operations(ms) == []
    misc.record_operation(ms, 'polswap:Wb')
    misc.record_operation(ms, 'polswap:Wb')
    misc.record_operation(ms, 'onebit:Kn')
    assert misc.applied_operations(ms) == ['polswap:Wb', 'onebit:Kn']


def _onebit_exp(onebit: list[str]):
    return SimpleNamespace(antennas=SimpleNamespace(onebit=onebit),
                           correlator_passes=[SimpleNamespace(msfile=Path('a.ms'))], vixfile='x.vix')


def test_onebit_is_not_scaled_twice(monkeypatch):
    scaled = []
    monkeypatch.setattr(process.mstools, 'scale1bit', lambda ms, ants: scaled.append(ants))
    monkeypatch.setattr(process.mstools.misc, 'applied_operations', lambda ms: ['onebit:Kn,Sr'])
    monkeypatch.setattr(process.mstools.misc, 'record_operation', lambda ms, entry: None)
    assert process.onebit(_onebit_exp(['Sr', 'Kn'])) is True
    assert scaled == []


def test_onebit_with_a_different_set_on_a_scaled_ms_fails(monkeypatch):
    scaled = []
    monkeypatch.setattr(process.mstools, 'scale1bit', lambda ms, ants: scaled.append(ants))
    monkeypatch.setattr(process.mstools.misc, 'applied_operations', lambda ms: ['onebit:Kn'])
    assert process.onebit(_onebit_exp(['Kn', 'Sr'])) is False
    assert scaled == []


def test_onebit_first_run_scales_and_records(monkeypatch):
    scaled, recorded = [], []
    monkeypatch.setattr(process.mstools, 'scale1bit', lambda ms, ants: scaled.append(ants))
    monkeypatch.setattr(process.mstools.misc, 'applied_operations', lambda ms: [])
    monkeypatch.setattr(process.mstools.misc, 'record_operation', lambda ms, entry: recorded.append(entry))
    assert process.onebit(_onebit_exp(['Kn'])) is True
    assert scaled == [['Kn']] and recorded == ['onebit:Kn']
