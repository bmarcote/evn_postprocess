"""The record of in-place MS operations lives in the experiment state (never in the MS), so polswap /
1-bit scaling are never applied twice to the same MS, while a re-created MS starts clean."""
import shutil
from pathlib import Path
from types import SimpleNamespace
from evn_postprocess import process


class _Exp(SimpleNamespace):
    def store(self):
        self.stores = getattr(self, 'stores', 0) + 1


def test_record_read_back_and_store(tmp_path):
    ms = tmp_path / "a.ms"
    ms.mkdir()
    exp = _Exp(applied_msops={})
    assert process.applied_ms_operations(exp, ms) == []
    process.record_ms_operation(exp, ms, 'polswap:Wb')
    process.record_ms_operation(exp, ms, 'polswap:Wb')
    process.record_ms_operation(exp, ms, 'onebit:Kn')
    assert process.applied_ms_operations(exp, ms) == ['polswap:Wb', 'onebit:Kn']
    assert exp.stores == 3
    assert list(ms.iterdir()) == []  # nothing written into the MS


def test_recreated_ms_ignores_the_old_record(tmp_path):
    ms = tmp_path / "a.ms"
    ms.mkdir()
    keep = tmp_path / "keep"  # hold the old inode so the new directory cannot reuse it
    exp = _Exp(applied_msops={})
    process.record_ms_operation(exp, ms, 'polswap:Wb')
    ms.rename(keep)
    ms.mkdir()
    assert process.applied_ms_operations(exp, ms) == []
    shutil.rmtree(keep)


def _onebit_exp(onebit: list[str]):
    return _Exp(antennas=SimpleNamespace(onebit=onebit), applied_msops={},
                correlator_passes=[SimpleNamespace(msfile=Path('a.ms'))], vixfile='x.vix')


def test_onebit_is_not_scaled_twice(monkeypatch):
    scaled = []
    monkeypatch.setattr(process.mstools, 'scale1bit', lambda ms, ants: scaled.append(ants))
    monkeypatch.setattr(process, 'applied_ms_operations', lambda exp, ms: ['onebit:Kn,Sr'])
    assert process.onebit(_onebit_exp(['Sr', 'Kn'])) is True
    assert scaled == []


def test_onebit_with_a_different_set_on_a_scaled_ms_fails(monkeypatch):
    scaled = []
    monkeypatch.setattr(process.mstools, 'scale1bit', lambda ms, ants: scaled.append(ants))
    monkeypatch.setattr(process, 'applied_ms_operations', lambda exp, ms: ['onebit:Kn'])
    assert process.onebit(_onebit_exp(['Kn', 'Sr'])) is False
    assert scaled == []


def test_onebit_first_run_scales_and_records(monkeypatch):
    scaled, recorded = [], []
    monkeypatch.setattr(process.mstools, 'scale1bit', lambda ms, ants: scaled.append(ants))
    monkeypatch.setattr(process, 'applied_ms_operations', lambda exp, ms: [])
    monkeypatch.setattr(process, 'record_ms_operation', lambda exp, ms, entry: recorded.append(entry))
    assert process.onebit(_onebit_exp(['Kn'])) is True
    assert scaled == [['Kn']] and recorded == ['onebit:Kn']


def test_scale1bit_scales_only_data_by_default(tmp_path):
    """Same behaviour as scripts/scale1bit.py: DATA scaled, WEIGHT only with scale_weights (-w)."""
    import numpy as np
    from casacore import tables as pt
    from evn_postprocess.mstools import operations
    ms = tmp_path / "s.ms"
    desc = pt.maketabdesc([pt.makescacoldesc('ANTENNA1', 0), pt.makescacoldesc('ANTENNA2', 0),
                           pt.makearrcoldesc('DATA', 0j, ndim=2, shape=[1, 1]),
                           pt.makearrcoldesc('WEIGHT', 0.0, ndim=1, shape=[1])])
    with pt.table(str(ms), desc, nrow=2, ack=False) as t:
        t.putcol('ANTENNA1', np.array([0, 0]))
        t.putcol('ANTENNA2', np.array([1, 0]))  # one cross (1-bit x 2-bit), one auto
        t.putcol('DATA', np.ones((2, 1, 1), dtype=complex))
        t.putcol('WEIGHT', np.ones((2, 1)))
    adesc = pt.maketabdesc([pt.makescacoldesc('NAME', '')])
    with pt.table(str(ms / "ANTENNA"), adesc, nrow=2, ack=False) as a:
        a.putcol('NAME', ['KN', 'EF'])
    with pt.table(str(ms), readonly=False, ack=False) as t:
        t.putkeyword('ANTENNA', 'Table: ' + str(ms / "ANTENNA"))
    operations.scale1bit(ms, 'Kn')
    with pt.table(str(ms), ack=False) as t:
        data, weight = t.getcol('DATA'), t.getcol('WEIGHT')
    assert np.isclose(data[0, 0, 0].real, np.sqrt(np.pi / 2.0 / 1.1329552)) and data[1, 0, 0] == 1
    assert np.all(weight == 1.0)


def test_configured_refant_from_policy_wins_and_is_filtered(monkeypatch):
    from evn_postprocess import experiment_state
    from evn_postprocess.policy import Policy
    monkeypatch.setattr(experiment_state, 'attached_toml', lambda exp: (_ for _ in ()).throw(FileNotFoundError()))
    exp = SimpleNamespace(policy=Policy(refant=['O8', 'XX', 'ef']), antennas=SimpleNamespace(observed=['Ef', 'O8']))
    assert process._configured_refant(exp) == ['O8', 'Ef']
