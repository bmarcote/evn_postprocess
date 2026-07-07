"""Tests for evn_postprocess.eevn (e-EVN synchronisation barriers)."""
from pathlib import Path

from evn_postprocess import eevn


class ExpStub:
    def __init__(self, expname, run_exps, eevnname=None):
        self.expname = expname
        self.eEVNname = eevnname
        self._run = run_exps

    def eEVN_experiments(self):
        return self._run


def setup_run(tmp_path, monkeypatch, current='EA100'):
    """Two-experiment e-EVN run layout: cwd = tmp/<current>, sibling in tmp/<other>."""
    for name in ('EA100', 'EB200'):
        (tmp_path / name).mkdir()
    monkeypatch.chdir(tmp_path / current)


def test_marker_write_and_path(tmp_path, monkeypatch):
    setup_run(tmp_path, monkeypatch)
    exp = ExpStub('EA100', ['EA100', 'EB200'], 'EA100')
    eevn.mark_fitsidi_ready(exp)
    assert (tmp_path / 'EA100' / 'ea100.fitsidi_ready').exists()


def test_siblings_mapping(tmp_path, monkeypatch):
    setup_run(tmp_path, monkeypatch)
    exp = ExpStub('EA100', ['EA100', 'EB200'], 'EA100')
    mapping = eevn.siblings(exp)
    assert mapping['EA100'] == Path('.')
    assert mapping['EB200'] == Path('..') / 'EB200'


def test_fitsidi_barrier_waits_then_opens(tmp_path, monkeypatch):
    setup_run(tmp_path, monkeypatch)
    exp = ExpStub('EA100', ['EA100', 'EB200'], 'EA100')
    assert set(eevn.fitsidi_missing(exp)) == {'EA100', 'EB200'}
    eevn.mark_fitsidi_ready(exp)
    assert eevn.fitsidi_missing(exp) == ['EB200']
    (tmp_path / 'EB200' / 'eb200.fitsidi_ready').write_text('done\n')
    assert eevn.fitsidi_missing(exp) == []


def test_non_eevn_has_no_barrier(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = ExpStub('EB101', ['EB101'])
    eevn.mark_fitsidi_ready(exp)
    assert eevn.fitsidi_missing(exp) == []


def test_final_antab_barrier(tmp_path, monkeypatch):
    setup_run(tmp_path, monkeypatch, current='EB200')
    exp = ExpStub('EB200', ['EA100', 'EB200'], 'EA100')
    assert eevn.final_antab_available(exp) is False
    pipe_in = tmp_path / 'EA100' / 'pipeline' / 'in'
    pipe_in.mkdir(parents=True)
    (pipe_in / 'ea100.antab').write_text('antab\n')
    assert eevn.final_antab_available(exp) is True
    assert eevn.leader_antab_dir(exp) == Path('..') / 'EA100' / 'pipeline' / 'in'


def test_final_antab_toplevel_fallback(tmp_path, monkeypatch):
    setup_run(tmp_path, monkeypatch, current='EB200')
    exp = ExpStub('EB200', ['EA100', 'EB200'], 'EA100')
    (tmp_path / 'EA100' / 'ea100.antab').write_text('antab\n')
    assert eevn.final_antab_available(exp) is True
    assert eevn.leader_antab_dir(exp) == Path('..') / 'EA100'
