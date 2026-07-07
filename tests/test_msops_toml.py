"""Tests for the toml-first (silent) MS-operations path in evn_postprocess.workflow.

Covers the Issue 6 contract: a complete [postprocess] section in the experiment toml
resolves every msops decision with no dialog, no dashboard notification, and applied
values matching the toml; and after msops the chosen parameters are written back so a
re-run is silent whatever path decided them.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest
from astropy import units as u
from astropy import coordinates as coord

from evn_postprocess import experiment
from evn_postprocess import experiment_state as es
from evn_postprocess import workflow


COMPLETE_POSTPROCESS = '''\
[postprocess]
weight_threshold = 0.85
polswap = ["Wb"]
polconvert = []
onebit = []
refant = ["Ef"]
'''


def make_exp(tmp_path, toml_text=None):
    dirs = experiment.Dirs(logs=Path('logs'), plots=Path('plots'), pipeline=Path('pipeline'),
                           pipe_in=Path('pipeline/in'), pipe_out=Path('pipeline/out'),
                           pipe_temp=Path('antenna_files'))
    exp = experiment.Experiment('EB101', dt.date(2026, 4, 10), 'tester', dirs)
    for name in ('Ef', 'Wb', 'Mc'):
        exp.antennas.append(experiment.Antenna(name=name))
    apass = experiment.CorrelatorPass(lisfile=Path('eb101.lis'), msfile=Path('eb101.ms'),
                                      fitsidifile='eb101_1_1.IDI', pipeline=True)
    exp.correlator_passes = [apass]
    tomlpath = tmp_path / 'eb101.toml'
    if toml_text is not None:
        tomlpath.write_text(toml_text)
    exp.exp_toml = es.load_toml(tomlpath)
    return exp


def test_toml_msops_available_gate(tmp_path):
    assert workflow._toml_msops_available(make_exp(tmp_path, COMPLETE_POSTPROCESS)) is True
    assert workflow._toml_msops_available(make_exp(tmp_path)) is False
    # Missing one list -> not available (absent != empty):
    partial = COMPLETE_POSTPROCESS.replace('onebit = []\n', '')
    assert workflow._toml_msops_available(make_exp(tmp_path, partial)) is False


def test_apply_toml_msops_matches_toml(tmp_path):
    exp = make_exp(tmp_path, COMPLETE_POSTPROCESS)
    workflow._apply_toml_msops(exp)
    assert exp.correlator_passes[0].flagged_weights.threshold == 0.85
    assert exp.antennas['Wb'].polswap is True
    assert exp.antennas['Ef'].polswap is False
    assert exp.antennas.polconvert == []
    assert exp.antennas.onebit == []
    assert exp.refant == ['Ef']


def test_apply_toml_msops_ignores_unknown_antenna(tmp_path):
    toml_text = COMPLETE_POSTPROCESS.replace('polswap = ["Wb"]', 'polswap = ["Zz"]')
    exp = make_exp(tmp_path, toml_text)
    workflow._apply_toml_msops(exp)  # must not raise
    assert exp.antennas.polswap == []


def test_msops_with_complete_toml_never_dialogs(tmp_path, monkeypatch):
    """The full msops step with a complete toml: no dialog, no notifier, ops applied."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path, COMPLETE_POSTPROCESS)

    def forbidden(*a, **k):
        raise AssertionError("interaction attempted despite a complete experiment toml")
    monkeypatch.setattr(workflow.dialog, 'make_dialog', forbidden)
    monkeypatch.setattr(workflow._comms, 'notify_dashboard_review', forbidden)
    monkeypatch.setattr(workflow.process, 'open_standardplot_files', forbidden)
    for fn in ('flag_weights', 'ysfocus', 'polswap', 'onebit', 'print_exp'):
        monkeypatch.setattr(workflow.process, fn, lambda exp, *a, **k: True)

    assert workflow.msops(exp) is True
    assert exp.correlator_passes[0].flagged_weights.threshold == 0.85
    assert exp.antennas['Wb'].polswap is True


def test_record_msops_writes_back(tmp_path):
    """Values decided by any path end up in the toml, making the next run silent."""
    exp = make_exp(tmp_path)  # no toml yet
    exp.correlator_passes[0].flagged_weights = experiment.FlagWeight(0.9, 3.2)
    exp.antennas['Mc'].polconvert = True
    exp.refant = ['Ef', 'Mc']
    workflow._record_msops_in_toml(exp)
    saved = es.load_toml(tmp_path / 'eb101.toml')
    assert saved.postprocess.weight_threshold == 0.9
    assert saved.postprocess.flagged_percent == 3.2
    assert saved.postprocess.polswap == []
    assert saved.postprocess.polconvert == ['Mc']
    assert saved.postprocess.refant == ['Ef', 'Mc']
    # And the round trip closes: the gate now passes.
    exp2 = make_exp(tmp_path.joinpath())  # reload from the same directory
    exp2.exp_toml = saved
    assert workflow._toml_msops_available(exp2) is True


def test_record_msops_never_blocks(tmp_path, monkeypatch):
    exp = make_exp(tmp_path)
    monkeypatch.setattr(exp.exp_toml, 'save', lambda *a, **k: (_ for _ in ()).throw(OSError('disk full')))
    workflow._record_msops_in_toml(exp)  # must not raise


# ------------------------------------------------- review confirmation (Issue 11)

def test_ask_review_confirmation_answers(monkeypatch):
    answers = iter(['', 'quit', 'nonsense', 'tconvert'])
    monkeypatch.setattr('builtins.input', lambda prompt='': next(answers))
    assert workflow._ask_review_confirmation() is None          # Enter -> approve
    assert workflow._ask_review_confirmation() == 'quit'
    assert workflow._ask_review_confirmation() == 'tconvert'    # invalid then valid step


def test_ask_review_confirmation_no_stdin(monkeypatch):
    def no_stdin(prompt=''):
        raise EOFError
    monkeypatch.setattr('builtins.input', no_stdin)
    assert workflow._ask_review_confirmation() == 'quit'


# ------------------------------------------------------ finalisation (Issue 12)

def test_record_final_in_toml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    exp.dirs.pipe_in.mkdir(parents=True)
    (exp.dirs.pipe_in / 'eb101.antab').write_text('antab\n')
    (tmp_path / 'polconvert_eb101.inp').write_text('inputs\n')
    exp.correlator_passes[0].flagged_weights = experiment.FlagWeight(0.9, 4.5)
    workflow._record_final_in_toml(exp)
    saved = es.load_toml(tmp_path / 'eb101.toml')
    assert saved.postprocess.antab_files == ['pipeline/in/eb101.antab']
    assert saved.postprocess.polconvert_input_files == ['polconvert_eb101.inp']
    assert saved.postprocess.flagged_percent == 4.5
