"""Tests for evn_postprocess.source_classify (heuristic source classification)."""
import datetime as dt
from pathlib import Path

import pytest
from astropy import units as u
from astropy import coordinates as coord

from evn_postprocess import experiment
from evn_postprocess import experiment_state as es
from evn_postprocess import source_classify as sc


def make_exp(expname, source_names, scan_sequence, scan_duration_s=120, durations=None,
             tmp_path=None):
    """Builds a minimal Experiment: sources at distinct coords, scans following *scan_sequence*.

    *durations* optionally maps a source name to its per-scan duration (seconds), so
    phase-referencing patterns (short calibrator scans, long target scans) can be built.
    """
    dirs = experiment.Dirs(logs=Path('logs'), plots=Path('plots'), pipeline=Path('pipeline'),
                           pipe_in=Path('pipeline/in'), pipe_out=Path('pipeline/out'),
                           pipe_temp=Path('antenna_files'))
    exp = experiment.Experiment(expname, dt.date(2026, 4, 10), 'tester', dirs)
    for i, name in enumerate(source_names):
        exp.sources.append(experiment.Source(
            name=name, coordinates=coord.SkyCoord(ra=(10 + i) * u.deg, dec=(20 + i) * u.deg),
            type=experiment.SourceType.other))
    start = dt.datetime(2026, 4, 10, 2, 0, 0)
    for i, src in enumerate(scan_sequence):
        dur = (durations or {}).get(src, scan_duration_s)
        exp.scans.append(experiment.Scan(scanno=f"No{i:04d}", starttime=start,
                                         duration_s=dur, source=src,
                                         stations_scheduled=('Ef', 'Wb')))
        start += dt.timedelta(seconds=dur)
    if tmp_path is not None:
        exp.exp_toml = es.load_toml(tmp_path / f"{expname.lower()}.toml")
    return exp


class FakeCatalog:
    """RFCCatalog stand-in: knows a fixed set of names, no position matches."""
    def __init__(self, known):
        self.known = set(known)
        self.sources = []  # no position matching in tests

    def get_source(self, name):
        return object() if name in self.known else None


@pytest.fixture
def no_catalogue(monkeypatch):
    monkeypatch.setattr(sc, '_load_rfc_catalogue', lambda: None)


def with_catalogue(monkeypatch, known):
    monkeypatch.setattr(sc, '_load_rfc_catalogue', lambda: FakeCatalog(known))


# --------------------------------------------------------------------- rules

def test_nme_experiment_all_targets(no_catalogue):
    exp = make_exp('N24C1', ['SRC1', 'SRC2'], ['SRC1', 'SRC2', 'SRC1'])
    assert sc.classify_sources(exp) == {'SRC1': 'target', 'SRC2': 'target'}


def test_ftest_experiment_all_targets(no_catalogue):
    exp = make_exp('F25X2', ['SRC1'], ['SRC1'])
    assert sc.classify_sources(exp) == {'SRC1': 'target'}


def test_bundled_fringe_finder(no_catalogue):
    exp = make_exp('EB101', ['3C345', 'TGT'], ['3C345'] + ['TGT'] * 10)
    assert sc.classify_sources(exp)['3C345'] == 'fringefinder'


def test_phase_referencing_with_catalogue(monkeypatch):
    """Known cal bracketing an unknown target: J-cal -> calibrator, target -> target."""
    with_catalogue(monkeypatch, known={'J1846+3229'})
    seq = ['J1846+3229', 'TGT1', 'J1846+3229', 'TGT1', 'J1846+3229'] * 4
    exp = make_exp('EB101', ['J1846+3229', 'TGT1'], seq,
                   durations={'J1846+3229': 60, 'TGT1': 240})
    guesses = sc.classify_sources(exp)
    assert guesses['J1846+3229'] == 'calibrator'
    assert guesses['TGT1'] == 'target'


def test_known_source_few_scans_is_fringefinder(monkeypatch):
    with_catalogue(monkeypatch, known={'J0000+0000', 'J1846+3229'})
    seq = ['J0000+0000'] + ['J1846+3229', 'TGT1'] * 10
    exp = make_exp('EB101', ['J0000+0000', 'J1846+3229', 'TGT1'], seq)
    assert sc.classify_sources(exp)['J0000+0000'] == 'fringefinder'


def test_degraded_mode_scan_statistics_only(no_catalogue):
    """Without catalogue: few scans -> fringefinder, alternation winner -> calibrator, rest target."""
    seq = ['FF1'] + ['CAL1', 'TGT1', 'CAL1', 'TGT1', 'CAL1'] * 3
    exp = make_exp('EB101', ['FF1', 'CAL1', 'TGT1'], seq, durations={'CAL1': 60, 'TGT1': 240})
    guesses = sc.classify_sources(exp)
    assert guesses['FF1'] == 'fringefinder'
    assert guesses['CAL1'] == 'calibrator'
    assert guesses['TGT1'] == 'target'


def test_only_observed_sources_classified(no_catalogue):
    exp = make_exp('EB101', ['TGT1', 'NEVEROBS'], ['TGT1'] * 5)
    assert 'NEVEROBS' not in sc.classify_sources(exp)


def test_explicit_other_in_toml_not_reclassified(tmp_path, no_catalogue):
    """A user-declared type = "other" is a decision, not an absence of one."""
    (tmp_path / 'eb101.toml').write_text('[sources."ODD1"]\ntype = "other"\n')
    exp = make_exp('EB101', ['ODD1', 'TGT1'], ['ODD1', 'TGT1'] * 5, tmp_path=tmp_path)
    guesses = sc.classify_sources(exp)
    assert 'ODD1' not in guesses and 'TGT1' in guesses


def test_typed_sources_not_touched(no_catalogue):
    exp = make_exp('EB101', ['CAL1', 'TGT1'], ['CAL1', 'TGT1'] * 5)
    exp.sources['CAL1'].type = experiment.SourceType.calibrator
    guesses = sc.classify_sources(exp)
    assert 'CAL1' not in guesses and 'TGT1' in guesses


# ------------------------------------------------------------ apply + record

def test_apply_records_guesses_in_toml(tmp_path, no_catalogue):
    exp = make_exp('EB101', ['CAL1', 'TGT1'], ['CAL1', 'TGT1', 'CAL1'] * 5,
                   durations={'CAL1': 60, 'TGT1': 240}, tmp_path=tmp_path)
    guesses = sc.apply_classification(exp)
    assert guesses and exp.sources['TGT1'].type == experiment.SourceType.target
    saved = es.load_toml(tmp_path / 'eb101.toml')
    assert saved.sources['TGT1'].type == 'target'
    assert saved.sources['TGT1'].guessed is True
    assert saved.sources['CAL1'].type == 'calibrator'


def test_complete_toml_means_noop(tmp_path, no_catalogue):
    tomlpath = tmp_path / 'eb101.toml'
    tomlpath.write_text('[sources."CAL1"]\ntype = "calibrator"\n[sources."TGT1"]\ntype = "target"\n')
    exp = make_exp('EB101', ['CAL1', 'TGT1'], ['CAL1', 'TGT1'] * 5, tmp_path=tmp_path)
    # emulate inputs._apply_toml having applied the types:
    exp.sources['CAL1'].type = experiment.SourceType.calibrator
    exp.sources['TGT1'].type = experiment.SourceType.target
    before = tomlpath.read_text()
    assert sc.apply_classification(exp) == {}
    assert tomlpath.read_text() == before  # no rewrite


def test_catalogue_failure_degrades(monkeypatch):
    def boom():
        raise RuntimeError('no catalogue')
    monkeypatch.setattr(sc, '_load_rfc_catalogue', lambda: None)  # already the contract:
    # _load_rfc_catalogue itself never raises; simulate its degraded return value.
    exp = make_exp('EB101', ['TGT1'], ['TGT1'] * 6)
    assert sc.classify_sources(exp)['TGT1'] == 'target'
