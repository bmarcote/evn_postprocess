"""Tests for the JiveDistributor delivery pieces (Issue 14): PI-info guarantee and
review-comments injection into the PI letter. The archive chain itself is exercised
only through mocks (it runs external JIVE commands)."""
import datetime as dt
from pathlib import Path

import pytest

from evn_postprocess import experiment
from evn_postprocess import experiment_state as es
from evn_postprocess import workflow
from evn_postprocess.distribution import DistributionError
from evn_postprocess.distribution.jive import JiveDistributor, COMMENTS_SENTINEL, NME_PREFIXES
from evn_postprocess.retrieval import RetrievalError


LETTER = '''\
Dear PI,

your data are ready.

Further remarks:

- Automatic remark already present.

Remarks on individual stations:

Wb:
Ef:

Best regards,
'''


def make_exp(tmp_path):
    dirs = experiment.Dirs(logs=Path('logs'), plots=Path('plots'), pipeline=Path('pipeline'),
                           pipe_in=Path('pipeline/in'), pipe_out=Path('pipeline/out'),
                           pipe_temp=Path('antenna_files'))
    exp = experiment.Experiment('EB101', dt.date(2026, 4, 10), 'tester', dirs)
    exp.exp_toml = es.load_toml(tmp_path / 'eb101.toml')
    return exp


@pytest.fixture(autouse=True)
def _interactive_mode():
    workflow.set_batch_mode(False)
    yield
    workflow.set_batch_mode(False)


# ------------------------------------------------------------------- PI info

def test_pi_from_toml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.toml').write_text('[[pi]]\nname = "Jane"\nemail = "jane@x.edu"\n')
    exp = make_exp(tmp_path)
    JiveDistributor()._ensure_pi_info(exp)
    assert exp.pi[0].name == 'Jane' and exp.pi[0].email == 'jane@x.edu'


def test_pi_missing_batch_fails_clearly(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    workflow.set_batch_mode(True)
    exp = make_exp(tmp_path)
    with pytest.raises(DistributionError) as excinfo:
        JiveDistributor()._ensure_pi_info(exp)
    assert 'PI' in str(excinfo.value) and '[[pi]]' in str(excinfo.value)


def test_pi_missing_interactive_prompts_and_persists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    answers = iter(['John Smith', 'john@inst.edu'])
    monkeypatch.setattr('builtins.input', lambda prompt='': next(answers))
    exp = make_exp(tmp_path)
    JiveDistributor()._ensure_pi_info(exp)
    assert exp.pi[0].email == 'john@inst.edu'
    saved = es.load_toml(tmp_path / 'eb101.toml')
    assert saved.pis[0].name == 'John Smith' and saved.pis[0].email == 'john@inst.edu'


# ----------------------------------------------------------- letter injection

def test_comments_appended_to_station_lines(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.piletter').write_text(LETTER)
    exp = make_exp(tmp_path)
    exp.exp_toml.record_comments(general='Good observation overall.',
                                 stations={'Wb': es.StationComment('minor', 'Missed one hour.'),
                                           'Ef': es.StationComment('success', '')})
    assert JiveDistributor()._apply_comments_to_letter(exp) is True
    text = (tmp_path / 'eb101.piletter').read_text()
    # General note goes after the 'Further remarks:' anchor via the sentinel.
    assert COMMENTS_SENTINEL in text
    assert text.index('Further remarks:') < text.index(COMMENTS_SENTINEL)
    assert 'Good observation overall.' in text
    # Per-station note appended to the matching antenna line, not a separate list.
    assert 'Wb: Missed one hour. (minor issues)' in text
    assert text.index('Remarks on individual stations:') < text.index('Wb: Missed one hour.')
    # success + empty note leaves the Ef line untouched.
    assert 'Ef:\n' in text


def test_reduced_bandwidth_note_not_appended(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.piletter').write_text(LETTER)
    exp = make_exp(tmp_path)
    # Wb only has the reduced-bandwidth note (already stated under Further remarks);
    # Ef has that note plus a real one.
    exp.exp_toml.record_comments(
        stations={'Wb': es.StationComment('minor', 'Observed with reduced bandwidth (6/8 subbands).'),
                  'Ef': es.StationComment('minor',
                                          'Missed one hour. Observed with reduced bandwidth (6/8 subbands).')})
    assert JiveDistributor()._apply_comments_to_letter(exp) is True
    text = (tmp_path / 'eb101.piletter').read_text()
    assert 'reduced bandwidth' not in text
    assert 'Wb:\n' in text  # bandwidth-only note dropped, line untouched
    assert 'Ef: Missed one hour. (minor issues)' in text


def test_comments_injection_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.piletter').write_text(LETTER)
    exp = make_exp(tmp_path)
    exp.exp_toml.record_comments(general='Note.')
    JiveDistributor()._apply_comments_to_letter(exp)
    JiveDistributor()._apply_comments_to_letter(exp)
    assert (tmp_path / 'eb101.piletter').read_text().count(COMMENTS_SENTINEL) == 1


def test_comments_without_letter_warns_not_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    exp.exp_toml.record_comments(general='Note.')
    assert JiveDistributor()._apply_comments_to_letter(exp) is False


def test_no_comments_means_untouched_letter(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.piletter').write_text(LETTER)
    exp = make_exp(tmp_path)
    assert JiveDistributor()._apply_comments_to_letter(exp) is True
    assert (tmp_path / 'eb101.piletter').read_text() == LETTER


# --------------------------------------------------- .jex source protection & contacts

# A representative .jex parse result: PI + co-I contacts and a schedsrc with a protected
# target ('X'), a public fringe-finder ('P'), and a reference/calibrator whose protection
# is a guess ('X?', applied as protected with a warning).
JEXP_INFO = {
    'piname': 'Jane Doe', 'pimail': 'jane@x.edu',
    'coname': 'John Roe', 'coimail': 'john@y.edu',
    'schedsrc': 'J1234+5678 (T|X), 3C84 (F|P), J0555+3948 (R|X?)',
}

_FETCH = 'evn_postprocess.retrieval.jive.fetch_jexp_info'


def _raise_retrieval(*_a, **_k):
    raise RetrievalError("no .jex file on the server")


def test_source_protection_skips_nme(tmp_path, monkeypatch):
    # NME runs (name starts with N/F) need no PI/protection: the .jex is never fetched.
    monkeypatch.chdir(tmp_path)

    def boom(_expname):
        raise AssertionError("fetch_jexp_info must not be called for an NME")

    monkeypatch.setattr(_FETCH, boom)
    for prefix in NME_PREFIXES:
        exp = make_exp(tmp_path)
        exp.expname = f"{prefix}24L1"
        assert JiveDistributor()._apply_source_protection(exp) is True


def test_source_protection_returns_false_when_jex_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(_FETCH, _raise_retrieval)
    assert JiveDistributor()._apply_source_protection(make_exp(tmp_path)) is False


def test_source_protection_sets_contacts_and_flags(tmp_path, monkeypatch):
    from astropy import coordinates as coord
    from astropy import units as u
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(_FETCH, lambda _expname: dict(JEXP_INFO))
    exp = make_exp(tmp_path)
    # A source already known from the vex/MS is updated in place; the others are appended.
    exp.sources.append(experiment.Source('J1234+5678',
                                          coord.SkyCoord(ra=1 * u.deg, dec=2 * u.deg, frame='icrs')))
    assert JiveDistributor()._apply_source_protection(exp) is True
    # PI + co-I recovered onto exp and persisted to the toml (deduped by email).
    assert {p.email for p in exp.pi} == {'jane@x.edu', 'john@y.edu'}
    saved = es.load_toml(tmp_path / 'eb101.toml')
    assert {pi.email for pi in saved.pis} == {'jane@x.edu', 'john@y.edu'}
    # per-source type + archive protection from schedsrc.
    assert exp.sources['J1234+5678'].type == experiment.SourceType.target
    assert exp.sources['J1234+5678'].protected is True
    assert exp.sources['3C84'].type == experiment.SourceType.fringefinder
    assert exp.sources['3C84'].protected is False           # 'P' = public
    assert exp.sources['J0555+3948'].type == experiment.SourceType.calibrator
    assert exp.sources['J0555+3948'].protected is True      # 'X?' = protected, as a guess


def test_source_protection_no_duplicate_contact(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(_FETCH,
                        lambda _e: {'piname': 'Jane Doe', 'pimail': 'jane@x.edu', 'schedsrc': '3C84 (F|P)'})
    exp = make_exp(tmp_path)
    exp.pi.append(experiment.PI('Jane Doe', 'jane@x.edu'))
    JiveDistributor()._apply_source_protection(exp)
    assert len([p for p in exp.pi if p.email == 'jane@x.edu']) == 1


def test_warn_manual_protection_prints_commands(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    JiveDistributor()._warn_manual_protection(make_exp(tmp_path))
    out = capsys.readouterr().out
    assert 'ACTION REQUIRED' in out
    assert 'EB101_260410' in out          # {EXPNAME}_{yymmdd} archive name
    assert 'auth_pipe.py' in out


def test_deliver_fails_and_warns_when_jex_unrecovered(tmp_path, monkeypatch, capsys):
    # All stages still run (stubbed to succeed), but because the .jex could not be
    # recovered a manual-protection error is printed at the end AND deliver() returns
    # False so the distribute step is flagged as failed for the operator to resolve.
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.piletter').write_text(LETTER)
    monkeypatch.setattr(_FETCH, _raise_retrieval)
    for fn in ('set_credentials', 'protect_experiment_files', 'archive',
               'send_letters', 'antenna_feedback', 'nme_report'):
        monkeypatch.setattr(f'evn_postprocess.process.{fn}', lambda _e: True)
    monkeypatch.setattr('evn_postprocess.process.print_exp',
                        lambda _e, display_in_terminal=True: True)
    monkeypatch.setattr('evn_postprocess.pipeline.archive', lambda _e: True)
    exp = make_exp(tmp_path)
    exp.pi.append(experiment.PI('Known', 'known@x.edu'))   # satisfy _ensure_pi_info
    assert JiveDistributor().deliver(exp) is False
    out = capsys.readouterr().out
    assert 'ACTION REQUIRED' in out and 'EB101_260410' in out
