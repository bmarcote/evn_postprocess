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
from evn_postprocess.distribution.jive import JiveDistributor, COMMENTS_SENTINEL


LETTER = '''\
Dear PI,

your data are ready.

Further remarks:

- Automatic remark already present.

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

def test_comments_injected_after_anchor(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.piletter').write_text(LETTER)
    exp = make_exp(tmp_path)
    exp.exp_toml.record_comments(general='Good observation overall.',
                                 stations={'Wb': es.StationComment('minor', 'Missed one hour.'),
                                           'Ef': es.StationComment('success', '')})
    assert JiveDistributor()._apply_comments_to_letter(exp) is True
    text = (tmp_path / 'eb101.piletter').read_text()
    assert COMMENTS_SENTINEL in text
    assert text.index('Further remarks:') < text.index(COMMENTS_SENTINEL)
    assert 'Good observation overall.' in text
    assert 'Wb: Missed one hour. (minor issues)' in text
    assert 'Ef:' not in text.split(COMMENTS_SENTINEL)[1].split('- Automatic')[0]  # success+empty skipped


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
