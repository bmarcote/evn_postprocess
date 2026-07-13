"""Issue 17: batch-mode end-to-end sweep of the workflow engine.

Runs run_workflow over stubbed step commands (no external binaries, no servers) and
asserts the engine contract: no prompt is ever raised in batch mode, the postpipe
pause writes REVIEW_REQUIRED and exits cleanly, the run resumes to completion on the
next invocation, failures leave a resumable state, and the public step/exec names are
preserved.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from evn_postprocess import experiment
from evn_postprocess import workflow


STEP_NAMES = ['lisfiles', 'checklis', 'j2ms2', 'standardplots', 'msops', 'tconvert',
              'polconvert', 'post_polconvert', 'standardplots2', 'antab', 'pipeinputs',
              'pipeline', 'postpipe', 'prearchive', 'distribute']


def make_exp(tmp_path, expname='EB101'):
    dirs = experiment.Dirs(logs=Path('logs'), plots=Path('plots'), pipeline=Path('pipeline'),
                           pipe_in=Path('pipeline/in'), pipe_out=Path('pipeline/out'),
                           pipe_temp=Path('antenna_files'))
    exp = experiment.Experiment(expname, dt.date(2026, 4, 10), 'tester', dirs)
    exp.write_log_file = lambda *a, **k: None
    return exp


@pytest.fixture
def engine(tmp_path, monkeypatch):
    """run_workflow with every step command stubbed to succeed, in batch mode."""
    monkeypatch.chdir(tmp_path)
    # run_workflow marks .done on the shared _WORKFLOW_STEPS Task objects; reset them so
    # each test starts from a clean slate (these module globals persist across tests).
    for step in workflow._WORKFLOW_STEPS:
        step.done = False
    executed = []

    def stub(name):
        def command(exp):
            executed.append(name)
            return True
        return command

    for step in workflow._WORKFLOW_STEPS:
        if step.name != 'initialize':
            monkeypatch.setattr(workflow, step.command, stub(step.name))

    def forbidden(*a, **k):
        raise AssertionError('interactive prompt raised in batch mode')
    monkeypatch.setattr('builtins.input', forbidden)
    monkeypatch.setattr(workflow.utils, 'notify', lambda *a, **k: None)
    monkeypatch.setattr(workflow, '_setup_loguru', lambda *a, **k: None)
    # Output-staleness validation is exercised by its own suite; here the steps are
    # stubs that produce no files, so it must not reset the done flags on resume.
    monkeypatch.setattr(workflow, '_validate_outputs', lambda *a, **k: None)
    workflow.set_batch_mode(True)
    yield executed
    workflow.set_batch_mode(False)


def test_step_names_preserved():
    assert [s.name for s in workflow._WORKFLOW_STEPS if s.name != 'initialize'] == STEP_NAMES


def test_exec_commands_preserved():
    for cmd in ('makelis', 'j2ms2', 'standardplots', 'tconvert', 'polconvert', 'antab',
                'pipe', 'append', 'archive-fits'):
        assert cmd in workflow._EXEC_COMMANDS


def test_batch_run_pauses_at_postpipe_and_resumes(tmp_path, engine):
    exp = make_exp(tmp_path)
    assert workflow.run_workflow(exp) is True
    # Paused after postpipe: marker written, later steps not executed.
    assert (tmp_path / workflow.REVIEW_FLAG_FILENAME).exists()
    assert 'postpipe' in engine and 'prearchive' not in engine
    done = {s.name for s in exp.steps if s.done}
    assert 'postpipe' in done and 'distribute' not in done
    # Resume: the remaining steps complete without a prompt.
    engine.clear()
    assert workflow.run_workflow(exp) is True
    assert engine[-2:] == ['prearchive', 'distribute']
    assert all(s.done for s in exp.steps)


def test_batch_failure_leaves_resumable_state(tmp_path, engine, monkeypatch):
    exp = make_exp(tmp_path)
    calls = {'n': 0}

    def failing_tconvert(e):
        calls['n'] += 1
        return calls['n'] > 1  # fail the first time, succeed on the retry

    monkeypatch.setattr(workflow, 'tconvert', failing_tconvert)
    assert workflow.run_workflow(exp) is False
    done = {s.name for s in exp.steps if s.done}
    assert 'msops' in done and 'tconvert' not in done  # state intact up to the failure
    # Resume: continues from tconvert, pauses at postpipe as usual.
    assert workflow.run_workflow(exp) is True
    assert 'postpipe' in {s.name for s in exp.steps if s.done}


def test_no_archive_flag_skips_distribute_step(tmp_path, engine):
    exp = make_exp(tmp_path)
    workflow.run_workflow(exp, archive=False)   # pause at postpipe
    workflow.run_workflow(exp, archive=False)   # resume to the end
    assert 'distribute' not in engine
    assert all(s.name != 'distribute' for s in exp.steps)


def test_archive_alias_still_resolves_to_distribute(tmp_path, engine):
    exp = make_exp(tmp_path)
    # The deprecated 'archive' step name maps to 'distribute'.
    valid, _ = workflow.validate_steps('archive')
    assert valid
    assert workflow._resolve_step_alias('archive') == 'distribute'


def test_skip_steps_bypasses_named_steps(tmp_path, engine, monkeypatch):
    exp = make_exp(tmp_path)
    # Attach a config toml that skips two steps.
    from evn_postprocess import experiment_state
    toml = experiment_state.load_toml(tmp_path / 'eb101.toml')
    monkeypatch.setattr(toml, 'skip_steps', ['polconvert', 'standardplots2'])
    exp.exp_toml = toml
    workflow.run_workflow(exp)          # pause at postpipe
    workflow.run_workflow(exp)          # resume to the end
    assert 'polconvert' not in engine and 'standardplots2' not in engine
    assert 'j2ms2' in engine            # non-skipped steps still run


def test_step_failure_notifies_and_is_resumable(tmp_path, engine, monkeypatch):
    exp = make_exp(tmp_path)
    notified = []
    monkeypatch.setattr(workflow.utils, 'notify',
                        lambda subject, body: notified.append((subject, body)))
    monkeypatch.setattr(workflow, 'tconvert', lambda e: False)  # hard failure
    assert workflow.run_workflow(exp) is False                  # -> caller exits non-zero
    assert any('FAILED' in body for _, body in notified)        # operator notified
    assert 'tconvert' not in {s.name for s in exp.steps if s.done}  # resumable
