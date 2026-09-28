"""Tests for the batch-mode helpers in evn_postprocess.workflow.

Covers:
  - the global batch-mode toggle (set_batch_mode/is_batch_mode);
  - the file-based review gate (_write_review_flag / _clear_review_flag);
  - the _signal_pause behaviour (writes a marker in batch mode, prints a Rich
    panel in interactive mode without writing the marker).
"""
from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from evn_postprocess import workflow
from evn_postprocess.policy import Policy


@pytest.fixture(autouse=True)
def _reset_batch_mode():
    """Make sure the global flags do not leak between tests."""
    workflow.set_batch_mode(False)
    workflow._note_operator_interaction()
    yield
    workflow.set_batch_mode(False)
    workflow._note_operator_interaction()


def _fake_exp(name: str = "TEST01") -> Mock:
    exp = Mock()
    exp.expname = name
    exp.policy = None
    exp.dirs.pipe_in = Path("pipeline/in")
    return exp


class TestBatchToggle:
    def test_default_is_interactive(self):
        assert workflow.is_batch_mode() is False

    def test_set_batch_mode_round_trip(self):
        workflow.set_batch_mode(True)
        assert workflow.is_batch_mode() is True
        workflow.set_batch_mode(False)
        assert workflow.is_batch_mode() is False


class TestReviewFlag:
    def test_write_creates_marker_with_step_and_reason(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _fake_exp()
        workflow._write_review_flag(exp, "msops", "weight_threshold missing")
        flag = tmp_path / workflow.REVIEW_FLAG_FILENAME
        assert flag.exists()
        text = flag.read_text()
        assert "step: msops" in text
        assert "experiment: TEST01" in text
        assert "weight_threshold missing" in text

    def test_clear_is_idempotent(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _fake_exp()
        # Clearing without a marker should not raise.
        workflow._clear_review_flag(exp)
        # Now create one and clear it.
        workflow._write_review_flag(exp, "postpipe", "review me")
        workflow._clear_review_flag(exp)
        assert not (tmp_path / workflow.REVIEW_FLAG_FILENAME).exists()


class TestReviewPause:
    def test_batch_mode_writes_marker_and_quits(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        workflow.set_batch_mode(True)
        exp = _fake_exp()
        assert workflow._review_pause(exp, "postpipe") == 'quit'
        flag = tmp_path / workflow.REVIEW_FLAG_FILENAME
        assert flag.exists()
        assert "step: postpipe" in flag.read_text()

    def test_interactive_mode_does_not_create_marker(self, tmp_path: Path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        workflow.set_batch_mode(False)
        monkeypatch.setattr('builtins.input', lambda prompt='': '')   # Enter -> approve
        exp = _fake_exp()
        assert workflow._review_pause(exp, "postpipe") is None
        assert not (tmp_path / workflow.REVIEW_FLAG_FILENAME).exists()
        # The panel mentions the step name; no need to assert exact ANSI markup.
        out = capsys.readouterr().out
        assert "postpipe" in out
        # The pause has to be impossible to scroll past, and to say what can be answered.
        assert "THE RUN IS WAITING FOR YOU" in out
        assert "How to answer" in out and "pipeline" in out
        # The options are listed once on screen, in their own box, not twice.
        assert out.count("re-run the EVN Pipeline") == 1


def _operator_left(monkeypatch, seconds: float = 0) -> None:
    """Pretends the operator was last at the terminal OPERATOR_AWAY_SECONDS + *seconds* ago."""
    monkeypatch.setattr(workflow, '_OPERATOR_LAST_SEEN',
                        time.monotonic() - workflow.OPERATOR_AWAY_SECONDS - seconds)


class TestOperatorPresenceSuppressesTheOutsideAnnouncements:
    """Errors and prompts reach Mattermost and the desktop only once the run has gone on for
    OPERATOR_AWAY_SECONDS without the operator. Right after they launched the command,
    answered a prompt or closed a dashboard, they are in front of the terminal already."""

    @staticmethod
    def _record(monkeypatch) -> tuple[list, list]:
        """Captures the two out-of-terminal announcements instead of sending them."""
        chat, desktop = [], []
        monkeypatch.setattr('evn_postprocess.comms.notify_operator',
                            lambda *a, **k: chat.append(a))
        monkeypatch.setattr('evn_postprocess.utils.notify',
                            lambda *a, **k: desktop.append(a))
        return chat, desktop

    def test_nothing_is_announced_right_after_the_dashboard(self, tmp_path: Path, monkeypatch,
                                                            capsys):
        monkeypatch.chdir(tmp_path)
        chat, desktop = self._record(monkeypatch)
        monkeypatch.setattr('builtins.input', lambda prompt='': '')
        workflow._note_operator_interaction()             # what open_pipeline_dashboard triggers
        assert workflow._review_pause(_fake_exp(), "postpipe") is None
        assert chat == [] and desktop == []
        # The terminal still says everything: only the outside announcements are dropped.
        assert "THE RUN IS WAITING FOR YOU" in capsys.readouterr().out

    def test_both_are_sent_once_the_operator_has_been_away(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        chat, desktop = self._record(monkeypatch)
        monkeypatch.setattr('builtins.input', lambda prompt='': '')
        _operator_left(monkeypatch)
        assert workflow._review_pause(_fake_exp(), "postpipe") is None
        assert len(chat) == 1 and "paused after 'postpipe'" in chat[0][1]
        assert len(desktop) == 1

    def test_batch_mode_always_notifies(self, tmp_path: Path, monkeypatch):
        """Nobody is at the terminal in batch mode, so these are the only signal there."""
        monkeypatch.chdir(tmp_path)
        chat, desktop = self._record(monkeypatch)
        workflow.set_batch_mode(True)
        workflow._note_operator_interaction()
        assert workflow._review_pause(_fake_exp(), "postpipe") == 'quit'
        assert len(chat) == 1 and len(desktop) == 1

    def test_a_failure_right_after_the_start_stays_in_the_terminal(self, monkeypatch):
        chat, desktop = self._record(monkeypatch)
        workflow._note_operator_interaction()             # what run_workflow does at start
        workflow._notify_step_failure(_fake_exp(), 'j2ms2', 'it broke', 12.0)
        assert chat == [] and desktop == []

    def test_a_failure_after_a_long_unattended_run_is_announced(self, monkeypatch):
        chat, desktop = self._record(monkeypatch)
        _operator_left(monkeypatch)
        workflow._notify_step_failure(_fake_exp(), 'pipeline', 'it broke', 3600.0)
        assert len(chat) == 1 and "failed at 'pipeline'" in chat[0][1]
        assert len(desktop) == 1

    def test_the_timer_starts_again_at_each_answer(self, monkeypatch):
        """Answering a prompt puts the operator back at the terminal."""
        _operator_left(monkeypatch)
        assert workflow._operator_away("x") is True
        monkeypatch.setattr('builtins.input', lambda prompt='': '')
        assert workflow._ask_review_confirmation("postpipe") is None
        assert workflow._operator_away("x") is False

    def test_the_threshold_is_five_minutes(self, monkeypatch):
        assert workflow.OPERATOR_AWAY_SECONDS == 300
        _operator_left(monkeypatch, seconds=-10)          # 4 min 50 s
        assert workflow._operator_away("x") is False
        _operator_left(monkeypatch, seconds=10)           # 5 min 10 s
        assert workflow._operator_away("x") is True


class TestPauseSteps:
    def test_default_is_postpipe(self):
        exp = _fake_exp()
        exp.policy = None
        assert workflow._pause_steps(exp) == (workflow.DEFAULT_PAUSE_AFTER,)

    def test_policy_overrides_and_resolves_aliases(self):
        exp = _fake_exp()
        exp.policy = Policy(pause_after=["pipeline", "archive"])
        # 'archive' is the deprecated alias of 'distribute'.
        assert workflow._pause_steps(exp) == ("pipeline", "distribute")


class TestContinueAfterAntabEditor:
    """Once antab_editor.py is closed, the operator decides whether the pipeline may start.
    An experiment whose .antab files already existed never gets asked: antfiles returns
    before the editor is ever opened, so a re-run walks straight to the next step."""

    def test_enter_continues(self, monkeypatch):
        monkeypatch.setattr('builtins.input', lambda prompt='': '')
        assert workflow._ask_continue_after_antab(_fake_exp()) is True

    def test_stop_stops(self, monkeypatch):
        monkeypatch.setattr('builtins.input', lambda prompt='': 'stop')
        assert workflow._ask_continue_after_antab(_fake_exp()) is False

    def test_the_question_is_hard_to_miss(self, monkeypatch, capsys):
        monkeypatch.setattr('builtins.input', lambda prompt='': '')
        workflow._ask_continue_after_antab(_fake_exp())
        out = capsys.readouterr().out
        assert 'ANTAB EDITOR CLOSED' in out
        assert 'run the EVN Pipeline' in out and 'stop here' in out

    def test_an_unknown_answer_asks_again(self, monkeypatch, capsys):
        answers = iter(['maybe', 'stop'])
        monkeypatch.setattr('builtins.input', lambda prompt='': next(answers))
        assert workflow._ask_continue_after_antab(_fake_exp()) is False
        assert "Answer with Enter (continue) or 'stop'" in capsys.readouterr().out

    def test_batch_mode_never_asks(self, monkeypatch):
        """Nobody is there to answer, so an unattended run carries on."""
        def no_input(prompt=''):
            raise AssertionError("batch mode must not prompt")
        monkeypatch.setattr('builtins.input', no_input)
        workflow.set_batch_mode(True)
        assert workflow._ask_continue_after_antab(_fake_exp()) is True

    def test_no_stdin_continues(self, monkeypatch):
        def no_stdin(prompt=''):
            raise EOFError
        monkeypatch.setattr('builtins.input', no_stdin)
        assert workflow._ask_continue_after_antab(_fake_exp()) is True

    def test_an_existing_antab_skips_the_editor_and_the_question(self, tmp_path: Path,
                                                                 monkeypatch):
        """The .antab files are already there: antfiles returns before opening the editor,
        so nothing is asked and the run goes straight on to the next step."""
        def no_input(prompt=''):
            raise AssertionError("nothing may be asked when the editor was not opened")
        monkeypatch.setattr('builtins.input', no_input)
        pipe_in = tmp_path / "pipeline" / "in"
        pipe_in.mkdir(parents=True)
        (pipe_in / "testexp.antab").write_text("")
        exp = _fake_exp()
        exp.dirs.pipe_in = pipe_in
        assert workflow.antfiles(exp) is True
