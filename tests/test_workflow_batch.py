"""Tests for the batch-mode helpers in evn_postprocess.workflow.

Covers:
  - the global batch-mode toggle (set_batch_mode/is_batch_mode);
  - the file-based review gate (_write_review_flag / _clear_review_flag);
  - the _signal_pause behaviour (writes a marker in batch mode, prints a Rich
    panel in interactive mode without writing the marker).
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from evn_postprocess import workflow
from evn_postprocess.policy import Policy


@pytest.fixture(autouse=True)
def _reset_batch_mode():
    """Make sure the global flag does not leak between tests."""
    workflow.set_batch_mode(False)
    yield
    workflow.set_batch_mode(False)


def _fake_exp(name: str = "TEST01") -> Mock:
    exp = Mock()
    exp.expname = name
    exp.policy = None
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
        assert "postpipe" in capsys.readouterr().out


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
