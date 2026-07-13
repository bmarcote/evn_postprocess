"""Tests for shell_command's quiet mode (Issue 7: lag-MS j2ms2 to log file only)."""
from __future__ import annotations

from evn_postprocess import utils


def test_echo_false_is_quiet_but_captures_and_logs(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)  # contain the logs/commands.sh side effect
    logf = tmp_path / 'quiet.log'
    out = utils.shell_command('echo hello-quiet', shell=True, logfile=logf, echo=False)
    captured = capsys.readouterr()
    # Not streamed to the terminal ...
    assert 'hello-quiet' not in captured.out
    # ... but still captured (returned) and teed to the log file.
    assert 'hello-quiet' in out
    assert 'hello-quiet' in logf.read_text()


def test_echo_true_streams_to_terminal(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    out = utils.shell_command('echo hello-loud', shell=True, echo=True)
    captured = capsys.readouterr()
    assert 'hello-loud' in captured.out
    assert 'hello-loud' in out
