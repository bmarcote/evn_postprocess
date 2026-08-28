"""Tests for shell_command's terminal output: quiet mode and the stderr colouring."""
from __future__ import annotations

import pytest

from evn_postprocess import process
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


class TestStderrWarningColour:
    """stderr is red, except the lines a caller declares to be notes rather than failures.

    getdata.pl reports everything through perl's ``warn()``, so its whole stderr is yellow-
    worthy noise except the genuine failures; on a multi-phase-centre run the "Ignoring
    <job line>" notes alone reach hundreds of thousands of lines, and a screen of red reads
    like a failed step (see process._GETDATA_WARN_RE).
    """

    def _colours(self, tmp_path, monkeypatch, capsys, line, warn_re):
        monkeypatch.chdir(tmp_path)
        utils.shell_command(f"echo {line!r} 1>&2", stderr_warn_re=warn_re)
        return capsys.readouterr().err

    @pytest.mark.parametrize('line', ['Ignoring - 1/EM164B_No1.cor_JN-1 - GOOD DONE PROD',
                                      'Skipping /ccs/expr/EM164B/sub12',
                                      '**** Warning: Duplicate job ids',
                                      "Warning: Permanently added 'ccs' to the known hosts."])
    def test_getdata_notes_are_yellow(self, tmp_path, monkeypatch, capsys, line):
        out = self._colours(tmp_path, monkeypatch, capsys, line, process._GETDATA_WARN_RE)
        assert '\033[33m' in out and '\033[31m' not in out

    @pytest.mark.parametrize('line', ['Could not open em164b.vix: No such file',
                                      'scp: /ccs/expr/x: Permission denied',
                                      'No job IDs read!'])
    def test_real_getdata_failures_stay_red(self, tmp_path, monkeypatch, capsys, line):
        out = self._colours(tmp_path, monkeypatch, capsys, line, process._GETDATA_WARN_RE)
        assert '\033[31m' in out and '\033[33m' not in out

    def test_without_a_pattern_all_stderr_stays_red(self, tmp_path, monkeypatch, capsys):
        out = self._colours(tmp_path, monkeypatch, capsys, 'Ignoring something', None)
        assert '\033[31m' in out and '\033[33m' not in out
