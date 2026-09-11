"""Tests for the shared per-correlator-pass progress bar (utils.pass_progress).

The long per-pass steps (j2ms2, tConvert, append antab, PolConvert) run several passes at
once and let the tools keep printing; past a handful of passes those messages scroll by too
fast to tell how far the run is, so a Rich bar is pinned underneath them. Two things are
load-bearing and easy to break: the bar must be *disabled* (a no-op) for the handful-of-
passes case, and the tools' output must reach ``sys.stdout``/``sys.stderr`` as Rich has
replaced them while the bar is live, not as they were when the command started.
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

from rich import progress

from evn_postprocess import utils


def _record_progress(monkeypatch) -> list[bool]:
    """Captures the ``disable`` flag every rich Progress is built with (see test_pass_concurrency)."""
    disabled: list[bool] = []
    real_progress = progress.Progress

    def spy(*columns, **kwargs):
        disabled.append(kwargs.get('disable', False))
        return real_progress(*columns, **kwargs)

    monkeypatch.setattr(progress, 'Progress', spy)
    return disabled


class TestShowPassProgress:
    """The bar only appears once the passes outnumber what the eye can follow."""

    def test_at_or_below_the_threshold_there_is_no_bar(self):
        assert utils.show_pass_progress(1, minimum=5) is False
        assert utils.show_pass_progress(5, minimum=5) is False

    def test_above_the_threshold_the_bar_is_shown(self):
        assert utils.show_pass_progress(6, minimum=5) is True
        assert utils.show_pass_progress(658, minimum=5) is True

    def test_no_passes_at_all_is_never_a_bar(self):
        assert utils.show_pass_progress(0) is False

    def test_the_default_threshold_is_read_at_call_time(self, monkeypatch):
        # Like pass_workers' ceiling: overridable while the process is already running.
        monkeypatch.setattr(utils, 'PASS_PROGRESS_MIN_PASSES', 2)
        assert utils.show_pass_progress(2) is False
        assert utils.show_pass_progress(3) is True

    def test_an_explicit_minimum_wins_over_the_module_default(self, monkeypatch):
        monkeypatch.setattr(utils, 'PASS_PROGRESS_MIN_PASSES', 2)
        assert utils.show_pass_progress(3, minimum=10) is False


class TestPassProgress:
    """One Progress, the same columns as everywhere else, and one advance per pass."""

    def test_disabled_at_or_below_the_threshold(self, monkeypatch):
        shown = _record_progress(monkeypatch)
        with utils.pass_progress('[green]j2ms2', utils.PASS_PROGRESS_MIN_PASSES):
            pass
        assert shown == [True], "the bar must be disabled at or below the threshold"

    def test_enabled_above_the_threshold(self, monkeypatch):
        shown = _record_progress(monkeypatch)
        with utils.pass_progress('[green]j2ms2', utils.PASS_PROGRESS_MIN_PASSES + 1):
            pass
        assert shown == [False], "the bar must be enabled above the threshold"

    def test_an_explicit_minimum_is_honoured(self, monkeypatch):
        shown = _record_progress(monkeypatch)
        with utils.pass_progress('[green]j2ms2', 3, minimum=2):
            pass
        assert shown == [False]

    def test_only_one_bar_is_built(self, monkeypatch):
        shown = _record_progress(monkeypatch)
        with utils.pass_progress('[green]tConvert', 8):
            pass
        assert len(shown) == 1

    def test_the_columns_match_the_other_per_pass_bars(self, monkeypatch):
        built: list[tuple] = []
        real_progress = progress.Progress

        def spy(*columns, **kwargs):
            built.append(columns)
            return real_progress(*columns, **kwargs)

        monkeypatch.setattr(progress, 'Progress', spy)
        with utils.pass_progress('[green]tConvert', 8):
            pass
        assert [type(column) for column in built[0]] == [
            progress.SpinnerColumn, progress.TextColumn, progress.BarColumn,
            progress.MofNCompleteColumn, progress.TextColumn, progress.TimeElapsedColumn,
            progress.TimeRemainingColumn]

    def test_the_yielded_callable_advances_one_pass_per_call(self, monkeypatch):
        advanced: list[int] = []
        real_progress = progress.Progress

        class _Counting(real_progress):
            def advance(self, task_id, advance=1):
                advanced.append(advance)
                super().advance(task_id, advance)

        monkeypatch.setattr(progress, 'Progress', _Counting)
        with utils.pass_progress('[green]PolConvert', 8) as advance_one:
            for _ in range(8):
                advance_one()
        assert advanced == [1] * 8

    def test_a_disabled_bar_still_accepts_the_advances(self, monkeypatch):
        # Callers advance unconditionally; below the threshold that must simply do nothing.
        monkeypatch.setattr(utils, 'PASS_PROGRESS_MIN_PASSES', 100)
        with utils.pass_progress('[green]j2ms2', 3) as advance_one:
            for _ in range(3):
                advance_one()

    def test_the_task_ends_up_complete(self, monkeypatch):
        bars: list[progress.Progress] = []
        real_progress = progress.Progress

        def spy(*columns, **kwargs):
            bar = real_progress(*columns, **kwargs)
            bars.append(bar)
            return bar

        monkeypatch.setattr(progress, 'Progress', spy)
        with utils.pass_progress('[green]tConvert', 4) as advance_one:
            for _ in range(4):
                advance_one()
        assert bars[0].tasks[0].total == 4
        assert bars[0].tasks[0].completed == 4
        assert bars[0].tasks[0].description == '[green]tConvert'


class _Capture:
    """A minimal stand-in for ``sys.stdout`` that just remembers what was written."""

    def __init__(self):
        self.text = ''

    def write(self, text: str) -> int:
        self.text += text
        return len(text)

    def flush(self) -> None:
        pass


class TestShellCommandLinePrefix:
    """The prefix keeps interleaved pass output attributable, and stays on the terminal."""

    def test_the_prefix_reaches_the_terminal_only(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)  # contain the logs/commands.sh side effect
        logf = tmp_path / 'prefixed.log'
        out = utils.shell_command('echo hello-prefixed', logfile=logf, line_prefix='[p1] ')
        captured = capsys.readouterr()
        assert '[p1] hello-prefixed' in captured.out
        # Neither the captured return value nor the log file carry the terminal decoration.
        assert out == 'hello-prefixed\n'
        assert '[p1]' not in logf.read_text()

    def test_stderr_lines_are_prefixed_and_still_red(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        utils.shell_command("echo 'boom' 1>&2", line_prefix='[p2] ')
        err = capsys.readouterr().err
        assert '[p2] \033[31mboom' in err

    def test_no_prefix_by_default(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        utils.shell_command('echo plain-line')
        assert capsys.readouterr().out.startswith('plain-line')

    def test_a_quiet_run_stays_silent_even_with_a_prefix(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        out = utils.shell_command('echo quiet-prefixed', echo=False, line_prefix='[p3] ')
        assert capsys.readouterr().out == ''
        assert 'quiet-prefixed' in out


class TestShellCommandFollowsALateStdout:
    """Output goes to sys.stdout as it is when the line arrives, not as it was at start.

    A progress bar started while a command is already running replaces sys.stdout with a
    Rich proxy; a pump thread holding on to the pre-swap stream would write straight over
    the live region. The handshake below (two marker files, no sleeps that matter) makes the
    swap happen strictly after the pump threads exist and strictly before the line is
    printed.
    """

    def test_the_stream_is_resolved_at_write_time(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        started, go = Path('started'), Path('go')
        command = f"touch {started}; while [ ! -f {go} ]; do sleep 0.01; done; echo late-line"
        result: dict[str, str] = {}

        def run():
            result['out'] = utils.shell_command(command, echo=True)

        runner = threading.Thread(target=run)
        runner.start()
        original, late = sys.stdout, _Capture()
        try:
            deadline = time.monotonic() + 30
            while not started.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert started.exists(), "the command never started"
            sys.stdout = late  # the swap a live progress bar performs
            go.touch()
            runner.join(30)
        finally:
            sys.stdout = original
        assert not runner.is_alive()
        assert 'late-line' in late.text, "the pump kept writing to the pre-swap stdout"
        assert result['out'] == 'late-line\n'
