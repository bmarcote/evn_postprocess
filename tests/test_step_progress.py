"""Tests for the per-pass progress footer of the long-running steps, and for the
concurrency of `append_antab`.

On a multi-phase-centre run (hundreds of correlator passes) the tools' own messages scroll
past far too fast to tell how far a step has got, so from more than
`utils.PASS_PROGRESS_MIN_PASSES` passes each of these steps pins a Rich progress bar under
its output and tags every echoed line with the pass it came from (`utils.pass_progress` +
`shell_command(line_prefix=...)`). At or below the threshold nothing changes.

`append_antab` additionally used to run every pass one after the other; it now runs them at
once, since a unit of work only ever writes the FITS-IDI files of its own pass.
"""
from __future__ import annotations

import datetime as dt
import threading
import time
from pathlib import Path

import pytest
from rich import progress

from evn_postprocess import experiment
from evn_postprocess import process
from evn_postprocess import utils


def make_exp(tmp_path: Path, passes: int) -> experiment.Experiment:
    """An experiment with *passes* correlator passes and their .lis files on disk."""
    dirs = experiment.Dirs(logs=tmp_path / 'logs', plots=tmp_path / 'plots',
                           pipeline=tmp_path / 'pipeline', pipe_in=tmp_path / 'pipeline/in',
                           pipe_out=tmp_path / 'pipeline/out', pipe_temp=tmp_path / 'antenna_files')
    for directory in (dirs.logs, dirs.pipe_in, dirs.pipe_out):
        directory.mkdir(parents=True, exist_ok=True)
    exp = experiment.Experiment('ES124', dt.date(2026, 4, 10), 'tester', dirs, no_lag=True)
    exp.correlator_passes = [
        experiment.CorrelatorPass(Path(f'es124_{i}_1.lis'), Path(f'es124-pass{i}.ms'),
                                  f'es124_{i}_1.IDI', True) for i in range(1, passes + 1)]
    for a_pass in exp.correlator_passes:
        a_pass.lisfile.touch()
    return exp


class Recorder:
    """Every stubbed `utils.shell_command` call, plus how many ran at once at the peak."""

    def __init__(self, fail_on: str = ''):
        self.calls: list[dict] = []
        self.peak = 0
        self._live = 0
        self._lock = threading.Lock()
        self._fail_on = fail_on

    def __call__(self, command, parameters=None, **kwargs):
        with self._lock:
            self._live += 1
            self.peak = max(self.peak, self._live)
            self.calls.append({'command': command, 'parameters': parameters, **kwargs})
        time.sleep(0.05)  # long enough for the concurrency to be observable
        with self._lock:
            self._live -= 1
        if self._fail_on and self._fail_on in str(parameters):
            raise ValueError(f"{command} exited with code 1")
        return ''

    def prefixes(self) -> set[str]:
        """The `line_prefix` of every recorded call ('' when the call passed none)."""
        return {call.get('line_prefix', '') for call in self.calls}


def record_progress(monkeypatch) -> list[bool]:
    """Captures the `disable` flag every Rich Progress is built with."""
    disabled: list[bool] = []
    real_progress = progress.Progress

    def spy(*columns, **kwargs):
        disabled.append(kwargs.get('disable', False))
        return real_progress(*columns, **kwargs)

    monkeypatch.setattr(progress, 'Progress', spy)
    return disabled


def count_advances(monkeypatch) -> list[int]:
    """Captures every advance of every Rich Progress task."""
    advanced: list[int] = []
    real_progress = progress.Progress

    class Counting(real_progress):
        def advance(self, task_id, advance=1):
            advanced.append(advance)
            super().advance(task_id, advance)

    monkeypatch.setattr(progress, 'Progress', Counting)
    return advanced


@pytest.fixture
def shell(monkeypatch, tmp_path) -> Recorder:
    """Stubs out `utils.shell_command`, recording every call, and works in tmp_path."""
    monkeypatch.chdir(tmp_path)
    recorder = Recorder()
    monkeypatch.setattr(utils, 'shell_command', recorder)
    return recorder


@pytest.fixture
def antab(monkeypatch, tmp_path):
    """The FITS-IDI/ANTAB layout `append_antab` needs, with the FITS checks stubbed out.

    `check_consistency` is asked twice with a different intent: once up front with
    `verbose=False` ("is this already done?", which must be False or the step returns early)
    and once at the end with its default ("did it work?", which must be True). The stub tells
    the two apart by that flag, which is what makes the whole step run deterministically
    without a real FITS file.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(process, 'check_consistency',
                        lambda fitsfile, verbose=True: verbose)

    def build(passes: int, antabs: int = 1) -> experiment.Experiment:
        exp = make_exp(tmp_path, passes)
        for i in range(1, passes + 1):
            Path(f'es124_{i}_1.IDI1').touch()
            Path(f'es124_{i}_1.IDI2').touch()
        if antabs == 1:
            (exp.dirs.pipe_in / 'es124.antab').touch()
        else:
            for i in range(1, antabs + 1):
                (exp.dirs.pipe_in / f'es124_{i}.antab').touch()
        return exp

    return build


class TestGetdataProgress:
    """getdata.pl runs one subprocess per pass; past the threshold they get a footer."""

    def test_no_bar_at_the_threshold(self, tmp_path, shell, monkeypatch):
        shown = record_progress(monkeypatch)
        process.getdata(make_exp(tmp_path, utils.PASS_PROGRESS_MIN_PASSES))
        assert shown == [True]

    def test_bar_past_the_threshold(self, tmp_path, shell, monkeypatch):
        shown = record_progress(monkeypatch)
        process.getdata(make_exp(tmp_path, utils.PASS_PROGRESS_MIN_PASSES + 1))
        assert shown == [False]

    def test_the_bar_counts_every_pass(self, tmp_path, shell, monkeypatch):
        advanced = count_advances(monkeypatch)
        assert process.getdata(make_exp(tmp_path, 8)) is True
        assert sum(advanced) == 8

    def test_lines_are_tagged_with_their_pass_only_behind_the_bar(self, tmp_path, shell):
        process.getdata(make_exp(tmp_path, 8))
        assert shell.prefixes() == {f"[es124_{i}_1] " for i in range(1, 9)}

    def test_no_tagging_below_the_threshold(self, tmp_path, shell):
        process.getdata(make_exp(tmp_path, 3))
        assert shell.prefixes() == {''}


class TestJ2ms2Progress:
    """j2ms2 gets the same footer; the auxiliary lag MS is not a pass and is not counted."""

    def test_no_bar_at_the_threshold(self, tmp_path, shell, monkeypatch):
        shown = record_progress(monkeypatch)
        process.j2ms2(make_exp(tmp_path, utils.PASS_PROGRESS_MIN_PASSES))
        assert shown == [True]

    def test_bar_past_the_threshold(self, tmp_path, shell, monkeypatch):
        shown = record_progress(monkeypatch)
        process.j2ms2(make_exp(tmp_path, utils.PASS_PROGRESS_MIN_PASSES + 1))
        assert shown == [False]

    def test_the_bar_counts_every_pass(self, tmp_path, shell, monkeypatch):
        advanced = count_advances(monkeypatch)
        assert process.j2ms2(make_exp(tmp_path, 8)) is True
        assert sum(advanced) == 8

    def test_lines_are_tagged_with_their_pass(self, tmp_path, shell):
        process.j2ms2(make_exp(tmp_path, 8))
        assert shell.prefixes() == {f"[es124_{i}_1] " for i in range(1, 9)}


class TestAppendAntabRunsThePassesAtOnce:
    """The passes are independent (each writes only its own FITS-IDI), so they run together."""

    def test_passes_run_concurrently(self, antab, monkeypatch):
        recorder = Recorder()
        monkeypatch.setattr(utils, 'shell_command', recorder)
        assert process.append_antab(antab(passes=4)) is True
        assert recorder.peak > 1, "the passes still ran one after the other"

    def test_never_more_at_once_than_the_io_ceiling(self, antab, monkeypatch):
        monkeypatch.setattr(utils, 'MAX_PASS_IO_WORKERS', 2)
        recorder = Recorder()
        monkeypatch.setattr(utils, 'shell_command', recorder)
        assert process.append_antab(antab(passes=6)) is True
        # Exactly at the ceiling: below it the cap would not be what bounds them.
        assert recorder.peak == 2

    def test_each_pass_gets_its_own_files(self, antab, shell):
        process.append_antab(antab(passes=3))
        tsys = [c for c in shell.calls if c['command'] == 'append_tsys.py']
        assert len(tsys) == 3
        for i, call in enumerate(sorted(tsys, key=lambda c: c['parameters'][2]), start=1):
            assert call['parameters'][2:] == [f'es124_{i}_1.IDI1', f'es124_{i}_1.IDI2']

    def test_the_gain_curve_goes_only_into_the_first_chunk(self, antab, shell):
        process.append_antab(antab(passes=3))
        gc_files = sorted(c['parameters'][2] for c in shell.calls if c['command'] == 'append_gc.py')
        assert gc_files == ['es124_1_1.IDI1', 'es124_2_1.IDI1', 'es124_3_1.IDI1']

    def test_several_antab_files_map_one_per_pass(self, antab, shell):
        process.append_antab(antab(passes=3, antabs=3))
        for call in shell.calls:
            antabfile, first_idi = call['parameters'][1], call['parameters'][2]
            assert Path(antabfile).name == f"es124_{first_idi.split('_')[1]}.antab"

    def test_a_single_antab_file_covers_every_pass(self, antab, shell):
        process.append_antab(antab(passes=3, antabs=1))
        assert {Path(c['parameters'][1]).name for c in shell.calls} == {'es124.antab'}

    def test_a_failing_tool_does_not_abandon_the_other_passes(self, antab, monkeypatch):
        recorder = Recorder(fail_on='es124_2_1')
        monkeypatch.setattr(utils, 'shell_command', recorder)
        # The tools' exit codes are not the verdict: the final consistency check is, and the
        # stub keeps saying the tables are there, so the step still succeeds.
        assert process.append_antab(antab(passes=3)) is True
        assert {c['parameters'][2].split('_')[1] for c in recorder.calls} == {'1', '2', '3'}

    def test_the_step_fails_when_the_tables_never_arrive(self, antab, shell, monkeypatch):
        monkeypatch.setattr(process, 'check_consistency', lambda fitsfile, verbose=True: False)
        assert process.append_antab(antab(passes=3)) is False


class TestAppendAntabProgress:
    """Same footer rule as the other per-pass steps, counted in units of work."""

    def test_no_bar_at_the_threshold(self, antab, shell, monkeypatch):
        shown = record_progress(monkeypatch)
        process.append_antab(antab(passes=utils.PASS_PROGRESS_MIN_PASSES))
        assert shown == [True]

    def test_bar_past_the_threshold(self, antab, shell, monkeypatch):
        shown = record_progress(monkeypatch)
        process.append_antab(antab(passes=utils.PASS_PROGRESS_MIN_PASSES + 1))
        assert shown == [False]

    def test_the_bar_counts_every_pass(self, antab, shell, monkeypatch):
        advanced = count_advances(monkeypatch)
        assert process.append_antab(antab(passes=8)) is True
        assert sum(advanced) == 8

    def test_lines_are_tagged_with_their_pass(self, antab, shell):
        process.append_antab(antab(passes=8))
        assert shell.prefixes() == {f"[es124_{i}_1] " for i in range(1, 9)}

    def test_a_single_pass_is_not_tagged(self, antab, shell):
        process.append_antab(antab(passes=1))
        assert shell.prefixes() == {''}
