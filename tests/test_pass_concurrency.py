"""Tests for how many correlator passes are worked on at once (utils.pass_workers).

Most EVN experiments have a handful of passes, so the ceiling never binds for them; it
exists for the multi-phase-centre runs that reach several hundred. The worker count used
to be hard-coded separately at every call site (four 4s and a couple of 10s), which is
what these tests lock down: one shared helper, and no magic number left behind.
"""
from __future__ import annotations

import datetime as dt
import re
import threading
import time
from pathlib import Path

import pytest

from evn_postprocess import experiment
from loguru import logger
from rich import progress

from evn_postprocess import process
from evn_postprocess import utils


SRC = Path(utils.__file__).parent


class TestPassWorkers:
    """All the passes at once, up to the cap, and never zero."""

    def test_fewer_passes_than_the_cap_all_run_at_once(self):
        assert utils.pass_workers(2, cap=16) == 2
        assert utils.pass_workers(5, cap=16) == 5

    def test_more_passes_than_the_cap_are_bounded(self):
        # EM164B, a real multi-phase-centre experiment, has 658 correlator passes.
        assert utils.pass_workers(658, cap=16) == 16

    def test_never_returns_zero(self):
        # ThreadPoolExecutor rejects max_workers=0, and a step can be reached with no
        # passes set up (get_metadata_from_ms even asks for len(passes) - 1).
        assert utils.pass_workers(0) == 1
        assert utils.pass_workers(-1) == 1

    def test_io_cap_is_lower_than_the_cpu_one(self):
        # One subprocess per pass saturates the disk long before it saturates the cores.
        assert utils.MAX_PASS_IO_WORKERS <= utils.MAX_PASS_WORKERS

    def test_both_ceilings_can_be_overridden_at_runtime(self, monkeypatch):
        # The CPU ceiling is the default argument; it must still be read at call time, so
        # it behaves like the IO one rather than being frozen at import.
        monkeypatch.setattr(utils, 'MAX_PASS_WORKERS', 3)
        assert utils.pass_workers(658) == 3
        assert utils.pass_workers(658, cap=7) == 7

    def test_defaults_are_sane_on_any_machine(self):
        assert utils.MAX_PASS_WORKERS >= 1 and utils.MAX_PASS_IO_WORKERS >= 1


class TestNoHardCodedWorkerCounts:
    """Every per-pass pool goes through the shared helper, not a literal."""

    def test_process_and_pipeline_use_pass_workers(self):
        for module in ('process.py', 'pipeline.py'):
            source = (SRC / module).read_text()
            assert not re.search(r"PoolExecutor\(\s*max_workers\s*=\s*\d", source), (
                f"{module} still hard-codes a worker count; use utils.pass_workers()")
            assert not re.search(r"PoolExecutor\(\s*\)", source), (
                f"{module} leaves a pool unbounded; use utils.pass_workers()")


def make_exp(tmp_path: Path, passes: int) -> experiment.Experiment:
    """An experiment with *passes* correlator passes, none of them converted yet."""
    dirs = experiment.Dirs(logs=tmp_path / 'logs', plots=tmp_path / 'plots',
                           pipeline=tmp_path / 'pipeline', pipe_in=tmp_path / 'pipeline/in',
                           pipe_out=tmp_path / 'pipeline/out', pipe_temp=tmp_path / 'antenna_files')
    dirs.logs.mkdir(parents=True, exist_ok=True)
    exp = experiment.Experiment('ES124', dt.date(2026, 4, 10), 'tester', dirs)
    exp.correlator_passes = [
        experiment.CorrelatorPass(Path(f'es124_{i}_1.lis'), Path(f'es124-pass{i}.ms'),
                                  f'es124_{i}_1.IDI', True) for i in range(1, passes + 1)]
    return exp


class _Recorder:
    """Every stubbed tConvert call, plus the peak number of them in flight at once."""

    def __init__(self):
        self.calls: list[dict] = []
        self.peak = 0
        self._live = 0
        self._lock = threading.Lock()

        self._failing: set[str] = set()

    def fail_on(self, *lisfiles: str) -> str:
        """Makes the passes converting these .lis files raise, as tConvert would."""
        self._failing.update(lisfiles)
        return lisfiles[0]

    def __call__(self, command, parameters=None, **kwargs):
        with self._lock:
            self._live += 1
            self.peak = max(self.peak, self._live)
            self.calls.append({'parameters': parameters, **kwargs})
        time.sleep(0.05)  # long enough for the concurrency to be observable
        with self._lock:
            self._live -= 1
        if parameters[1] in self._failing:
            raise ValueError(f"{command} returned exit code 1")
        return ''


@pytest.fixture
def tconvert(monkeypatch, tmp_path) -> _Recorder:
    """Stubs out tConvert itself and the du-based chunk sizing, recording every call."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(process, '_tconvert_chunk_arg', lambda a_pass: 'chunk_size=4GB')
    recorder = _Recorder()
    monkeypatch.setattr(utils, 'shell_command', recorder)
    return recorder


class TestTconvertConcurrency:
    """tConvert converts the passes together, under the same ceiling as j2ms2."""

    def test_passes_convert_concurrently(self, tmp_path, tconvert):
        assert process.tconvert(make_exp(tmp_path, passes=4)) is True
        assert len(tconvert.calls) == 4
        assert tconvert.peak > 1, "the passes still ran one after the other"

    def test_never_more_at_once_than_the_io_ceiling(self, tmp_path, tconvert, monkeypatch):
        monkeypatch.setattr(utils, 'MAX_PASS_IO_WORKERS', 2)
        assert process.tconvert(make_exp(tmp_path, passes=6)) is True
        assert tconvert.peak <= 2

    def test_single_pass_keeps_its_live_terminal_output(self, tmp_path, tconvert):
        process.tconvert(make_exp(tmp_path, passes=1))
        assert tconvert.calls[0]['echo'] is True

    def test_several_passes_go_quiet_to_their_own_log(self, tmp_path, tconvert):
        process.tconvert(make_exp(tmp_path, passes=3))
        assert all(call['echo'] is False for call in tconvert.calls)
        # One shared base name; open_unique_log gives each pass its own numbered sibling.
        assert {str(call['logfile']) for call in tconvert.calls} == \
            {str(tmp_path / 'logs' / 'tconvert.log')}

    def test_already_converted_passes_are_skipped(self, tmp_path, tconvert):
        Path('es124_1_1.IDI1').touch()
        process.tconvert(make_exp(tmp_path, passes=2))
        assert [call['parameters'][1] for call in tconvert.calls] == ['es124_2_1.lis']

    def test_a_pass_that_does_not_fit_stops_before_anything_converts(self, tmp_path,
                                                                     tconvert, monkeypatch):
        # The chunk size is where the disk-space check lives; it must be resolved for every
        # pass up front, so a failure cannot surface with conversions already running.
        def only_the_first_fits(a_pass):
            if a_pass.fitsidifile != 'es124_1_1.IDI':
                raise IOError("Not enough disk space to create the FITS-IDI files.")
            return 'chunk_size=4GB'

        monkeypatch.setattr(process, '_tconvert_chunk_arg', only_the_first_fits)
        with pytest.raises(IOError):
            process.tconvert(make_exp(tmp_path, passes=3))
        assert tconvert.calls == []


class TestTconvertProgressAndErrors:
    """Past the threshold a progress bar replaces the silence; failures are named at exit."""

    def test_no_bar_for_a_handful_of_passes(self, tmp_path, tconvert, monkeypatch):
        shown = _record_progress(monkeypatch)
        process.tconvert(make_exp(tmp_path, passes=process._TCONVERT_PROGRESS_MIN_PASSES))
        assert shown == [True], "the bar must be disabled at or below the threshold"

    def test_bar_shown_once_past_the_threshold(self, tmp_path, tconvert, monkeypatch):
        shown = _record_progress(monkeypatch)
        process.tconvert(make_exp(tmp_path, passes=process._TCONVERT_PROGRESS_MIN_PASSES + 1))
        assert shown == [False], "the bar must be enabled above the threshold"

    def test_bar_counts_every_pass(self, tmp_path, tconvert, monkeypatch):
        advanced = []
        real_progress = progress.Progress

        class _Counting(real_progress):
            def advance(self, task_id, advance=1):
                advanced.append(advance)
                super().advance(task_id, advance)

        monkeypatch.setattr(progress, 'Progress', _Counting)
        process.tconvert(make_exp(tmp_path, passes=8))
        assert sum(advanced) == 8

    def test_one_failure_does_not_abandon_the_other_passes(self, tmp_path, tconvert,
                                                           monkeypatch):
        failing = tconvert.fail_on('es124_3_1.lis')
        assert process.tconvert(make_exp(tmp_path, passes=4)) is False
        # The other three still ran to completion rather than being cancelled.
        assert len(tconvert.calls) == 4
        assert failing in [call['parameters'][1] for call in tconvert.calls]

    def test_every_failure_is_reported_at_exit(self, tmp_path, tconvert):
        tconvert.fail_on('es124_1_1.lis', 'es124_3_1.lis')
        reported: list[str] = []
        sink = logger.add(lambda m: reported.append(m.record['message']), level='ERROR')
        try:
            assert process.tconvert(make_exp(tmp_path, passes=4)) is False
        finally:
            logger.remove(sink)
        assert any('2 of the 4 correlator pass(es)' in m for m in reported)
        assert any('es124_1_1.lis' in m for m in reported)
        assert any('es124_3_1.lis' in m for m in reported)

    def test_all_passes_succeeding_reports_success(self, tmp_path, tconvert):
        assert process.tconvert(make_exp(tmp_path, passes=8)) is True


def _record_progress(monkeypatch) -> list[bool]:
    """Captures the ``disable`` flag every rich Progress is built with."""
    disabled: list[bool] = []
    real_progress = progress.Progress

    def spy(*columns, **kwargs):
        disabled.append(kwargs.get('disable', False))
        return real_progress(*columns, **kwargs)

    monkeypatch.setattr(progress, 'Progress', spy)
    return disabled
