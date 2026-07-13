"""Tests for the three-channel reporting module (Issue 2).

The three sinks are independent: a Rich terminal message, the loguru debug file
(logs/logging_messages.log), and the replayable command log (logs/commands.sh).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from evn_postprocess import reporting


@pytest.fixture(autouse=True)
def _reset_step():
    reporting.set_current_step(None)
    reporting._last_recorded_step = None
    yield
    reporting.set_current_step(None)
    reporting._last_recorded_step = None


def test_paths_live_under_logs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert reporting.debug_log_path() == Path('logs') / 'logging_messages.log'
    assert reporting.command_log_path() == Path('logs') / 'commands.sh'
    # The command log is deliberately NOT named after the old debug file.
    assert reporting.command_log_path().name != 'post_process.log'
    assert reporting.debug_log_path().name != 'post_process.log'


def test_logs_dir_created(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    d = reporting.logs_dir()
    assert d.is_dir()


def test_record_command_writes_runnable_line(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    reporting.set_current_step('msops')
    reporting.record_command('mstools run flag_weights exp.ms 0.9')
    content = reporting.command_log_path().read_text()
    assert 'mstools run flag_weights exp.ms 0.9' in content
    assert '# --- step: msops ---' in content
    assert content.startswith('#!/bin/sh')


def test_record_command_headers_once_per_step(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    reporting.set_current_step('j2ms2')
    reporting.record_command('j2ms2 -v eb101.lis')
    reporting.record_command('getdata.pl -proj EB101 -lis eb101.lis')
    reporting.set_current_step('tconvert')
    reporting.record_command('tConvert eb101.ms eb101_1_1.IDI')
    content = reporting.command_log_path().read_text()
    # One header per step, not per command.
    assert content.count('# --- step: j2ms2 ---') == 1
    assert content.count('# --- step: tconvert ---') == 1
    # Runbook order preserved.
    assert content.index('j2ms2 -v') < content.index('getdata.pl') < content.index('tConvert')


def test_record_command_empty_is_ignored(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    reporting.record_command('   ')
    assert not reporting.command_log_path().exists()


def test_announce_never_raises_on_bad_markup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # A stray bracket must not blow up the operator channel.
    reporting.announce('weird [unclosed markup for EB101')


def test_channels_are_independent(tmp_path, monkeypatch):
    # Writing a command does not create the debug-log file, and vice versa: separate sinks.
    monkeypatch.chdir(tmp_path)
    reporting.record_command('echo hi', step='init')
    assert reporting.command_log_path().exists()
    assert not reporting.debug_log_path().exists()
