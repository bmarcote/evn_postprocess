"""CLI-level tests for the operating-mode selection (Issue 1, Task 8).

These drive the real ``postprocess`` entry point in a subprocess, so they exercise
argparse and the top-level wiring. They import the full package (which pulls in
python-casacore), so they run on Linux/JIVE; on a casacore-broken host they are
skipped rather than crashing the collector.
"""
from __future__ import annotations

import subprocess
import sys

import pytest


def _run(args, cwd=None):
    """Runs `postprocess <args>` via the module entry point, returning CompletedProcess."""
    return subprocess.run([sys.executable, '-c',
                           'from evn_postprocess.main import main; main()', *args],
                          capture_output=True, text=True, cwd=cwd, timeout=120)


def _importable() -> bool:
    """True when the full package imports here (casacore present and not segfaulting)."""
    result = subprocess.run([sys.executable, '-c', 'import evn_postprocess.main'],
                            capture_output=True, text=True, timeout=120)
    return result.returncode == 0


pytestmark = pytest.mark.skipif(not _importable(),
                                reason="full package (python-casacore) not importable on this host")


def test_help_lists_mode_and_config():
    result = _run(['--help'])
    assert result.returncode == 0
    assert '--mode' in result.stdout
    assert '--config' in result.stdout


def test_help_drops_old_backend_flags():
    result = _run(['--help'])
    assert '--retrieval' not in result.stdout
    assert '--pipeline' not in result.stdout
    assert '--distribution' not in result.stdout


def test_unknown_mode_rejected_at_parse_time():
    # argparse choices reject an unknown --mode with exit code 2, before any step runs.
    result = _run(['-e', 'EB101', '--mode', 'bogus', 'info'])
    assert result.returncode == 2
    assert 'bogus' in (result.stderr + result.stdout)


def test_config_missing_file_errors(tmp_path):
    result = _run(['-e', 'EB101', '--mode', 'sweeps', '--config', 'no-such-file.toml', 'info'],
                  cwd=str(tmp_path))
    assert result.returncode == 1
    assert 'config file not found' in (result.stderr + result.stdout).lower()


class TestInfoNeedsAStartedExperiment:
    """`info`, `dashboard` and `edit` report on an experiment; they never create one.

    Running them in an empty directory used to go through the initialization path, which
    retrieved the vex file and built the directory structure before failing.
    """

    def _run(self, tmp_path, monkeypatch, argv):
        """Drives main() in-process (the only way to prove nothing initializes) and puts
        loguru back to its default afterwards, since main() installs its own sinks."""
        from loguru import logger
        from evn_postprocess import main as main_mod
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, 'argv', ['postprocess', '-e', 'EB101'] + argv)

        def must_not_initialize(*a, **k):
            raise AssertionError(f"'{' '.join(argv)}' must not initialize an experiment")

        monkeypatch.setattr(main_mod.workflow, 'initialize_experiment', must_not_initialize)
        try:
            with pytest.raises(SystemExit) as excinfo:
                main_mod.main()
            return excinfo.value.code
        finally:
            logger.remove()
            logger.add(sys.stderr)

    def test_info_exits_without_creating_anything(self, tmp_path, monkeypatch):
        assert self._run(tmp_path, monkeypatch, ['info']) == 1
        assert list(tmp_path.iterdir()) == []

    def test_dashboard_exits_without_creating_anything(self, tmp_path, monkeypatch):
        assert self._run(tmp_path, monkeypatch, ['dashboard']) == 1
        assert list(tmp_path.iterdir()) == []

    def test_edit_exits_without_creating_anything(self, tmp_path, monkeypatch):
        assert self._run(tmp_path, monkeypatch, ['edit', 'refant']) == 1
        assert list(tmp_path.iterdir()) == []
