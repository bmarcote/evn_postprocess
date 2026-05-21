"""Tests for evn_postprocess.tools.resolve and tools.run."""
from __future__ import annotations

from pathlib import Path

import pytest

from evn_postprocess import tools


class TestResolve:
    """Resolution order is env-override -> auto-env -> computers.toml -> $PATH -> default."""

    def test_explicit_env_var_wins(self, monkeypatch, tmp_path: Path):
        binary = tmp_path / "myTool"
        binary.touch()
        monkeypatch.setenv("MY_OVERRIDE", str(binary))
        assert tools.resolve("tConvert", env_var="MY_OVERRIDE") == str(binary)

    def test_auto_env_var(self, monkeypatch, tmp_path: Path):
        binary = tmp_path / "tConvert"
        binary.touch()
        monkeypatch.setenv("EVN_TCONVERT", str(binary))
        # Re-uppercased name; dots/dashes are normalised to underscores so
        # "feedback.pl" ↦ EVN_FEEDBACK_PL works too.
        assert tools.resolve("tConvert") == str(binary)

    def test_auto_env_var_with_dotted_name(self, monkeypatch, tmp_path: Path):
        binary = tmp_path / "feedback.pl"
        binary.touch()
        monkeypatch.setenv("EVN_FEEDBACK_PL", str(binary))
        assert tools.resolve("feedback.pl") == str(binary)

    def test_falls_back_to_path(self, monkeypatch):
        # `python` is virtually always available on the PATH where the tests run.
        monkeypatch.delenv("EVN_PYTHON", raising=False)
        which_path = tools.resolve("python", default=None)
        assert which_path  # not empty / not None

    def test_default_when_nothing_found(self, monkeypatch):
        monkeypatch.delenv("EVN_DEFINITELY_NOT_A_REAL_TOOL", raising=False)
        # Use a name that cannot exist on PATH.
        assert tools.resolve("definitely_not_a_real_tool", default="fallback") == "fallback"

    def test_raises_when_missing_and_no_default(self, monkeypatch):
        monkeypatch.delenv("EVN_DEFINITELY_NOT_A_REAL_TOOL", raising=False)
        with pytest.raises(tools.ToolMissingError):
            tools.resolve("definitely_not_a_real_tool")


class TestRun:
    """tools.run executes shell=False with explicit cwd and captured output."""

    def test_runs_with_args_and_captures_output(self, tmp_path: Path, monkeypatch):
        # `echo` is reliably present on Linux $PATH (the project's only OS target).
        result = tools.run("echo", ["hello", "world"], cwd=tmp_path, check=True)
        assert result.returncode == 0
        assert "hello world" in result.stdout

    def test_respects_cwd(self, tmp_path: Path):
        result = tools.run("pwd", [], cwd=tmp_path, check=True)
        # `pwd` may resolve symlinks (e.g. on /tmp), so compare resolved paths.
        assert Path(result.stdout.strip()).resolve() == tmp_path.resolve()

    def test_check_true_raises_on_nonzero(self, tmp_path: Path):
        import subprocess
        with pytest.raises(subprocess.CalledProcessError):
            tools.run("false", [], cwd=tmp_path, check=True)

    def test_check_false_returns_nonzero(self, tmp_path: Path):
        result = tools.run("false", [], cwd=tmp_path, check=False)
        assert result.returncode != 0
