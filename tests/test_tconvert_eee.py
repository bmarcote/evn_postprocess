"""Tests for the temporary 'run tConvert on eee' workaround (process.tconvert).

The system tConvert is currently broken, so by default (exp.tconvert_in_eee) the tconvert
step copies each correlator pass MS to jops@eee:/data0/temp/, runs tConvert there, copies the
FITS-IDI files back, and removes the remote temp directory. Passes are independent and run in
parallel. These tests pin down that orchestration (and the local fallback) without any network.
"""
from __future__ import annotations

import datetime as dt
import subprocess
from pathlib import Path

import pytest

from evn_postprocess import experiment, process, servers


def _exp(tmp_path: Path, in_eee: bool = True) -> experiment.Experiment:
    dirs = experiment.Dirs(logs=tmp_path, plots=tmp_path, pipeline=tmp_path,
                           pipe_in=tmp_path, pipe_out=tmp_path, pipe_temp=tmp_path)
    exp = experiment.Experiment("TEST01", dt.date(2026, 5, 29), "marcote", dirs)
    exp.tconvert_in_eee = in_eee
    exp.correlator_passes = [
        experiment.CorrelatorPass(Path("test01_1.lis"), Path("test01_1.ms"), "test01_1_1.IDI", True),
        experiment.CorrelatorPass(Path("test01_2.lis"), Path("test01_2.ms"), "test01_2_1.IDI", False),
    ]
    return exp


@pytest.fixture
def fake_remote(monkeypatch):
    """Record every rsync/ssh call and stub out size/glob/server lookups (no network)."""
    calls: list[tuple] = []

    def fake_rsync(origin, dest, **kwargs):
        calls.append(("rsync", origin, dest, kwargs))
        return True

    def fake_ssh(host, command, **kwargs):
        calls.append(("ssh", host, command, kwargs))
        return "tConvert ok\n"

    monkeypatch.setattr(process.utils, "rsync", fake_rsync)
    monkeypatch.setattr(process.utils, "ssh", fake_ssh)
    monkeypatch.setattr(process._servers, "retrieve_servers",
                        lambda: servers.Servers([servers.Server("eee", "jops", "eee",
                                                                Path("/data0"))]))
    # Small MS -> chunk_size=4GB, and no FITS-IDI present yet so every pass runs.
    monkeypatch.setattr(process, "_du_kbytes", lambda _p: 1000)
    monkeypatch.setattr(process.glob, "glob", lambda _pat: [])
    return calls


class TestTconvertInEee:
    def test_full_remote_sequence(self, tmp_path: Path, fake_remote):
        calls = fake_remote
        assert process.tconvert(_exp(tmp_path)) is True

        # tConvert is run once per pass, from inside that pass' own remote temp dir.
        tconvert_runs = [c for c in calls if c[0] == "ssh" and "tConvert" in c[2]]
        assert len(tconvert_runs) == 2
        for _, host, cmd, kwargs in tconvert_runs:
            assert host == "jops@eee"
            assert "-o chunk_size=4GB" in cmd
            assert kwargs.get("stderr") == subprocess.STDOUT

        # Each pass: its MS+lis are pushed up together and its FITS-IDI fetched back.
        up = [c for c in calls if c[0] == "rsync" and str(c[2]).startswith("jops@eee:")]
        assert sorted(c[1] for c in up) == [["test01_1.ms", "test01_1.lis"],
                                            ["test01_2.ms", "test01_2.lis"]]
        back = [c for c in calls if c[0] == "rsync" and c[2] == "."]
        assert {str(c[1]) for c in back} == {"jops@eee:/data0/temp/test01_1/test01_1_1.IDI*",
                                             "jops@eee:/data0/temp/test01_2/test01_2_1.IDI*"}

        # Every pass dir is created (mkdir) and torn down (rm -rf).
        mkdirs = [c for c in calls if c[0] == "ssh" and "mkdir" in c[2]]
        cleanups = [c for c in calls if c[0] == "ssh" and "rm -rf" in c[2] and "mkdir" not in c[2]]
        assert len(mkdirs) == 2 and len(cleanups) == 2

    def test_cleanup_runs_even_on_failure(self, tmp_path: Path, monkeypatch, fake_remote):
        calls = fake_remote

        def boom(host, command, **kwargs):
            calls.append(("ssh", host, command, kwargs))
            if "tConvert" in command:
                raise ValueError("tConvert exploded")
            return ""

        monkeypatch.setattr(process.utils, "ssh", boom)

        with pytest.raises(ValueError, match="tConvert exploded"):
            process.tconvert(_exp(tmp_path))

        # The pass temp dir is still torn down after the failure (the final ssh per pass).
        assert any(c[0] == "ssh" and "rm -rf" in c[2] and "mkdir" not in c[2] for c in calls)

    def test_already_converted_passes_are_skipped(self, tmp_path: Path, monkeypatch, fake_remote):
        calls = fake_remote
        monkeypatch.setattr(process.glob, "glob", lambda _pat: ["already_there.IDI"])
        assert process.tconvert(_exp(tmp_path)) is True
        assert calls == []  # Nothing to do -> no remote interaction at all.


class TestLocalFallback:
    def test_no_eee_when_disabled(self, tmp_path: Path, monkeypatch):
        ran: list[list[str]] = []
        monkeypatch.setattr(process, "_du_kbytes", lambda _p: 1000)
        monkeypatch.setattr(process.glob, "glob", lambda _pat: [])
        monkeypatch.setattr(process.utils, "shell_command",
                            lambda binary, args, **kw: ran.append(args) or "")
        monkeypatch.setattr(process.utils, "ssh",
                            lambda *a, **k: pytest.fail("ssh called with workaround disabled"))
        assert process.tconvert(_exp(tmp_path, in_eee=False)) is True
        assert len(ran) == 2
        assert all("chunk_size=4GB" in a for a in ran)
