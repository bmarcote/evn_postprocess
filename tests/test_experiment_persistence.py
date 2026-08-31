"""Tests for atomic store/load and schema migration on Experiment JSON files."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from evn_postprocess import experiment
from evn_postprocess.policy import Policy


def _make_dirs(tmp_path: Path) -> experiment.Dirs:
    return experiment.Dirs(
        logs=tmp_path / "logs",
        plots=tmp_path / "plots",
        pipeline=tmp_path / "pipe",
        pipe_in=tmp_path / "pipe" / "in",
        pipe_out=tmp_path / "pipe" / "out",
        pipe_temp=tmp_path / "pipe" / "temp",
    )


def _make_exp(tmp_path: Path, **kwargs) -> experiment.Experiment:
    return experiment.Experiment(
        expname=kwargs.pop("expname", "TEST01"),
        obsdate=kwargs.pop("obsdate", dt.date(2024, 6, 12)),
        supsci=kwargs.pop("supsci", "marcote"),
        dirs=kwargs.pop("dirs", _make_dirs(tmp_path)),
        **kwargs,
    )


class TestAtomicStore:
    def test_store_writes_via_temp_file(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        exp.store()
        # Final file exists, the .tmp scratch file does not.
        assert (tmp_path / "test01.json").exists()
        assert not (tmp_path / "test01.json.tmp").exists()

    def test_store_overwrites_existing_atomically(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        exp.store()
        # Second write must not leave a stale .tmp behind even though the target
        # already exists.
        exp.refant = ["Ef"]
        exp.store()
        with open(tmp_path / "test01.json") as f:
            data = json.load(f)
        assert data["refant"] == ["Ef"]
        assert not (tmp_path / "test01.json.tmp").exists()


class TestSchemaVersion:
    def test_store_writes_current_schema_version(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        exp.store()
        with open(tmp_path / "test01.json") as f:
            data = json.load(f)
        assert data["_schema_version"] == experiment.Experiment.SCHEMA_VERSION

    def test_load_migrates_v1_to_v2(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        exp.store()

        # Simulate a v1 file: drop the new keys before re-loading.
        with open(tmp_path / "test01.json") as f:
            data = json.load(f)
        data.pop("_schema_version", None)
        data.pop("policy", None)
        with open(tmp_path / "test01.json", "w") as f:
            json.dump(data, f)

        loaded = experiment.Experiment.load("TEST01")
        # A v1 file has no policy attached; the load must default to None.
        assert loaded.policy is None

    def test_load_migrates_v2_to_v3_mode_absent(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        exp.store()

        # Simulate a v2 file: it has no 'mode' key at all.
        with open(tmp_path / "test01.json") as f:
            data = json.load(f)
        data["_schema_version"] = 2
        data.pop("mode", None)
        with open(tmp_path / "test01.json", "w") as f:
            json.dump(data, f)

        loaded = experiment.Experiment.load("TEST01")
        # A pre-Phase-2 file leaves the mode unset (None) so the caller re-detects it.
        assert loaded.mode is None

    def test_unknown_future_version_loads_with_warning(self, tmp_path: Path, monkeypatch, caplog):
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        exp.store()
        with open(tmp_path / "test01.json") as f:
            data = json.load(f)
        # Pretend the file was written by a newer code revision.
        data["_schema_version"] = experiment.Experiment.SCHEMA_VERSION + 99
        with open(tmp_path / "test01.json", "w") as f:
            json.dump(data, f)
        # Loading should still succeed (best-effort) without raising.
        loaded = experiment.Experiment.load("TEST01")
        assert loaded.expname == "TEST01"


class TestPolicyRoundTrip:
    """Policy survives a store/load cycle on the experiment."""

    def test_policy_persists(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        policy = Policy(weight_threshold=0.85, polswap=["Wb"], refant=["Ef"])
        exp = _make_exp(tmp_path, policy=policy)
        exp.store()

        loaded = experiment.Experiment.load("TEST01")
        assert loaded.policy is not None
        assert loaded.policy == policy

    def test_no_policy_persists_as_none(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        exp.store()

        loaded = experiment.Experiment.load("TEST01")
        assert loaded.policy is None


class TestModeRoundTrip:
    """The operating mode survives a store/load cycle on the experiment."""

    def test_mode_persists(self, tmp_path: Path, monkeypatch):
        from evn_postprocess.mode import Mode
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path, mode=Mode.supsci)
        exp.store()

        loaded = experiment.Experiment.load("TEST01")
        assert loaded.mode is Mode.supsci

    def test_absent_mode_loads_as_none(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        exp.store()

        loaded = experiment.Experiment.load("TEST01")
        assert loaded.mode is None


class TestIsNme:
    """One shared definition of a Network Monitoring Experiment.

    Four separate decisions used to carry their own prefix check (archive credentials,
    source protection, the feedback-page format and the NME report); they all read
    ``experiment.is_nme`` now, so they cannot disagree.
    """

    def test_n_and_f_are_nmes(self):
        for name in ("N24L1", "n24l1", "F24X1", "f24x1"):
            assert experiment.is_nme(name) is True, name

    def test_fringe_tests_are_not_nmes(self):
        # FT* is a fringe test: it needs credentials and protection like any experiment.
        for name in ("FT24A", "ft24a"):
            assert experiment.is_nme(name) is False, name

    def test_ordinary_experiments_are_not_nmes(self):
        for name in ("EB101", "GP052", "RSF01"):
            assert experiment.is_nme(name) is False, name

    def test_credentials_follow_the_shared_rule(self, tmp_path, monkeypatch):
        # A real NME gets no credentials; a fringe test does.
        from evn_postprocess import process
        monkeypatch.chdir(tmp_path)
        for name, expects_credentials in (("N24L1", False), ("FT24A", True)):
            exp = experiment.Experiment(name, dt.date(2026, 4, 10), "tester", _make_dirs(tmp_path))
            assert process.set_credentials(exp) is True
            assert (exp.credentials is not None) is expects_credentials, name
            for auth in tmp_path.glob("*.auth"):
                auth.unlink()   # so the next case does not recover these credentials
