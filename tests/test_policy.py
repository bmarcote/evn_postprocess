"""Tests for evn_postprocess.policy."""
from __future__ import annotations

import textwrap
import tomllib
from pathlib import Path

import pytest

from evn_postprocess.policy import Policy


class TestPolicyDefaults:
    """Defaults and constructor behaviour."""

    def test_default_policy_has_no_decisions(self):
        p = Policy()
        assert p.weight_threshold is None
        assert p.polswap == []
        assert p.polconvert == []
        assert p.onebit == []
        assert p.refant == []
        # Default pause_after preserves the historical post-postpipe pause.
        assert p.pause_after == ["postpipe"]
        assert p.skip_archive is False
        assert p.batch is False

    def test_requires_input_flags_missing_threshold(self):
        assert Policy().requires_input() == ["weight_threshold"]

    def test_requires_input_empty_when_threshold_set(self):
        assert Policy(weight_threshold=0.9).requires_input() == []


class TestPolicyRoundTrip:
    """Dict-based serialisation round-trips."""

    def test_to_dict_from_dict(self):
        original = Policy(
            weight_threshold=0.85,
            polswap=["Wb"],
            polconvert=["Kt"],
            onebit=["Ef"],
            refant=["Ef", "Mc"],
            pause_after=["postpipe", "pipeline"],
            skip_archive=True,
            batch=True,
        )
        data = original.to_dict()
        restored = Policy.from_dict(data)
        assert restored == original

    def test_from_dict_ignores_unknown_keys(self):
        # Forward-compatibility with future TOML fields.
        data = {"weight_threshold": 0.9, "future_field": "ignored"}
        p = Policy.from_dict(data)
        assert p.weight_threshold == 0.9


class TestPolicyTOML:
    """Loading from a TOML file on disk."""

    def test_load_full_policy(self, tmp_path: Path):
        toml = tmp_path / "policy.toml"
        toml.write_text(textwrap.dedent(
            """
            weight_threshold = 0.85
            polswap          = ["Wb"]
            polconvert       = ["Kt"]
            onebit           = []
            refant           = ["Ef"]
            pause_after      = ["postpipe"]
            skip_archive     = false
            batch            = true
            """
        ))
        p = Policy.load(toml)
        assert p.weight_threshold == 0.85
        assert p.polswap == ["Wb"]
        assert p.polconvert == ["Kt"]
        assert p.refant == ["Ef"]
        assert p.batch is True

    def test_load_missing_file(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            Policy.load(tmp_path / "does_not_exist.toml")

    def test_load_invalid_toml(self, tmp_path: Path):
        toml = tmp_path / "bad.toml"
        toml.write_text("this is = not valid toml [")
        with pytest.raises(tomllib.TOMLDecodeError):
            Policy.load(toml)


class TestPolicyMerge:
    """Non-destructive override behaviour."""

    def test_merge_overrides_only_provided_fields(self):
        base = Policy(weight_threshold=0.9, polswap=["Wb"])
        merged = base.merge(refant=["Ef"], polswap=["Mc"])
        assert merged.refant == ["Ef"]
        assert merged.polswap == ["Mc"]
        # Original is untouched.
        assert base.refant == []
        assert base.polswap == ["Wb"]

    def test_merge_drops_none_values(self):
        # Allows callers to pass argparse defaults without overwriting policy fields.
        base = Policy(weight_threshold=0.9)
        merged = base.merge(weight_threshold=None, refant=["Ef"])
        assert merged.weight_threshold == 0.9
        assert merged.refant == ["Ef"]
