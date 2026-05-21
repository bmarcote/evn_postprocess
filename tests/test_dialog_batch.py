"""Tests for the batch-mode dialog backend (evn_postprocess.dialog.PolicyDriven)."""
from __future__ import annotations

from unittest.mock import Mock

import pytest

from evn_postprocess import dialog, experiment
from evn_postprocess.policy import Policy


def _make_pass(name: str = "ms.ms"):
    """Returns a CorrelatorPass-like Mock that accepts attribute writes."""
    p = Mock(spec=experiment.CorrelatorPass)
    p.flagged_weights = None
    p.msfile = Mock()
    p.msfile.name = name
    return p


def _make_exp(*, policy: Policy | None, antennas=None, passes=None) -> Mock:
    exp = Mock(spec=experiment.Experiment)
    exp.policy = policy
    exp.refant = []
    # Fake antennas collection that supports `in`, `[name]` and `.names`.
    antennas_dict = {a.name: a for a in (antennas or [])}

    class _AntCollection:
        def __init__(self, items):
            self._items = items
        @property
        def names(self):
            return list(self._items.keys())
        def __contains__(self, key):
            return key in self._items
        def __getitem__(self, key):
            return self._items[key]
    exp.antennas = _AntCollection(antennas_dict)
    exp.correlator_passes = list(passes or [])
    return exp


def _make_antenna(name: str):
    a = Mock(spec=experiment.Antenna)
    a.name = name
    a.polswap = False
    a.polconvert = False
    a.onebit = False
    return a


class TestMakeDialog:
    def test_factory_returns_policy_driven_in_batch_mode(self):
        d = dialog.make_dialog(batch=True)
        assert isinstance(d, dialog.PolicyDriven)

    def test_factory_returns_terminal_in_interactive_mode(self):
        d = dialog.make_dialog(batch=False)
        assert isinstance(d, dialog.Terminal)


class TestPolicyDrivenAskMSOperations:
    """End-to-end behaviour of PolicyDriven.askMSoperations."""

    def test_missing_policy_raises_batch_interaction_error(self):
        exp = _make_exp(policy=None)
        with pytest.raises(dialog.BatchInteractionError):
            dialog.PolicyDriven().askMSoperations(exp)

    def test_missing_threshold_raises_batch_interaction_error(self):
        exp = _make_exp(policy=Policy())  # weight_threshold defaults to None
        with pytest.raises(dialog.BatchInteractionError):
            dialog.PolicyDriven().askMSoperations(exp)

    def test_applies_policy_to_antennas_and_passes(self):
        ants = [_make_antenna("Ef"), _make_antenna("Wb"), _make_antenna("Mc")]
        passes = [_make_pass(), _make_pass()]
        policy = Policy(
            weight_threshold=0.85,
            polswap=["Wb"],
            polconvert=["Mc"],
            onebit=["Ef"],
            refant=["Ef"],
        )
        exp = _make_exp(policy=policy, antennas=ants, passes=passes)

        result = dialog.PolicyDriven().askMSoperations(exp)
        assert result is True

        # Each pass got a fresh FlagWeight using the threshold from the policy.
        for p in passes:
            assert p.flagged_weights is not None
            assert p.flagged_weights.threshold == 0.85
            assert p.flagged_weights.percentage == -1

        # The right antennas got their flags flipped.
        ant_by_name = {a.name: a for a in ants}
        assert ant_by_name["Wb"].polswap is True
        assert ant_by_name["Mc"].polconvert is True
        assert ant_by_name["Ef"].onebit is True
        # Untouched antennas keep their defaults.
        assert ant_by_name["Ef"].polswap is False
        assert ant_by_name["Wb"].polconvert is False

        # Refant fallback only fills the experiment refant when it's empty.
        assert exp.refant == ["Ef"]

    def test_unknown_antenna_in_policy_is_ignored(self):
        ants = [_make_antenna("Ef")]
        policy = Policy(weight_threshold=0.9, polswap=["NotARealAntenna"])
        exp = _make_exp(policy=policy, antennas=ants, passes=[_make_pass()])
        # Should not raise just because the policy mentions an unknown antenna;
        # the operator may have set it before metadata was read.
        result = dialog.PolicyDriven().askMSoperations(exp)
        assert result is True
        assert ants[0].polswap is False  # untouched

    def test_existing_flagged_weights_are_preserved_when_threshold_matches(self):
        # If the previous run already flagged with the same threshold, we must
        # NOT overwrite the percentage back to -1 (that would re-trigger the
        # heavy flag_weights pass).
        from evn_postprocess.experiment import FlagWeight
        passes = [_make_pass()]
        passes[0].flagged_weights = FlagWeight(threshold=0.85, percentage=12.0)
        exp = _make_exp(policy=Policy(weight_threshold=0.85), passes=passes)

        dialog.PolicyDriven().askMSoperations(exp)
        assert passes[0].flagged_weights.percentage == 12.0  # untouched

    def test_show_scan_overview_is_a_noop_in_batch(self):
        exp = _make_exp(policy=Policy(weight_threshold=0.9))
        # Must always return True without prompting/printing/blocking.
        assert dialog.PolicyDriven().show_scan_overview(exp) is True
