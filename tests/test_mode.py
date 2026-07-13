"""Tests for the operating-mode module (detection, resolution, backend mapping)."""
from __future__ import annotations

import pytest

from evn_postprocess import mode
from evn_postprocess.mode import Mode


# --------------------------------------------------------------------- detection

@pytest.fixture
def patch_os(monkeypatch):
    """Returns a helper that stubs the username and group membership seen by detect()."""
    def _apply(user: str, groups: set[str]):
        monkeypatch.setattr(mode, '_username', lambda: user)
        monkeypatch.setattr(mode, '_user_groups', lambda: set(groups))
    return _apply


def test_detect_jops_user_is_supsci(patch_os):
    patch_os('jops', set())
    assert mode.detect() is Mode.supsci


def test_detect_supsci_group_is_supsci(patch_os):
    patch_os('alice', {'staff', 'supsci'})
    assert mode.detect() is Mode.supsci


def test_detect_sweeps_group_is_sweeps(patch_os):
    patch_os('robot', {'sweeps'})
    assert mode.detect() is Mode.sweeps


def test_detect_plain_user_is_regular(patch_os):
    patch_os('alice', {'staff'})
    assert mode.detect() is Mode.regular


def test_detect_missing_groups_falls_back_to_regular(patch_os):
    # No matching group present at all -> regular, never an exception.
    patch_os('alice', set())
    assert mode.detect() is Mode.regular


def test_detect_supsci_wins_over_sweeps(patch_os):
    # A user in both groups is treated as supsci (checked first).
    patch_os('alice', {'supsci', 'sweeps'})
    assert mode.detect() is Mode.supsci


def test_detect_never_raises_on_missing_group(monkeypatch):
    # A real detect() call with a group that does not exist must not raise.
    monkeypatch.setattr(mode, '_username', lambda: 'nobody-xyz')
    monkeypatch.setattr(mode, '_user_groups', mode._user_groups)  # real implementation
    assert mode.detect() in (Mode.supsci, Mode.regular, Mode.sweeps)


# -------------------------------------------------------------------- resolution

def test_resolve_cli_wins(patch_os):
    patch_os('alice', set())  # would detect regular
    assert mode.resolve(cli_mode='supsci', stored_mode=None) is Mode.supsci


def test_resolve_stored_when_no_cli(patch_os):
    patch_os('jops', {'supsci'})  # would detect supsci
    assert mode.resolve(cli_mode=None, stored_mode='regular') is Mode.regular


def test_resolve_detects_when_nothing_given(patch_os):
    patch_os('jops', set())
    assert mode.resolve(cli_mode=None, stored_mode=None) is Mode.supsci


def test_resolve_override_warns(patch_os, caplog):
    patch_os('alice', set())
    import loguru
    messages = []
    handler_id = loguru.logger.add(lambda m: messages.append(m.record['message']), level='WARNING')
    try:
        result = mode.resolve(cli_mode='sweeps', stored_mode='supsci')
    finally:
        loguru.logger.remove(handler_id)
    assert result is Mode.sweeps
    assert any('Overriding the stored mode' in m for m in messages)


def test_resolve_no_warn_when_cli_matches_stored(patch_os):
    patch_os('alice', set())
    import loguru
    messages = []
    handler_id = loguru.logger.add(lambda m: messages.append(m.record['message']), level='WARNING')
    try:
        result = mode.resolve(cli_mode='supsci', stored_mode='supsci')
    finally:
        loguru.logger.remove(handler_id)
    assert result is Mode.supsci
    assert not any('Overriding' in m for m in messages)


# ----------------------------------------------------------------- backend map

@pytest.mark.parametrize('the_mode, retrieval, distribution', [
    (Mode.supsci, 'jive', 'jive'),
    (Mode.regular, 'none', 'none'),
    (Mode.sweeps, 'sweeps', 'sweeps'),
])
def test_backends_for(the_mode, retrieval, distribution):
    backends = mode.backends_for(the_mode)
    assert backends.retrieval == retrieval
    assert backends.pipeline == 'aips'  # every mode runs the AIPS pipeline
    assert backends.distribution == distribution


def test_backends_for_accepts_string():
    assert mode.backends_for('regular') == mode.backends_for(Mode.regular)


def test_backends_for_unknown_raises():
    with pytest.raises(ValueError):
        mode.backends_for('nonsense')


def test_mode_is_json_value_string():
    # str-based enum serialises as its value, so JSON persistence is trivial.
    assert Mode.supsci.value == 'supsci'
    assert Mode('sweeps') is Mode.sweeps
