"""Tests for the pipelines and distribution backend registries (Issues 8 and 13).

Backend *selection* is now driven by the operating mode (see evn_postprocess.mode);
each family keeps only its registry (register / available_backends / get_*).
"""
import pytest

from evn_postprocess import pipelines, distribution, mode
from evn_postprocess.mode import Mode
from evn_postprocess.pipelines import NonePipeline
from evn_postprocess.distribution import NoneDistributor


class ExpStub:
    expname = 'EB101'


# ------------------------------------------------------------------ pipelines

def test_pipeline_registry():
    assert {'aips', 'none', 'vpipe'} <= set(pipelines.available_backends())
    assert isinstance(pipelines.get_pipeline('none'), NonePipeline)


def test_pipeline_unknown_backend():
    with pytest.raises(pipelines.PipelineError) as excinfo:
        pipelines.get_pipeline('casa')
    assert 'casa' in str(excinfo.value) and 'aips' in str(excinfo.value)


def test_pipeline_vpipe_not_implemented():
    with pytest.raises(pipelines.PipelineError) as excinfo:
        pipelines.get_pipeline('vpipe')
    assert 'not implemented' in str(excinfo.value)


def test_pipeline_none_satisfies_all_stages():
    backend = pipelines.get_pipeline('none')
    exp = ExpStub()
    assert backend.prepare(exp) and backend.run(exp) and backend.collect(exp)


def test_pipeline_selected_by_mode():
    # Every mode runs the AIPS pipeline; the mapping picks a registered backend.
    for a_mode in (Mode.supsci, Mode.regular, Mode.sweeps):
        name = mode.backends_for(a_mode).pipeline
        assert name == 'aips'
        assert name in pipelines.available_backends()


def test_family_cli_plumbing_removed():
    # The per-family CLI override is gone; selection is mode-driven now.
    assert not hasattr(pipelines, 'set_cli_mode')
    assert not hasattr(pipelines, 'selected_mode')
    assert not hasattr(distribution, 'set_cli_mode')
    assert not hasattr(distribution, 'selected_mode')


def test_pipeline_aips_class_importable():
    from evn_postprocess.pipelines.aips import AipsPipeline
    assert AipsPipeline.name == 'aips'


# ---------------------------------------------------------------- distribution

def test_distribution_registry():
    assert {'jive', 'none', 'sweeps'} <= set(distribution.available_backends())
    assert isinstance(distribution.get_distributor('none'), NoneDistributor)


def test_distribution_unknown_backend():
    with pytest.raises(distribution.DistributionError) as excinfo:
        distribution.get_distributor('ftp')
    assert 'ftp' in str(excinfo.value) and 'jive' in str(excinfo.value)


def test_distribution_sweeps_not_implemented():
    with pytest.raises(distribution.DistributionError) as excinfo:
        distribution.get_distributor('sweeps')
    assert 'not implemented' in str(excinfo.value)


class _Pass:
    def __init__(self, fitsidifile, msfile='eb101.ms'):
        self.fitsidifile = fitsidifile
        self.msfile = msfile


class _ExpWithPasses:
    expname = 'EB101'
    def __init__(self, passes):
        self.correlator_passes = passes


def test_distribution_none_verifies_fitsidi_present(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101_1_1.IDI1').write_text('idi')
    exp = _ExpWithPasses([_Pass('eb101_1_1.IDI')])
    assert distribution.get_distributor('none').deliver(exp) is True


def test_distribution_none_fails_on_missing_fitsidi(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no IDI files present
    exp = _ExpWithPasses([_Pass('eb101_1_1.IDI')])
    assert distribution.get_distributor('none').deliver(exp) is False


def test_distribution_none_fails_with_no_passes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert distribution.get_distributor('none').deliver(_ExpWithPasses([])) is False


def test_distribution_selected_by_mode():
    assert mode.backends_for(Mode.supsci).distribution == 'jive'
    assert mode.backends_for(Mode.regular).distribution == 'none'
    assert mode.backends_for(Mode.sweeps).distribution == 'sweeps'
    for a_mode in (Mode.supsci, Mode.regular, Mode.sweeps):
        assert mode.backends_for(a_mode).distribution in distribution.available_backends()


def test_distribution_jive_upload_feedback_stub():
    from evn_postprocess.distribution.jive import JiveDistributor
    assert JiveDistributor().upload_feedback(ExpStub()) is True
