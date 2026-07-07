"""Tests for the pipelines and distribution backend registries (Issues 8 and 13)."""
import pytest

from evn_postprocess import pipelines, distribution
from evn_postprocess.pipelines import NonePipeline
from evn_postprocess.distribution import NoneDistributor


class ExpStub:
    expname = 'EB101'


class TomlStub:
    def __init__(self, pipeline=None, dist=None):
        self.pipeline = pipeline
        self.distribution = dist


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


def test_pipeline_mode_selection():
    assert pipelines.selected_mode(None) == 'aips'
    assert pipelines.selected_mode(TomlStub(pipeline='none')) == 'none'


def test_pipeline_cli_override():
    pipelines.set_cli_mode('aips')
    try:
        assert pipelines.selected_mode(TomlStub(pipeline='none')) == 'aips'  # CLI wins
    finally:
        pipelines.set_cli_mode(None)
    with pytest.raises(pipelines.PipelineError):
        pipelines.set_cli_mode('bogus')


def test_distribution_cli_override():
    distribution.set_cli_mode('none')
    try:
        assert distribution.selected_mode(TomlStub(dist='jive')) == 'none'  # CLI wins
    finally:
        distribution.set_cli_mode(None)
    with pytest.raises(distribution.DistributionError):
        distribution.set_cli_mode('bogus')


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


def test_distribution_none_is_noop():
    assert distribution.get_distributor('none').deliver(ExpStub()) is True


def test_distribution_mode_selection():
    assert distribution.selected_mode(None) == 'jive'
    assert distribution.selected_mode(TomlStub(dist='none')) == 'none'


def test_distribution_jive_upload_feedback_stub():
    from evn_postprocess.distribution.jive import JiveDistributor
    assert JiveDistributor().upload_feedback(ExpStub()) is True
