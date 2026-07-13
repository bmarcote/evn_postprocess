"""Tests for the evn_postprocess.retrieval sub-package (registry + NoneRetriever)."""
from pathlib import Path

import pytest

from evn_postprocess import retrieval
from evn_postprocess.retrieval.local import NoneRetriever


MINIMAL_VEX = 'VEX_rev = 1.5;\n$EXPER;\n  def EB101;\n    exper_name = EB101;\n  enddef;\n'


def make_local_inputs(tmp_path, lis=True, toml=False):
    (tmp_path / 'EB101.vix').write_text(MINIMAL_VEX)
    if lis:
        (tmp_path / 'eb101.lis').write_text('dummy pass\n')
    if toml:
        (tmp_path / 'eb101.toml').write_text('[observation]\nexpname = "EB101"\n')


# ------------------------------------------------------------------- registry

def test_registry_contains_builtin_backends():
    assert 'none' in retrieval.available_backends()
    assert 'jive' in retrieval.available_backends()


def test_get_retriever_none():
    assert isinstance(retrieval.get_retriever('none'), NoneRetriever)


def test_unknown_backend_fails_at_selection():
    with pytest.raises(retrieval.RetrievalError) as excinfo:
        retrieval.get_retriever('ftp')
    assert 'ftp' in str(excinfo.value)
    assert 'none' in str(excinfo.value)  # registered names listed


def test_sweeps_retrieval_not_implemented():
    # 'sweeps' is registered but a stub; selecting it fails explicitly.
    assert 'sweeps' in retrieval.available_backends()
    with pytest.raises(retrieval.RetrievalError) as excinfo:
        retrieval.get_retriever('sweeps')
    assert 'not implemented' in str(excinfo.value)


def test_third_party_registration():
    class Dummy(retrieval.Retriever):
        name = 'dummy'
        def fetch(self, workdir, expname):
            return retrieval.InputSet(vexfile=Path('x.vix'))
        def fetch_lisfiles(self, exp):
            return True
        def fetch_station_files(self, exp):
            return True
    retrieval.register('dummy', Dummy)
    try:
        assert isinstance(retrieval.get_retriever('dummy'), Dummy)
    finally:
        retrieval._REGISTRY.unregister('dummy')


# --------------------------------------------------------------- NoneRetriever

def test_none_fetch_complete_directory(tmp_path):
    make_local_inputs(tmp_path, lis=True, toml=True)
    inputset = NoneRetriever().fetch(tmp_path, 'EB101')
    assert inputset.vexfile.name == 'EB101.vix'
    assert [f.name for f in inputset.lisfiles] == ['eb101.lis']
    assert inputset.tomlfile.name == 'eb101.toml'


def test_none_fetch_without_toml(tmp_path):
    make_local_inputs(tmp_path, lis=True, toml=False)
    assert NoneRetriever().fetch(tmp_path, 'EB101').tomlfile is None


def test_none_fetch_missing_vex(tmp_path):
    with pytest.raises(retrieval.RetrievalError) as excinfo:
        NoneRetriever().fetch(tmp_path, 'EB101')
    msg = str(excinfo.value)
    assert 'EB101' in msg and 'does not create or download' in msg


def test_none_fetch_missing_lis_names_pattern(tmp_path):
    make_local_inputs(tmp_path, lis=False)
    with pytest.raises(retrieval.RetrievalError) as excinfo:
        NoneRetriever().fetch(tmp_path, 'EB101')
    msg = str(excinfo.value)
    assert 'eb101*.lis' in msg and 'does not create or download' in msg


def test_none_fetch_excludes_lag_lis(tmp_path):
    make_local_inputs(tmp_path, lis=True)
    (tmp_path / 'eb101-lag.lis').write_text('lag aux\n')
    inputset = NoneRetriever().fetch(tmp_path, 'EB101')
    assert [f.name for f in inputset.lisfiles] == ['eb101.lis']


def test_none_fetch_lisfiles_always_fails(tmp_path):
    class Exp:
        expname = 'EB101'
    with pytest.raises(retrieval.RetrievalError) as excinfo:
        NoneRetriever().fetch_lisfiles(Exp())
    assert 'eb101*.lis' in str(excinfo.value) and 'does not create' in str(excinfo.value)


class ExpStub:
    """Minimal experiment stand-in for fetch_station_files."""
    def __init__(self, tempdir, observed):
        class Dirs:
            pipe_temp = tempdir
        self.dirs = Dirs()
        class Ants:
            pass
        self.antennas = Ants()
        self.antennas.observed = observed


def test_none_station_files_present(tmp_path):
    (tmp_path / 'ef.antabfs').touch()
    (tmp_path / 'ef.log').touch()
    assert NoneRetriever().fetch_station_files(ExpStub(tmp_path, ['Ef'])) is True


def test_none_station_files_all_missing(tmp_path):
    with pytest.raises(retrieval.RetrievalError) as excinfo:
        NoneRetriever().fetch_station_files(ExpStub(tmp_path, ['Ef']))
    assert 'antabfs' in str(excinfo.value)


def test_none_station_files_partially_missing_is_soft(tmp_path):
    (tmp_path / 'ef.antabfs').touch()
    # Wb has nothing: warning only, not an error.
    assert NoneRetriever().fetch_station_files(ExpStub(tmp_path, ['Ef', 'Wb'])) is True
