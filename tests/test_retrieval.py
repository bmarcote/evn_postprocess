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


# ------------------------------------------------------------- jive.fetch_jexp_info

from evn_postprocess.retrieval import jive as _jive  # noqa: E402
from evn_postprocess import servers as _servers_mod   # noqa: E402
from evn_postprocess import utils as _utils_mod       # noqa: E402

_JEXP_SERVERS = _servers_mod.Servers([
    _servers_mod.Server(name='jexp', user='jops', host='archive', path=Path('/home/jops/Expadmin/Jexp')),
])

_SAMPLE_JEX = (
    "# JIVE experiment description\n"
    "piname = Jane Doe;\n"
    "pimail = jane@x.edu;\n"
    "coname = John Roe;\n"
    "coimail = john@y.edu;\n"
    "schedsrc = J1234+5678 (T|X), 3C84 (F|P);\n"
    "emptyval = ;\n"
    "bare line without equals\n"
)


def test_fetch_jexp_info_parses_and_cleans_up(monkeypatch):
    captured = {}

    def fake_scp(origin, dest, **kwargs):
        captured['origin'] = origin
        captured['dest'] = dest
        Path(dest).write_text(_SAMPLE_JEX, encoding='utf-8')
        return True

    monkeypatch.setattr(_servers_mod, 'retrieve_servers', lambda: _JEXP_SERVERS)
    monkeypatch.setattr(_utils_mod, 'scp', fake_scp)
    info = _jive.fetch_jexp_info('EB101')
    # remote path is {jexp server}/{lowercased expname}.jex
    assert captured['origin'] == 'jops@archive:/home/jops/Expadmin/Jexp/eb101.jex'
    assert info['piname'] == 'Jane Doe' and info['coimail'] == 'john@y.edu'
    assert info['schedsrc'].startswith('J1234+5678 (T|X)')
    assert info['emptyval'] is None             # empty value -> None
    assert 'bare line without equals' not in info
    assert not Path(captured['dest']).exists()  # the .jex copy must not be kept


def test_fetch_jexp_info_scp_failure_raises(monkeypatch):
    def boom(origin, dest, **kwargs):
        raise ValueError("scp exit 1: no such file")

    monkeypatch.setattr(_servers_mod, 'retrieve_servers', lambda: _JEXP_SERVERS)
    monkeypatch.setattr(_utils_mod, 'scp', boom)
    with pytest.raises(retrieval.RetrievalError) as excinfo:
        _jive.fetch_jexp_info('EB101')
    assert 'eb101.jex' in str(excinfo.value)


def test_fetch_jexp_info_missing_server_raises(monkeypatch):
    monkeypatch.setattr(_servers_mod, 'retrieve_servers', lambda: _servers_mod.Servers([]))
    with pytest.raises(retrieval.RetrievalError) as excinfo:
        _jive.fetch_jexp_info('EB101')
    assert 'jexp' in str(excinfo.value)


# ------------------------------------------- jive.fetch_from_vlbeer (skip already-downloaded)

import datetime as _dt                     # noqa: E402
from types import SimpleNamespace as _NS   # noqa: E402

_VLBEER = _NS(user='evn', host='vlbeer.ira.inaf.it', path="vlbi_arch/{obsdate.strftime('%y%b')}")
_REMOTE_ANTABFS = ['eb101_ef.antabfs', 'eb101_wb.antabfs']


class _Ants:
    names = []
    logfsfile = []
    antabfsfile = []
    observed = []

    def __getitem__(self, key):
        raise ValueError(key)   # unknown antenna -> caught by the marking loop


def _vlbeer_exp(pipe_temp):
    return _NS(expname='EB101', obsdate=_dt.date(2026, 4, 10),
               dirs=_NS(pipe_temp=pipe_temp), antennas=_Ants(), store=lambda: None)


def _install_vlbeer_fakes(monkeypatch):
    """Fake ssh (lists the remote antabfs) and scp (creates the fetched files); records calls."""
    calls = {'scp': [], 'ssh': []}

    def fake_ssh(host, commands, **kwargs):
        calls['ssh'].append(commands)
        if 'antabfs' in commands:
            base = commands.split()[-1].rsplit('/', 1)[0]
            return '\n'.join(f"{base}/{n}" for n in _REMOTE_ANTABFS) + '\n'
        raise ValueError("ls: no such file")   # no log/flag files on vlbeer

    def fake_scp(origin, dest, timeout=None, **kwargs):
        calls['scp'].append(origin)
        name = origin.split('/')[-1]
        dest_dir = Path(dest.rstrip('/'))
        if '*' in name:
            if 'antabfs' in name:
                for n in _REMOTE_ANTABFS:
                    (dest_dir / n).write_text('FRESH', encoding='utf-8')
            return True
        (dest_dir / name).write_text('FRESH', encoding='utf-8')
        return True

    monkeypatch.setattr(_utils_mod, 'ssh', fake_ssh)
    monkeypatch.setattr(_utils_mod, 'scp', fake_scp)
    return calls


def _antabfs_scp(calls):
    return [o for o in calls['scp'] if 'antabfs' in o]


def test_vlbeer_first_download_uses_single_wildcard(tmp_path, monkeypatch):
    calls = _install_vlbeer_fakes(monkeypatch)
    _jive.fetch_from_vlbeer(_vlbeer_exp(tmp_path), _VLBEER)
    a_scp = _antabfs_scp(calls)
    assert len(a_scp) == 1 and a_scp[0].endswith('*antabfs')     # one wildcard scp
    assert not any('antabfs' in c for c in calls['ssh'])         # no listing needed
    assert (tmp_path / 'eb101_ef.antabfs').exists() and (tmp_path / 'eb101_wb.antabfs').exists()


def test_vlbeer_rerun_fetches_only_new_and_keeps_edited(tmp_path, monkeypatch):
    (tmp_path / 'eb101_ef.antabfs').write_text('EDITED', encoding='utf-8')  # hand-edited copy
    calls = _install_vlbeer_fakes(monkeypatch)
    _jive.fetch_from_vlbeer(_vlbeer_exp(tmp_path), _VLBEER)
    a_scp = _antabfs_scp(calls)
    assert any('antabfs' in c for c in calls['ssh'])                      # listed vlbeer
    assert any(o.endswith('eb101_wb.antabfs') for o in a_scp)             # fetched the new one
    assert not any(o.endswith('eb101_ef.antabfs') for o in a_scp)         # did not re-fetch
    assert not any(o.endswith('*antabfs') for o in a_scp)                 # no wildcard clobber
    assert (tmp_path / 'eb101_ef.antabfs').read_text() == 'EDITED'        # preserved, not overwritten


def test_vlbeer_rerun_nothing_new_fetches_nothing(tmp_path, monkeypatch):
    (tmp_path / 'eb101_ef.antabfs').write_text('EDITED-EF', encoding='utf-8')
    (tmp_path / 'eb101_wb.antabfs').write_text('EDITED-WB', encoding='utf-8')
    calls = _install_vlbeer_fakes(monkeypatch)
    _jive.fetch_from_vlbeer(_vlbeer_exp(tmp_path), _VLBEER)
    assert any('antabfs' in c for c in calls['ssh'])
    assert _antabfs_scp(calls) == []                                     # nothing re-fetched
    assert (tmp_path / 'eb101_ef.antabfs').read_text() == 'EDITED-EF'
    assert (tmp_path / 'eb101_wb.antabfs').read_text() == 'EDITED-WB'


class TestVlbaCalibrationFiles:
    """Global (EVN+VLBA) experiments need the VLBA calibration files for antab_editor.

    `get_vlba_antab` must place `{exp}cal.vlba` and `gbt_gains.key` in antenna_files/,
    and must never turn a missing file into a step failure (the operator can copy them
    by hand and re-run the antab step).
    """

    def _exp(self, tmp_path):
        import datetime as dt
        from evn_postprocess import experiment
        dirs = experiment.Dirs(logs=tmp_path / 'logs', plots=tmp_path / 'plots',
                               pipeline=tmp_path / 'p', pipe_in=tmp_path / 'p/in',
                               pipe_out=tmp_path / 'p/out', pipe_temp=tmp_path / 'antenna_files')
        exp = experiment.Experiment('GP052', dt.date(2026, 4, 10), 'tester', dirs)
        exp.antennas = experiment.Antennas([experiment.Antenna(name='Ef'),
                                            experiment.Antenna(name='Br')])
        return exp

    def test_detects_a_global_experiment(self, tmp_path):
        from evn_postprocess import experiment
        from evn_postprocess.retrieval import jive
        exp = self._exp(tmp_path)
        assert jive.has_vlba_stations(exp) is True
        exp.antennas = experiment.Antennas([experiment.Antenna(name='Ef'),
                                            experiment.Antenna(name='Wb')])
        assert jive.has_vlba_stations(exp) is False

    def test_fetches_both_files(self, tmp_path, monkeypatch):
        from evn_postprocess.retrieval import jive
        exp = self._exp(tmp_path)
        gains = tmp_path / 'gbt_gains.key'
        gains.write_text('GAIN EF\n')
        monkeypatch.setattr(jive, 'GAINS_KEY', gains)

        copied = []

        def fake_scp(origin, destination, **kwargs):
            copied.append(origin)
            Path(destination).write_text('cal\n')
            return True

        monkeypatch.setattr(jive.utils, 'scp', fake_scp)
        assert jive.get_vlba_antab(exp) is True
        assert (exp.dirs.pipe_temp / 'gp052cal.vlba').exists()
        assert (exp.dirs.pipe_temp / 'gbt_gains.key').exists()
        # The cal file is looked for in the correlator's log2vex directory for this date.
        assert copied and 'GP052_20260410/gp052cal.vlba' in copied[0]

    def test_a_missing_file_is_reported_not_raised(self, tmp_path, monkeypatch):
        from evn_postprocess.retrieval import jive
        exp = self._exp(tmp_path)
        monkeypatch.setattr(jive, 'GAINS_KEY', tmp_path / 'absent_gains.key')

        def failing_scp(*a, **k):
            raise ValueError("no such file on the remote host")

        monkeypatch.setattr(jive.utils, 'scp', failing_scp)
        assert jive.get_vlba_antab(exp) is False   # reported, never raised

    def test_no_server_configuration_is_reported_not_raised(self, tmp_path, monkeypatch):
        from evn_postprocess.retrieval import jive
        exp = self._exp(tmp_path)

        def no_config():
            raise FileNotFoundError("computers.toml not found")

        monkeypatch.setattr(jive.servers, 'retrieve_servers', no_config)
        assert jive.get_vlba_antab(exp) is False


class TestMakeLisPhaseCentres:
    """`make_lis -m SRC` keeps the calibrator data in one pass when a scan carries many
    phase centres; without it they would be duplicated in every resulting pass.
    """

    def _exp(self, tmp_path, n_centres):
        import datetime as dt
        from evn_postprocess import experiment
        dirs = experiment.Dirs(logs=tmp_path / 'logs', plots=tmp_path / 'plots',
                               pipeline=tmp_path / 'p', pipe_in=tmp_path / 'p/in',
                               pipe_out=tmp_path / 'p/out', pipe_temp=tmp_path / 'p/tmp')
        exp = experiment.Experiment('EM123', dt.date(2026, 4, 10), 'tester', dirs)
        centres = tuple(f"TARGET{i}" for i in range(n_centres))
        exp.scans = experiment.Scans([
            experiment.Scan(scanno='No0001', starttime=dt.datetime(2026, 4, 10, 10, 0),
                            duration_s=300, source='3C286', stations_scheduled=('Ef',)),
            experiment.Scan(scanno='No0002', starttime=dt.datetime(2026, 4, 10, 10, 10),
                            duration_s=300, source=centres[0] if centres else '3C286',
                            stations_scheduled=('Ef',), phase_centers=centres)])
        return exp

    def test_no_filter_for_a_few_phase_centres(self, tmp_path):
        from evn_postprocess.retrieval import jive
        assert jive._make_lis_filter(self._exp(tmp_path, 0)) == ''
        assert jive._make_lis_filter(self._exp(tmp_path, jive.MAX_PHASE_CENTERS)) == ''

    def test_filters_on_the_first_source_of_the_scan(self, tmp_path):
        from evn_postprocess.retrieval import jive
        exp = self._exp(tmp_path, jive.MAX_PHASE_CENTERS + 1)
        assert jive._make_lis_filter(exp) == ' -m TARGET0'

    def test_the_option_reaches_make_lis(self, tmp_path, monkeypatch):
        from evn_postprocess.retrieval import jive
        from evn_postprocess.servers import Server, Servers
        exp = self._exp(tmp_path, jive.MAX_PHASE_CENTERS + 1)
        monkeypatch.setattr(jive.servers, 'retrieve_servers',
                            lambda: Servers([Server('ccs', 'jops', 'ccs', Path('/ccs/expr/{expname}'))]))
        monkeypatch.setattr(jive, 'lis_files_in_ccs', lambda exp, server: False)
        commands: list[str] = []
        monkeypatch.setattr(jive.utils, 'ssh', lambda host, cmd, **kw: commands.append(cmd))
        assert jive.create_lis_files(exp) is True
        assert commands and commands[0].endswith('/ccs/bin/make_lis -e EM123 -m TARGET0')
