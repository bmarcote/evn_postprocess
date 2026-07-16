"""Tests for evn_postprocess.inputs (vex-only experiment bootstrap).

Pure-function tests operate on parsed-vex dicts; the integration tests write a small
but grammar-complete vex fixture and run the real parser (ply) and Experiment build.
"""
import datetime as dt
import pytest

from evn_postprocess import inputs


# A minimal but complete vex fixture: $EXPER with e-EVN description, two stations,
# one source, one scan with two station lines (multi-valued key).
EEVN_VEX = '''\
VEX_rev = 1.5;
$EXPER;
  def EA100;
    exper_name = EA100;
    exper_description = "e-EVN: EA100, EB200";
    exper_nominal_start = 2026y100d02h00m00s;
    exper_nominal_stop = 2026y100d10h00m00s;
  enddef;
$STATION;
  def Ef;
    ref $SITE = EFLSBERG;
  enddef;
  def Wb;
    ref $SITE = WSTRBORK;
  enddef;
$SOURCE;
  def J1848+3244;
    source_name = J1848+3244;
    ra = 18h48m22.0s;
    dec = 32d44'33.0";
  enddef;
$SCHED;
  scan No0001;
    start = 2026y100d02h00m00s;
    mode = mode1;
    source = J1848+3244;
    station = Ef : 0 sec : 120 sec;
    station = Wb : 0 sec : 120 sec;
  endscan;
'''

PLAIN_VEX = EEVN_VEX.replace('exper_description = "e-EVN: EA100, EB200";',
                             'exper_description = "Plain experiment";'
                             ).replace('EA100', 'EB101')


# ------------------------------------------------------------- pure functions

EXPER_DICT = {'EXPER': {'EA100': {'exper_name': 'EA100',
                                  'exper_description': 'e-EVN: EA100, EB200',
                                  'exper_nominal_start': '2026y100d02h00m00s'}},
              'SCHED': {'No0001': {'start': '2026y101d03h00m00s'}}}


def test_parse_obsdate_from_exper_block():
    assert inputs.parse_obsdate(EXPER_DICT, 'x.vix') == dt.date(2026, 4, 10)  # doy 100


def test_parse_obsdate_falls_back_to_sched():
    data = {'EXPER': {'EA100': {}}, 'SCHED': {'No0001': {'start': '2026y101d03h00m00s'},
                                              'No0002': {'start': '2026y101d05h00m00s'}}}
    assert inputs.parse_obsdate(data, 'x.vix') == dt.date(2026, 4, 11)


def test_parse_obsdate_error_names_file():
    with pytest.raises(inputs.InputsError) as excinfo:
        inputs.parse_obsdate({'EXPER': {}, 'SCHED': {}}, 'weird.vix')
    assert 'weird.vix' in str(excinfo.value)


def test_parse_eevn_member():
    eevnname, exps = inputs.parse_eevn(EXPER_DICT, 'EB200', 'x.vix')
    assert eevnname == 'EA100'
    assert exps == ['EA100', 'EB200']


def test_parse_eevn_run_leader():
    eevnname, exps = inputs.parse_eevn(EXPER_DICT, 'EA100', 'x.vix')
    assert eevnname == 'EA100'  # EXP1 itself carries the run name too


def test_parse_eevn_regular_experiment():
    data = {'EXPER': {'EB101': {'exper_description': 'Nice observation'}}}
    assert inputs.parse_eevn(data, 'EB101', 'x.vix') == (None, ['EB101'])


def test_parse_expname():
    assert inputs.parse_expname(EXPER_DICT, 'x.vix') == 'EA100'
    assert inputs.parse_expname({}, '/somewhere/eb101.vix') == 'EB101'


def test_find_local_vex(tmp_path):
    assert inputs.find_local_vex('EB101', tmp_path) is None
    (tmp_path / 'other.vex').touch()
    assert inputs.find_local_vex('EB101', tmp_path).name == 'other.vex'  # single-vex fallback
    (tmp_path / 'EB101.vix').touch()
    assert inputs.find_local_vex('EB101', tmp_path).name == 'EB101.vix'  # exact name preferred


# --------------------------------------------------------------- integration

def test_load_experiment_regular(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    vexfile = tmp_path / 'eb101.vex'
    vexfile.write_text(PLAIN_VEX)
    (tmp_path / 'eb101.toml').write_text(
        '[observation]\nsupsci = "marcote"\n'
        '[[pi]]\nname = "Jane Doe"\nemail = "jane@inst.edu"\n'
        '[sources."J1848+3244"]\ntype = "target"\nprotected = true\n')
    exp = inputs.load_experiment(vexfile, supsci='someone')
    assert exp.expname == 'EB101'
    assert exp.obsdate == dt.date(2026, 4, 10)
    assert exp.eEVNname is None
    assert sorted(exp.antennas.names) == ['Ef', 'Wb']
    assert len(exp.scans) == 1
    assert exp.scans[0].stations_scheduled == ('Ef', 'Wb')
    assert exp.supsci == 'marcote'                      # toml wins over the argument
    assert exp.pi[0].name == 'Jane Doe'
    from evn_postprocess.experiment import SourceType
    assert exp.sources['J1848+3244'].type == SourceType.target
    assert exp.sources['J1848+3244'].protected is True
    # The canonical vix name must exist afterwards (Experiment.vixfile convention):
    assert (tmp_path / 'EB101.vix').exists()


def test_load_experiment_eevn_member(tmp_path, monkeypatch):
    """Acceptance: vex with 'e-EVN: EA100, EB200', initialized as EB200."""
    monkeypatch.chdir(tmp_path)
    vexfile = tmp_path / 'ea100.vix'   # e-EVN: the vex carries EXP1's name
    vexfile.write_text(EEVN_VEX)
    exp = inputs.load_experiment(vexfile, supsci='someone', expname='EB200')
    assert exp.expname == 'EB200'
    assert exp.eEVNname == 'EA100'
    assert exp.eEVN_experiments() == ['EA100', 'EB200']
    assert (tmp_path / 'EB200.vix').exists()            # symlink to the shared vex


def test_load_experiment_no_servers_needed(tmp_path, monkeypatch):
    """No MASTER_PROJECTS/.jexp/.expsum/computers.toml anywhere: must still work."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.vex').write_text(PLAIN_VEX)
    exp = inputs.load_experiment(tmp_path / 'eb101.vex', supsci='someone')
    assert exp.expname == 'EB101' and exp.obsdate == dt.date(2026, 4, 10)


def test_load_experiment_unparseable_vex(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    bad = tmp_path / 'eb101.vex'
    bad.write_text('VEX_rev = 1.5;\n$EXPER\nthis is = not; valid vex ;;\n')
    with pytest.raises(inputs.InputsError) as excinfo:
        inputs.load_experiment(bad, supsci='someone')
    assert 'eb101.vex' in str(excinfo.value)
    assert 'line' in str(excinfo.value)


def test_load_experiment_missing_vex(tmp_path):
    with pytest.raises(inputs.InputsError) as excinfo:
        inputs.load_experiment(tmp_path / 'nope.vex', supsci='someone')
    assert 'nope.vex' in str(excinfo.value)


def test_multi_phase_centre_scan_detected(tmp_path, monkeypatch):
    """A scan with several source lines: all recorded as phase centres (Issue 16)."""
    monkeypatch.chdir(tmp_path)
    multi = PLAIN_VEX.replace('    source = J1848+3244;\n',
                              '    source = J1848+3244;\n    source = J1848+3245;\n')
    (tmp_path / 'eb101.vex').write_text(multi)
    exp = inputs.load_experiment(tmp_path / 'eb101.vex', supsci='someone')
    scan = exp.scans[0]
    assert scan.source == 'J1848+3244'
    assert scan.phase_centers == ('J1848+3244', 'J1848+3245')
    assert exp.multi_phase_center is True
    assert exp.phase_center_sources == {'J1848+3244': ['J1848+3244', 'J1848+3245']}


def test_single_phase_centre_unchanged(tmp_path, monkeypatch):
    """Regression: a standard single-source scan keeps the historical behaviour."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.vex').write_text(PLAIN_VEX)
    exp = inputs.load_experiment(tmp_path / 'eb101.vex', supsci='someone')
    scan = exp.scans[0]
    assert scan.phase_centers == () and scan.all_sources == ('J1848+3244',)
    assert exp.multi_phase_center is False
    assert exp.phase_center_sources == {}


def test_scan_phase_centers_json_roundtrip():
    import datetime as _dt
    from evn_postprocess.experiment import Scan
    scan = Scan('No0001', _dt.datetime(2026, 4, 10, 2), 120, 'SRC', ('Ef',),
                phase_centers=('SRC', 'SRC2'))
    assert Scan.from_dict(scan.to_dict()) == scan
    # Old JSON state files (without the key) keep loading:
    old = scan.to_dict()
    del old['phase_centers']
    assert Scan.from_dict(old).phase_centers == ()


def test_toml_source_not_in_vex_is_ignored(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'eb101.vex').write_text(PLAIN_VEX)
    (tmp_path / 'eb101.toml').write_text('[sources."J9999+9999"]\ntype = "target"\n')
    exp = inputs.load_experiment(tmp_path / 'eb101.vex', supsci='someone')  # must not raise
    assert 'J9999+9999' not in exp.sources
