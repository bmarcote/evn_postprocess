"""The auxiliary lag-space products live in their own directory (lag_spectrum.LAG_DIR) and never
look like a correlator pass: the lag .lis names neither the pass's MS nor its FITS-IDI files,
pass discovery in the experiment directory does not see it, and legacy root-level products are
moved into the directory."""
from pathlib import Path

import pytest

from evn_postprocess import experiment, lag_spectrum, lisfiles, utils


HEADER = "GM084A gm084a.vix Prod gm084a_cont.ms gm084a_1_1.IDI\n"
JOBS = ("+ 53590/GM084A_No0001.cor   - No0001 No0001 32 1.0 Xpol 599 GOOD DONE PROD prod_cont "
        "gm084a.vix \"GIT             r2147483647\" -- eo:setup_ref_station=Ef\n"
        "- 53593/GM084A_No0001.cor   - No0001 No0001 1024 1.0 Xpol 599 GOOD DONE PROD prod_line "
        "gm084a.vix \"GIT             r2147483647\" -- eo:setup_ref_station=Ef\n")


class ExpStub:
    expname = 'GM084A'

    def __init__(self):
        self.correlator_passes = [experiment.CorrelatorPass(
            lisfile=Path('gm084a_cont.lis'), msfile=Path('gm084a_cont.ms'),
            fitsidifile='gm084a_1_1.IDI', pipeline=True)]
        self.lag_pass = None
        self.no_lag = False
        self.eEVNname = None

        class Sources:
            fringefinder = ['3C84']
            calibrator = ['IRAS21391']
        self.sources = Sources()


@pytest.fixture
def expdir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'gm084a_cont.lis').write_text(HEADER + JOBS)
    return tmp_path


def test_lag_lisfile_is_isolated_and_names_its_own_outputs(expdir):
    exp = ExpStub()
    lag_lis = lag_spectrum.create_lisfile(exp, exp.correlator_passes[0])
    assert lag_lis == Path('lag-spectrum/gm084a-lag.lis')
    header, job1, job2 = lag_lis.read_text().splitlines()
    # Own MS and IDI names (never the pass's), paths valid from inside lag-spectrum/.
    assert header == "GM084A ../gm084a.vix Prod gm084a-lag.ms gm084a-lag.IDI"
    assert 'gm084a_1_1.IDI' not in lag_lis.read_text()
    assert job1.startswith("+ ../53590/GM084A_No0001.cor   - No0001")
    assert job2.startswith("- ../53593/GM084A_No0001.cor   - No0001")
    assert " ../gm084a.vix \"GIT" in job1 and "eo:setup_ref_station=Ef" in job1
    # The pass's own .lis is untouched.
    assert (expdir / 'gm084a_cont.lis').read_text() == HEADER + JOBS


def test_lag_lisfile_requires_the_ms_in_the_header(expdir):
    exp = ExpStub()
    exp.correlator_passes[0].msfile = Path('other.ms')
    with pytest.raises(ValueError):
        lag_spectrum.create_lisfile(exp, exp.correlator_passes[0])


def test_pass_discovery_does_not_see_lag_products(expdir):
    lag_spectrum.create_lisfile(ExpStub(), ExpStub().correlator_passes[0])
    assert lisfiles._pass_lisfiles('gm084a*.lis') == ['gm084a_cont.lis']
    assert lisfiles._pass_lisfiles('*.lis') == ['gm084a_cont.lis']


def test_legacy_lag_products_are_moved(expdir):
    legacy_lis = expdir / 'gm084a-lag.lis'
    legacy_lis.write_text(HEADER.replace('gm084a_cont.ms', 'gm084a-lag.ms') + JOBS)
    legacy_ms = expdir / 'gm084a-lag.ms'
    legacy_ms.mkdir()
    (legacy_ms / 'table.dat').write_text('x')
    exp = ExpStub()
    exp.lag_pass = experiment.CorrelatorPass(lisfile=Path('gm084a-lag.lis'),
                                             msfile=Path('gm084a-lag.ms'), fitsidifile='', pipeline=False)

    lag_spectrum.migrate_legacy_products(exp)

    assert not legacy_lis.exists() and not legacy_ms.exists()
    assert (expdir / 'lag-spectrum/gm084a-lag.ms/table.dat').read_text() == 'x'
    # The moved .lis is rebuilt: it no longer names the first pass's IDI files.
    assert 'gm084a_1_1.IDI' not in (expdir / 'lag-spectrum/gm084a-lag.lis').read_text()
    assert exp.lag_pass.msfile == Path('lag-spectrum/gm084a-lag.ms')
    assert exp.lag_pass.lisfile == Path('lag-spectrum/gm084a-lag.lis')
    # Nothing to do the second time.
    lag_spectrum.migrate_legacy_products(exp)
    assert exp.lag_pass.msfile == Path('lag-spectrum/gm084a-lag.ms')


def test_migration_is_a_noop_without_legacy_products(expdir):
    lag_spectrum.migrate_legacy_products(ExpStub())
    assert not (expdir / 'lag-spectrum').exists()


def test_shell_command_cwd(expdir, monkeypatch):
    recorded = []
    monkeypatch.setattr(utils.reporting, 'record_command', recorded.append)
    (expdir / 'lag-spectrum').mkdir()
    out = utils.shell_command('pwd', cwd=lag_spectrum.LAG_DIR, echo=False)
    assert Path(out.strip()) == expdir / 'lag-spectrum'
    assert recorded == ['(cd lag-spectrum && pwd)']


def test_run_j2ms2_runs_inside_lag_dir_and_is_not_a_pass(expdir, monkeypatch):
    calls = []

    def fake_shell(command, params, **kwargs):
        calls.append((command, params, kwargs))
        (Path(kwargs['cwd']) / 'gm084a-lag.ms').mkdir()
        return ''
    monkeypatch.setattr(lag_spectrum.utils, 'shell_command', fake_shell)
    exp = ExpStub()

    assert lag_spectrum.run_j2ms2(exp) is True
    (command, params, kwargs), = calls
    assert command == 'j2ms2' and kwargs['cwd'] == Path('lag-spectrum')
    assert params[:2] == ['-v', 'gm084a-lag.lis']
    assert 'fo:filter/source=3C84,IRAS21391' in params
    assert kwargs['logfile'] == Path('lag-spectrum/j2ms2-lag.log')
    assert exp.lag_pass.msfile == Path('lag-spectrum/gm084a-lag.ms')
    assert len(exp.correlator_passes) == 1
    # Already there: not rebuilt.
    assert lag_spectrum.run_j2ms2(exp) is False and len(calls) == 1


def test_run_j2ms2_failure_is_non_fatal_and_cleans_up(expdir, monkeypatch):
    def failing_shell(command, params, **kwargs):
        (Path(kwargs['cwd']) / 'gm084a-lag.ms').mkdir()
        raise ValueError('j2ms2 failed')
    monkeypatch.setattr(lag_spectrum.utils, 'shell_command', failing_shell)
    assert lag_spectrum.run_j2ms2(ExpStub()) is False
    assert not (expdir / 'lag-spectrum/gm084a-lag.ms').exists()


def test_run_j2ms2_no_lag(expdir):
    exp = ExpStub()
    exp.no_lag = True
    assert lag_spectrum.run_j2ms2(exp) is False
    assert not (expdir / 'lag-spectrum').exists()
