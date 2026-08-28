"""Tests for the pre-distribution verification step (evn_postprocess.verification).

The three checks are thin wrappers around external tools, so what is worth testing is the
parsing: telling the weight noise that every MS -> FITS-IDI conversion produces apart from
an actual loss of visibilities, and reading the loss/gain/nZero line of
check-multipart-fits.py. Both parsers are fed the real output shapes of those tools.
"""
from __future__ import annotations

import datetime as dt
import subprocess
from pathlib import Path

import pytest
from astropy.io import fits

from evn_postprocess import experiment
from evn_postprocess import tools
from evn_postprocess import verification


# A healthy run: the seconds and the visibility counts agree everywhere and only the
# weights differ, which is what compare-ms-idi.py reports for most (baseline, source)
# pairs after a perfectly good conversion.
HEALTHY = """Successful readonly open of default-locked table es124-IC2810NUC.ms: 22 columns, 4380328 rows
es124_2_1.IDI1
es124_2_1.IDI2
('EfEf', 'Ic2810nuc') :
      15394.0000s wgt=245480.8214   7697 times in   MS: es124-IC2810NUC.ms
     15394.0000s wgt=243547.6475   7697 times in  IDI: : es124_2_1.IDI*
('WbWb', 'J1800+3848') :
        300.0000s wgt=  1102.8989    150 times in   MS: es124-IC2810NUC.ms
       300.0000s wgt=  1088.0003    150 times in  IDI: : es124_2_1.IDI*
Checked 2 data sets, 225 common keys with 196 problems identified
"""


@pytest.fixture(autouse=True)
def in_experiment_dir(tmp_path, monkeypatch):
    """Every test runs inside its own directory: the checks glob the working directory,
    and both the tools' output and the replayable command log are written next to it."""
    monkeypatch.chdir(tmp_path)


def make_exp(tmp_path: Path, passes: int = 1) -> experiment.Experiment:
    """An experiment with *passes* correlator passes, rooted at *tmp_path*."""
    dirs = experiment.Dirs(logs=tmp_path / 'logs', plots=tmp_path / 'plots',
                           pipeline=tmp_path / 'pipeline', pipe_in=tmp_path / 'pipeline/in',
                           pipe_out=tmp_path / 'pipeline/out', pipe_temp=tmp_path / 'antenna_files')
    dirs.logs.mkdir(parents=True, exist_ok=True)
    exp = experiment.Experiment('ES124', dt.date(2026, 4, 10), 'tester', dirs)
    exp.correlator_passes = [
        experiment.CorrelatorPass(Path(f'es124_{i}_1.lis'), Path(f'es124-pass{i}.ms'),
                                  f'es124_{i}_1.IDI', True) for i in range(1, passes + 1)]
    return exp


def write_idi(name: str, tables: tuple[str, ...] = ('SYSTEM_TEMPERATURE', 'GAIN_CURVE')) -> None:
    """Writes a minimal FITS file carrying (only) the named extensions."""
    fits.HDUList([fits.PrimaryHDU()]
                 + [fits.BinTableHDU(name=table) for table in tables]).writeto(name)


class TestIdiFiles:
    """The FITS-IDI chunks of a pass, in chunk order and without the stray files."""

    def test_chunks_are_ordered_numerically(self, tmp_path):
        for n in (1, 2, 10, 9):
            Path(f'es124_1_1.IDI{n}').touch()
        exp = make_exp(tmp_path)
        assert verification._idi_files(exp.correlator_passes[0]) == [
            'es124_1_1.IDI1', 'es124_1_1.IDI2', 'es124_1_1.IDI9', 'es124_1_1.IDI10']

    def test_pconvert_leftovers_are_ignored(self, tmp_path):
        Path('es124_1_1.IDI1').touch()
        Path('es124_1_1.IDI1.PCONVERT').touch()
        exp = make_exp(tmp_path)
        assert verification._idi_files(exp.correlator_passes[0]) == ['es124_1_1.IDI1']


class TestCheckAntab:
    """The Tsys/gain-curve tables prearchive appended must be in the first file of a pass."""

    def test_both_tables_present(self, tmp_path):
        write_idi('es124_1_1.IDI1')
        Path('es124_1_1.IDI2').touch()  # later chunks never carry the tables
        assert verification.check_antab(make_exp(tmp_path)).ok

    def test_missing_table_is_named(self, tmp_path):
        write_idi('es124_1_1.IDI1', tables=('SYSTEM_TEMPERATURE',))
        check = verification.check_antab(make_exp(tmp_path))
        assert not check.ok
        assert 'GAIN_CURVE' in check.details[0] and 'SYSTEM_TEMPERATURE' not in check.details[0]

    def test_unreadable_file_counts_as_missing(self, tmp_path):
        Path('es124_1_1.IDI1').write_text('not a FITS file at all')
        check = verification.check_antab(make_exp(tmp_path))
        assert not check.ok
        assert 'SYSTEM_TEMPERATURE and GAIN_CURVE' in check.details[0]

    def test_no_fits_idi_at_all(self, tmp_path):
        check = verification.check_antab(make_exp(tmp_path))
        assert not check.ok and 'es124_1_1.IDI*' in check.details[0]


def stub_tool(monkeypatch, stdout: str, returncode: int = 0, stderr: str = ''):
    """Replaces tools.run with one returning a canned result, recording the argv it got."""
    seen: list[list[str]] = []

    def fake_run(name, args, **kwargs):
        seen.append([name, *args])
        return subprocess.CompletedProcess([name, *args], returncode, stdout, stderr)

    monkeypatch.setattr(tools, 'run', fake_run)
    return seen


class TestCheckMultipart:
    """check-multipart-fits.py only prints the FITS-IDI sets where something is off."""

    def test_silent_output_means_every_set_is_contiguous(self, tmp_path, monkeypatch):
        seen = stub_tool(monkeypatch, stdout='')
        assert verification.check_multipart(make_exp(tmp_path, passes=2)).ok
        assert seen == [['check-multipart-fits.py', 'es124_1_1.IDI*', 'es124_2_1.IDI*']]

    def test_small_loss_passes(self, tmp_path, monkeypatch):
        stub_tool(monkeypatch, stdout='es124_1_1 loss=1.9999891519546509s gain=0.0s nZero=0\n')
        assert verification.check_multipart(make_exp(tmp_path)).ok

    def test_large_loss_fails(self, tmp_path, monkeypatch):
        stub_tool(monkeypatch, stdout='es124_1_1 loss=45.5s gain=0.0s nZero=0\n')
        check = verification.check_multipart(make_exp(tmp_path))
        assert not check.ok
        assert '45.5 s of data lost' in check.details[0]

    def test_zero_timestamps_only_warn(self, tmp_path, monkeypatch):
        stub_tool(monkeypatch,
                  stdout='/d/es124_1_1.IDI2 [of 4]: start=1.0 end=2.0 nZero=17\n'
                         'es124_1_1 loss=0.0s gain=0.0s nZero=17\n')
        assert verification.check_multipart(make_exp(tmp_path)).ok

    def test_tool_crash_is_a_problem(self, tmp_path, monkeypatch):
        stub_tool(monkeypatch, stdout='', returncode=1, stderr='Traceback\nMemoryError\n')
        check = verification.check_multipart(make_exp(tmp_path))
        assert not check.ok and 'MemoryError' in check.details[0]

    def test_missing_tool_is_a_problem(self, tmp_path, monkeypatch):
        def missing(*a, **k):
            raise tools.ToolMissingError('nope')
        monkeypatch.setattr(tools, 'run', missing)
        assert not verification.check_multipart(make_exp(tmp_path)).ok


class TestCompareProblems:
    """Weight noise is expected; anything else in compare-ms-idi.py's report is not."""

    def test_weight_noise_alone_is_not_a_problem(self):
        assert verification._compare_problems(HEALTHY, 'L') == []

    def test_exposure_and_visibility_mismatch(self):
        problems = verification._compare_problems(
            "('WbWb', 'J1310+3220') :\n"
            "        240.0000s wgt=  1903.5890    120 times in   MS: es124.ms\n"
            "       238.0000s wgt=  1888.0005    119 times in  IDI: : es124_1_1.IDI*\n"
            "Checked 2 data sets, 1 common keys with 1 problems identified\n", 'L')
        assert len(problems) == 1
        assert '240.0000 s in 120 visibilities in the MS' in problems[0]
        assert '238.0000 s in 119' in problems[0]

    def test_weight_beyond_tolerance(self):
        problems = verification._compare_problems(
            "('EfWb', 'Foo') :\n"
            "        240.0000s wgt=  1903.5890 once in   MS: es124.ms\n"
            "       240.0000s wgt=   903.0005 once in  IDI: : es124_1_1.IDI*\n"
            "Checked 2 data sets, 1 common keys with 1 problems identified\n", 'L')
        assert len(problems) == 1 and 'more than the 5% expected' in problems[0]

    def test_negative_weights(self):
        problems = verification._compare_problems(
            "('EfEf', 'Neg') :\n"
            "        240.0000s wgt=  1903.5890    3<0    120 times in   MS: es124.ms\n"
            "        240.0000s wgt=  1903.5890    120 times in  IDI: : es124_1_1.IDI*\n"
            "Checked 2 data sets, 1 common keys with 1 problems identified\n", 'L')
        assert len(problems) == 1 and 'negative weights' in problems[0]

    def test_extra_keys_report(self):
        problems = verification._compare_problems(
            "==== Problem report ====\n"
            "  IDI: : es124_1_1.IDI*\n"
            " Extra keys:\n"
            "\t('JbJb', 'J1800+3848') found      12 times\n"
            " =========================\n"
            "Checked 2 data sets, 1 common keys "
            "with 0 problems identified and 1 non-common keys in 1 formats\n", 'L')
        assert len(problems) == 1 and "('JbJb', 'J1800+3848')" in problems[0]

    def test_incomplete_run_is_a_problem(self):
        problems = verification._compare_problems('Successful readonly open of es124.ms\n', 'L')
        assert len(problems) == 1 and 'did not get as far as comparing' in problems[0]


class TestCompareMsIdi:
    """One comparison per correlator pass, MS against the FITS-IDI files made from it."""

    def test_passes_and_calls_the_tool_per_pass(self, tmp_path, monkeypatch):
        exp = make_exp(tmp_path, passes=2)
        for a_pass in exp.correlator_passes:
            a_pass.msfile.mkdir()  # an MS is a directory
            Path(f'{a_pass.fitsidifile}1').touch()
        seen = stub_tool(monkeypatch, stdout=HEALTHY)
        assert verification.compare_ms_idi(exp).ok
        assert seen == [['compare-ms-idi.py', '--ms', 'es124-pass1.ms', '--idi', 'es124_1_1.IDI1'],
                        ['compare-ms-idi.py', '--ms', 'es124-pass2.ms', '--idi', 'es124_2_1.IDI1']]

    def test_missing_ms_is_reported_not_crashed(self, tmp_path, monkeypatch):
        Path('es124_1_1.IDI1').touch()
        stub_tool(monkeypatch, stdout=HEALTHY)
        check = verification.compare_ms_idi(make_exp(tmp_path))
        assert not check.ok and 'the MS is gone' in check.details[0]


class TestVerify:
    """The step itself: every check runs, and any failure keeps the run out of distribute."""

    def test_all_checks_pass(self, tmp_path, monkeypatch):
        exp = make_exp(tmp_path)
        exp.correlator_passes[0].msfile.mkdir()
        write_idi('es124_1_1.IDI1')
        stub_tool(monkeypatch, stdout=HEALTHY)
        assert verification.verify(exp) is True

    def test_a_failed_check_fails_the_step(self, tmp_path, monkeypatch):
        exp = make_exp(tmp_path)
        exp.correlator_passes[0].msfile.mkdir()
        write_idi('es124_1_1.IDI1', tables=('SYSTEM_TEMPERATURE',))  # no GAIN_CURVE
        stub_tool(monkeypatch, stdout=HEALTHY)
        assert verification.verify(exp) is False

    def test_tool_output_is_kept_for_inspection(self, tmp_path, monkeypatch):
        exp = make_exp(tmp_path)
        exp.correlator_passes[0].msfile.mkdir()
        write_idi('es124_1_1.IDI1')
        stub_tool(monkeypatch, stdout=HEALTHY)
        verification.verify(exp)
        assert 'Checked 2 data sets' in (exp.dirs.logs / verification._LOGFILE).read_text()

    def test_without_correlator_passes_it_refuses(self, tmp_path):
        assert verification.verify(make_exp(tmp_path, passes=0)) is False
