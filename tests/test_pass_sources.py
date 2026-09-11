"""Tests for the per-pass source lists.

The rule under test, end to end: a correlator pass lists exactly the sources that have
visibilities in its own MS -- no more, no less -- and that list is what reaches the EVN
pipeline input files and the pipeline feedback pages.

It used to be broken for multi-phase-centre experiments (e.g. EM164B, 658 passes): every
pass got the source list of pass 1, so 657 passes advertised sources they hold no data for.
"""
from __future__ import annotations

import contextlib
import datetime as dt
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from astropy import units as u
from astropy import coordinates as coord

from evn_postprocess import experiment
from evn_postprocess import pipeline
from evn_postprocess import process
from evn_postprocess.mstools import msdata


# ------------------------------------------------------------------ fake casacore tables

class _FakeTable:
    """Stand-in for a casacore table: only getcol, getkeyword and len() are used.

    Attributes:
        columns (dict): Column name -> full column content (a list).
        keywords (dict): Keyword name -> value (used for the FIELD subtable path).
        nrows (int): Number of rows the table reports through len().
        getcol_calls (list): Every (column, startrow, nrow) asked for, in order.
    """

    def __init__(self, columns: dict, keywords: dict | None = None, nrows: int = 0):
        self.columns = columns
        self.keywords = keywords if keywords is not None else {}
        self.nrows = nrows
        self.getcol_calls: list[tuple[str, int, int]] = []

    def getcol(self, column: str, startrow: int = 0, nrow: int = -1):
        """Returns the (possibly partial) content of a column, recording the request."""
        self.getcol_calls.append((column, startrow, nrow))
        values = self.columns[column]
        return values if nrow == -1 else values[startrow:startrow + nrow]

    def getkeyword(self, keyword: str):
        """Returns a table keyword (e.g. the path of the FIELD subtable)."""
        return self.keywords[keyword]

    def __len__(self) -> int:
        return self.nrows


def _fake_table_function(tables: dict[str, _FakeTable]):
    """Returns a misc.table replacement yielding the _FakeTable registered for that path."""
    @contextlib.contextmanager
    def _table(msfile, readonly: bool = True, ack: bool = False):
        yield tables[str(msfile)]

    return _table


class TestSourceNamesWithData:
    """msdata.source_names_with_data answers what the MAIN table really contains."""

    def _tables(self, field_names: list[str], field_ids: list[int]) -> dict[str, _FakeTable]:
        field = _FakeTable({'NAME': field_names}, nrows=len(field_names))
        main = _FakeTable({'FIELD_ID': field_ids}, keywords={'FIELD': 'FIELD_SUBTABLE'},
                          nrows=len(field_ids))
        return {'the.ms': main, 'FIELD_SUBTABLE': field}

    def test_only_the_fields_present_in_the_main_table(self, monkeypatch):
        # The FIELD table lists four phase centres; only two of them were correlated in.
        tables = self._tables(['3C84', 'TARGET1', 'TARGET2', 'J1234+5678'], [3, 3, 0, 3, 0, 0])
        monkeypatch.setattr(msdata.misc, 'table', _fake_table_function(tables))
        assert msdata.source_names_with_data('the.ms') == ['3C84', 'J1234+5678']

    def test_all_fields_when_they_all_carry_data(self, monkeypatch):
        tables = self._tables(['A', 'B'], [0, 1, 1, 0])
        monkeypatch.setattr(msdata.misc, 'table', _fake_table_function(tables))
        assert msdata.source_names_with_data('the.ms') == ['A', 'B']

    def test_no_sources_for_an_empty_main_table(self, monkeypatch):
        tables = self._tables(['A', 'B'], [])
        monkeypatch.setattr(msdata.misc, 'table', _fake_table_function(tables))
        assert msdata.source_names_with_data('the.ms') == []

    def test_the_main_table_is_read_in_chunks_of_field_id_only(self, monkeypatch):
        # An MPC MS reaches a hundred million rows: neither the whole FIELD_ID column nor
        # (much worse) the DATA column may ever be read in one go.
        tables = self._tables(['A', 'B', 'C'], [0] * 4 + [2] * 5)
        monkeypatch.setattr(msdata.misc, 'table', _fake_table_function(tables))
        assert msdata.source_names_with_data('the.ms', chunk_rows=4) == ['A', 'C']
        assert tables['the.ms'].getcol_calls == [('FIELD_ID', 0, 4), ('FIELD_ID', 4, 4),
                                                 ('FIELD_ID', 8, 1)]


# --------------------------------------------------------------- the multi-phase-centre path

ALL_FIELDS = ['3C84', 'TARGET1', 'TARGET2', 'J1234+5678']


def _sky(ra_deg: float, dec_deg: float) -> coord.SkyCoord:
    """A SkyCoord at the given (degrees) position."""
    return coord.SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg)


class _FakeMs:
    """Stand-in for mstools.Ms with only the attributes get_metadata_from_ms reads.

    Its FIELD table lists every phase centre of the experiment (as a real MPC MS does),
    which is precisely why the source list cannot be taken from it alone.
    """

    def __init__(self, msfile, runstats: bool = False):
        self.msfile = Path(msfile)
        self.antennas = [SimpleNamespace(name='Ef', observed=True, subbands=(0,), weights=(1, 0, 0, 0, 0, 0, 0),
                                         polconvert=False, polswap=False, onebit=False, logfsfile=False,
                                         antabfsfile=False)]
        self.freqsetup = SimpleNamespace(nspw=1, nchan=64, meanfreq=1.6 * u.GHz, bandwidth=32 * u.MHz,
                                         polarizations=(msdata.misc.Stokes.RR,))
        self.sources = [SimpleNamespace(name=name, coordinates=_sky(10.0 + i, 40.0), intent=None)
                        for i, name in enumerate(ALL_FIELDS)]
        self.scans = {}


def _mpc_experiment(tmp_path: Path) -> experiment.Experiment:
    """An EM164B-like experiment: two multi-phase-centre passes and four typed sources."""
    dirs = experiment.Dirs(logs=tmp_path, plots=tmp_path, pipeline=tmp_path, pipe_in=tmp_path,
                           pipe_out=tmp_path, pipe_temp=tmp_path)
    sources = experiment.Sources()
    sources.append(experiment.Source(name='3C84', coordinates=_sky(10.0, 40.0),
                                     type=experiment.SourceType.fringefinder))
    sources.append(experiment.Source(name='TARGET1', coordinates=_sky(11.0, 40.0),
                                     type=experiment.SourceType.target))
    sources.append(experiment.Source(name='TARGET2', coordinates=_sky(12.0, 40.0),
                                     type=experiment.SourceType.target, protected=True))
    sources.append(experiment.Source(name='J1234+5678', coordinates=_sky(13.0, 40.0),
                                     type=experiment.SourceType.calibrator))
    exp = experiment.Experiment('EM164B', dt.date(2026, 4, 10), 'tester', dirs, sources=sources)
    exp.correlator_passes = [experiment.CorrelatorPass(Path(f'em164b_{i}_1.lis'), Path(f'em164b_{i}_1.ms'),
                                                       f'em164b_{i}_1.IDI', True) for i in (1, 2)]
    return exp


class TestMpcPassSources:
    """Each multi-phase-centre pass keeps the sources of its OWN MS."""

    def _with_data(self, monkeypatch, per_ms: dict[str, list[str]]):
        """Makes mstools.Ms/source_names_with_data answer from the given {ms name: sources}."""
        monkeypatch.setattr(process.mstools, 'Ms', _FakeMs)
        monkeypatch.setattr(process.mstools, 'source_names_with_data',
                            lambda msfile, **kwargs: per_ms[Path(msfile).name])

    def test_pass_two_gets_a_different_source_list_than_pass_one(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _mpc_experiment(tmp_path)
        self._with_data(monkeypatch, {'em164b_1_1.ms': ['3C84', 'TARGET1'],
                                      'em164b_2_1.ms': ['3C84', 'TARGET2']})
        assert process.get_metadata_from_ms(exp) is True
        # Pass 1 drops the two FIELD entries with no visibilities (TARGET2, J1234+5678).
        assert exp.correlator_passes[0].sources.names == ['3C84', 'TARGET1']
        assert exp.correlator_passes[1].sources.names == ['3C84', 'TARGET2']

    def test_the_setup_is_still_shared_with_the_first_pass(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _mpc_experiment(tmp_path)
        self._with_data(monkeypatch, {'em164b_1_1.ms': ['3C84', 'TARGET1'],
                                      'em164b_2_1.ms': ['3C84', 'TARGET2']})
        assert process.get_metadata_from_ms(exp) is True
        first, second = exp.correlator_passes
        assert second.antennas is first.antennas
        assert second.freqsetup is first.freqsetup
        assert second.scans is first.scans

    def test_the_source_metadata_is_preserved(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _mpc_experiment(tmp_path)
        self._with_data(monkeypatch, {'em164b_1_1.ms': ['3C84', 'TARGET1'],
                                      'em164b_2_1.ms': ['3C84', 'TARGET2']})
        assert process.get_metadata_from_ms(exp) is True
        second = exp.correlator_passes[1]
        # Reused from pass 1 (same object), so type/protected/coordinates cannot drift.
        assert second.sources['3C84'] is exp.correlator_passes[0].sources['3C84']
        assert second.sources['3C84'].type == experiment.SourceType.fringefinder
        # Rebuilt from the experiment: a phase centre with no data in pass 1 is still a target.
        assert second.sources['TARGET2'].type == experiment.SourceType.target
        assert second.sources['TARGET2'].protected is True

    def test_an_unreadable_ms_falls_back_to_the_first_pass(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp = _mpc_experiment(tmp_path)
        monkeypatch.setattr(process.mstools, 'Ms', _FakeMs)

        def _sources(msfile, **kwargs):
            if Path(msfile).name == 'em164b_2_1.ms':
                raise RuntimeError("table em164b_2_1.ms is corrupted")
            return ['3C84', 'TARGET1']

        monkeypatch.setattr(process.mstools, 'source_names_with_data', _sources)
        # One broken MS must not abort the step for the other 657 passes.
        assert process.get_metadata_from_ms(exp) is True
        assert exp.correlator_passes[1].sources.names == ['3C84', 'TARGET1']


# ------------------------------------------------------------------- pipeline consumers

class TestFeedbackPagesUsePassSources:
    """Every feedback page shows the sources of the pass it belongs to."""

    def _exp(self, tmp_path: Path, per_pass: list[list[str]]) -> experiment.Experiment:
        """An experiment whose n passes carry the given source names (all targets)."""
        dirs = experiment.Dirs(logs=tmp_path, plots=tmp_path, pipeline=tmp_path, pipe_in=tmp_path,
                               pipe_out=tmp_path, pipe_temp=tmp_path)
        exp_sources = experiment.Sources()
        for i, name in enumerate(sorted({n for names in per_pass for n in names})):
            exp_sources.append(experiment.Source(name=name, coordinates=_sky(10.0 + i, 40.0),
                                                 type=experiment.SourceType.target))
        exp = experiment.Experiment('EM164B', dt.date(2026, 4, 10), 'tester', dirs, sources=exp_sources)
        for i, names in enumerate(per_pass, 1):
            apass = experiment.CorrelatorPass(Path(f'em164b_{i}_1.lis'), Path(f'em164b_{i}_1.ms'),
                                              f'em164b_{i}_1.IDI', True)
            for name in names:
                apass.sources.append(experiment.Source(name=name, coordinates=exp_sources[name].coordinates,
                                                       type=experiment.SourceType.target))
            exp.correlator_passes.append(apass)
        return exp

    def _record(self, monkeypatch) -> list[dict]:
        """Replaces feedback.generate_feedback_page by a recorder of its arguments."""
        calls: list[dict] = []

        def _generate(expname, sources=None, nme=False, contact=None, directory="."):
            calls.append({'expname': expname, 'sources': list(sources) if sources is not None else None})
            return Path(directory) / f"{expname}.html"

        monkeypatch.setattr(pipeline.feedback, 'generate_feedback_page', _generate)
        return calls

    def test_each_page_gets_its_own_pass_sources(self, tmp_path, monkeypatch):
        exp = self._exp(tmp_path, [['3C84', 'TARGET1'], ['3C84', 'TARGET2']])
        calls = self._record(monkeypatch)
        assert pipeline.pipeline_feedback(exp) is True
        assert calls == [{'expname': 'em164b_1', 'sources': ['3C84', 'TARGET1']},
                         {'expname': 'em164b_2', 'sources': ['3C84', 'TARGET2']}]

    def test_the_single_pass_page_also_uses_its_pass_sources(self, tmp_path, monkeypatch):
        exp = self._exp(tmp_path, [['3C84', 'TARGET1']])
        calls = self._record(monkeypatch)
        assert pipeline.pipeline_feedback(exp) is True
        assert calls == [{'expname': 'em164b', 'sources': ['3C84', 'TARGET1']}]

    def test_a_pass_without_sources_falls_back_to_the_experiment_list(self, tmp_path, monkeypatch):
        exp = self._exp(tmp_path, [['3C84', 'TARGET1'], []])
        calls = self._record(monkeypatch)
        assert pipeline.pipeline_feedback(exp) is True
        assert calls[0]['sources'] == ['3C84', 'TARGET1']
        assert calls[1]['sources'] == [s.name for s in exp.sources]


class TestInputFileAllSourcesIsDeterministic:
    """The {all_sources} line of the pipeline input file must not change between runs."""

    def _exp(self, tmp_path: Path) -> tuple[Mock, Path]:
        """A single-pipeline-pass MPC experiment with three targets, one ff and two calibrators."""
        pipe_in = tmp_path / "in"
        pipe_temp = tmp_path / "temp"
        pipe_in.mkdir()
        pipe_temp.mkdir()
        sources = experiment.Sources()
        for i, (name, srctype) in enumerate([('TARGET1', experiment.SourceType.target),
                                             ('TARGET2', experiment.SourceType.target),
                                             ('TARGET3', experiment.SourceType.target),
                                             ('3C84', experiment.SourceType.fringefinder),
                                             ('J1111+1111', experiment.SourceType.calibrator),
                                             ('J2222+2222', experiment.SourceType.calibrator)]):
            sources.append(experiment.Source(name=name, coordinates=_sky(10.0 + i, 40.0), type=srctype))

        exp = Mock(spec=experiment.Experiment)
        exp.expname = "testexp"
        exp.supsci = "tester"
        exp.refant = ["Ef"]
        exp.multi_phase_center = True
        exp.dirs = Mock()
        exp.dirs.pipe_in = pipe_in
        exp.dirs.pipe_temp = pipe_temp
        apass = Mock(spec=experiment.CorrelatorPass)
        apass.pipeline = True
        apass.sources = sources
        exp.correlator_passes = [apass]
        return exp, pipe_in

    def _all_sources_line(self, exp, pipe_in: Path) -> str:
        """Runs create_input_file on a clean directory and returns the {all_sources} value."""
        inp_file = pipe_in / "testexp.inp.txt"
        inp_file.unlink(missing_ok=True)
        with patch("subprocess.run") as mock_run, \
             patch("evn_postprocess.pipeline.resources.files") as mock_resources:
            mock_run.return_value.stdout = "100"
            mock_resources.return_value.joinpath.return_value.read_text.return_value = "sources = {all_sources}\n"
            assert pipeline.create_input_file(exp) is True
        return inp_file.read_text().strip()

    def test_the_sources_keep_the_target_fringefinder_calibrator_order(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp, pipe_in = self._exp(tmp_path)
        assert self._all_sources_line(exp, pipe_in) == \
            "sources = TARGET1, TARGET2, TARGET3, 3C84, J1111+1111, J2222+2222"

    def test_repeated_runs_write_the_same_line(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        exp, pipe_in = self._exp(tmp_path)
        lines = {self._all_sources_line(exp, pipe_in) for _ in range(5)}
        assert len(lines) == 1
