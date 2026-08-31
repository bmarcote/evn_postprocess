"""Tests for the dashboard HTML template and the scan time ranges it displays.

The dashboard page skeleton lives in ``templates/dashboard.html.template`` (not in a
Python string), so these tests cover the contract between the two: the placeholders the
program substitutes, the guards that reject a badly edited template, and the per-scan
vex time range that feeds the scan-overview tooltips.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from evn_postprocess import experiment
from evn_postprocess import plotting
from evn_postprocess.experiment_state import STATION_STATUSES


def _make_exp(tmp_path: Path, expname: str = 'EB101') -> experiment.Experiment:
    dirs = experiment.Dirs(logs=tmp_path / 'logs', plots=tmp_path / 'plots',
                           pipeline=tmp_path / 'p', pipe_in=tmp_path / 'p/in',
                           pipe_out=tmp_path / 'p/out', pipe_temp=tmp_path / 'p/tmp')
    exp = experiment.Experiment(expname=expname, obsdate=dt.date(2024, 5, 21),
                                supsci='marcote', dirs=dirs)
    exp.antennas = experiment.Antennas([experiment.Antenna(name='Ef', observed=True),
                                        experiment.Antenna(name='Jb', observed=True)])
    exp.scans = experiment.Scans([
        experiment.Scan(scanno='No0001', starttime=dt.datetime(2024, 5, 21, 10, 23), duration_s=360,
                        source='J1234+5678', stations_scheduled=('Ef', 'Jb'), stations_observed=('Ef',)),
        experiment.Scan(scanno='No0002', starttime=dt.datetime(2024, 5, 21, 23, 58), duration_s=600,
                        source='3C286', stations_scheduled=('Ef',)),
    ])
    return exp


def _fake_template(monkeypatch, text: str) -> None:
    """Make _build_dashboard_html read *text* instead of the packaged template file."""
    class _Res:
        def joinpath(self, name):
            return self
        def read_text(self, encoding='utf-8'):
            return text
    monkeypatch.setattr(plotting.resources, 'files', lambda package: _Res())


class TestDashboardTemplate:
    def test_template_ships_with_the_package(self):
        text = plotting.resources.files('evn_postprocess.templates').joinpath(
            plotting._DASHBOARD_TEMPLATE).read_text(encoding='utf-8')
        assert text.lstrip().startswith('<!DOCTYPE html>')
        assert text.rstrip().endswith('</html>')

    def test_placeholders_are_substituted(self, tmp_path):
        html = plotting._build_dashboard_html(_make_exp(tmp_path))
        assert '<title>EB101 — EVN Post-Processing Dashboard</title>' in html
        assert '<span id="exp-title">EB101</span>' in html
        assert '{{EXPNAME}}' not in html

    def test_unknown_placeholder_is_rejected(self, tmp_path, monkeypatch):
        _fake_template(monkeypatch, '<html>{{EXPNAME}} {{SOMETHING_NEW}}</html>')
        with pytest.raises(RuntimeError, match='SOMETHING_NEW'):
            plotting._build_dashboard_html(_make_exp(tmp_path))

    def test_missing_station_status_is_rejected(self, tmp_path, monkeypatch):
        # Every STATION_STATUSES value must still be hand-coded in the Comments-tab JS.
        _fake_template(monkeypatch, '<html>{{EXPNAME}}</html>')
        with pytest.raises(RuntimeError, match='station status'):
            plotting._build_dashboard_html(_make_exp(tmp_path))

    def test_station_statuses_present_in_the_shipped_template(self, tmp_path):
        html = plotting._build_dashboard_html(_make_exp(tmp_path))
        for status in STATION_STATUSES:
            assert f"'{status}'" in html

    def test_scan_rows_carry_the_timerange_tooltip(self, tmp_path):
        # The scan-overview JS must keep binding the row title to the scan time range.
        html = plotting._build_dashboard_html(_make_exp(tmp_path))
        assert 'const tip = s.timerange' in html
        assert '<tr${tip ? ` title="${tip}"` : \'\'}>' in html


class TestScanTimerange:
    def _scan(self, start, duration_s):
        return experiment.Scan(scanno='No0001', starttime=start, duration_s=duration_s,
                               source='J1234+5678', stations_scheduled=('Ef',))

    def test_minutes_for_a_normal_scan(self):
        scan = self._scan(dt.datetime(2024, 5, 21, 10, 23, 0), 360)
        assert plotting._format_scan_timerange(scan) == '21/05/2024 10:23:00-10:29:00 UTC (6.0 min)'

    def test_seconds_for_a_short_scan(self):
        scan = self._scan(dt.datetime(2024, 5, 21, 10, 23, 0), 45)
        assert plotting._format_scan_timerange(scan) == '21/05/2024 10:23:00-10:23:45 UTC (45 s)'

    def test_end_repeats_the_date_across_midnight(self):
        scan = self._scan(dt.datetime(2024, 5, 21, 23, 58, 0), 600)
        assert plotting._format_scan_timerange(scan) == \
            '21/05/2024 23:58:00-22/05/2024 00:08:00 UTC (10.0 min)'

    def test_unparsed_duration_shows_the_start_only(self):
        # vex parsing failures store duration_s == 0 (see Experiment._parse_vex).
        scan = self._scan(dt.datetime(2024, 5, 21, 10, 23, 0), 0)
        assert plotting._format_scan_timerange(scan) == '21/05/2024 10:23:00 UTC'


class TestSummaryScans:
    def test_every_scan_row_exposes_its_timerange(self, tmp_path):
        summary = plotting._build_experiment_summary(_make_exp(tmp_path))
        assert [s['timerange'] for s in summary['scans']] == [
            '21/05/2024 10:23:00-10:29:00 UTC (6.0 min)',
            '21/05/2024 23:58:00-22/05/2024 00:08:00 UTC (10.0 min)',
        ]


class TestProgressTab:
    """The Progress tab shows the workflow steps and which of them have already run.

    The step list itself comes from ``workflow.step_progress`` (the same source as
    ``postprocess list``); these tests cover the contract between it, the ``/api/progress``
    endpoint, and the template's JS.
    """

    def test_template_has_the_tab_and_its_view(self, tmp_path):
        html = plotting._build_dashboard_html(_make_exp(tmp_path))
        assert 'onclick="showTab(\'progress\')"' in html
        assert 'id="view-progress"' in html
        # showTab must know about the new view, or selecting it would leave it hidden.
        assert "'pipeline', 'plots', 'comments', 'progress'" in html
        assert "/api/progress" in html

    def test_progress_reports_every_workflow_step_in_order(self, tmp_path):
        from evn_postprocess import workflow
        steps = workflow.step_progress(exp=_make_exp(tmp_path))
        assert [s['name'] for s in steps] == [s.name for s in workflow._WORKFLOW_STEPS]
        assert all(s['doc'] for s in steps)

    def test_done_flags_come_from_the_experiment(self, tmp_path):
        from dataclasses import replace
        from evn_postprocess import workflow
        exp = _make_exp(tmp_path)
        exp.steps = [replace(s, done=(s.name in ('lisfiles', 'checklis')))
                     for s in workflow._WORKFLOW_STEPS if s.name != 'initialize']
        done = {s['name']: s['done'] for s in workflow.step_progress(exp=exp)}
        assert done['lisfiles'] is True and done['checklis'] is True
        assert done['j2ms2'] is False
        # 'initialize' never reaches the stored step list: an Experiment proves it ran.
        assert done['initialize'] is True

    def test_unknown_experiment_reports_everything_pending(self, tmp_path, monkeypatch):
        from evn_postprocess import workflow
        monkeypatch.chdir(tmp_path)
        steps = workflow.step_progress(expname='NOSUCHEXP')
        assert steps and not any(s['done'] for s in steps)

    def test_endpoint_serves_the_progress(self, tmp_path, monkeypatch):
        import json as _json
        monkeypatch.chdir(tmp_path)
        exp = _make_exp(tmp_path)
        served: list = []
        handler = plotting._DashboardHandler.__new__(plotting._DashboardHandler)
        monkeypatch.setattr(plotting._DashboardHandler, 'exp', exp)
        monkeypatch.setattr(plotting._DashboardHandler, '_serve_json',
                            lambda self, data: served.append(data))
        handler._serve_progress()
        assert served and {'name', 'doc', 'done'} <= set(served[0][0])
        _json.dumps(served[0])  # must be JSON-serialisable as served
