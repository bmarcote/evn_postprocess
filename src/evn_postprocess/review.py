"""Station summary and review helpers.

Owns the operator-facing review data (PRD Module Design -> `review`):

  - :func:`station_summary` computes, per station, whether it observed at all, the
    time ranges it missed (scheduled scans without its data), and whether it observed
    with reduced bandwidth (fewer subbands than the experiment setup).
  - :func:`announce_antab_summary` renders that summary as a rich terminal panel and
    sends the same text through the configured notifier, immediately before
    antab_editor launches (PRD stories 10-11).

The same StationSummary feeds the dashboard Comments tab auto-notes (Issue 9).
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger
from rich.console import Console
from rich.panel import Panel

from . import experiment


@dataclass
class StationReport:
    """Review findings for one station.

    ``missed_ranges`` contains (start, end) pairs of consecutive scheduled scans in
    which the station has no data; empty when it observed everything (or when no
    per-scan observed information is available yet).
    """
    name: str
    observed: bool = True
    missed_ranges: list[tuple[dt.datetime, dt.datetime]] = field(default_factory=list)
    n_subbands: int | None = None
    max_subbands: int | None = None

    @property
    def reduced_bandwidth(self) -> bool:
        """True when the station observed fewer subbands than the experiment setup."""
        return (self.observed and self.n_subbands is not None
                and self.max_subbands is not None and self.n_subbands < self.max_subbands)

    @property
    def status(self) -> str:
        """The traffic-light status: 'major' (did not observe), 'minor', or 'success'."""
        if not self.observed:
            return 'major'
        if self.missed_ranges or self.reduced_bandwidth:
            return 'minor'
        return 'success'


@dataclass
class StationSummary:
    """Per-station review findings for the whole experiment (name-keyed)."""
    stations: dict[str, StationReport] = field(default_factory=dict)

    @property
    def with_findings(self) -> list[StationReport]:
        """The stations that need attention (any status but 'success')."""
        return [r for r in self.stations.values() if r.status != 'success']


def _missed_ranges(exp: experiment.Experiment, station: str) -> list[tuple[dt.datetime, dt.datetime]]:
    """Time ranges of consecutive scheduled scans where *station* has no data.

    Uses Scan.stations_observed when filled (from the MS metadata). A scan without
    observed information neither counts as missed (no false positives before the MS
    exists) nor extends an open range: it closes the current range, so two misses
    separated by an information gap are reported as two ranges, not merged into one.
    """
    ranges: list[tuple[dt.datetime, dt.datetime]] = []
    current: tuple[dt.datetime, dt.datetime] | None = None
    for scan in exp.scans:
        if not scan.stations_observed or station not in scan.stations_scheduled:
            if current is not None:  # information gap: close the open range
                ranges.append(current)
                current = None
            continue
        end = scan.starttime + dt.timedelta(seconds=scan.duration_s)
        if station not in scan.stations_observed:
            current = (current[0], end) if current is not None else (scan.starttime, end)
        elif current is not None:
            ranges.append(current)
            current = None
    if current is not None:
        ranges.append(current)
    return ranges


def station_summary(exp: experiment.Experiment) -> StationSummary:
    """Computes the per-station review findings from the experiment metadata.

    Covers: did-not-observe (scheduled but no data), missed time ranges (scheduled vs
    observed scans), and reduced bandwidth (subband count vs the experiment maximum).
    """
    summary = StationSummary()
    subband_counts = [len(a.subbands) for a in exp.antennas if a.observed and a.subbands]
    max_subbands = max(subband_counts) if subband_counts else None
    for ant in exp.antennas:
        if not ant.scheduled:
            continue
        report = StationReport(name=ant.name, observed=ant.observed,
                               n_subbands=len(ant.subbands) if ant.subbands else None,
                               max_subbands=max_subbands)
        if ant.observed:
            report.missed_ranges = _missed_ranges(exp, ant.name)
        summary.stations[ant.name] = report
    return summary


def summary_text(exp: experiment.Experiment, summary: StationSummary) -> str:
    """Renders the station summary as plain text (terminal panel body and notifier message)."""
    lines = [f"Station summary for {exp.expname} (before antab_editor):"]
    if not summary.with_findings:
        lines.append("  All scheduled stations observed the full experiment. Nothing to fix.")
        return '\n'.join(lines)
    for report in summary.with_findings:
        if not report.observed:
            lines.append(f"  {report.name}: DID NOT OBSERVE.")
            continue
        details = []
        for start, end in report.missed_ranges:
            details.append(f"missed {start.strftime('%d %H:%M')}-{end.strftime('%H:%M')} UT")
        if report.reduced_bandwidth:
            details.append(f"reduced bandwidth ({report.n_subbands}/{report.max_subbands} subbands)")
        lines.append(f"  {report.name}: {'; '.join(details)}.")
    lines.append("Remember to address these in the ANTAB files (antab_editor opens next).")
    return '\n'.join(lines)


FEEDBACKDB_CONFIG = Path.home() / '.config' / 'evn_postprocess' / 'feedbackdb.toml'


def station_feedback(exp: experiment.Experiment) -> dict[str, str]:
    """Fetches the station feedback comments for *exp* from the EVN feedback database.

    Connection settings come from ``~/.config/evn_postprocess/feedbackdb.toml``::

        host = "..."          # required
        database = "..."      # required
        user = "..."          # required
        password = "..."      # required (or use a socket-based auth setup)
        port = 3306           # optional
        query = "SELECT station, comment FROM station_feedback WHERE expname = %(expname)s"
                               # optional; %(expname)s is bound to the experiment name

    Silent-skip contract (PRD story 18): a missing config file, missing MySQL client
    library, unreachable server, or failing query all return {} and log at DEBUG level
    only, so the dashboard behaves identically outside JIVE.

    Returns:
        Mapping station code -> feedback comment (empty on any failure).
    """
    if not FEEDBACKDB_CONFIG.exists():
        logger.debug(f"No feedback-database config at {FEEDBACKDB_CONFIG}; skipping the lookup.")
        return {}
    try:
        if FEEDBACKDB_CONFIG.stat().st_mode & 0o077:
            logger.warning(f"{FEEDBACKDB_CONFIG} is readable by other users but contains a "
                           "database password; `chmod 600` it.")
    except OSError:
        pass
    try:
        import tomllib
        with open(FEEDBACKDB_CONFIG, 'rb') as f:
            config = tomllib.load(f)
        try:
            import pymysql as mysql_client
        except ImportError:
            import MySQLdb as mysql_client
        query = config.get('query', "SELECT station, comment FROM station_feedback "
                                    "WHERE expname = %(expname)s")
        connection = mysql_client.connect(host=config['host'], user=config['user'],
                                          password=config['password'], database=config['database'],
                                          port=int(config.get('port', 3306)), connect_timeout=10)
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, {'expname': exp.expname.upper()})
                rows = cursor.fetchall()
        finally:
            connection.close()
        feedback = {str(station).capitalize(): str(comment) for station, comment in rows if comment}
        logger.debug(f"Feedback database returned comments for {len(feedback)} station(s).")
        return feedback
    except Exception as e:  # silent-skip contract: never disturb the run
        logger.debug(f"Station feedback lookup skipped ({type(e).__name__}: {e}).")
        return {}


def default_station_comments(exp: experiment.Experiment) -> dict[str, dict]:
    """Builds the default per-station comments for the dashboard Comments tab.

    Combines the feedback-database comment (when available) with the automatic
    findings of :func:`station_summary` (did-not-observe, missed ranges, reduced
    bandwidth). The status follows the summary ('success'/'minor'/'major').

    Returns:
        Mapping station -> {'status': str, 'note': str} for every scheduled station.
    """
    summary = station_summary(exp)
    db_comments = station_feedback(exp)
    defaults: dict[str, dict] = {}
    for name, report in summary.stations.items():
        notes = []
        if db_comments.get(name):
            notes.append(db_comments[name])
        if not report.observed:
            notes.append("Did not observe.")
        for start, end in report.missed_ranges:
            notes.append(f"Missed {start.strftime('%d %b %H:%M')}-{end.strftime('%H:%M')} UT.")
        if report.reduced_bandwidth:
            notes.append(f"Observed with reduced bandwidth "
                         f"({report.n_subbands}/{report.max_subbands} subbands).")
        defaults[name] = {'status': report.status, 'note': ' '.join(notes)}
    return defaults


def announce_antab_summary(exp: experiment.Experiment, notifier=None) -> StationSummary:
    """Prints the pre-antab_editor station summary and sends it via the notifier.

    Never blocks: rendering or notification failures are logged and ignored, since
    this is purely informational (the antab_editor step follows regardless).
    """
    summary = station_summary(exp)
    text = summary_text(exp, summary)
    try:
        Console().print(Panel(text, title="[bold]Stations to check in the ANTAB[/bold]",
                              border_style="yellow", padding=(1, 2)))
    except Exception as e:  # rendering must never stop the workflow
        logger.warning(f"Could not render the antab station summary ({e}); plain text:\n{text}")
    from . import comms
    comms.notify_operator(exp, "antab_editor about to start", text, notifier)
    return summary
