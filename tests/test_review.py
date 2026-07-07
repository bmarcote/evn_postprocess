"""Tests for evn_postprocess.review (pre-antab station summary)."""
import datetime as dt
from pathlib import Path

from evn_postprocess import experiment, review


def make_exp():
    dirs = experiment.Dirs(logs=Path('logs'), plots=Path('plots'), pipeline=Path('pipeline'),
                           pipe_in=Path('pipeline/in'), pipe_out=Path('pipeline/out'),
                           pipe_temp=Path('antenna_files'))
    return experiment.Experiment('EB101', dt.date(2026, 4, 10), 'tester', dirs)


def add_scan(exp, i, scheduled, observed, duration_s=120):
    start = dt.datetime(2026, 4, 10, 2, 0, 0) + dt.timedelta(seconds=i * duration_s)
    exp.scans.append(experiment.Scan(scanno=f"No{i:04d}", starttime=start, duration_s=duration_s,
                                     source='SRC', stations_scheduled=tuple(scheduled),
                                     stations_observed=tuple(observed)))


def test_station_did_not_observe():
    exp = make_exp()
    exp.antennas.append(experiment.Antenna(name='Ef', subbands=(0, 1, 2, 3)))
    exp.antennas.append(experiment.Antenna(name='Tr', observed=False))
    summary = review.station_summary(exp)
    assert summary.stations['Tr'].status == 'major'
    assert summary.stations['Ef'].status == 'success'
    text = review.summary_text(exp, summary)
    assert 'Tr' in text and 'DID NOT OBSERVE' in text


def test_missed_time_ranges_merge_consecutive_scans():
    exp = make_exp()
    exp.antennas.append(experiment.Antenna(name='Ef'))
    exp.antennas.append(experiment.Antenna(name='Wb'))
    both, only_ef = ('Ef', 'Wb'), ('Ef',)
    for i, observed in enumerate([both, only_ef, only_ef, both, only_ef, both]):
        add_scan(exp, i, scheduled=('Ef', 'Wb'), observed=observed)
    report = review.station_summary(exp).stations['Wb']
    assert report.status == 'minor'
    assert len(report.missed_ranges) == 2          # scans 1-2 merged, scan 4 separate
    start, end = report.missed_ranges[0]
    assert (end - start).total_seconds() == 240    # two consecutive 120 s scans


def test_information_gap_closes_the_missed_range():
    """Two misses separated by a scan without observed info: two ranges, not one."""
    exp = make_exp()
    exp.antennas.append(experiment.Antenna(name='Ef'))
    exp.antennas.append(experiment.Antenna(name='Wb'))
    only_ef, no_info = ('Ef',), ()
    for i, observed in enumerate([only_ef, no_info, only_ef]):
        add_scan(exp, i, scheduled=('Ef', 'Wb'), observed=observed)
    report = review.station_summary(exp).stations['Wb']
    assert len(report.missed_ranges) == 2
    for start, end in report.missed_ranges:
        assert (end - start).total_seconds() == 120  # the info gap is not absorbed


def test_no_observed_info_means_no_false_positives():
    exp = make_exp()
    exp.antennas.append(experiment.Antenna(name='Ef'))
    add_scan(exp, 0, scheduled=('Ef',), observed=())  # MS metadata not loaded yet
    assert review.station_summary(exp).stations['Ef'].status == 'success'


def test_reduced_bandwidth():
    exp = make_exp()
    exp.antennas.append(experiment.Antenna(name='Ef', subbands=(0, 1, 2, 3)))
    exp.antennas.append(experiment.Antenna(name='Ir', subbands=(0, 1)))
    report = review.station_summary(exp).stations['Ir']
    assert report.reduced_bandwidth is True and report.status == 'minor'
    assert '2/4 subbands' in review.summary_text(exp, review.station_summary(exp))


def test_unscheduled_station_excluded():
    exp = make_exp()
    exp.antennas.append(experiment.Antenna(name='Ef'))
    exp.antennas.append(experiment.Antenna(name='Zz', scheduled=False, observed=False))
    assert 'Zz' not in review.station_summary(exp).stations


def test_announce_sends_notifier_message():
    exp = make_exp()
    exp.antennas.append(experiment.Antenna(name='Tr', observed=False))
    sent = {}
    class Notifier:
        def send_message(self, subject, body, attachments=None):
            sent['subject'], sent['body'] = subject, body
            return True
    review.announce_antab_summary(exp, Notifier())
    assert 'EB101' in sent['subject'] and 'Tr' in sent['body']


def test_announce_never_blocks_on_notifier_failure():
    exp = make_exp()
    exp.antennas.append(experiment.Antenna(name='Ef'))
    class Broken:
        def send_message(self, *a, **k):
            raise RuntimeError('mattermost down')
    review.announce_antab_summary(exp, Broken())  # must not raise


# ------------------------------------------------------- station feedback (DB)

def test_station_feedback_silently_skips_without_config(tmp_path, monkeypatch):
    monkeypatch.setattr(review, 'FEEDBACKDB_CONFIG', tmp_path / 'feedbackdb.toml')
    assert review.station_feedback(make_exp()) == {}


def test_station_feedback_silently_skips_on_connection_error(tmp_path, monkeypatch):
    config = tmp_path / 'feedbackdb.toml'
    config.write_text('host = "db.example"\ndatabase = "evn"\nuser = "x"\npassword = "y"\n')
    monkeypatch.setattr(review, 'FEEDBACKDB_CONFIG', config)
    # Whatever mysql client is (or is not) installed, connecting to db.example fails;
    # the lookup must degrade to {} without raising.
    assert review.station_feedback(make_exp()) == {}


def test_default_station_comments_merge(monkeypatch):
    exp = make_exp()
    exp.antennas.append(experiment.Antenna(name='Ef', subbands=(0, 1, 2, 3)))
    exp.antennas.append(experiment.Antenna(name='Tr', observed=False))
    exp.antennas.append(experiment.Antenna(name='Ir', subbands=(0, 1)))
    monkeypatch.setattr(review, 'station_feedback',
                        lambda e: {'Ef': 'Maser problems during the first hour.'})
    defaults = review.default_station_comments(exp)
    assert defaults['Ef']['note'] == 'Maser problems during the first hour.'
    assert defaults['Ef']['status'] == 'success'         # DB comment alone: no auto-finding
    assert defaults['Tr'] == {'status': 'major', 'note': 'Did not observe.'}
    assert defaults['Ir']['status'] == 'minor' and 'reduced bandwidth' in defaults['Ir']['note']
