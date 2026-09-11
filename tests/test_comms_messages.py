"""Tests for what the operator is told, and when.

Every message reaching them starts with `**Processing of EXPNAME**` and says what
happened and what is needed, with the same words the terminal shows. The workflow must
send one whenever it stops and waits for a human: a failure, a manual step, a pause.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from evn_postprocess import comms, experiment, plotting, process, workflow


def _exp(tmp_path):
    dirs = experiment.Dirs(logs=tmp_path / 'logs', plots=tmp_path / 'plots',
                           pipeline=tmp_path / 'p', pipe_in=tmp_path / 'p/in',
                           pipe_out=tmp_path / 'p/out', pipe_temp=tmp_path / 'p/tmp')
    return experiment.Experiment('EB101', dt.date(2026, 4, 10), 'marcote', dirs)


class Recorder(comms.Notifier):
    """A notifier that keeps what it was asked to send."""

    def __init__(self):
        self.sent: list[tuple[str, str]] = []

    def send_message(self, subject, body, attachments=None):
        self.sent.append((subject, body))
        return True

    def supports_interactive(self):
        return False

    def wait_for_reply(self, timeout_seconds=3600):
        return None


class TestOperatorMessage:
    def test_it_starts_with_the_experiment_header(self, tmp_path):
        text = comms.operator_message(_exp(tmp_path), 'The pipeline failed.')
        assert text.startswith('**Processing of EB101**\n\n')
        assert text.endswith('The pipeline failed.')

    def test_the_experiment_name_is_upper_case(self, tmp_path):
        exp = _exp(tmp_path)
        exp.expname = 'eb101'
        assert comms.operator_message(exp, 'x').startswith('**Processing of EB101**')

    def test_what_is_needed_is_a_section_of_its_own(self, tmp_path):
        text = comms.operator_message(_exp(tmp_path), 'It stopped.', 'Run `postprocess run`.')
        assert '\n\n**What is needed:**\nRun `postprocess run`.' in text

    def test_no_empty_section_when_nothing_is_needed(self, tmp_path):
        assert 'What is needed' not in comms.operator_message(_exp(tmp_path), 'Done.', '  ')


class TestNotifyOperator:
    def test_the_subject_names_the_experiment_and_the_situation(self, tmp_path):
        rec = Recorder()
        assert comms.notify_operator(_exp(tmp_path), "failed at 'antab'", 'It broke.',
                                     'Fix it.', rec)
        subject, body = rec.sent[0]
        assert subject == "Processing of EB101 — failed at 'antab'"
        assert body.startswith('**Processing of EB101**')
        assert 'It broke.' in body and 'Fix it.' in body

    def test_nothing_is_sent_without_a_notifier(self, tmp_path):
        assert comms.notify_operator(_exp(tmp_path), 'h', 's', notifier=None) is False
        assert comms.notify_operator(_exp(tmp_path), 'h', 's',
                                     notifier=comms.NoneNotifier()) is False

    def test_a_broken_notifier_never_stops_the_workflow(self, tmp_path):
        class Broken(Recorder):
            def send_message(self, *a, **k):
                raise RuntimeError('mattermost down')

        assert comms.notify_operator(_exp(tmp_path), 'h', 's', notifier=Broken()) is False

    def test_a_pause_says_where_it_stopped_and_what_to_do(self, tmp_path):
        rec = Recorder()
        comms.notify_step_pause(_exp(tmp_path), 'antab', 'Wait for EB101B.', rec)
        subject, body = rec.sent[0]
        assert subject == "Processing of EB101 — paused at 'antab'"
        assert '`antab`' in body and 'Wait for EB101B.' in body


class TestTheWorkflowNotifiesWhenItNeedsTheOperator:
    @pytest.fixture(autouse=True)
    def _notifier(self, monkeypatch):
        rec = Recorder()
        monkeypatch.setattr(workflow, '_NOTIFIER', rec)
        return rec

    def test_a_failure_tells_the_step_the_reason_and_how_to_resume(self, tmp_path, monkeypatch):
        rec = workflow._NOTIFIER
        workflow._notify_step_failure(_exp(tmp_path), 'pipeline', 'the pipeline crashed', 3.0)
        subject, body = rec.sent[0]
        assert subject == "Processing of EB101 — failed at 'pipeline'"
        assert 'the pipeline crashed' in body
        assert '`postprocess run`' in body and 'pipeline' in body

    def test_a_step_needing_a_human_sends_its_own_words(self, tmp_path, monkeypatch):
        """A StepFailed message reaches the operator; a bare False cannot say more."""
        rec = workflow._NOTIFIER
        exp = _exp(tmp_path)
        monkeypatch.setattr(exp, 'store', lambda *a, **k: None)

        def needs_a_human(_):
            raise workflow.StepFailed('`antab_editor.py` must be run by hand.')

        monkeypatch.setitem(workflow.__dict__, 'needs_a_human', needs_a_human)
        step = workflow.Task(name='antab', command='needs_a_human', doc='')
        assert workflow._run_step(exp, step) is False
        assert step.done is False  # still pending, so `postprocess run` resumes from it
        assert '`antab_editor.py` must be run by hand.' in rec.sent[0][1]

    def test_a_step_returning_false_still_notifies(self, tmp_path, monkeypatch):
        rec = workflow._NOTIFIER
        exp = _exp(tmp_path)
        monkeypatch.setitem(workflow.__dict__, 'just_fails', lambda _: False)
        assert workflow._run_step(exp, workflow.Task(name='msops', command='just_fails',
                                                     doc='')) is False
        assert rec.sent[0][0] == "Processing of EB101 — failed at 'msops'"


class TestMattermostFormatting:
    def test_the_body_is_posted_as_it_is(self, tmp_path, monkeypatch):
        """The body carries its own header; a second heading would repeat the name."""
        config = comms.CommsConfig(mode='mattermost', username='jive.marcote',
                                   mm_server_url='https://mm.example', mm_token='t',
                                   mm_channel_id='chan')
        notifier = comms.MattermostNotifier(config)
        posted = {}
        monkeypatch.setattr(notifier, '_ensure_channel', lambda: None)
        monkeypatch.setattr(notifier, '_api',
                            lambda method, endpoint, data=None: posted.update(data or {}) or
                            {'create_at': 1})
        body = comms.operator_message(_exp(tmp_path), 'It stopped.', 'Run it again.')
        assert notifier.send_message('Processing of EB101 — failed', body)
        assert posted['message'] == body
        assert posted['message'].count('Processing of EB101') == 1

    def test_any_file_can_be_attached_not_only_plots(self, tmp_path, monkeypatch):
        """The PI letter travels as .eml/.html; only a missing file is skipped."""
        config = comms.CommsConfig(mode='mattermost', username='jive.marcote',
                                   mm_server_url='https://mm.example', mm_token='t',
                                   mm_channel_id='chan')
        notifier = comms.MattermostNotifier(config)
        uploaded, posted = [], {}
        monkeypatch.setattr(notifier, '_ensure_channel', lambda: None)
        monkeypatch.setattr(notifier, '_upload_file',
                            lambda path: uploaded.append(path.name) or f"id-{path.name}")
        monkeypatch.setattr(notifier, '_api',
                            lambda method, endpoint, data=None: posted.update(data or {}) or
                            {'create_at': 1})
        for name in ('eb101.piletter.eml', 'eb101.piletter.html', 'plot.png'):
            (tmp_path / name).write_text('x')
        files = [tmp_path / n for n in ('eb101.piletter.eml', 'eb101.piletter.html', 'plot.png',
                                        'absent.png')]
        assert notifier.send_message('subject', 'body', files)
        assert uploaded == ['eb101.piletter.eml', 'eb101.piletter.html', 'plot.png']
        assert posted['file_ids'] == [f"id-{name}" for name in uploaded]


class TestTheEndOfThePipelineIsAnnounced:
    """The operator has to be told the pipeline finished and the dashboard is waiting.

    Serving the dashboard blocks until they stop it themselves, so the review pause that
    follows is reached only once they are demonstrably at the terminal — and stays quiet on
    purpose. The announcement therefore has to go out while the server is starting, which is
    also the first moment the tunnel command (and its port) is known.
    """

    def test_the_message_carries_the_url_and_the_tunnel_command(self, tmp_path):
        exp = _exp(tmp_path)
        recorder = Recorder()
        workflow.set_notifier(recorder)
        try:
            workflow._announce_pipeline_dashboard(exp)("http://localhost:8050",
                                                       "ssh -L 8050:localhost:8050 tester@eee2")
        finally:
            workflow.set_notifier(comms.NoneNotifier())
        assert recorder.sent, "the end of the pipeline was not announced"
        subject, body = recorder.sent[0][0], recorder.sent[0][1]
        assert 'pipeline results are ready' in subject
        assert 'ssh -L 8050:localhost:8050 tester@eee2' in body
        assert 'http://localhost:8050' in body

    def test_a_failing_announcement_never_stops_the_dashboard(self):
        """The chat being down must not be what keeps the dashboard from coming up."""
        def boom(url, tunnel):
            raise RuntimeError("the chat is down")

        # Returns normally: serve_dashboard goes straight on to serve_forever after this.
        assert plotting._announce_ready(boom, 'http://localhost:8050', 'ssh -L ...', 'EB101') is None

    def test_no_callback_is_not_an_announcement(self):
        assert plotting._announce_ready(None, 'http://localhost:8050', 'ssh -L ...', 'EB101') is None

    def test_the_callback_is_handed_to_the_dashboard(self, tmp_path, monkeypatch):
        got = {}
        monkeypatch.setattr(plotting, 'serve_dashboard',
                            lambda exp, plots_dir, pipeline_dir=None, on_ready=None:
                            got.update(on_ready=on_ready, pipeline_dir=pipeline_dir))
        exp = _exp(tmp_path)
        process.open_pipeline_dashboard(exp, on_ready=lambda url, tunnel: None)
        assert callable(got['on_ready'])
        assert got['pipeline_dir'] == exp.dirs.pipe_out


class TestAPostTooLongForTheChat:
    """A message past Mattermost's MaxPostSize is refused outright, attachments and all.

    EM164B (658 correlator passes) produced a PI letter of ~130 000 characters: the
    'the PI letter is ready' message was silently lost while the shorter ones arrived. It now
    travels as a short head plus the full text attached.
    """

    def _notifier(self, monkeypatch):
        config = comms.CommsConfig(mode='mattermost', username='jive.marcote',
                                   mm_server_url='https://mm.example', mm_token='t',
                                   mm_channel_id='chan')
        notifier = comms.MattermostNotifier(config)
        uploaded, posted = [], {}
        monkeypatch.setattr(notifier, '_ensure_channel', lambda: None)
        monkeypatch.setattr(notifier, '_upload_file',
                            lambda path: uploaded.append((path.name, path.read_text()))
                            or f"id-{path.name}")
        monkeypatch.setattr(notifier, '_api',
                            lambda method, endpoint, data=None: posted.update(data or {}) or
                            {'create_at': 1})
        return notifier, uploaded, posted

    def test_a_message_that_fits_is_untouched(self):
        body = "short enough\nby far"
        assert comms._shorten_for_post(body, limit=1000) == (body, None)

    def test_a_long_message_is_cut_on_a_line_boundary(self):
        body = '\n'.join(f"- correlator pass #{i}" for i in range(500))
        posted, full = comms._shorten_for_post(body, limit=400)
        assert full == body                       # nothing of it is lost
        assert len(posted) <= 400
        assert 'too long for the chat' in posted and comms._FULL_MESSAGE_NAME in posted
        assert posted.splitlines()[0] == "- correlator pass #0"

    def test_the_full_text_is_attached_and_the_head_posted(self, monkeypatch):
        notifier, uploaded, posted = self._notifier(monkeypatch)
        monkeypatch.setattr(comms, 'MM_MAX_POST_CHARS', 500)
        body = '\n'.join(f"- correlator pass #{i}" for i in range(500))
        assert notifier.send_message('the PI letter is ready', body) is True
        assert [name for name, _ in uploaded] == [comms._FULL_MESSAGE_NAME]
        assert dict(uploaded)[comms._FULL_MESSAGE_NAME] == body
        assert posted['file_ids'] == [f"id-{comms._FULL_MESSAGE_NAME}"]
        assert len(posted['message']) <= 500

    def test_the_real_attachments_still_travel_with_it(self, monkeypatch, tmp_path):
        notifier, uploaded, posted = self._notifier(monkeypatch)
        monkeypatch.setattr(comms, 'MM_MAX_POST_CHARS', 500)
        (tmp_path / 'eb101.piletter.eml').write_text('draft')
        body = '\n'.join(f"- correlator pass #{i}" for i in range(500))
        assert notifier.send_message('subject', body, [tmp_path / 'eb101.piletter.eml'])
        assert [name for name, _ in uploaded] == ['eb101.piletter.eml',
                                                  comms._FULL_MESSAGE_NAME]

    def test_the_overflow_file_does_not_outlive_the_call(self, monkeypatch):
        notifier, uploaded, _ = self._notifier(monkeypatch)
        monkeypatch.setattr(comms, 'MM_MAX_POST_CHARS', 200)
        paths = []
        monkeypatch.setattr(notifier, '_upload_file',
                            lambda path: paths.append(path) or f"id-{path.name}")
        notifier.send_message('subject', 'x\n' * 500)
        assert paths and not paths[0].exists() and not paths[0].parent.exists()

    def test_a_failing_post_says_what_was_lost(self, monkeypatch, caplog):
        notifier, _, _ = self._notifier(monkeypatch)

        def refuse(method, endpoint, data=None):
            raise ValueError("HTTP 400: message too long")

        monkeypatch.setattr(notifier, '_api', refuse)
        errors = []
        from loguru import logger
        sink = logger.add(lambda m: errors.append(m.record['message']), level='ERROR')
        try:
            assert notifier.send_message('the PI letter is ready', 'body') is False
        finally:
            logger.remove(sink)
        assert any('the PI letter is ready' in m for m in errors)


class TestAttachmentTypes:
    """What each attachment is declared as (the .eml draft of the PI letter included)."""

    def test_types_are_guessed_and_base64_safe(self):
        assert comms._mime_type(Path('plot.png')) == 'image/png'
        assert comms._mime_type(Path('eb101.piletter.html')) == 'text/html'
        # Unknown, and message/* (the .eml): a plain download, which is what base64 allows.
        assert comms._mime_type(Path('eb101.piletter')) == 'application/octet-stream'
        assert comms._mime_type(Path('eb101.piletter.eml')) == 'application/octet-stream'


class TestTheFinishedRunSaysWhatWasSpotted:
    """The message closing a successful run is the only summary the operator reads."""

    @pytest.fixture(autouse=True)
    def _notifier(self, monkeypatch):
        rec = Recorder()
        monkeypatch.setattr(workflow, '_NOTIFIER', rec)
        return rec

    def test_the_completion_message_carries_the_findings(self, tmp_path):
        exp = _exp(tmp_path)
        exp.antennas.append(experiment.Antenna(name='Ef', logfsfile=True, antabfsfile=True))
        exp.antennas.append(experiment.Antenna(name='Tr', observed=False))
        workflow._announce_completion(exp)
        subject, body = workflow._NOTIFIER.sent[0]
        assert subject == 'Processing of EB101 — the post-processing is complete'
        assert '**Did not observe:**\n- Tr' in body
        assert 'both `.log` and `.antabfs`: Ef.' in body
        assert 'What is needed' not in body      # the run is over: nothing is asked

    def test_nothing_is_sent_when_there_is_nothing_to_report(self, tmp_path):
        workflow._announce_completion(_exp(tmp_path))
        assert workflow._NOTIFIER.sent == []
