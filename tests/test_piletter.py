"""Tests for evn_postprocess.distribution.piletter: the PI letter built from the template,
its three renderings (plain text, HTML, .eml draft), and what is written to disk."""
import datetime as dt
import email
import email.policy
from pathlib import Path

from astropy import units as u

from evn_postprocess import experiment
from evn_postprocess.distribution import piletter
from evn_postprocess import experiment_state as es


class Pol:
    """Stand-in for an mstools Stokes member (only its name reaches the letter)."""

    def __init__(self, name):
        self.name = name


def make_exp(tmp_path, expname='EB101B'):
    """An experiment with a PI, one correlator pass, and four antennas worth remarking on."""
    dirs = experiment.Dirs(logs=Path('logs'), plots=Path('plots'), pipeline=Path('pipeline'),
                           pipe_in=Path('pipeline/in'), pipe_out=Path('pipeline/out'),
                           pipe_temp=Path('antenna_files'))
    exp = experiment.Experiment(expname, dt.date(2026, 4, 10), 'tester', dirs)
    exp.pi = [experiment.PI('Jane Doe', 'jane@obs.edu')]
    exp.antennas = experiment.Antennas([
        experiment.Antenna(name='Ef', subbands=(0, 1, 2, 3)),
        experiment.Antenna(name='Wb', subbands=(0, 1), polconvert=True),
        experiment.Antenna(name='Ys', subbands=(0, 1, 2, 3), opacity=True),
        experiment.Antenna(name='Tr', observed=False)])
    a_pass = experiment.CorrelatorPass(Path(f'{expname.lower()}.lis'), Path(f'{expname.lower()}.ms'),
                                       f'{expname.upper()}_1_1.IDI', True)
    a_pass.freqsetup = experiment.Subbands(subbands=4, channels=64, frequency=1.6384 * u.GHz,
                                           bandwidth=128 * u.MHz,
                                           polarizations=(Pol('RR'), Pol('LL')))
    a_pass.flagged_weights = experiment.FlagWeight(0.7, 1.234)
    a_pass.antennas = exp.antennas
    exp.correlator_passes = [a_pass]
    exp.exp_toml = es.load_toml(tmp_path / f'{expname.lower()}.toml')
    return exp


def save_comments(exp, **kwargs):
    """Records comments and saves them: the letter always re-reads the toml from disk."""
    exp.exp_toml.record_comments(**kwargs)
    exp.exp_toml.save()


# ------------------------------------------------------------------ the content

def test_letter_carries_metadata_headers_and_the_correlation_setup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    letter = piletter.build(make_exp(tmp_path))
    assert letter.headers['To'] == 'Jane Doe <jane@obs.edu>'
    assert letter.headers['Cc'] == 'jops@jive.eu'
    assert letter.subject == 'EVN experiment EB101B available on the EVN Archive'
    assert 'Dear Jane Doe,' in letter.body
    assert '10 April 2026' in letter.body
    assert 'archive.jive.eu/scripts/arch.php?exp=EB101B' in letter.body
    assert '4 x 32-MHz subbands' in letter.body and '64 spectral channels' in letter.body
    # The weight flagging is post-processing done at JIVE, not a correlation parameter: the
    # letter never mentions the threshold nor how much data it removed.
    assert 'weight below' not in letter.body and 'of the data' not in letter.body


def test_the_fitsidi_name_is_only_written_when_there_are_several_passes(tmp_path, monkeypatch):
    """With one pass there is nothing to tell apart, so the file name is left out."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    body = piletter.build(exp).body
    # 'FITS-IDI files' alone appears in the opening paragraph; the named-file clause does not.
    assert 'FITS-IDI files:' not in body
    assert 'dual polarization (RR, LL).\n' in body        # the bullet ends right there

    second = experiment.CorrelatorPass(Path('eb101b_2.lis'), Path('eb101b_2.ms'),
                                       'EB101B_2_1.IDI', True)
    second.freqsetup = exp.correlator_passes[0].freqsetup
    second.antennas = exp.antennas
    exp.correlator_passes.append(second)
    body = piletter.build(exp).body
    assert '**Correlator pass #1**: ' in body and '**Correlator pass #2**: ' in body
    assert 'FITS-IDI files: `EB101B_1_1.IDI`.' in body
    assert 'FITS-IDI files: `EB101B_2_1.IDI`.' in body


def test_acknowledgment_names_the_project_not_the_epoch(tmp_path, monkeypatch):
    """EB101B is the second epoch of project EB101: the PI acknowledges the project code."""
    monkeypatch.chdir(tmp_path)
    assert 'project code: EB101.' in piletter.build(make_exp(tmp_path)).body
    assert 'project code: N24L1.' in piletter.build(make_exp(tmp_path, 'N24L1')).body


def test_automatic_remarks_cover_polconvert_bandwidth_and_opacity(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    body = piletter.build(make_exp(tmp_path)).body
    assert 'antenna Wb originally observed linear polarizations' in body
    assert 'Wb only observed subbands 1-2' in body
    assert 'antenna Ys have been corrected for opacity' in body


def test_a_placeholder_nothing_fills_never_reaches_the_letter(tmp_path, monkeypatch):
    """A template ahead of this module (a half-finished upgrade) must not send the PI a
    letter with a literal '{...}' in it: the placeholder is dropped and a warning logged."""
    monkeypatch.chdir(tmp_path)
    template = piletter._template_text().replace(
        '{extra_acknowledgments}', '{extra_acknowledgments}\n{not_implemented_yet}')
    monkeypatch.setattr(piletter, '_template_text', lambda: template)
    body = piletter.build(make_exp(tmp_path)).body
    assert '{' not in body and '}' not in body


def test_braces_written_in_the_dashboard_survive(tmp_path, monkeypatch):
    """Only the template is cleaned up; a comment is content and is left exactly as typed."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    save_comments(exp, general='Flagged the {bad_scan} range by hand.')
    assert 'Flagged the {bad_scan} range by hand.' in piletter.build(exp).body


def test_emerlin_stations_add_their_own_acknowledgment(tmp_path, monkeypatch):
    """An array with e-MERLIN out-stations owes e-MERLIN an acknowledgment of its own."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    assert 'e-MERLIN' not in piletter.build(exp).body        # Ef, Wb, Ys, Tr: no e-MERLIN

    exp.antennas.append(experiment.Antenna(name='Kn'))
    exp.antennas.append(experiment.Antenna(name='De'))
    exp.antennas.append(experiment.Antenna(name='Pi', observed=False))
    body = piletter.build(exp).body
    assert 'the e-MERLIN antennas De and Kn' in body         # Pi never observed: left out
    assert ('> e-MERLIN is a National Facility operated by the University of Manchester at '
            'Jodrell Bank Observatory on behalf of STFC.') in body
    # Its own quote block, after the EVN one, not glued to it.
    assert body.count('> ') == 2
    assert body.index('project code: EB101.') < body.index('> e-MERLIN is a National')


def test_dashboard_comments_reach_the_letter(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    save_comments(exp, general='Everything went well.',
                  stations={'Ef': es.StationComment('minor', 'Maser problems for one hour.'),
                            'Ys': es.StationComment('success', '')})
    body = piletter.build(exp).body
    assert '- Everything went well.' in body
    # The note is the whole message: the dashboard status never reaches the letter.
    assert '- **Ef**: Maser problems for one hour.\n' in body
    assert '**Ys**' not in body                    # reviewed, nothing to say: not listed
    assert '- **Tr**: Did not observe.' in body    # automatic finding
    assert 'minor issues' not in body and 'could not observe' not in body


def test_station_remarks_open_with_the_antennas_that_observed(tmp_path, monkeypatch):
    """The first bullet of the section is the array that took the data, comma-separated."""
    monkeypatch.chdir(tmp_path)
    body = piletter.build(make_exp(tmp_path)).body
    remarks = body.split('## Remarks on individual stations')[1]
    assert remarks.strip().splitlines()[0] == '- **Antennas that observed**: Ef, Wb, Ys.'
    assert 'Tr' not in remarks.splitlines()[1]      # Tr did not observe: not in the list


def test_the_dashboard_status_never_reaches_the_letter(tmp_path, monkeypatch):
    """The traffic-light status is ours; the PI reads the note, whatever the status is."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    for status in ('major', 'minor', 'success'):
        save_comments(exp, stations={'Tr': es.StationComment(status, 'Receiver broken.')})
        body = piletter.build(exp).body
        assert '- **Tr**: Receiver broken.\n' in body
        assert 'minor issues' not in body and 'could not observe' not in body


def test_station_comments_are_reread_from_disk(tmp_path, monkeypatch):
    """The dashboard runs in another process: its saved comments must win over the copy in memory."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    save_comments(exp, stations={'Ef': es.StationComment('minor', 'Written in the dashboard.')})
    other = es.load_toml(tmp_path / 'eb101b.toml')   # what the dashboard process wrote later
    other.record_comments(stations={'Ef': es.StationComment('minor', 'Edited afterwards.')})
    other.save()
    assert 'Edited afterwards.' in piletter.build(exp).body


def test_reduced_bandwidth_sentence_is_not_repeated_per_station(tmp_path, monkeypatch):
    """It is already written once, for all the antennas, in the general remarks."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    save_comments(exp, stations={
        'Wb': es.StationComment('success', 'Observed with reduced bandwidth (2/4 subbands).'),
        'Ef': es.StationComment('minor', 'Missed one hour. Observed with reduced bandwidth (2/4 subbands).')})
    body = piletter.build(exp).body
    assert '- **Ef**: Missed one hour.\n' in body
    assert '**Wb**' not in body                     # only the bandwidth sentence: nothing left
    assert body.count('reduced bandwidth') == 0


def test_credentials_only_when_asked_for(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    exp.credentials = experiment.Credentials('EB101B', 's3cr3t')
    assert 's3cr3t' in piletter.build(exp).body
    assert 's3cr3t' not in piletter.build(exp, with_credentials=False).body
    exp.credentials = None
    assert 'Data access' not in piletter.build(exp).body


def test_empty_sections_leave_no_hole(tmp_path, monkeypatch):
    """An experiment with nothing to remark still produces a complete, tidy letter."""
    monkeypatch.chdir(tmp_path)
    dirs = experiment.Dirs(logs=Path('l'), plots=Path('p'), pipeline=Path('pipe'),
                           pipe_in=Path('in'), pipe_out=Path('out'), pipe_temp=Path('tmp'))
    exp = experiment.Experiment('EB101', dt.date(2026, 4, 10), 'tester', dirs)
    exp.exp_toml = es.load_toml(tmp_path / 'eb101.toml')
    text = piletter.render_text(piletter.build(exp))
    assert 'General remarks' not in text and 'Correlation parameters' not in text
    assert 'Acknowledgment' in text and '\n\n\n' not in text
    assert 'Dear PI,' in text


# ------------------------------------------------------------------ the renderings

def test_plain_text_flows_and_spells_out_the_links(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    text = piletter.render_text(piletter.build(make_exp(tmp_path)))
    assert text.startswith('To: Jane Doe <jane@obs.edu>')
    # Nothing is hard-wrapped: the reader's program reflows it, so a paragraph is one line.
    assert 'The data from your EVN project EB101B (observed on 10 April 2026) are now ' \
           'available to download from the EVN Data Archive ' \
           '(https://archive.jive.eu/scripts/arch.php?exp=EB101B).' in text
    assert max(len(line) for line in text.splitlines()) > 78
    assert 'Acknowledgment\n--------------' in text
    assert '(https://archive.jive.eu/scripts/arch.php?exp=EB101B)' in text  # link spelled out
    assert 'usersupport@jive.eu,' in text and 'mailto:' not in text  # plain address
    assert '\n   The European VLBI Network' in text                 # the quote is indented
    assert text.rstrip().endswith('EVN User Support, JIVE (https://www.jive.eu)')


def test_html_hyperlinks_everything_and_greys_out_the_acknowledgment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    html = piletter.render_html(piletter.build(make_exp(tmp_path)))
    assert '<a href="https://archive.jive.eu/scripts/arch.php?exp=EB101B"' in html
    assert '<a href="mailto:usersupport@jive.eu"' in html
    assert '<a href="https://www.evlbi.org/evn-data-reduction-guide"' in html
    quote = html.split('<blockquote')[1].split('</blockquote>')[0]
    assert 'font-style:italic' in quote and 'color:#6a737d' in quote
    assert 'The European VLBI Network is a joint facility' in quote
    # No Markdown markup survives the rendering.
    for markup in ('](', '**', '## '):
        assert markup not in html


def test_html_paragraphs_flow_and_the_signature_keeps_its_break(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    html = piletter.render_html(piletter.build(make_exp(tmp_path)))
    body = html.split('<p style')[2]                       # the opening paragraph
    assert '<br>' not in body.split('</p>')[0]             # one flowing paragraph
    assert 'Tester<br>EVN User Support' in html            # the signature does break


def test_markdown_for_the_chat_is_one_line_per_paragraph(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    markdown = piletter.render_markdown(piletter.build(make_exp(tmp_path)))
    assert markdown.startswith('Dear Jane Doe,')          # no mail headers in the chat
    assert '## Correlation parameters' in markdown
    assert '[EVN Data Archive](https://archive.jive.eu/scripts/arch.php?exp=EB101B)' in markdown
    opening = markdown.splitlines()[2]
    assert opening.startswith('The data from your EVN project **EB101B**') and opening.endswith('products.')


def test_eml_is_a_draft_with_recipients_and_both_alternatives(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    exp.credentials = experiment.Credentials('EB101B', 's3cr3t')
    message = email.message_from_bytes(piletter.render_eml(piletter.build(exp)),
                                       policy=email.policy.default)
    assert message['To'] == 'Jane Doe <jane@obs.edu>' and message['Cc'] == 'jops@jive.eu'
    assert message['Subject'] == 'EVN experiment EB101B available on the EVN Archive'
    assert message['X-Unsent'] == '1'                      # opens as a draft, not as received mail
    assert message.is_multipart()
    types = {part.get_content_type() for part in message.walk()}
    assert {'text/plain', 'text/html'} <= types
    plain = message.get_body(('plain',)).get_content()
    assert plain.startswith('Dear Jane Doe,') and 's3cr3t' in plain
    assert 's3cr3t' in message.get_body(('html',)).get_content()


# ------------------------------------------------------------------ files & delivery

def test_write_letter_produces_every_format(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    exp.credentials = experiment.Credentials('EB101B', 's3cr3t')
    written = piletter.write_letter(exp)
    assert set(written) == {'text', 'auth', 'html', 'eml'}
    assert all(path.exists() for path in written.values())
    # The archived letter never carries the credentials; the one sent to the PI does.
    assert 's3cr3t' not in written['text'].read_text()
    assert 's3cr3t' in written['auth'].read_text()
    assert 's3cr3t' in written['html'].read_text()


def test_the_archived_letter_names_no_recipient(tmp_path, monkeypatch):
    """The .piletter is archived publicly: no PI email address in it, and no credentials."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    exp.credentials = experiment.Credentials('EB101B', 's3cr3t')
    written = piletter.write_letter(exp)
    archived = written['text'].read_text()
    assert 'jane@obs.edu' not in archived                 # the PI's address is not published
    assert not archived.startswith('To:') and '\nCc:' not in archived
    assert 's3cr3t' not in archived
    assert 'Subject: EVN experiment EB101B' in archived   # it still says what it is
    assert 'Dear Jane Doe,' in archived                   # the PI's name is not the issue
    # The copy that is actually sent keeps them.
    assert 'jane@obs.edu' in written['auth'].read_text()
    assert 'jane@obs.edu' in written['eml'].read_text()


def test_no_credentials_means_no_auth_letter(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    written = piletter.write_letter(make_exp(tmp_path))
    assert set(written) == {'text', 'html', 'eml'}
    assert not Path('eb101b.piletter_auth').exists()


def test_regenerating_an_unchanged_letter_leaves_no_backup(tmp_path, monkeypatch):
    """Same content, same bytes: no .bak noise on every run (the .eml boundary is fixed)."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    piletter.write_letter(exp)
    piletter.write_letter(exp)
    assert list(tmp_path.glob('*.bak')) == []


def test_regenerating_keeps_the_previous_version(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    piletter.write_letter(exp)
    Path('eb101b.piletter').write_text('edited by hand\n')
    piletter.write_letter(exp)
    assert Path('eb101b.piletter.bak').read_text() == 'edited by hand\n'
    assert 'Dear Jane Doe,' in Path('eb101b.piletter').read_text()


def test_notify_letter_ready_posts_the_letter_with_its_attachments(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    piletter.write_letter(exp)
    sent = {}

    class Recorder:
        def send_message(self, subject, body, attachments=None):
            sent.update(subject=subject, body=body, attachments=attachments)
            return True

    assert piletter.notify_letter_ready(exp, Recorder()) is True
    assert 'Dear Jane Doe,' in sent['body'] and 'jane@obs.edu' in sent['body']
    assert [path.name for path in sent['attachments']] == ['eb101b.piletter.eml',
                                                           'eb101b.piletter.html']


def test_notify_letter_ready_without_comms_is_a_no_op(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    piletter.write_letter(exp)
    assert piletter.notify_letter_ready(exp, None) is False
