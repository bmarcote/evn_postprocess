"""Creation and delivery of the PI letter (JIVE delivery).

Part of the ``jive`` distribution backend: only a delivery that archives the data at JIVE
writes to the PI, so nothing outside :mod:`evn_postprocess.distribution.jive` uses this
module (the ``none`` and ``sweeps`` backends inherit the no-op
:meth:`~evn_postprocess.distribution.Distributor.prepare_letter`).

One template, one generator, three renderings. The letter is *generated* from
``templates/piletter.md.template`` for every experiment (it is no longer a file retrieved
from the piletters server and patched in place), filling it with the experiment metadata
and with what the support scientist wrote in the dashboard Comments tab:

  - :func:`build` collects everything into a :class:`Letter` (RFC-822 headers + a Markdown
    body). This is the single source of truth for the content.
  - :func:`render_text`, :func:`render_html` and :func:`render_markdown` turn that body into
    the plain-text letter (archived and sent as `.piletter`), the styled HTML version (the
    acknowledgment in grey italics, every link a real hyperlink), and the Markdown version
    posted to the chat so it can be copied straight into an email.
  - :func:`render_eml` packs headers + text + HTML into an ``.eml`` file: opening it in a
    local mail client gives the support scientist a ready-to-send draft (we deliberately
    never send mail from here; no mail credentials live in this program).
  - :func:`write_letter` writes the files and :func:`notify_letter_ready` posts the letter
    (with the `.eml` and `.html` attached) to the operator's chat.

The Markdown subset understood by the renderers is deliberately small and fixed:
``## headings``, ``- bullets``, ``> block quotes``, paragraphs, and the inline forms
``[text](url)``, ``<user@host>``, bare URLs, ``**bold**`` and ``` `code` ```.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from email.message import EmailMessage
from importlib import resources
from pathlib import Path

from loguru import logger
from astropy import units as u

from .. import experiment, experiment_state, review


TEMPLATE_NAME = 'piletter.md.template'

<<<<<<< HEAD
=======
# What the per-station status adds to the station's line in the letter (the note itself is
# written by the support scientist in the dashboard).
STATUS_LABELS = {'minor': ' (minor issues)', 'major': ' (could not observe)', 'success': ''}

>>>>>>> d499383ef8bc92d1fc9aaf2ee21dd4b1b9e1d214
# The e-MERLIN out-stations. When any of them is in the array the PI owes e-MERLIN its own
# acknowledgment on top of the EVN one. Jb is deliberately not here: Jodrell Bank observes
# with the EVN in its own right, and its presence alone does not make it an e-MERLIN run.
EMERLIN_ANTENNAS = ('De', 'Da', 'Pi', 'Kn')

# The acknowledgment e-MERLIN asks for, quoted verbatim in the letter.
EMERLIN_ACKNOWLEDGMENT = ("e-MERLIN is a National Facility operated by the University of "
                          "Manchester at Jodrell Bank Observatory on behalf of STFC.")

# The reduced-bandwidth sentence is dropped from the per-station notes: the same information
# is written once, for all affected antennas, in the general remarks. Matches the wording
# produced by review.default_station_comments.
_BANDWIDTH_NOTE_RE = re.compile(r"\s*Observed with reduced bandwidth\s*\([^)]*\)\.?", re.IGNORECASE)

<<<<<<< HEAD
=======
# A note that already says the station did not (or could not) observe makes the 'major'
# STATUS_LABELS suffix a repetition ("Did not observe. (could not observe)"), so the suffix
# is left out for it.
_NO_OBSERVE_NOTE_RE = re.compile(r"(did|could)\s+not\s+observe", re.IGNORECASE)

>>>>>>> d499383ef8bc92d1fc9aaf2ee21dd4b1b9e1d214
# The inline Markdown forms, in one pass so nothing is processed twice (an URL inside a
# [text](url) link must not be auto-linked again, and no escaping can eat the markup).
_INLINE_RE = re.compile(r"\[(?P<ltext>[^\]]+)\]\((?P<lurl>[^)\s]+)\)"
                        r"|<(?P<mail>[^<>@\s]+@[^<>\s]+)>"
                        r"|(?P<url>https?://[^\s<>()]+)"
                        r"|\*\*(?P<bold>[^*]+)\*\*"
                        r"|`(?P<code>[^`]+)`")

# Inline styles (not a <style> block): they are what survives a copy-paste from the browser
# into a mail client, which is exactly how this letter is meant to travel.
_CSS = {'body': "font-family:Helvetica,Arial,sans-serif; font-size:11pt; line-height:1.5; "
                "color:#1f2328; max-width:46em;",
        'h2': "font-family:Helvetica,Arial,sans-serif; font-size:11.5pt; color:#0b3d6b; "
              "margin:1.6em 0 0.5em; padding-bottom:0.2em; border-bottom:1px solid #d8dee6;",
        'p': "margin:0.8em 0;",
        'ul': "margin:0.8em 0; padding-left:1.4em;",
        'li': "margin:0.3em 0;",
        # The EVN acknowledgment: quoted text the PI copies into their paper, so it is set
        # apart from the letter itself in grey italics.
        'quote': "margin:1.2em 0 1.2em 0.5em; padding:0.2em 0 0.2em 1em; "
                 "border-left:3px solid #c8d1db; color:#6a737d; font-style:italic;",
        'a': "color:#1a5fb4;",
        'code': "font-family:Menlo,Consolas,monospace; font-size:10.5pt; background:#f2f4f7; "
                "padding:0.1em 0.35em; border-radius:3px;"}


@dataclass
class Letter:
    """One PI letter: the mail headers and the Markdown body that renders to every format."""
    expname: str
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ''

    @property
    def subject(self) -> str:
        """The mail subject line (empty when the template defines no Subject header)."""
        return self.headers.get('Subject', '')


# --------------------------------------------------------------------------- content blocks

def _comments(exp: experiment.Experiment) -> experiment_state.CommentsSection:
    """The reviewed [comments] of *exp*, re-read from disk.

    The support scientist writes them in the dashboard, which runs in its own process: an
    in-memory copy loaded before the review pause would miss everything they typed, so the
    toml is always reloaded (``fresh=True``) before a letter is built.
    """
    return experiment_state.attached_toml(exp, fresh=True).comments


def _block(title: str, body: str) -> str:
    """One optional ``## title`` section, or '' when there is nothing to say.

    Args:
        title: Section heading, without the Markdown ``##``.
        body: Markdown body of the section (already formatted); '' drops the whole section.

    Returns:
        The section, starting with a blank line so it detaches from the previous paragraph.
    """
    return f"\n## {title}\n\n{body.strip()}\n" if body.strip() else ''


def _join(items: list[str], oxford: bool = False) -> str:
    """The English enumeration used all over the letter: 'A', 'A and B', 'A, B and C'.

    Args:
        items: At least one item.
        oxford: True to add the comma before 'and', for items that contain commas themselves.

    Returns:
        The joined text.
    """
    if len(items) == 1:
        return items[0]
    return f"{', '.join(items[:-1])}{',' if oxford else ''} and {items[-1]}"


def _antennas(names: list[str]) -> str:
    """'antenna Ef' or 'antennas Ef, Wb and Ys', for the remarks that name a group of them."""
    return f"antenna{'s' if len(names) > 1 else ''} {_join(names)}"


def _credentials_block(exp: experiment.Experiment) -> str:
    """The archive username/password section, empty when the experiment is public.

    Args:
        exp: Experiment object.

    Returns:
        The 'Data access' section, or '' when no credentials are set.
    """
    if exp.credentials is None or exp.credentials.password is None:
        return ''
    return _block("Data access",
                  "The data are password-protected. Please use the following credentials to "
                  "access them:\n\n"
                  f"- Username: `{exp.credentials.username}`\n"
                  f"- Password: `{exp.credentials.password}`")


def _pass_line(a_pass: experiment.CorrelatorPass, index: int, total: int) -> str:
    """One bullet describing the correlation setup of a single correlator pass.

    The FITS-IDI file name is only named when the experiment has more than one pass: it is
    what tells the passes apart. With a single pass there is nothing to tell apart, and the
    PI already sees the file names in the archive.

    Args:
        a_pass: The correlator pass.
        index: Zero-based position of the pass in the experiment.
        total: How many passes the experiment has (a single pass is not numbered).

    Returns:
        A Markdown bullet, without the leading '- '.
    """
    label = f"**Correlator pass #{index + 1}**: " if total > 1 else ''
    fitsidi = f" FITS-IDI files: `{a_pass.fitsidifile}`." if total > 1 else ''
    if a_pass.freqsetup is None:
        return f"{label}Correlation setup not available.{fitsidi}"
    setup = a_pass.freqsetup
    per_subband = (setup.bandwidth / setup.subbands).to(u.MHz).value
    pols = ', '.join(getattr(p, 'name', str(p)) for p in setup.polarizations)
    polabel = {1: 'single', 2: 'dual', 4: 'full'}.get(len(setup.polarizations), 'mixed')
    return (f"{label}Central frequency {setup.frequency.to(u.GHz):0.04}, "
            f"{setup.subbands} x {per_subband:g}-MHz subbands "
            f"({setup.bandwidth.to(u.MHz):g} in total), {setup.channels} spectral channels "
            f"per subband, {polabel} polarization ({pols}).{fitsidi}")


def _passes_block(exp: experiment.Experiment) -> str:
    """The correlation-parameters section: one bullet per correlator pass.

    The weight-flagging note (threshold and percentage of removed visibilities) is
    deliberately left out: it is post-processing done at JIVE, not a correlation
    parameter the PI needs to be told about.

    Args:
        exp: Experiment object.

    Returns:
        The 'Correlation parameters' section, or '' when the experiment has no passes.
    """
    if not exp.correlator_passes:
        return ''
    total = len(exp.correlator_passes)
    lines = [f"- {_pass_line(a_pass, i, total)}" for i, a_pass in enumerate(exp.correlator_passes)]
    body = "Your experiment was correlated with the following parameters:\n\n" + '\n'.join(lines)
    return _block("Correlation parameters", body)


def _polconvert_remark(exp: experiment.Experiment) -> str:
    """The PolConvert note, when some antenna observed in linear polarization."""
    if not exp.antennas.polconvert:
        return ''
    return (f"Note that the {_antennas(exp.antennas.polconvert)} originally observed linear "
            "polarizations, which were transformed to circular ones during post-processing via "
            "the PolConvert program (Martí-Vidal et al. 2016, A&A, 587, A143). Thanks to this "
            "correction, you can automatically recover the absolute EVPA value when using the "
            "antenna as reference station during fringe-fitting.")


def _subband_range(subbands: tuple[int, ...] | list[int]) -> str:
    """Renders an antenna's observed subbands as '3-6' when consecutive, else as the list.

    Subbands are numbered from 1 in the letter and from 0 in the metadata.
    """
    numbers = list(subbands)
    if numbers == list(range(min(numbers), max(numbers) + 1)):
        return f"{min(numbers) + 1}-{max(numbers) + 1}"
    return f"{tuple(numbers)}"


def _bandwidth_remark(exp: experiment.Experiment) -> str:
    """The note about the antennas that could only observe part of the bandwidth.

    Antennas observing the same subband range are grouped together. When the correlator
    passes do not share one frequency setup, the pass number is named for each range.

    Args:
        exp: Experiment object.

    Returns:
        The remark, or '' when every antenna observed the full bandwidth.
    """
    if any(a_pass.freqsetup is None for a_pass in exp.correlator_passes):
        logger.warning(f"{exp.expname}: not all correlator passes have a frequency setup yet; "
                       "the bandwidth-limitation remark in the PI letter may be incomplete.")
    passes = [(i, a_pass) for i, a_pass in enumerate(exp.correlator_passes)
              if a_pass.freqsetup is not None]
    if not passes:
        return ''
    # One setup for all of them: the first pass already says it all, and no pass is named.
    per_pass = len({a_pass.freqsetup.subbands for _, a_pass in passes}) > 1
    if not per_pass:
        passes = passes[:1]

    ranges: dict[str, list[str]] = {}   # subband range -> the antennas limited to it
    for i, a_pass in passes:
        for antenna in a_pass.antennas:
            if not 0 < len(antenna.subbands) < a_pass.freqsetup.subbands:
                continue
            key = _subband_range(antenna.subbands)
            key += f" (in correlator pass #{i + 1})" if per_pass else ''
            if antenna.name not in ranges.setdefault(key, []):
                ranges[key].append(antenna.name)

    if not ranges:
        return ''
    limited = [f"{_join(antennas)} only observed subbands {key}" for key, antennas in ranges.items()]
    return f"Note that {_join(limited, oxford=True)}, due to their local bandwidth limitations."


def _opacity_remark(exp: experiment.Experiment) -> str:
    """The note about the antennas whose Tsys/gain curves were corrected for opacity."""
    if not exp.antennas.opacity:
        return ''
    return (f"Note that the data from the {_antennas(exp.antennas.opacity)} have been corrected "
            "for opacity in the Tsys/Gain Curve measurements.")


def _remarks_block(exp: experiment.Experiment) -> str:
    """The general-remarks section: what the support scientist wrote, then the automatic notes.

    Args:
        exp: Experiment object.

    Returns:
        The 'General remarks' section, or '' when there is nothing to remark.
    """
    general = _comments(exp).general
    remarks = [general.strip()] if general and general.strip() else []
    remarks += [remark for remark in (_polconvert_remark(exp), _bandwidth_remark(exp),
                                      _opacity_remark(exp)) if remark]
    return _block("General remarks", '\n'.join(f"- {remark}" for remark in remarks))


def station_entries(exp: experiment.Experiment) -> dict[str, tuple[str, str]]:
    """The per-station (status, note) pairs that go into the letter.

    The automatic findings (did not observe, missed time ranges) are the starting point, so a
    letter is complete even when the dashboard was never opened; whatever the support scientist
    saved in the Comments tab of the dashboard overrides them, station by station.

    Args:
        exp: Experiment object.

    Returns:
        Mapping station name -> (status, note), including the entries with an empty note.
    """
    entries: dict[str, tuple[str, str]] = {}
    for name, report in review.station_summary(exp).stations.items():
        notes = ["Did not observe."] if not report.observed else []
        notes += [f"Missed {start.strftime('%d %b %H:%M')}-{end.strftime('%H:%M')} UT."
                  for start, end in report.missed_ranges]
        entries[name] = (report.status, ' '.join(notes))
    for name, comment in _comments(exp).stations.items():
        entries[name] = (comment.status, comment.note)
    return entries


def _observing_antennas(exp: experiment.Experiment) -> list[str]:
    """The antennas that took part in the observation, in the order they are stored."""
    return [antenna.name for antenna in exp.antennas if antenna.observed]


def _station_remarks_block(exp: experiment.Experiment) -> str:
    """The per-station section: the antennas that observed, then the ones with something to say.

    The first bullet always names the antennas that did observe, so the PI can see the array
    at a glance. After it, one bullet per station with a note: the reduced-bandwidth sentence
    is stripped from every note (it is already written once, for all affected antennas, in the
<<<<<<< HEAD
    general remarks). The note itself is the whole message: the traffic-light status the
    support scientist set in the dashboard is for us, and adding it to the letter as
    '(minor issues)' / '(could not observe)' only restates in jargon what the note already
    says in words.
=======
    general remarks) and the status adds its label, unless the note already says as much (a
    station that did not observe is not also labelled '(could not observe)').
>>>>>>> d499383ef8bc92d1fc9aaf2ee21dd4b1b9e1d214

    Args:
        exp: Experiment object.

    Returns:
        The 'Remarks on individual stations' section, or '' when there is nothing to say.
    """
    lines = []
    if (observed := _observing_antennas(exp)):
        lines.append(f"- **Antennas that observed**: {', '.join(observed)}.")
<<<<<<< HEAD
    for name, (_status, note) in sorted(station_entries(exp).items()):
        if (text := _BANDWIDTH_NOTE_RE.sub('', note).strip()):
            lines.append(f"- **{name}**: {text}")
=======
    for name, (status, note) in sorted(station_entries(exp).items()):
        text = _BANDWIDTH_NOTE_RE.sub('', note).strip()
        if text:
            label = STATUS_LABELS.get(status, '')
            if status == 'major' and _NO_OBSERVE_NOTE_RE.search(text):
                label = ''   # the note already says it: no ' (could not observe)' on top
            lines.append(f"- **{name}**: {text}{label}")
>>>>>>> d499383ef8bc92d1fc9aaf2ee21dd4b1b9e1d214
    return _block("Remarks on individual stations", '\n'.join(lines))


def _emerlin_antennas(exp: experiment.Experiment) -> list[str]:
    """The e-MERLIN out-stations that observed, in the order :data:`EMERLIN_ANTENNAS` lists them."""
    observing = {antenna.name.capitalize() for antenna in exp.antennas if antenna.observed}
    return [name for name in EMERLIN_ANTENNAS if name in observing]


def _extra_acknowledgments(exp: experiment.Experiment) -> str:
    """The acknowledgments the array owes on top of the EVN one, '' when there are none.

    Only e-MERLIN for now: an array with any of its out-stations in it carries a second
    quoted acknowledgment, introduced by its own sentence so the PI knows why it is there.

    Args:
        exp: Experiment object.

    Returns:
        Markdown to drop straight after the EVN acknowledgment quote (a leading newline
        included), or '' when the EVN one is all there is.
    """
    if not (emerlin := _emerlin_antennas(exp)):
        return ''
    return (f"\nThe observations also included the e-MERLIN {_antennas(emerlin)}, so "
            f"publications must carry this acknowledgment as well:\n\n"
            f"> {EMERLIN_ACKNOWLEDGMENT}\n")


# --------------------------------------------------------------------------- the letter

def _project_code(expname: str) -> str:
    """The project code for the acknowledgment: the experiment name without its epoch letter.

    ``EB101B`` is the second epoch of project ``EB101``; the PI must acknowledge the project.
    """
    return expname[:-1].upper() if expname[-1].isalpha() and len(expname) > 1 else expname.upper()


def _template_text() -> str:
    """Reads the packaged PI-letter template."""
    return resources.files("evn_postprocess.templates").joinpath(TEMPLATE_NAME).read_text(encoding='utf-8')


def _split_headers(text: str) -> tuple[dict[str, str], str]:
    """Splits the rendered template into its RFC-822 header block and its Markdown body.

    The template starts with ``Key: value`` lines (To, Cc, Subject) followed by a blank line;
    they become real mail headers in the .eml and the first lines of the plain-text letter.

    Args:
        text: The rendered template.

    Returns:
        (headers, body). Headers with an empty value are dropped (e.g. an unknown Cc).
    """
    headers: dict[str, str] = {}
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if not line.strip():
            return headers, '\n'.join(lines[i + 1:]).strip('\n')
        key, _, value = line.partition(':')
        if value.strip():
            headers[key.strip()] = value.strip()
    return headers, ''


def build(exp: experiment.Experiment, with_credentials: bool = True,
          with_recipients: bool = True) -> Letter:
    """Builds the PI letter of *exp* from the template, the metadata and the dashboard comments.

    Args:
        exp: Experiment object.
        with_credentials: False to leave the archive username/password out.
        with_recipients: False to leave the To/Cc headers out, so the PI's email address is
            not in the copy that is archived publicly alongside the data. Both are False for
            that copy, and both True for the letter that is actually sent.

    Returns:
        The :class:`Letter`, ready to be rendered into any of the output formats. Headers
        left empty are dropped, so a letter without recipients simply has none.
    """
    pis = [pi for pi in exp.pi if pi.email]
    obsdate = exp.obsdate.strftime('%d %B %Y') if exp.obsdate else 'an unknown date'
    replacements = {
        '{to}': ', '.join(f"{pi.name} <{pi.email}>" for pi in pis) if with_recipients else '',
        '{cc}': 'jops@jive.eu' if with_recipients else '',
        '{subject}': f"EVN experiment {exp.expname.upper()} available on the EVN Archive",
        '{piname}': ' and '.join(pi.name for pi in exp.pi if pi.name) or 'PI',
        '{expname}': exp.expname.upper(),
        '{expcode}': _project_code(exp.expname),
        '{obsdate}': obsdate,
        '{archive_url}': exp.archive_page,
        '{credentials}': _credentials_block(exp) if with_credentials else '',
        '{passes}': _passes_block(exp),
        '{remarks}': _remarks_block(exp),
        '{station_remarks}': _station_remarks_block(exp),
        '{extra_acknowledgments}': _extra_acknowledgments(exp),
        '{supsci}': exp.supsci.capitalize() if exp.supsci else 'EVN User Support',
    }
    text = _template_text()
<<<<<<< HEAD
    # Checked on the template, and dropped from it, *before* anything is substituted in: a
    # comment written in the dashboard may legitimately contain braces, and only the template
    # can have a typo'd (or, after a partial upgrade, a not-yet-implemented) placeholder.
    # Leaving one in would send the PI a letter with a literal '{extra_acknowledgments}' in it.
    if (unknown := set(re.findall(r"\{[a-z_]+\}", text)) - replacements.keys()):
        logger.warning(f"The PI letter template has placeholders nothing fills: "
                       f"{', '.join(sorted(unknown))}. They are left out of the letter; the "
                       f"template and this module are probably out of step.")
        for placeholder in unknown:
            text = text.replace(placeholder, '')
=======
    # Checked on the template, not on the result: a comment written in the dashboard may
    # legitimately contain braces, and only the template can have a typo'd placeholder.
    if (unknown := set(re.findall(r"\{[a-z_]+\}", text)) - replacements.keys()):
        logger.warning(f"The PI letter template has placeholders nothing fills: "
                       f"{', '.join(sorted(unknown))}.")
>>>>>>> d499383ef8bc92d1fc9aaf2ee21dd4b1b9e1d214
    for placeholder, value in replacements.items():
        text = text.replace(placeholder, value)
    headers, body = _split_headers(re.sub(r"\n{3,}", "\n\n", text))
    return Letter(expname=exp.expname, headers=headers, body=body.strip() + '\n')


# --------------------------------------------------------------------------- renderers

def _blocks(body: str) -> list[tuple[str, list[str]]]:
    """Parses the Markdown body into ('heading'|'quote'|'list'|'paragraph', lines) blocks.

    Args:
        body: The Markdown body of the letter.

    Returns:
        The blocks in order. Paragraph and quote blocks keep their lines separate so each
        renderer decides how to join them; list blocks hold one entry per bullet.
    """
    blocks: list[tuple[str, list[str]]] = []
    for line in body.splitlines():
        stripped = line.strip()
        kind, content = 'paragraph', stripped
        if not stripped:
            kind = ''
        elif stripped.startswith('## '):
            kind, content = 'heading', stripped[3:].strip()
        elif stripped.startswith('> '):
            kind, content = 'quote', stripped[2:].strip()
        elif stripped.startswith('- '):
            kind, content = 'list', stripped[2:].strip()

        if not kind:
            blocks.append(('', []))          # a blank line always closes the open block
        elif blocks and blocks[-1][0] == kind and kind != 'heading':
            blocks[-1][1].append(content)
        else:
            blocks.append((kind, [content]))
    return [(kind, lines) for kind, lines in blocks if kind]


def _flows(lines: list[str]) -> list[list[str]]:
    """Groups the lines of a block into the flows separated by a hard line break.

    Inside a paragraph a newline is only a convenience of the template: the lines are one
    flow of text, re-wrapped by every renderer. A line ending with a backslash (the
    CommonMark hard break, used e.g. between the signature lines) closes the flow instead.

    Args:
        lines: The lines of one paragraph or quote block.

    Returns:
        One list of lines per flow, with the backslash markers removed.
    """
    flows: list[list[str]] = [[]]
    for line in lines:
        if line.endswith('\\'):
            flows[-1].append(line[:-1].rstrip())
            flows.append([])
        else:
            flows[-1].append(line)
    return [flow for flow in flows if flow]


def _inline(text: str, *, html: bool) -> str:
    """Renders the inline Markdown of *text* to HTML or to plain text.

    Args:
        text: One line of Markdown (no block markup left).
        html: True for HTML (hyperlinks, <b>, <code>), False for plain text.

    Returns:
        The rendered line. In HTML everything outside the markup is escaped.
    """
    def escape(raw: str) -> str:
        return raw.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;') if html else raw

    def link(url: str, label: str) -> str:
        if html:
            return f'<a href="{url}" style="{_CSS["a"]}">{escape(label)}</a>'
        # Plain text: '(url)' after the label, unless the label already is the address.
        bare = url.removeprefix('mailto:').removeprefix('https://').removeprefix('http://')
        return label if label in (url, bare, bare.rstrip('/')) else f"{label} ({url})"

    out, last = [], 0
    for match in _INLINE_RE.finditer(text):
        out.append(escape(text[last:match.start()]))
        last = match.end()
        if match.group('ltext'):
            out.append(link(match.group('lurl'), match.group('ltext')))
        elif match.group('mail'):
            out.append(link(f"mailto:{match.group('mail')}", match.group('mail')))
        elif match.group('url'):
            out.append(link(match.group('url'), match.group('url')))
        elif match.group('bold'):
            out.append(f"<b>{escape(match.group('bold'))}</b>" if html else match.group('bold'))
        else:
            out.append(f'<code style="{_CSS["code"]}">{escape(match.group("code"))}</code>'
                       if html else match.group('code'))
    out.append(escape(text[last:]))
    return ''.join(out)


def render_markdown(letter: Letter) -> str:
    """The letter as Markdown: the format the chat renders and the support scientist copies.

    Each paragraph becomes a single line, because a chat (Mattermost) shows every newline as
    a line break and the template's own wrapping would leak into the message.

    Args:
        letter: The letter to render.

    Returns:
        The Markdown body (no mail headers: the chat message carries its own).
    """
    out = []
    for kind, lines in _blocks(letter.body):
        out.append('')
        if kind == 'heading':
            out.append(f"## {lines[0]}")
        elif kind == 'list':
            out += [f"- {item}" for item in lines]
        else:
            prefix = '> ' if kind == 'quote' else ''
            out += [f"{prefix}{' '.join(flow)}" for flow in _flows(lines)]
    return '\n'.join(out).strip() + '\n'


def render_text(letter: Letter, headers: bool = True) -> str:
    """The letter as plain text: the file that is archived and pasted into a plain email.

    Headings get an underline, quotes are indented, and links become 'text (url)'. Nothing
    is wrapped: every paragraph, bullet and quote is one long line, so the mail client (or
    the editor, or the terminal) reflows it to whatever width the reader has. A letter
    hard-wrapped here would keep the breaks of a 78-column terminal in a window of any
    other size.

    Args:
        letter: The letter to render.
        headers: False to leave the To/Cc/Subject block out (the .eml carries them as real
            mail headers instead).

    Returns:
        The plain-text letter.
    """
    out = [f"{key}: {value}" for key, value in letter.headers.items()] if headers else []
    for kind, lines in _blocks(letter.body):
        plain = [_inline(line, html=False) for line in lines]
        out.append('')
        if kind == 'heading':
            out += [plain[0], '-' * len(plain[0])]
        elif kind == 'list':
            out += [f"- {item}" for item in plain]
        elif kind == 'quote':
            out += [f"   {' '.join(flow)}" for flow in _flows(plain)]
        else:
            out += [' '.join(flow) for flow in _flows(plain)]
    return '\n'.join(out).strip() + '\n'


def render_html(letter: Letter) -> str:
    """The letter as a styled HTML document: hyperlinks everywhere, acknowledgment in grey italics.

    Args:
        letter: The letter to render.

    Returns:
        A complete HTML page. Every style is inline, so a copy-paste of the rendered page into
        a mail client keeps the formatting.
    """
    out = ['<!DOCTYPE html>', '<html>', '<head><meta charset="utf-8">',
           f"<title>{letter.subject or letter.expname.upper()}</title></head>",
           f'<body style="{_CSS["body"]}">']
    for kind, lines in _blocks(letter.body):
        rendered = [_inline(line, html=True) for line in lines]
        if kind == 'heading':
            out.append(f'<h2 style="{_CSS["h2"]}">{rendered[0]}</h2>')
        elif kind == 'list':
            out.append(f'<ul style="{_CSS["ul"]}">')
            out += [f'<li style="{_CSS["li"]}">{item}</li>' for item in rendered]
            out.append('</ul>')
        elif kind == 'quote':
            text = '<br>'.join(' '.join(flow) for flow in _flows(rendered))
            out.append(f'<blockquote style="{_CSS["quote"]}">{text}</blockquote>')
        else:
            text = '<br>'.join(' '.join(flow) for flow in _flows(rendered))
            out.append(f'<p style="{_CSS["p"]}">{text}</p>')
    out += ['</body>', '</html>', '']
    return '\n'.join(out)


def render_eml(letter: Letter) -> bytes:
    """The letter as an ``.eml`` draft: headers, plain text, and the HTML alternative.

<<<<<<< HEAD
    Opening the file in a local mail client gives a message with the recipients, the subject
    and the formatted body already in place. Whether it opens *editable* is up to the client:
    ``X-Unsent: 1`` is an Outlook-for-Windows convention (and it is written first, which is
    where the clients that read it expect it); Outlook for Mac, Thunderbird and Apple Mail
    ignore it and show the file as a message to read, from which the operator sends the
    letter with 'Edit as New Message' / 'Forward'. No ``From`` or ``Date`` header is written,
    so nothing marks the file as already sent either.
=======
    Opening the file in a local mail client (Thunderbird, Outlook, Apple Mail) gives a message
    with the recipients, the subject and the formatted body already in place. ``X-Unsent``
    tells the clients that honour it to open it as a draft rather than as received mail.
>>>>>>> d499383ef8bc92d1fc9aaf2ee21dd4b1b9e1d214

    Args:
        letter: The letter to render.

    Returns:
        The .eml file content.
    """
    message = EmailMessage()
<<<<<<< HEAD
    message['X-Unsent'] = '1'
    for key, value in letter.headers.items():
        message[key] = value
=======
    for key, value in letter.headers.items():
        message[key] = value
    message['X-Unsent'] = '1'
>>>>>>> d499383ef8bc92d1fc9aaf2ee21dd4b1b9e1d214
    message.set_content(render_text(letter, headers=False))
    message.add_alternative(render_html(letter), subtype='html')
    # A fixed MIME boundary (the default is random): regenerating an unchanged letter must
    # produce an identical file, or every run would leave a spurious .bak behind.
    message.set_boundary(f"===============evn-{letter.expname.lower()}==")
    return message.as_bytes()


# --------------------------------------------------------------------------- files & delivery

def letter_paths(exp: experiment.Experiment) -> dict[str, Path]:
    """The paths of every PI-letter product of *exp* (existing or not).

    Keys: 'text' (the archived letter, no credentials), 'auth' (the letter sent to the PI),
    'html' and 'eml'.
    """
    stem = exp.expname.lower()
    return {'text': Path(f"{stem}.piletter"), 'auth': Path(f"{stem}.piletter_auth"),
            'html': Path(f"{stem}.piletter.html"), 'eml': Path(f"{stem}.piletter.eml")}


def _write(path: Path, content: str | bytes) -> None:
    """Writes *content*, keeping one backup of the previous version in '<path>.bak'.

    The letter is regenerated from the template every time (after the pipeline, and again
    before the delivery, so the latest dashboard comments are in it). Anything edited by hand
    in the meantime would be lost, so the previous version is always kept next to it.
    """
    data = content.encode() if isinstance(content, str) else content
    if path.exists() and (previous := path.read_bytes()) != data:
        backup = path.with_suffix(path.suffix + '.bak')
        backup.write_bytes(previous)
        logger.debug(f"Previous {path.name} kept as {backup.name}.")
    path.write_bytes(data)


def write_letter(exp: experiment.Experiment, with_credentials: bool | None = None) -> dict[str, Path]:
    """Generates the PI letter of *exp* and writes every format to the experiment directory.

    Always writes the plain-text letter (`.piletter`) and the HTML rendering. The `.piletter`
    is the copy archived together with the data, so it carries neither the credentials nor
    the To/Cc headers: the PI's email address does not belong in a public archive. When the
    experiment has archive credentials, the letter actually sent to the PI is written too
    (`.piletter_auth` and the `.eml` draft), with both.

    Args:
        exp: Experiment object.
        with_credentials: Force the credentials in (True) or out (False) of the sent letter;
            None (default) includes them whenever the experiment has them.

    Returns:
        The files written, keyed as in :func:`letter_paths`.
    """
    paths = letter_paths(exp)
    has_credentials = exp.credentials is not None and exp.credentials.password is not None
    include = has_credentials if with_credentials is None else with_credentials

    _write(paths['text'], render_text(build(exp, with_credentials=False, with_recipients=False)))
    sent = build(exp, with_credentials=include)
    _write(paths['html'], render_html(sent))
    _write(paths['eml'], render_eml(sent))
    written = {'text': paths['text'], 'html': paths['html'], 'eml': paths['eml']}
    if include:
        _write(paths['auth'], render_text(sent))
        written['auth'] = paths['auth']
    logger.info(f"PI letter written: {', '.join(str(p) for p in written.values())}.")
    return written


def notify_letter_ready(exp: experiment.Experiment, notifier=None) -> bool:
    """Posts the finished PI letter to the operator's chat, ready to be copied or opened.

    The message carries the letter as Markdown (so the chat renders it and it can be copied
    straight into an email) and the `.eml` and `.html` files as attachments: downloading the
    `.eml` and opening it locally gives a draft with recipients, subject and formatting
    already set. This program never sends the mail itself (it holds no mail credentials).

    Args:
        exp: Experiment object.
        notifier: A ``comms.Notifier``, or None when comms are not configured.

    Returns:
        True when the message was sent.
    """
    from .. import comms   # local import: comms is optional plumbing, this module is not
    paths = letter_paths(exp)
    letter = build(exp)
    recipients = ', '.join(f"{pi.name} ({pi.email})" for pi in exp.pi if pi.email) or 'the PI'
    needed = (f"1. Send the letter to {recipients}, with `jops@jive.eu` in copy.\n"
              f"2. Either copy the text below into your mail client, or download "
              f"`{paths['eml'].name}` and open it locally: it opens as a draft with the "
              f"recipients, the subject and the formatting already in place.")
    attachments = [path for path in (paths['eml'], paths['html']) if path.exists()]
    return comms.notify_operator(exp, "the PI letter is ready to be sent",
                                 f"The PI letter is ready:\n\n---\n\n{render_markdown(letter)}\n---",
                                 needed, notifier, attachments)
