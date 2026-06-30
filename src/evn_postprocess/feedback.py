"""Generation of the EVN Pipeline feedback HTML page.

This is a Python port of the historical ``feedback.pl`` Perl script (originally by
Cormac Reynolds, 2001). It produces the ``{expname}.html`` feedback page that links to
the EVN pipeline output products (data summaries, plots, maps, ...) for an experiment,
marking any product missing on disk as "(not available)", and interleaves the
human-written comments stored in the ``{expname}.comment`` file.

The page skeleton lives in ``templates/feedback.html.template``; only the dynamic body
(the per-section links and comments) is assembled here.
"""
from __future__ import annotations

import os
import re
import stat
import datetime as dt
from pathlib import Path
from importlib import resources
from typing import Iterable, Sequence

from loguru import logger


# Colours reused from the original script.
_PURPLE = "#4400CC"
_RED = "#FF0000"

# Sections rendered as a single link to "{expname}_{NAME}.pdf" (optionally with a
# second descriptive sentence). Order matters: it defines the order on the page.
# Each entry is (section_name, link_text, trailing_text_or_None).
_PDF_SECTIONS: tuple[tuple[str, str, str | None], ...] = (
    ("POSSM_AUTOCORR", "Plots of the autocorrelations", None),
    ("VPLOT_UNCAL", "Plots of the uncalibrated amplitude and phase against time", None),
    ("POSSM_UNCAL", "Plots of the uncalibrated amplitude and phase against frequency channel", None),
    ("POSSM_CPOL", "The uncalibrated amplitude and phase of the crosshand correlations "
                   "against frequency channel", None),
    ("TSYS", "TSYS against time", None),
    ("GAIN", "Telescope sensitivities ", "from the a priori TSYS and Gain curves (the square of "
             "this number gives the antenna noise (SEFD) in Jy - the smaller the better)."),
    ("FRING_PHAS", "Fringe-fit phase solutions ", "(including Parallactic Angle correction)."),
    ("FRING_DELAY", "Fringe-fit delay solutions", None),
    ("FRING_RATE", "Fringe-fit rate solutions", None),
    ("BANDPASS", "Telescope bandpasses", None),
    ("VPLOT_CAL", "Calibrated amplitude and phase against time ", "(a priori amplitude "
                  "calibration and fringe-fit solutions applied)."),
    ("POSSM_CAL", "Calibrated amplitude and phase against frequency channel", None),
)


def _read_comments(comment_file: Path) -> dict[str, str]:
    """Parses a ``{expname}.comment`` file into a {section_name: comment_text} mapping.

    The file is a sequence of blocks separated by ``///`` lines. Each block starts with a
    section keyword (e.g. ``GENERAL``, ``TSYS``) followed by the free-text comment. This
    mirrors the original Perl behaviour: all lines are joined with spaces, split on
    ``///``, and each block's leading keyword is stripped to leave the comment text.
    """
    if not comment_file.exists():
        return {}

    joined = " ".join(line.rstrip("\n") for line in comment_file.read_text().splitlines())
    comments: dict[str, str] = {}
    for block in joined.split("///"):
        block = block.strip()
        if not block:
            continue
        # The keyword is the first whitespace-delimited token; the rest is the comment.
        parts = block.split(None, 1)
        name = parts[0]
        comments[name] = parts[1].strip() if len(parts) > 1 else ""
    return comments


def _guess_sources(expname: str, directory: Path) -> list[str]:
    """Returns the source names found from ``{expname}_{SOURCE}_UVPLT.pdf`` files in *directory*."""
    pattern = re.compile(rf"^{re.escape(expname)}_(.*)_UVPLT\.pdf$")
    sources: list[str] = []
    for entry in sorted(p.name for p in directory.iterdir()):
        match = pattern.match(entry)
        if match:
            sources.append(match.group(1))
    return sources


def _mkhref(link: str, directory: Path, text: str) -> str:
    """Returns an ``<a href>`` snippet for *text* pointing at *link*.

    If the file does not exist on disk (resolved relative to *directory*), the link is
    replaced by the text followed by a red "(not available)" marker, exactly as the
    original script did.
    """
    target = directory / link
    if target.exists():
        return f'<a href ="{link}">\n{text}</a> \n'
    return f'{text}<font color="{_RED}"> (not available) </font> \n'


def _comment_block(name: str, comments: dict[str, str]) -> str:
    """Renders the trailing "Comments." block for a section (closes <p>, adds <hr>)."""
    return (f'<font color="{_PURPLE}">\n'
            "Comments. <br /> \n"
            "</font>\n"
            f"{comments.get(name, '')}\n"
            "<br /> \n</p> \n<hr /> \n \n<p> \n")


def _pdf_section(expname: str, directory: Path, name: str, text: str,
                 text2: str | None, comments: dict[str, str]) -> str:
    """Renders a single-link section pointing at ``{expname}_{name}.pdf``."""
    link = f"./{expname}_{name}.pdf"
    out = _mkhref(link, directory, text)
    if text2:
        out += text2
    out += "<br />\n"
    out += _comment_block(name, comments)
    return out


def _map_links(expname: str, directory: Path, source: str, name: str,
               suffixes: Sequence[str], texts: Sequence[str]) -> str:
    """Renders the per-source list of links ``{expname}_{source}_{name}.{suffix}``."""
    out = ""
    n = len(suffixes)
    for i, (suffix, text) in enumerate(zip(suffixes, texts)):
        link = f"./{expname}_{source}_{name}.{suffix}"
        out += " " + _mkhref(link, directory, text)
        out += ", or \n" if i + 1 < n else ". <br />\n"
    return out


def _map_section(expname: str, directory: Path, sources: Sequence[str], name: str,
                 heading: str, suffixes: Sequence[str], comments: dict[str, str],
                 show_source_label: bool, texts: Sequence[str] | None = None,
                 always_comment: bool = False) -> str:
    """Renders a per-source map section.

    Args:
        heading: Introductory sentence printed before the per-source links.
        suffixes: File suffixes to link for each source (e.g. ``("pdf", "FITS")``).
        show_source_label: If True, prepend ``"{source}:"`` before each source's links
            (used when the link texts are the suffixes themselves). If False, the link
            text is the source name instead.
        texts: Explicit link texts (one per suffix). If None, the suffixes are used as the
            link texts.
        always_comment: If True, always append the comment block; otherwise only when at
            least one source was rendered (matching the original ``if nmaps > 0`` guard).
    """
    out = heading
    for source in sources:
        if show_source_label:
            out += f"{source}:"
            link_texts = list(texts) if texts is not None else list(suffixes)
        else:
            # The link text is the source name (one link per suffix).
            link_texts = [source] * len(suffixes)
        out += _map_links(expname, directory, source, name, suffixes, link_texts)

    if sources or always_comment:
        out += _comment_block(name, comments)
    return out


def _build_body(expname: str, directory: Path, sources: Sequence[str],
                comments: dict[str, str]) -> str:
    """Assembles the dynamic HTML body (everything between the intro and the footer)."""
    parts: list[str] = []

    # --- GENERAL section: data summary + scan listing links, then general comments. ---
    general = (f'<p>\n<font color="{_PURPLE}">\nGeneral Comments. \n</font>\n(')
    general += _mkhref(f"./{expname}.DTSUM", directory, "Brief data summary")
    general += " and "
    general += _mkhref(f"./{expname}.SCAN", directory, "scan listing")
    general += ") <br />\n"
    general += comments.get("GENERAL", "")
    general += "<br /> \n</p>  \n"
    parts.append(general)

    parts.append("\n<hr /> \n \n<p> \n")

    # --- Simple PDF sections. ---
    for name, text, text2 in _PDF_SECTIONS:
        parts.append(_pdf_section(expname, directory, name, text, text2, comments))

    # --- Per-source map sections. ---
    parts.append(_map_section(
        expname, directory, sources, "IMAPN",
        "Naturally weighted dirty map (not useful for bright sources) produced before "
        "self-cal of: <br />", ("pdf", "FITS"), comments, show_source_label=True))

    parts.append(_map_section(
        expname, directory, sources, "IMAPU",
        "Uniformly weighted dirty map (not useful for bright sources) produced before "
        "self-cal of: <br />", ("pdf", "FITS"), comments, show_source_label=True))

    parts.append(_map_section(
        expname, directory, sources, "CALIB_PHAS1",
        "Phase corrections applied to a priori calibrated and fringe-fitted data by "
        "self-calibration. <br />\n", ("pdf",), comments, show_source_label=False,
        always_comment=True))

    parts.append(_map_section(
        expname, directory, sources, "CALIB_AMP2",
        "Amplitude corrections applied to a priori calibrated and fringe-fitted data by "
        "self-calibration. <br />\n", ("pdf", "TXT", "ampcal"), comments,
        show_source_label=True, texts=("pdf", "text file", "statistical summary"),
        always_comment=True))

    parts.append(_pdf_section(
        expname, directory, "SENS", "Telescope sensitivities",
        " (the total AMP gain applied during both a priori and self calibration; the "
        "square of this number gives the antenna noise (SEFD) in Jy).\n", comments))

    parts.append(_map_section(
        expname, directory, sources, "CLPHS",
        "Residual closure phase (visibility closure phase with model closure phase "
        "subtracted) for: <br />", ("pdf",), comments, show_source_label=False))

    parts.append(_map_section(
        expname, directory, sources, "VPLOT_MODEL",
        "Calibrated visibilities and the source model of: <br />", ("pdf",), comments,
        show_source_label=False))

    parts.append(_map_section(
        expname, directory, sources, "UVPLT",
        "Calibrated visibilities against <em>u,v</em> distance for: <br />",
        ("pdf", "png"), comments, show_source_label=True))

    parts.append(_map_section(
        expname, directory, sources, "UVCOV",
        "<em>u,v</em> coverage for: <br />", ("pdf", "png"), comments,
        show_source_label=True))

    parts.append(_map_section(
        expname, directory, sources, "ICLN",
        "<em>Crude</em> maps of sources: <br />", ("pdf", "FITS"), comments,
        show_source_label=True))

    parts.append("</p>\n\n")
    return "".join(parts)


def generate_feedback_page(expname: str, sources: Iterable[str] | None = None,
                           nme: bool = False, contact: str | None = None,
                           directory: str | Path = ".") -> Path:
    """Generates the ``{expname}.html`` pipeline-feedback page.

    Port of ``feedback.pl``. Links every pipeline output product for the experiment,
    marks the missing ones as "(not available)", and embeds the comments from
    ``{expname}.comment``.

    Args:
        expname: Experiment name / file prefix (e.g. ``"n26k2"`` or ``"n26k2_1"`` for a
            given pipeline pass). Used verbatim to build the output product file names.
        sources: Source names to render in the per-source sections. If None, they are
            guessed from the ``{expname}_{SOURCE}_UVPLT.pdf`` files present in *directory*.
        nme: If True, format the page for a Network Monitoring Experiment; otherwise for a
            user experiment (only affects the title and heading).
        contact: Support scientist contact e-mail. A bare username (no ``@``) is completed
            with ``@jive.eu``. If None, no specific address requirement is enforced.
        directory: Directory containing the pipeline products and where the ``.html`` file
            is written. Defaults to the current directory.

    Returns:
        Path: The path to the written HTML file.
    """
    directory = Path(directory)

    if contact and "@" not in contact:
        contact = f"{contact}@jive.eu"

    # Resolve and normalise the source list (always upper-cased, as in the original).
    if sources is None:
        source_list = _guess_sources(expname, directory)
    else:
        source_list = list(sources)
    source_list = [s.upper() for s in source_list]

    comments = _read_comments(directory / f"{expname}.comment")

    title = ("European VLBI Network NME Feedback" if nme
             else "European VLBI Network Pipeline Feedback")
    heading = ("EVN Network Monitoring Experiment (NME) Pipeline Feedback" if nme
               else "EVN User Experiment Pipeline Feedback")
    # ``date``-like timestamp, kept on a single line as the original `date` output.
    last_updated = dt.datetime.now().strftime("%a %b %d %H:%M:%S %Y")

    body = _build_body(expname, directory, source_list, comments)

    template = resources.files("evn_postprocess.templates").joinpath(
        "feedback.html.template").read_text()
    replacements = {
        "{title}": title,
        "{heading}": heading,
        "{expname_upper}": expname.upper(),
        "{last_updated}": last_updated,
        "{contact}": contact or "",
        "{body}": body,
    }
    for placeholder, value in replacements.items():
        template = template.replace(placeholder, value)

    htmlfile = directory / f"{expname}.html"
    htmlfile.write_text(template)
    # Match the original `chmod 0755`.
    os.chmod(htmlfile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

    logger.info(f"Created pipeline feedback page: {htmlfile}")
    return htmlfile
