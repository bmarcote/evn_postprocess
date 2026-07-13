"""Experiment TOML state: load, resolve, and write back ``EXPNAME.toml``.

This module owns the experiment TOML file described in ``docs/experiment.toml.example``.
The file complements the .vex/.lis inputs with information they cannot express (source
types, PI contacts, scan selection, backend choices) and records every decision taken
during post-processing so a re-run is reproducible and silent.

Section ownership (see the PRD "Experiment toml layout"):
  - USER_SECTIONS are written by the user or the retrieval module and are never
    modified by this module, with one exception: :meth:`ExperimentToml.record_sources`
    may add heuristic source classifications (marked ``guessed = true``).
  - PROGRAM_SECTIONS are written by the program via the ``record_*`` methods.

All reads go through plain-Python copies of the values; the raw :mod:`tomlkit`
document is kept on the loaded object so write-backs preserve user formatting and
comments byte-for-byte in untouched sections. Saving is atomic (temp file + rename).

Missing files and missing sections are NOT errors (the PRD "warn, never block" rule):
loading a non-existent path returns an empty, valid container. Malformed TOML or
wrong-typed values raise :class:`ExperimentTomlError` naming file, section, and key.
"""
from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import tomlkit
from tomlkit.exceptions import TOMLKitError

# Sections owned by the user (or the retrieval module). Never rewritten by the
# program, except for heuristic source classifications recorded into 'sources'.
USER_SECTIONS = ('observation', 'pi', 'sources', 'retrieval', 'pipeline', 'distribution')
# Sections owned by the program (filled via the record_* methods).
PROGRAM_SECTIONS = ('postprocess', 'comments')

SOURCE_TYPES = ('target', 'calibrator', 'fringefinder', 'other')
STATION_STATUSES = ('success', 'minor', 'major')

# Keys accepted by record_parameters, i.e. the full [postprocess] vocabulary.
POSTPROCESS_KEYS = ('weight_threshold', 'flagged_percent', 'polswap', 'polconvert',
                    'onebit', 'refant', 'antab_files', 'polconvert_input_files',
                    'gain_corrections')

# Defaults applied by resolve_parameters when neither toml nor policy define a backend.
DEFAULT_RETRIEVAL = 'jive'
DEFAULT_PIPELINE = 'aips'
DEFAULT_DISTRIBUTION = 'jive'

_GENERATED_HEADER = ("Experiment file for evn_postprocess. "
                     "See docs/experiment.toml.example for the full schema.")


class ExperimentTomlError(ValueError):
    """Raised when an experiment TOML file is malformed or has wrong-typed values.

    The message always names the file and, when applicable, the section and key.
    Subclasses ValueError (bad input data), unlike the backend errors
    (Retrieval/Pipeline/DistributionError) which subclass RuntimeError
    (operational failures): callers should not catch these by a common base.
    """


def _unwrap(value: Any) -> Any:
    """Returns a plain-Python copy of a tomlkit value (dicts/lists/scalars)."""
    if hasattr(value, 'unwrap'):
        return value.unwrap()
    if isinstance(value, dict):
        return {k: _unwrap(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unwrap(v) for v in value]
    return value


def _expect(value: Any, types: type | tuple, filename: str, section: str, key: str, desc: str) -> Any:
    """Validates the type of a toml value, raising ExperimentTomlError naming file/section/key."""
    if not isinstance(value, types):
        raise ExperimentTomlError(f"{filename}: [{section}] key '{key}' must be {desc}, "
                                  f"got {type(value).__name__} ({value!r}).")
    return value


def _expect_str_list(value: Any, filename: str, section: str, key: str) -> list[str]:
    """Validates that a toml value is a list of strings."""
    _expect(value, list, filename, section, key, 'a list of strings')
    for entry in value:
        _expect(entry, str, filename, section, key, 'a list of strings')
    return list(value)


def parse_scan_selection(value: Any, filename: str = '<memory>', key: str = 'scans') -> list[int] | None:
    """Expands a scan selection into an explicit, sorted, deduplicated list of scan numbers.

    Accepted forms: an int (single scan), a string with individual scans and/or inclusive
    ranges separated by commas (``"4"``, ``"3-10"``, ``"1-5,20-30,45"``), or a TOML list
    mixing ints and such strings. ``None`` means "all scans" and is returned unchanged.

    Raises:
        ExperimentTomlError: On non-numeric tokens, reversed ranges (``"10-3"``), or
            non-positive scan numbers; the message quotes the offending token.
    """
    if value is None:
        return None
    scans: set[int] = set()
    entries = value if isinstance(value, list) else [value]
    for entry in entries:
        if isinstance(entry, bool) or not isinstance(entry, (int, str)):
            raise ExperimentTomlError(f"{filename}: key '{key}': scan entries must be integers or "
                                      f"strings, got {type(entry).__name__} ({entry!r}).")
        tokens = [t.strip() for t in entry.split(',')] if isinstance(entry, str) else [entry]
        for token in tokens:
            if isinstance(token, int):
                low = high = token
            elif token.isdigit():
                low = high = int(token)
            elif token.count('-') == 1 and all(p.strip().isdigit() for p in token.split('-')):
                low, high = (int(p.strip()) for p in token.split('-'))
            else:
                raise ExperimentTomlError(f"{filename}: key '{key}': cannot parse scan token "
                                          f"'{token}' (expected e.g. '4' or '3-10').")
            if low <= 0 or high <= 0:
                raise ExperimentTomlError(f"{filename}: key '{key}': scan numbers must be positive "
                                          f"in token '{token}'.")
            if low > high:
                raise ExperimentTomlError(f"{filename}: key '{key}': reversed range in token "
                                          f"'{token}' ({low} > {high}).")
            scans.update(range(low, high + 1))
    return sorted(scans)


@dataclass
class ObservationSection:
    """[observation]: experiment name, support scientist, and the scans to process.

    ``scans`` is the resolved explicit list of scan numbers (None means all scans).
    """
    expname: str | None = None
    supsci: str | None = None
    scans: list[int] | None = None


@dataclass
class PIEntry:
    """One [[pi]] entry: a PI/contact person for the experiment."""
    name: str = ''
    email: str = ''


@dataclass
class SourceEntry:
    """One [sources.NAME] entry. ``guessed`` marks a program-made heuristic classification."""
    type: str | None = None
    protected: bool = False
    guessed: bool = False


@dataclass
class PostprocessSection:
    """[postprocess]: parameters chosen/derived during post-processing (program-written).

    ``None`` means "not set yet"; an explicit empty list means "none needed" and is
    meaningfully different from None for the precedence rule.
    """
    weight_threshold: float | None = None
    flagged_percent: float | None = None
    polswap: list[str] | None = None
    polconvert: list[str] | None = None
    onebit: list[str] | None = None
    refant: list[str] | None = None
    antab_files: list[str] | None = None
    polconvert_input_files: list[str] | None = None
    gain_corrections: dict[str, float] | None = None


@dataclass
class StationComment:
    """One [comments.stations.XX] entry: status ('success'|'minor'|'major') and free note."""
    status: str = 'success'
    note: str = ''


@dataclass
class CommentsSection:
    """[comments]: general experiment note plus per-station comments (program-written)."""
    general: str = ''
    stations: dict[str, StationComment] = field(default_factory=dict)


@dataclass
class ResolvedParameters:
    """Effective decision values after applying the precedence rule.

    Precedence per parameter: experiment toml value -> policy value -> None (meaning
    "ask in interactive mode / pause in batch"). The three backend modes fall back to
    their documented defaults instead of None. ``unresolved`` lists the parameter names
    that ended up None and therefore still need a human (or policy) answer.
    """
    weight_threshold: float | None = None
    polswap: list[str] | None = None
    polconvert: list[str] | None = None
    onebit: list[str] | None = None
    refant: list[str] | None = None
    scans: list[int] | None = None
    retrieval: str = DEFAULT_RETRIEVAL
    pipeline: str = DEFAULT_PIPELINE
    distribution: str = DEFAULT_DISTRIBUTION
    unresolved: list[str] = field(default_factory=list)


class ExperimentToml:
    """The loaded ``EXPNAME.toml``: parsed sections plus the raw tomlkit document.

    Build instances with :func:`load_toml`. Reading happens through the parsed section
    attributes; writing happens ONLY through the ``record_*`` methods followed by
    :meth:`save`, which preserve user formatting/comments and never touch user-owned
    sections (except heuristic additions in [sources]).
    """

    def __init__(self, path: Path | None, document: tomlkit.TOMLDocument):
        self.path = path
        self.document = document
        filename = str(path) if path is not None else '<memory>'
        data = _unwrap(document)
        self.observation = self._parse_observation(data, filename)
        self.pis = self._parse_pis(data, filename)
        self.sources = self._parse_sources(data, filename)
        self.retrieval = self._parse_mode(data, 'retrieval', filename)
        self.pipeline = self._parse_mode(data, 'pipeline', filename)
        self.distribution = self._parse_mode(data, 'distribution', filename)
        self.postprocess = self._parse_postprocess(data, filename)
        self.comments = self._parse_comments(data, filename)
        self.skip_steps = self._parse_skip_steps(data, filename)

    # ------------------------------------------------------------------ parsing

    @staticmethod
    def _parse_observation(data: dict, filename: str) -> ObservationSection:
        """Parses [observation], resolving the scan selection into explicit numbers."""
        section = data.get('observation', {})
        _expect(section, dict, filename, 'observation', '(section)', 'a table')
        obs = ObservationSection()
        if 'expname' in section:
            obs.expname = _expect(section['expname'], str, filename, 'observation', 'expname', 'a string')
        if 'supsci' in section:
            obs.supsci = _expect(section['supsci'], str, filename, 'observation', 'supsci', 'a string')
        if 'scans' in section:
            obs.scans = parse_scan_selection(section['scans'], filename, 'scans')
        return obs

    @staticmethod
    def _parse_pis(data: dict, filename: str) -> list[PIEntry]:
        """Parses the [[pi]] array of tables."""
        entries = data.get('pi', [])
        if isinstance(entries, dict):  # tolerate a single [pi] table
            entries = [entries]
        _expect(entries, list, filename, 'pi', '(section)', 'an array of tables ([[pi]])')
        pis = []
        for entry in entries:
            _expect(entry, dict, filename, 'pi', '(entry)', 'a table with name/email')
            name = _expect(entry.get('name', ''), str, filename, 'pi', 'name', 'a string')
            email = _expect(entry.get('email', ''), str, filename, 'pi', 'email', 'a string')
            pis.append(PIEntry(name=name, email=email))
        return pis

    @staticmethod
    def _parse_sources(data: dict, filename: str) -> dict[str, SourceEntry]:
        """Parses [sources.NAME] subtables into a name-keyed dict."""
        section = data.get('sources', {})
        _expect(section, dict, filename, 'sources', '(section)', 'a table of [sources.NAME] entries')
        sources: dict[str, SourceEntry] = {}
        for name, entry in section.items():
            _expect(entry, dict, filename, 'sources', name, 'a table (e.g. [sources."J1848+3244"])')
            src = SourceEntry()
            if 'type' in entry:
                src.type = _expect(entry['type'], str, filename, 'sources', f'{name}.type', 'a string')
                if src.type not in SOURCE_TYPES:
                    raise ExperimentTomlError(f"{filename}: [sources] key '{name}.type' must be one of "
                                              f"{SOURCE_TYPES}, got '{src.type}'.")
            src.protected = _expect(entry.get('protected', False), bool, filename, 'sources',
                                    f'{name}.protected', 'a boolean')
            src.guessed = _expect(entry.get('guessed', False), bool, filename, 'sources',
                                  f'{name}.guessed', 'a boolean')
            sources[name] = src
        return sources

    @staticmethod
    def _parse_mode(data: dict, section_name: str, filename: str) -> str | None:
        """Parses a backend-selection section ([retrieval]/[pipeline]/[distribution])."""
        section = data.get(section_name, {})
        _expect(section, dict, filename, section_name, '(section)', 'a table')
        if 'mode' in section:
            return _expect(section['mode'], str, filename, section_name, 'mode', 'a string')
        return None

    @staticmethod
    def _parse_postprocess(data: dict, filename: str) -> PostprocessSection:
        """Parses the program-written [postprocess] section."""
        section = data.get('postprocess', {})
        _expect(section, dict, filename, 'postprocess', '(section)', 'a table')
        post = PostprocessSection()
        for key in ('weight_threshold', 'flagged_percent'):
            if key in section:
                value = _expect(section[key], (int, float), filename, 'postprocess', key, 'a number')
                setattr(post, key, float(value))
        for key in ('polswap', 'polconvert', 'onebit', 'refant', 'antab_files', 'polconvert_input_files'):
            if key in section:
                setattr(post, key, _expect_str_list(section[key], filename, 'postprocess', key))
        if 'gain_corrections' in section:
            gains = _expect(section['gain_corrections'], dict, filename, 'postprocess',
                            'gain_corrections', 'a table of station = factor entries')
            post.gain_corrections = {}
            for station, factor in gains.items():
                _expect(factor, (int, float), filename, 'postprocess',
                        f'gain_corrections.{station}', 'a number')
                post.gain_corrections[station] = float(factor)
        return post

    @staticmethod
    def _parse_skip_steps(data: dict, filename: str) -> list[str]:
        """Parses the top-level ``skip_steps`` list (steps to bypass; used in sweeps mode)."""
        if 'skip_steps' not in data:
            return []
        return _expect_str_list(data['skip_steps'], filename, '(root)', 'skip_steps')

    @staticmethod
    def _parse_comments(data: dict, filename: str) -> CommentsSection:
        """Parses the program-written [comments] section."""
        section = data.get('comments', {})
        _expect(section, dict, filename, 'comments', '(section)', 'a table')
        comments = CommentsSection()
        if 'general' in section:
            comments.general = _expect(section['general'], str, filename, 'comments', 'general', 'a string')
        stations = section.get('stations', {})
        _expect(stations, dict, filename, 'comments', 'stations', 'a table of [comments.stations.XX]')
        for station, entry in stations.items():
            _expect(entry, dict, filename, 'comments', f'stations.{station}', 'a table')
            status = _expect(entry.get('status', 'success'), str, filename, 'comments',
                             f'stations.{station}.status', 'a string')
            if status not in STATION_STATUSES:
                raise ExperimentTomlError(f"{filename}: [comments] key 'stations.{station}.status' must "
                                          f"be one of {STATION_STATUSES}, got '{status}'.")
            note = _expect(entry.get('note', ''), str, filename, 'comments',
                           f'stations.{station}.note', 'a string')
            comments.stations[station] = StationComment(status=status, note=note)
        return comments

    # --------------------------------------------------------------- write-back

    def _section_table(self, name: str) -> tomlkit.items.Table:
        """Returns the tomlkit table for a section, creating it if absent."""
        if name not in self.document:
            self.document[name] = tomlkit.table()
        return self.document[name]

    def record_parameters(self, **values: Any) -> None:
        """Records post-processing decisions/results into [postprocess].

        Only the supplied non-None keyword arguments change (see POSTPROCESS_KEYS);
        previously recorded values are kept. The whole [postprocess] table is rebuilt
        from the merged state on each call — it is program-owned, and rebuilding avoids
        the tomlkit pitfall of new keys rendering after an existing sub-table header
        (which would re-parse into the wrong section). User sections are untouched.

        Raises:
            ExperimentTomlError: On a keyword that is not a valid [postprocess] key.
        """
        unknown = set(values) - set(POSTPROCESS_KEYS)
        if unknown:
            raise ExperimentTomlError(f"record_parameters: unknown [postprocess] keys {sorted(unknown)}; "
                                      f"valid keys are {POSTPROCESS_KEYS}.")
        merged = {key: getattr(self.postprocess, key) for key in POSTPROCESS_KEYS}
        merged.update({key: value for key, value in values.items() if value is not None})
        table = tomlkit.table()
        for key in POSTPROCESS_KEYS:
            if key == 'gain_corrections' or merged[key] is None:
                continue  # scalar/list keys first; the sub-table must come last
            table[key] = merged[key]
        if merged['gain_corrections'] is not None:
            gains = tomlkit.table()
            for station, factor in merged['gain_corrections'].items():
                gains[station] = float(factor)
            table['gain_corrections'] = gains
        self.document['postprocess'] = table
        self._refresh_parsed()

    def record_comments(self, general: str | None = None,
                        stations: dict[str, StationComment | dict] | None = None) -> None:
        """Records the experiment/station review comments into [comments].

        Existing entries for the same station are updated (never duplicated); stations
        not mentioned are preserved. ``general=None`` leaves the general note untouched.

        Raises:
            ExperimentTomlError: On a station status outside STATION_STATUSES.
        """
        merged_general = general if general is not None else self.comments.general
        merged_stations = dict(self.comments.stations)
        for station, comment in (stations or {}).items():
            if isinstance(comment, dict):
                comment = StationComment(status=comment.get('status', 'success'),
                                         note=comment.get('note', ''))
            if comment.status not in STATION_STATUSES:
                raise ExperimentTomlError(f"record_comments: status for station '{station}' must be "
                                          f"one of {STATION_STATUSES}, got '{comment.status}'.")
            merged_stations[station] = comment
        # [comments] is program-owned: rebuild it whole from the merged state (same
        # sub-table-ordering rationale as record_parameters; also guarantees a station
        # entry is updated in place and never duplicated).
        table = tomlkit.table()
        table['general'] = merged_general
        if merged_stations:
            stations_table = tomlkit.table()
            for station, comment in merged_stations.items():
                entry = tomlkit.table()
                entry['status'] = comment.status
                entry['note'] = comment.note
                stations_table[station] = entry
            table['stations'] = stations_table
        self.document['comments'] = table
        self._refresh_parsed()

    def record_pi(self, entries: list[PIEntry | dict]) -> None:
        """Records operator-supplied PI contacts into the [[pi]] array of tables.

        [pi] is user-owned; this is only meant for the distribution-time prompt
        (PRD story 25), persisting what the operator typed so the next run does not
        ask again. Entries whose email is already present are never duplicated or
        modified.
        """
        existing_emails = {pi.email for pi in self.pis}
        new_entries = []
        for entry in entries:
            if isinstance(entry, dict):
                entry = PIEntry(name=entry.get('name', ''), email=entry.get('email', ''))
            if entry.email and entry.email not in existing_emails:
                new_entries.append(entry)
                existing_emails.add(entry.email)  # dedupe within the same batch too
        if not new_entries:
            return
        if 'pi' not in self.document:
            self.document['pi'] = tomlkit.aot()
        for entry in new_entries:
            table = tomlkit.table()
            table['name'] = entry.name
            table['email'] = entry.email
            self.document['pi'].append(table)
        self._refresh_parsed()

    def record_sources(self, classifications: dict[str, str]) -> None:
        """Records heuristic source classifications into [sources], marked ``guessed = true``.

        This is the single allowed write into a user-owned section: a source whose type
        was explicitly set by the user (present and not marked guessed) is NEVER
        overridden. Previously guessed entries are updated with the new guess.

        Raises:
            ExperimentTomlError: On a source type outside SOURCE_TYPES.
        """
        table = self._section_table('sources')
        for name, srctype in classifications.items():
            if srctype not in SOURCE_TYPES:
                raise ExperimentTomlError(f"record_sources: type for source '{name}' must be one of "
                                          f"{SOURCE_TYPES}, got '{srctype}'.")
            existing = self.sources.get(name)
            if existing is not None and existing.type is not None and not existing.guessed:
                continue  # user-set: never overridden by a heuristic
            if name not in table:
                table[name] = tomlkit.table()
            table[name]['type'] = srctype
            table[name]['guessed'] = True
        self._refresh_parsed()

    def _refresh_parsed(self) -> None:
        """Re-parses the section attributes from the (mutated) tomlkit document."""
        self.__init__(self.path, self.document)  # noqa: PLC2801 -- deliberate re-init

    def save(self, path: Path | None = None) -> None:
        """Writes the document atomically (temp file in the same directory, then rename).

        Untouched sections keep their exact original bytes, comments included, because
        the underlying tomlkit document preserves formatting.

        Raises:
            ExperimentTomlError: If no target path is known (in-memory container and
                no ``path`` argument).
        """
        target = Path(path) if path is not None else self.path
        if target is None:
            raise ExperimentTomlError("save: no path known for this experiment toml; pass one explicitly.")
        # target.parent is '.' for bare filenames, so it is always a usable directory.
        fd, tmpname = tempfile.mkstemp(dir=str(target.parent), prefix=f".{target.name}.",
                                       suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                f.write(tomlkit.dumps(self.document))
            os.replace(tmpname, target)
        except BaseException:
            Path(tmpname).unlink(missing_ok=True)
            raise
        self.path = target


def load_toml(path: str | Path) -> ExperimentToml:
    """Loads an experiment toml file into an :class:`ExperimentToml` container.

    A missing file is not an error: an empty container bound to *path* is returned
    (the PRD "every section is optional" rule), so a later ``record_*`` + ``save``
    creates the file.

    Raises:
        ExperimentTomlError: On invalid TOML (naming the file) or wrong-typed values
            (naming file, section, and key).
    """
    path = Path(path)
    if not path.exists():
        document = tomlkit.document()
        document.add(tomlkit.comment(_GENERATED_HEADER))
        return ExperimentToml(path, document)
    try:
        document = tomlkit.parse(path.read_text(encoding='utf-8'))
    except TOMLKitError as e:
        raise ExperimentTomlError(f"{path}: invalid TOML: {e}") from e
    return ExperimentToml(path, document)


def toml_path_for(expname: str, directory: Path | None = None) -> Path:
    """Returns the conventional experiment toml path: ``{directory}/{expname.lower()}.toml``."""
    return (directory if directory is not None else Path('.')) / f"{expname.lower()}.toml"


def attached_toml(exp, fresh: bool = False) -> ExperimentToml:
    """Returns the ExperimentToml attached to *exp*, loading (and attaching) it if needed.

    The single implementation of the attach/reload logic used by the workflow steps,
    the dashboard handler, and the distribution backends.

    Args:
        exp: An Experiment (or anything with ``expname`` and an ``exp_toml`` slot).
        fresh: Reload from disk even when a toml is already attached. MUST be True
            before any write from a long-lived process: another process (e.g. the
            dashboard server, or the paused workflow) may have saved the file since
            it was first loaded, and saving a stale in-memory document would silently
            discard those edits (lost update). Reads are cheap; writes stay atomic.
    """
    current = getattr(exp, 'exp_toml', None)
    if current is not None and not fresh:
        return current
    path = current.path if (current is not None and current.path is not None) \
        else toml_path_for(exp.expname)
    exp.exp_toml = load_toml(path)
    return exp.exp_toml


def resolve_parameters(exp_toml: ExperimentToml | None, policy=None) -> ResolvedParameters:
    """Applies the precedence rule: experiment toml > policy.toml > None/defaults.

    For list parameters, an explicit empty list in the toml means "none needed" and
    wins over the policy; only an *absent* key falls through. The policy's list fields
    default to [] which historically means "unset", so an empty policy list also falls
    through to None. Backend modes fall back to their documented defaults. The returned
    ``unresolved`` names every parameter that is still None (supersedes
    ``Policy.requires_input`` for callers that have a toml).

    Args:
        exp_toml: The loaded experiment toml, or None if no toml exists.
        policy: An ``evn_postprocess.policy.Policy`` instance, or None.
    """
    resolved = ResolvedParameters()
    post = exp_toml.postprocess if exp_toml is not None else PostprocessSection()
    resolved.weight_threshold = post.weight_threshold if post.weight_threshold is not None \
        else (policy.weight_threshold if policy is not None else None)
    for key in ('polswap', 'polconvert', 'onebit', 'refant'):
        toml_value = getattr(post, key)
        if toml_value is not None:
            setattr(resolved, key, list(toml_value))
        elif policy is not None and getattr(policy, key):
            setattr(resolved, key, list(getattr(policy, key)))
    if exp_toml is not None:
        resolved.scans = exp_toml.observation.scans
        resolved.retrieval = exp_toml.retrieval or DEFAULT_RETRIEVAL
        resolved.pipeline = exp_toml.pipeline or DEFAULT_PIPELINE
        resolved.distribution = exp_toml.distribution or DEFAULT_DISTRIBUTION
    resolved.unresolved = [key for key in ('weight_threshold', 'polswap', 'polconvert',
                                           'onebit', 'refant')
                           if getattr(resolved, key) is None]
    return resolved


def summary_lines(exp_toml: ExperimentToml) -> list[str]:
    """Builds human-readable lines describing the toml-sourced values, for `postprocess info`.

    Each line states the value and that it originates from the experiment toml file, so
    the operator can tell them apart from vex/lis-derived metadata.
    """
    lines: list[str] = []
    src = f"(from {exp_toml.path.name if exp_toml.path else 'experiment toml'})"
    obs = exp_toml.observation
    if obs.expname:
        lines.append(f"Experiment: {obs.expname} {src}")
    if obs.supsci:
        lines.append(f"Support scientist: {obs.supsci} {src}")
    if obs.scans is not None:
        lines.append(f"Scans to process: {len(obs.scans)} selected ({obs.scans[0]}..{obs.scans[-1]}) {src}")
    for pi in exp_toml.pis:
        lines.append(f"PI: {pi.name} <{pi.email}> {src}")
    for name, entry in exp_toml.sources.items():
        guessed = ' [guessed]' if entry.guessed else ''
        protected = ' [protected]' if entry.protected else ''
        lines.append(f"Source {name}: {entry.type or 'unset'}{guessed}{protected} {src}")
    for label, mode in (('Retrieval', exp_toml.retrieval), ('Pipeline', exp_toml.pipeline),
                        ('Distribution', exp_toml.distribution)):
        if mode:
            lines.append(f"{label} mode: {mode} {src}")
    post = exp_toml.postprocess
    if post.weight_threshold is not None:
        lines.append(f"Weight threshold: {post.weight_threshold} {src}")
    for key in ('polswap', 'polconvert', 'onebit', 'refant'):
        value = getattr(post, key)
        if value is not None:
            lines.append(f"{key}: {', '.join(value) if value else '(none)'} {src}")
    if exp_toml.comments.general:
        lines.append(f"General note: {exp_toml.comments.general} {src}")
    for station, comment in exp_toml.comments.stations.items():
        lines.append(f"Station {station}: {comment.status}"
                     f"{' - ' + comment.note if comment.note else ''} {src}")
    return lines
