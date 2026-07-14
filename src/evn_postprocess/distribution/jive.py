"""The 'jive' distribution backend: the historical JIVE/EVN-archive delivery.

Provisional implementation delegating to the existing process/pipeline helpers (same
commands, same order as the pre-refactor archive step); Issue 14 completes the
extraction (PI letter from the toml [pi]/[comments], interactive PI-info prompt, and
the feedback-database upload). Imported only when the 'jive' backend is selected.
"""
from __future__ import annotations

import re

from loguru import logger
from rich import print as rprint

from . import Distributor


# Experiment-name prefixes for Network Monitoring Experiments (and fringe tests). These
# carry no PI and no protected sources, so the JIVE .jex protection lookup is skipped.
NME_PREFIXES = ('N', 'F')

COMMENTS_SENTINEL = "- Notes from the post-processing review:"
STATUS_LABELS = {'minor': ' (minor issues)', 'major': ' (could not observe)', 'success': ''}
# The reduced-bandwidth sentence is dropped from the per-station notes: it is already
# written once, for all affected antennas, under 'Further remarks:' by
# process.update_piletter. Matches the wording produced in review.default_station_comments.
_BANDWIDTH_NOTE_RE = re.compile(r"\s*Observed with reduced bandwidth\s*\([^)]*\)\.?", re.IGNORECASE)


def _station_note(entry) -> str:
    """The per-station note to append to the antenna line in the PI letter.

    Strips the reduced-bandwidth sentence (already covered under 'Further remarks:')
    and appends the status label. Returns '' when nothing meaningful remains.

    Args:
        entry: a StationComment (status + free-text note).

    Returns:
        The note text to append after the antenna name, or '' to skip the station.
    """
    note = _BANDWIDTH_NOTE_RE.sub('', entry.note).strip()
    if not note:
        return ''
    return f"{note}{STATUS_LABELS.get(entry.status, '')}"


class JiveDistributor(Distributor):
    """The EVN-archive delivery as performed at JIVE (default backend)."""
    name = 'jive'

    def deliver(self, exp) -> bool:
        """Recover source protection, PI-info check, comments into the letter, then deliver.

        First the PI/co-I contacts and the per-source archive protection are recovered from
        the JIVE .jex file (skipped for NME runs); this precedes every stage so the
        auth_pipe.py 'protect' stage acts on the right sources. When the .jex cannot be
        recovered the stages still run, but a manual-protection error is printed at the end
        and this returns False (the protected sources are then unknown and must be set by
        hand), so the distribute step is flagged as failed for the operator to resolve.

        The stages run strictly in order and STOP at the first failure (naming the
        failed stage): in particular nothing is archived when the credentials or the
        file protection failed, so no data can be published unprotected. The
        historical implementation evaluated the whole chain eagerly (``&``); this is
        a deliberate behaviour fix from the code review.

        Raises:
            DistributionError: In batch mode when PI name/email are missing.
        """
        from .. import pipeline, process
        # Recover the PI/co-I contacts and per-source archive protection from the JIVE
        # .jex file BEFORE the auth_pipe.py 'protect' stage below, so that stage protects
        # exactly the sources the PI scheduled as protected. False means the .jex could
        # not be recovered: the stages still run, but a manual-protection error is printed
        # and deliver() returns False at the very end (see _warn_manual_protection).
        protection_resolved = self._apply_source_protection(exp)
        self._ensure_pi_info(exp)
        self._apply_comments_to_letter(exp)
        stages = [('credentials', process.set_credentials),
                  ('protect', process.protect_experiment_files),
                  ('summary', lambda e: process.print_exp(e, display_in_terminal=False)),
                  ('archive-data', process.archive),
                  ('archive-pipeline', pipeline.archive),
                  ('pi-letter', process.send_letters),
                  ('station-feedback', process.antenna_feedback),
                  ('nme-report', process.nme_report),
                  ('feedback-upload', self.upload_feedback)]
        for stage_name, stage in stages:
            if not stage(exp):
                logger.error(f"Distribution stage '{stage_name}' failed for {exp.expname}; "
                             "stopping the delivery (later stages not attempted).")
                return False
        if not protection_resolved:
            # All stages ran, but the .jex was never recovered so the sources were NOT
            # protected. Fail the step (return False) so the operator must act; the banner
            # explains what to do by hand.
            self._warn_manual_protection(exp)
            return False
        return True

    @staticmethod
    def _exp_toml(exp):
        """Returns the experiment toml attached to *exp*, loading it if needed."""
        from .. import experiment_state
        return experiment_state.attached_toml(exp)

    def _apply_source_protection(self, exp) -> bool:
        """Reads the JIVE .jex file and sets PI/co-I contacts and per-source protection.

        Runs at the start of the delivery (before the auth_pipe.py 'protect' stage) so
        that process.protect_experiment_files acts on exactly the sources the PI scheduled
        as protected. The .jex file is read remotely and discarded; only the extracted
        contacts and protection flags are stored on the Experiment and the toml.

        NME runs (experiment name starting with N/F, see NME_PREFIXES) need neither PI
        contact nor protection and are skipped. When the .jex cannot be recovered the
        protected sources are unknown, so this returns False; deliver() then prints an
        explicit manual-action error at the end and itself returns False (the stages have
        already run, but the operator must set the protection by hand).

        Returns:
            bool: True when protection was resolved (or is not needed for an NME); False
                when the .jex could not be recovered and manual protection is required.
        """
        if exp.expname[0].upper() in NME_PREFIXES:
            logger.info(f"{exp.expname} is an NME (name starts with N/F): no PI contact or "
                        "source protection needed; skipping the .jex lookup.")
            return True

        from ..retrieval import RetrievalError
        from ..retrieval import jive as jive_retrieval
        try:
            jexp_info = jive_retrieval.fetch_jexp_info(exp.expname)
        except RetrievalError as e:
            logger.warning(f"Could not recover the .jex file for {exp.expname}: {e}")
            return False

        self._apply_contacts(exp, jexp_info)
        self._apply_source_flags(exp, jexp_info)
        exp.store()
        return True

    def _apply_contacts(self, exp, jexp_info: dict) -> None:
        """Adds the PI and (optional) co-I contacts from the .jex file to exp.pi + toml.

        A contact already on exp.pi (same name and email) is not re-added; the toml write
        is deduped by email inside experiment_state.record_pi.
        """
        from .. import experiment as _experiment
        contacts: list[tuple[str, str]] = []
        if jexp_info.get('piname') and jexp_info.get('pimail'):
            contacts.append((jexp_info['piname'], jexp_info['pimail']))
        else:
            logger.warning(f"The .jex file for {exp.expname} has no PI name/email; the PI "
                           "contact will come from the toml or the operator prompt.")
        if jexp_info.get('coname') and jexp_info.get('coimail'):
            contacts.append((jexp_info['coname'], jexp_info['coimail']))
        for name, email in contacts:
            if not any(pi.name == name and pi.email == email for pi in exp.pi):
                exp.pi.append(_experiment.PI(name, email))
        if contacts:
            exp_toml = self._exp_toml(exp)
            exp_toml.record_pi([{'name': name, 'email': email} for name, email in contacts])
            exp_toml.save()
            logger.info(f"Recovered {len(contacts)} contact(s) from the .jex file for "
                        f"{exp.expname}: {', '.join(name for name, _ in contacts)}.")

    def _apply_source_flags(self, exp, jexp_info: dict) -> None:
        """Sets per-source type and archive protection from the .jex ``schedsrc`` field.

        ``schedsrc`` is a comma-separated list of ``(name|type|protected)`` entries: type
        is T (target), R (reference/calibrator) or C/F (fringe-finder); protected is 'X'
        when the source data must be password-protected in the EVN archive. Sources
        already known from the vex/MS are updated in place; any not yet present are added
        with placeholder coordinates so they are still protected downstream.
        """
        from astropy import coordinates as coord
        from astropy import units as u
        from .. import experiment as _experiment
        schedsrc = jexp_info.get('schedsrc')
        if not schedsrc:
            logger.warning(f"The .jex file for {exp.expname} has no scheduled-source list "
                           "(schedsrc); no source protection could be set from it.")
            return
        type_map = {'T': _experiment.SourceType.target, 'R': _experiment.SourceType.calibrator,
                    'C': _experiment.SourceType.fringefinder, 'F': _experiment.SourceType.fringefinder}
        protected_names: list[str] = []
        for token in schedsrc.split(','):
            token = token.strip()
            if not token:
                continue
            # Split on '|' (not on whitespace) so an unprotected entry serialized with an
            # empty protection field -- '(NAME|T|)' -- still yields the name and type.
            fields = [field.strip() for field in token.strip('()').split('|')]
            if len(fields) < 2 or not fields[0]:
                logger.warning(f"Skipping malformed schedsrc entry '{token}' in the .jex "
                               f"file for {exp.expname} (expected (name|type|protected)).")
                continue
            src_name, src_type_str = fields[0], fields[1]
            src_protected = fields[2] if len(fields) >= 3 else ''
            src_type = type_map.get(src_type_str.upper(), _experiment.SourceType.other)
            is_protected = src_protected.upper() == 'X'
            if src_name in exp.sources.names:
                exp.sources[src_name].type = src_type
                exp.sources[src_name].protected = is_protected
            else:
                placeholder = coord.SkyCoord(ra=0 * u.deg, dec=0 * u.deg, frame='icrs')
                exp.sources.append(_experiment.Source(name=src_name, coordinates=placeholder,
                                                      type=src_type, protected=is_protected))
            if is_protected:
                protected_names.append(src_name)
        if protected_names:
            logger.info(f"Sources to protect for {exp.expname} (from .jex): "
                        f"{', '.join(protected_names)}.")
        else:
            logger.info(f"The .jex file for {exp.expname} marks no source as protected.")

    def _warn_manual_protection(self, exp) -> None:
        """Prints the end-of-delivery manual-protection error when the .jex was not found.

        Without the .jex the sources that must be protected are unknown, so the
        auth_pipe.py stage protected nothing. The operator has to set the protection by
        hand; the exact commands (with this experiment's archive name) are printed to make
        that straightforward.
        """
        archive_exp = f"{exp.expname.upper()}_{exp.obsdate.strftime('%y%m%d')}"
        logger.error(f"Source protection for {exp.expname} could NOT be determined: the .jex "
                     "file was not recovered. Check and protect the sources manually.")
        rprint("\n[bold red]" + "=" * 74 + "[/bold red]")
        rprint(f"[bold red]ACTION REQUIRED — {exp.expname}: the .jex file could not be "
               "recovered.[/bold red]")
        rprint("[bold red]The sources that must be protected are UNKNOWN and NOTHING was "
               "protected[/bold red]")
        rprint("[bold red]in the EVN archive. Check which sources need protection and set it "
               "by hand:[/bold red]")
        rprint(f"[red]    auth_pipe.py -e {archive_exp} -s '<SRC1 SRC2 ...>' -p source[/red]")
        rprint(f"[red]    auth_pipe.py -e {archive_exp} -s '<SRC1 SRC2 ...>' -p pipe[/red]")
        rprint("[bold red]" + "=" * 74 + "[/bold red]\n")

    def _ensure_pi_info(self, exp) -> None:
        """Guarantees PI name/email are known before the letter is prepared.

        Sources, in order: the experiment (exp.pi), the toml [[pi]] entries, and
        finally an interactive prompt whose answers are persisted to both. In batch
        mode a missing contact raises DistributionError naming the fields
        (PRD story 25).

        Raises:
            DistributionError: In batch mode, when no complete PI contact exists.
        """
        from . import DistributionError
        from .. import experiment as _experiment
        from .. import workflow  # function-level: workflow imports this sub-package
        exp_toml = self._exp_toml(exp)
        if not exp.pi:
            for entry in exp_toml.pis:
                if entry.name and entry.email:
                    exp.pi.append(_experiment.PI(entry.name, entry.email))
        if any(pi.name and pi.email for pi in exp.pi):
            return
        if workflow.is_batch_mode():
            raise DistributionError(
                f"No PI contact information for {exp.expname}: the PI letter cannot be "
                "prepared. Add name and email in [[pi]] entries of the experiment toml.")
        logger.warning(f"No PI contact information for {exp.expname}; asking the operator.")
        try:
            name = input("PI name: ").strip()
            email = input("PI email: ").strip()
            while not email:
                email = input("PI email (required): ").strip()
        except EOFError as e:  # no interactive stdin after all (cron without --batch)
            raise DistributionError(
                f"No PI contact information for {exp.expname} and no interactive terminal "
                "to ask for it. Add name and email in [[pi]] entries of the experiment "
                "toml (or run with --batch for the clean batch behaviour).") from e
        exp.pi.append(_experiment.PI(name, email))
        exp.store()
        exp_toml.record_pi([{'name': name, 'email': email}])
        exp_toml.save()

    def _apply_comments_to_letter(self, exp) -> bool:
        """Injects the review [comments] into the PI letter.

        Per-station notes are appended to the matching antenna line under the
        'Remarks on individual stations' section (the antenna name, capitalised, is
        matched the same way as process.update_piletter's "Could not observe." fill),
        rather than being repeated as a separate list. The reduced-bandwidth sentence
        is stripped from each note because it is already written once, for all affected
        antennas, under 'Further remarks:'. The general experiment note is inserted
        after the 'Further remarks:' anchor, guarded by a sentinel line.

        Idempotent: the general note is guarded by the sentinel; each per-station note
        is only appended when it is not already present on the antenna line. A missing
        letter logs a warning and returns False without blocking the delivery (the
        operator reviews the letter before sending anyway).
        """
        from pathlib import Path
        from ..utils import PILETTER_REMARKS_ANCHOR
        exp_toml = self._exp_toml(exp)
        comments = exp_toml.comments
        station_notes = {name: _station_note(entry) for name, entry in comments.stations.items()}
        station_notes = {name: note for name, note in station_notes.items() if note}
        if not comments.general and not station_notes:
            return True  # nothing to add
        letter = Path(f"{exp.expname.lower()}.piletter")
        if not letter.exists():
            logger.warning(f"No PI letter ({letter}) to add the review comments to.")
            return False

        lines = letter.read_text(encoding='utf-8').splitlines(keepends=True)
        unplaced = []
        for name, note in sorted(station_notes.items()):
            label = f"{name.capitalize()}:"
            for i, line in enumerate(lines):
                if label in line:
                    if note not in line:
                        stripped = line.rstrip('\n')
                        newline = line[len(stripped):]
                        lines[i] = f"{stripped} {note}{newline}"
                    break
            else:
                unplaced.append(name)
        if unplaced:
            logger.warning(f"No individual-station line for {', '.join(sorted(unplaced))} in "
                           f"{letter}; their review notes were not added.")

        text = ''.join(lines)
        if comments.general and COMMENTS_SENTINEL not in text:
            if PILETTER_REMARKS_ANCHOR not in text:
                logger.warning(f"No '{PILETTER_REMARKS_ANCHOR}' anchor in {letter}; the general "
                               "review note was not inserted (add it manually if needed).")
            else:
                anchor_end = text.index(PILETTER_REMARKS_ANCHOR) + len(PILETTER_REMARKS_ANCHOR)
                # find() (not index()): the anchor may be the very last line without a newline.
                eol = text.find('\n', anchor_end)
                eol = len(text) if eol == -1 else eol + 1
                block = f"\n{COMMENTS_SENTINEL}\n    {comments.general}\n"
                text = text[:eol] + block + text[eol:]

        letter.write_text(text, encoding='utf-8')
        logger.info(f"Review comments inserted into {letter}.")
        return True

    def upload_feedback(self, exp) -> bool:
        """Uploads the experiment results to the EVN feedback database (Grafana-visible).

        Defined stub (PRD story 41): the schema and endpoint are not decided yet; the
        [comments] and [postprocess] sections of the experiment toml are the intended
        payload. Returns True so callers can already invoke it unconditionally.
        """
        logger.debug(f"upload_feedback({exp.expname}): not implemented yet (stub).")
        return True
