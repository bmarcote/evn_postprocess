"""The 'jive' distribution backend: the historical JIVE/EVN-archive delivery.

Provisional implementation delegating to the existing process/pipeline helpers (same
commands, same order as the pre-refactor archive step); Issue 14 completes the
extraction (interactive PI-info prompt and the feedback-database upload). Imported only
when the 'jive' backend is selected.

The PI letter belongs to this backend and to no other: it is built and delivered by the
sibling :mod:`evn_postprocess.distribution.piletter` module, through
:meth:`JiveDistributor.prepare_letter` (also the `piletter` workflow step) and
:meth:`JiveDistributor.send_letter` (the 'pi-letter' delivery stage).
"""
from __future__ import annotations

import re

from loguru import logger
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from astropy import coordinates as coord
from astropy import units as u

from . import DistributionError, Distributor, piletter
from .. import experiment, experiment_state, pipeline, process, utils
from .. import workflow  # cycle with workflow importing this sub-package: used at call time only
from ..retrieval import RetrievalError, jive as jive_retrieval


# One .jex schedsrc entry: 'NAME (TYPE|FLAG)'. TYPE is T (target), R (reference/
# calibrator) or C/F (fringe-finder). FLAG is 'X' (password-protect in the EVN archive)
# or 'P' (public), with an optional trailing '?' when the setting is a guess.
_SCHEDSRC_ENTRY_RE = re.compile(r'^(?P<name>\S+)\s*\((?P<type>\w)\|(?P<flag>\w?)(?P<guess>\?)?\)$')


class JiveDistributor(Distributor):
    """The EVN-archive delivery as performed at JIVE (default backend)."""
    name = 'jive'
    sends_letter = True

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
        # Recover the PI/co-I contacts and per-source archive protection from the JIVE
        # .jex file BEFORE the auth_pipe.py 'protect' stage below, so that stage protects
        # exactly the sources the PI scheduled as protected. False means the .jex could
        # not be recovered: the stages still run, but a manual-protection error is printed
        # and deliver() returns False at the very end (see _warn_manual_protection).
        protection_resolved = self._apply_source_protection(exp)
        self._ensure_pi_info(exp)
        # Rebuild the letter here, before any stage: it must carry the comments the support
        # scientist wrote in the dashboard during the review pause, and the contacts just
        # recovered from the .jex file.
        self.prepare_letter(exp)
        stages = [('credentials', process.set_credentials),
                  ('protect', process.protect_experiment_files),
                  ('summary', lambda e: process.print_exp(e, display_in_terminal=False)),
                  ('archive-data', process.archive),
                  ('archive-pipeline', pipeline.archive),
                  ('pi-letter', self.send_letter),
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
        return experiment_state.attached_toml(exp)

    def _apply_source_protection(self, exp) -> bool:
        """Reads the JIVE .jex file and sets PI/co-I contacts and per-source protection.

        Runs at the start of the delivery (before the auth_pipe.py 'protect' stage) so
        that process.protect_experiment_files acts on exactly the sources the PI scheduled
        as protected. The .jex file is read remotely and discarded; only the extracted
        contacts and protection flags are stored on the Experiment and the toml.

        NME runs (see :func:`experiment.is_nme`) need neither PI contact nor protection
        and are skipped. When the .jex cannot be recovered the
        protected sources are unknown, so this returns False; deliver() then prints an
        explicit manual-action error at the end and itself returns False (the stages have
        already run, but the operator must set the protection by hand).

        Returns:
            bool: True when protection was resolved (or is not needed for an NME); False
                when the .jex could not be recovered and manual protection is required.
        """
        if experiment.is_nme(exp.expname):
            logger.info(f"{exp.expname} is an NME: no PI contact or source protection "
                        "needed; skipping the .jex lookup.")
            return True

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
                exp.pi.append(experiment.PI(name, email))
        if contacts:
            exp_toml = self._exp_toml(exp)
            exp_toml.record_pi([{'name': name, 'email': email} for name, email in contacts])
            exp_toml.save()
            logger.info(f"Recovered {len(contacts)} contact(s) from the .jex file for "
                        f"{exp.expname}: {', '.join(name for name, _ in contacts)}.")

    def _apply_source_flags(self, exp, jexp_info: dict) -> None:
        """Sets per-source type and archive protection from the .jex ``schedsrc`` field.

        ``schedsrc`` is a comma-separated list of ``NAME (TYPE|FLAG)`` entries (see
        _SCHEDSRC_ENTRY_RE): TYPE is T (target), R (reference/calibrator) or C/F
        (fringe-finder); FLAG is 'X' when the source data must be password-protected in
        the EVN archive and 'P' when public, with a trailing '?' when the .jex records
        the protection as a guess (applied as-is, with a warning to verify it). Sources
        already known from the vex/MS are updated in place; any not yet present are added
        with placeholder coordinates so they are still protected downstream.
        """
        schedsrc = jexp_info.get('schedsrc')
        if not schedsrc:
            logger.warning(f"The .jex file for {exp.expname} has no scheduled-source list "
                           "(schedsrc); no source protection could be set from it.")
            return
        type_map = {'T': experiment.SourceType.target, 'R': experiment.SourceType.calibrator,
                    'C': experiment.SourceType.fringefinder, 'F': experiment.SourceType.fringefinder}
        protected_names: list[str] = []
        for token in schedsrc.split(','):
            token = token.strip()
            if not token:
                continue
            match = _SCHEDSRC_ENTRY_RE.match(token)
            if match is None:
                logger.warning(f"Skipping malformed schedsrc entry '{token}' in the .jex "
                               f"file for {exp.expname} (expected 'NAME (TYPE|FLAG)').")
                continue
            src_name = match.group('name')
            src_type = type_map.get(match.group('type').upper(), experiment.SourceType.other)
            is_protected = match.group('flag').upper() == 'X'
            if match.group('guess') is not None:
                logger.warning(f"The .jex protection flag for {src_name} is a guess "
                               f"('{match.group('flag')}?'); verify the archive protection "
                               f"of {exp.expname} manually.")
            if src_name in exp.sources.names:
                exp.sources[src_name].type = src_type
                exp.sources[src_name].protected = is_protected
            else:
                placeholder = coord.SkyCoord(ra=0 * u.deg, dec=0 * u.deg, frame='icrs')
                exp.sources.append(experiment.Source(name=src_name, coordinates=placeholder,
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
        exp_toml = self._exp_toml(exp)
        if not exp.pi:
            for entry in exp_toml.pis:
                if entry.name and entry.email:
                    exp.pi.append(experiment.PI(entry.name, entry.email))
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
        exp.pi.append(experiment.PI(name, email))
        exp.store()
        exp_toml.record_pi([{'name': name, 'email': email}])
        exp_toml.save()

    def prepare_letter(self, exp) -> bool:
        """Generates the PI letter from the template with everything known so far.

        Called twice: by the `piletter` workflow step at the end of the pipeline, so the
        operator can review it during the review pause, and again at the start of the
        delivery, so the letter that is archived and sent carries the comments written in
        the dashboard Comments tab meanwhile. The letter is always built from the template
        (see :mod:`evn_postprocess.distribution.piletter`); nothing is patched into an
        existing file, and the previous version is kept as a `.bak`.

        Args:
            exp: Experiment object.

        Returns:
            True when the letter was written; False (with a warning) when it could not be,
            which never blocks the delivery on its own.
        """
        try:
            piletter.write_letter(exp)
        except Exception as e:
            logger.warning(f"Could not write the PI letter of {exp.expname}: {e}")
            return False
        return True

    def send_letter(self, exp) -> bool:
        """The 'pi-letter' delivery stage: archive the letter and hand it to the operator.

        The letter is written again first (the archive credentials exist by now, so the
        version sent to the PI carries them), the plain-text copy is archived with the data,
        and the letter is posted to the operator's chat: the Markdown body to copy into an
        email, plus the `.eml` draft and the `.html` version attached. Sending the mail
        itself stays manual on purpose -- this program holds no mail credentials.

        Args:
            exp: Experiment object.

        Returns:
            True unless the letter could not be written.
        """
        if not self.prepare_letter(exp):
            return False
        paths = piletter.letter_paths(exp)
        to_send = paths['auth'] if paths['auth'].exists() else paths['text']

        utils.shell_command("archive.pl",
                            ["-stnd", "-e", f"{exp.expname}_{exp.obsdate.strftime('%y%m%d')}",
                             str(paths['text'])])
        piletter.notify_letter_ready(exp, workflow.notifier())

        body = f"[bold]Send[/bold] [bold green]{to_send}[/bold green] [bold]to [/bold]" \
               f"[bold cyan]{', '.join(p.name for p in exp.pi)}[/bold cyan]: " \
               f"[bold]{', '.join(p.email for p in exp.pi)}[/bold]" \
               f"\nAnd CC [cyan]jops@jive.eu[/cyan]\n\n" \
               f"[bold]Formatted versions:[/bold] [green]{paths['eml']}[/green] " \
               f"(open it locally: it becomes a draft with recipients, subject and " \
               f"formatting) and [green]{paths['html']}[/green] (open in a browser and " \
               f"copy-paste)."
        Console().print(Panel(body, title="[bold yellow]Send the PI Letter[/bold yellow]",
                              border_style="yellow", padding=(1, 2)))
        return True

    def upload_feedback(self, exp) -> bool:
        """Uploads the experiment results to the EVN feedback database (Grafana-visible).

        Defined stub (PRD story 41): the schema and endpoint are not decided yet; the
        [comments] and [postprocess] sections of the experiment toml are the intended
        payload. Returns True so callers can already invoke it unconditionally.
        """
        logger.debug(f"upload_feedback({exp.expname}): not implemented yet (stub).")
        return True
