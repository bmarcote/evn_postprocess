"""The 'jive' distribution backend: the historical JIVE/EVN-archive delivery.

Provisional implementation delegating to the existing process/pipeline helpers (same
commands, same order as the pre-refactor archive step); Issue 14 completes the
extraction (PI letter from the toml [pi]/[comments], interactive PI-info prompt, and
the feedback-database upload). Imported only when the 'jive' backend is selected.
"""
from __future__ import annotations

from loguru import logger

from . import Distributor


COMMENTS_SENTINEL = "- Notes from the post-processing review:"
STATUS_LABELS = {'minor': ' (minor issues)', 'major': ' (could not observe)', 'success': ''}


class JiveDistributor(Distributor):
    """The EVN-archive delivery as performed at JIVE (default backend)."""
    name = 'jive'

    def deliver(self, exp) -> bool:
        """PI-info check, review comments into the letter, then the delivery stages.

        The stages run strictly in order and STOP at the first failure (naming the
        failed stage): in particular nothing is archived when the credentials or the
        file protection failed, so no data can be published unprotected. The
        historical implementation evaluated the whole chain eagerly (``&``); this is
        a deliberate behaviour fix from the code review.

        Raises:
            DistributionError: In batch mode when PI name/email are missing.
        """
        from .. import pipeline, process
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
        return True

    @staticmethod
    def _exp_toml(exp):
        """Returns the experiment toml attached to *exp*, loading it if needed."""
        from .. import experiment_state
        return experiment_state.attached_toml(exp)

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
        """Injects the review [comments] into the PI letter, after 'Further remarks:'.

        Adds the general experiment note and the per-station notes (stations with a
        note or a non-success status). Idempotent: a sentinel line guards against
        double insertion on re-runs. A missing letter or anchor logs a warning and
        returns False without blocking the delivery (the operator reviews the letter
        before sending anyway).
        """
        from pathlib import Path
        from ..utils import PILETTER_REMARKS_ANCHOR
        exp_toml = self._exp_toml(exp)
        comments = exp_toml.comments
        station_lines = [f"    {name}: {entry.note}{STATUS_LABELS.get(entry.status, '')}"
                         for name, entry in sorted(comments.stations.items())
                         if entry.note or entry.status != 'success']
        if not comments.general and not station_lines:
            return True  # nothing to add
        letter = Path(f"{exp.expname.lower()}.piletter")
        if not letter.exists():
            logger.warning(f"No PI letter ({letter}) to add the review comments to.")
            return False
        text = letter.read_text(encoding='utf-8')
        if COMMENTS_SENTINEL in text:
            logger.debug("Review comments already present in the PI letter; not re-inserting.")
            return True
        if PILETTER_REMARKS_ANCHOR not in text:
            logger.warning(f"No '{PILETTER_REMARKS_ANCHOR}' anchor in {letter}; review "
                           "comments not inserted (add them manually if needed).")
            return False
        block = [COMMENTS_SENTINEL]
        if comments.general:
            block.append(f"    {comments.general}")
        block.extend(station_lines)
        anchor_end = text.index(PILETTER_REMARKS_ANCHOR) + len(PILETTER_REMARKS_ANCHOR)
        # find() (not index()): the anchor may be the very last line without a newline.
        eol = text.find('\n', anchor_end)
        eol = len(text) if eol == -1 else eol + 1
        letter.write_text(text[:eol] + '\n' + '\n'.join(block) + '\n' + text[eol:],
                          encoding='utf-8')
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
