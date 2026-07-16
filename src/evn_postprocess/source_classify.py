"""Heuristic source classification: target / calibrator / fringe finder.

Used when the experiment toml does not declare (all) source types. The rules, in order
(see the PRD "Source classification heuristics" and Issue 3):

  1. NME / fringe-test experiments (name starting with 'N' or 'F'): all sources are
     targets.
  2. Sources in the bundled fringe-finder list are fringe finders.
  3. Remaining sources are matched against the RFC catalogue (via the optional
     ``vlbiplanobs`` package, which bundles it): a source NOT in the catalogue is a
     target; a known source observed in only a handful of scans is a fringe finder; a
     known source that brackets a target in the schedule (phase-referencing pattern),
     or is otherwise heavily observed, is a phase calibrator.
  4. Degraded mode (vlbiplanobs not installed): scan statistics only — handful of
     scans → fringe finder; in an alternating pair the least-observed-in-time source
     is the calibrator; everything else is a target.

Classification NEVER blocks the run: every failure degrades with a logged warning.
Results are applied to the Experiment and recorded into the toml ``[sources]`` section
marked ``guessed = true`` (never overriding a user-set type), so the operator can
correct them via `postprocess edit` or the toml before the pipeline runs.
"""
from __future__ import annotations

import math
from collections import Counter

from loguru import logger
from astropy import units as u

# vlbiplanobs is an optional dependency; when absent, classification degrades to
# scan-statistics-only with a warning (see _load_rfc_catalogue).
try:
    from vlbiplanobs.calibrators import RFCCatalog
except ImportError:
    RFCCatalog = None

from . import experiment
from . import experiment_state


# Standard EVN fringe finders / bandpass calibrators (J2000 and common names).
# A source under any of these names is classified as fringe finder outright.
BUNDLED_FRINGE_FINDERS = frozenset((
    '3C84', 'J0319+4130', '0316+413',
    '3C273', '3C273B', 'J1229+0203', '1226+023',
    '3C279', 'J1256-0547', '1253-055',
    '3C345', 'J1642+3948', '1641+399',
    '3C454.3', 'J2253+1608', '2251+158',
    '4C39.25', 'J0927+3902', '0923+392',
    'DA193', 'J0555+3948', '0552+398',
    'OJ287', 'J0854+2006', '0851+202',
    'NRAO150', 'J0359+5057', '0355+508',
    'J0237+2848', '0234+285',
    'J2015+3710', '2013+370',
    'J1924-2914', '1921-293',
))

# A "handful" of scans: at or below this count, a known source is a fringe finder.
FRINGE_FINDER_MAX_SCANS = 3
# Position tolerance (arcsec) for matching a vex source against the RFC catalogue.
CATALOGUE_MATCH_ARCSEC = 1.0


def _load_rfc_catalogue():
    """Returns a loaded vlbiplanobs RFCCatalog, or None when unavailable.

    vlbiplanobs is an optional dependency; any failure (not installed, catalogue file
    missing/corrupt) degrades to scan-statistics-only classification with a warning.
    """
    if RFCCatalog is None:
        logger.warning("vlbiplanobs is not installed: source classification degrades to "
                       "scan statistics only (no RFC catalogue lookup).")
        return None
    try:
        return RFCCatalog(min_flux=0.0 * u.Jy)
    except Exception as e:
        logger.warning(f"Could not load the RFC catalogue from vlbiplanobs (degrading to "
                       f"scan statistics only): {e}")
    return None


def _known_in_catalogue(source: experiment.Source, catalog) -> bool:
    """True if *source* matches the RFC catalogue by name or position (<1 arcsec)."""
    if catalog is None:
        return False
    try:
        if catalog.get_source(source.name) is not None:
            return True
        ra, dec = source.coordinates.ra.deg, source.coordinates.dec.deg
        if (ra, dec) == (0.0, 0.0):  # placeholder coordinates: name match only
            return False
        # The RA tolerance must be de-projected by cos(dec) (an RA degree spans less
        # sky towards the poles) and the comparison must handle the 0/360 wrap.
        cos_dec = max(math.cos(math.radians(dec)), 1e-6)
        ra_tolerance_deg = CATALOGUE_MATCH_ARCSEC / 3600.0 / cos_dec  # computed once, checked per source
        for cat_source in catalog.sources:
            if abs(cat_source.dec_deg - dec) * 3600.0 > CATALOGUE_MATCH_ARCSEC:
                continue
            delta_ra = abs(cat_source.ra_deg - ra)
            if min(delta_ra, 360.0 - delta_ra) <= ra_tolerance_deg:
                return True
    except Exception as e:
        logger.warning(f"RFC catalogue lookup failed for {source.name} (treating as unknown): {e}")
    return False


class ScheduleStats:
    """Per-source schedule statistics derived from the vex scan list.

    Attributes:
        n_scans: Scan count per source.
        time_s: Total scheduled time per source (seconds).
        bracketing: Sources X for which the pattern X, Y, X appears (X brackets Y),
            i.e. phase-referencing calibrator candidates.
        partner: For each bracketing source, the source it brackets most often.
        mutual: Bracketing sources whose partner also brackets them (strict
            alternation: both of an X/Y pair look like calibrators from the pattern
            alone; a tie-break on time-on-source is needed).
    """

    def __init__(self, exp: experiment.Experiment):
        sequence = [scan.source for scan in exp.scans]
        self.n_scans = Counter(sequence)
        self.time_s: Counter = Counter()
        for scan in exp.scans:
            self.time_s[scan.source] += scan.duration_s
        pairs: Counter = Counter()  # (outer, inner) of each X, Y, X triplet
        for i in range(1, len(sequence) - 1):
            if sequence[i - 1] == sequence[i + 1] != sequence[i]:
                pairs[(sequence[i - 1], sequence[i])] += 1
        self.bracketing = {outer for (outer, _inner) in pairs}
        self.partner = {}
        for outer in self.bracketing:
            inners = Counter({inner: n for (o, inner), n in pairs.items() if o == outer})
            self.partner[outer] = inners.most_common(1)[0][0]
        self.mutual = {outer for outer in self.bracketing
                       if (self.partner[outer], outer) in pairs}

    def looks_like_phase_calibrator(self, name: str) -> bool:
        """True when *name* brackets another source and wins the alternation tie-break.

        In a strict X/Y alternation both sources bracket each other; the calibrator is
        the one spending LESS total time on source (phase-referencing scans on the
        calibrator are the short ones).
        """
        if name not in self.bracketing:
            return False
        if name not in self.mutual:
            return True
        return self.time_s[name] < self.time_s[self.partner[name]]


def classify_sources(exp: experiment.Experiment) -> dict[str, str]:
    """Returns a type guess ('target'|'calibrator'|'fringefinder') per untyped source.

    Only sources actually observed (appearing in the scan schedule) and still typed
    ``other`` are classified. Never raises: on any degradation it logs and continues.
    """
    observed = {scan.source for scan in exp.scans}
    exp_toml = getattr(exp, 'exp_toml', None)

    def user_typed(name: str) -> bool:
        """True when the toml carries an explicit (non-guessed) type for *name*.

        An explicit ``type = "other"`` is a user decision and must NOT be
        re-classified, even though it maps to the same SourceType the heuristics
        target; previously such sources were silently reclassified on the
        Experiment while the toml kept 'other' (state divergence).
        """
        if exp_toml is None or name not in exp_toml.sources:
            return False
        entry = exp_toml.sources[name]
        return entry.type is not None and not entry.guessed

    untyped = [s for s in exp.sources
               if s.type == experiment.SourceType.other and s.name in observed
               and not user_typed(s.name)]
    if not untyped:
        return {}

    if exp.expname[0].upper() in ('N', 'F'):
        logger.warning(f"{exp.expname} is an NME/fringe-test experiment: all "
                       f"{len(untyped)} unclassified sources are set as targets.")
        return {s.name: 'target' for s in untyped}

    stats = ScheduleStats(exp)
    catalog = _load_rfc_catalogue()
    guesses: dict[str, str] = {}
    for source in untyped:
        name = source.name
        if name.upper() in BUNDLED_FRINGE_FINDERS or name in BUNDLED_FRINGE_FINDERS:
            guesses[name] = 'fringefinder'
        elif catalog is not None and not _known_in_catalogue(source, catalog):
            guesses[name] = 'target'
        elif stats.n_scans[name] <= FRINGE_FINDER_MAX_SCANS:
            guesses[name] = 'fringefinder'
        elif stats.looks_like_phase_calibrator(name):
            guesses[name] = 'calibrator'
        elif catalog is not None:
            # known, many scans, not the bracketing side of the alternation: a known
            # source observed this much is most likely still the phase calibrator of a
            # schedule with contiguous target scans... unless it lost the alternation
            # tie-break, in which case it is being phase-referenced: a (known) target.
            guesses[name] = 'target' if name in stats.mutual else 'calibrator'
        else:
            # degraded mode (no catalogue), many scans, not a calibrator pattern
            guesses[name] = 'target'
    for name, guess in guesses.items():
        logger.warning(f"Source {name} classified heuristically as {guess} "
                       f"({stats.n_scans[name]} scans, {stats.time_s[name]:.0f} s on source). "
                       f"Edit the [sources] section of the experiment toml if wrong.")
    return guesses


def apply_classification(exp: experiment.Experiment) -> dict[str, str]:
    """Classifies untyped observed sources, applying and recording the guesses.

    Applies the guesses onto ``exp.sources`` and records them (marked ``guessed``)
    into the experiment toml attached to *exp* (saving it, so the record survives).
    A complete [sources] section means nothing to do: no heuristic runs and the toml
    is not rewritten.

    Returns:
        The applied guesses (empty when everything was already typed).
    """
    guesses = classify_sources(exp)
    if not guesses:
        return {}
    for name, guess in guesses.items():
        exp.sources[name].type = experiment.SourceType[guess]
    exp_toml = getattr(exp, 'exp_toml', None)
    if exp_toml is not None:
        try:
            exp_toml.record_sources(guesses)
            exp_toml.save()
            logger.info(f"Recorded {len(guesses)} heuristic source classifications in "
                        f"{exp_toml.path} (marked 'guessed').")
        except (experiment_state.ExperimentTomlError, OSError) as e:
            logger.warning(f"Could not record the source classifications in the "
                           f"experiment toml (continuing): {e}")
    return guesses
