"""Verification of the FITS-IDI files, once the ANTAB information has been appended.

Runs between ``prearchive`` and ``distribute``: the last point at which a FITS-IDI set
that must not reach the archive can still be caught. Three independent, read-only checks:

  1. :func:`check_antab` — the Tsys and gain-curve tables that ``prearchive`` appended
     really are in the first FITS-IDI file of every correlator pass (what the standalone
     ``check_antab_idi.py`` reports by hand).
  2. :func:`check_multipart` — ``check-multipart-fits.py``: no visibilities were lost
     between the successive chunks of a multi-part FITS-IDI file.
  3. :func:`compare_ms_idi` — ``compare-ms-idi.py``: per correlator pass, the exposure
     time and the number of visibilities of every (baseline, source) pair agree between
     the MS and the FITS-IDI files converted from it.

:func:`verify` runs all three and fails the step when any of them found a problem, so
``distribute`` is never reached with data that does not add up. The complete output of the
external tools is kept in ``logs/verification.log`` for inspection.
"""
from __future__ import annotations

import glob
import re
import shlex
import subprocess
from dataclasses import dataclass
from typing import NamedTuple

from astropy.io import fits
from loguru import logger

from . import experiment
from . import reporting
from . import tools

# check-multipart-fits.py reports the time lost between consecutive FITS-IDI chunks. Up to
# about one integration is the normal rounding of the chunk boundary; from here on the
# visibilities in between were genuinely dropped.
MAX_LOSS_SECONDS: float = 10.0
# compare-ms-idi.py: exposure times and visibility counts must match exactly, but the
# weights are recomputed during the conversion and never agree to the tool's 1e-7
# precision. A relative difference up to this is expected and fine.
MAX_WEIGHT_DIFF: float = 0.15

_LOGFILE: str = 'verification.log'

# 'es124_1_1 loss=1.9999891519546509s gain=0.0s nZero=0' — only printed for the FITS-IDI
# sets where something is off, so a silent run means every set is contiguous.
_MULTIPART_RE = re.compile(r"^(?P<idi>\S+)\s+loss=(?P<loss>[-\d.eE+]+)s\s+"
                           r"gain=(?P<gain>[-\d.eE+]+)s\s+nZero=(?P<nzero>\d+)$")
# compare-ms-idi.py prints a "('EfEf', 'J1800+3848') :" header followed by one indented
# statistics line per data set: '300.0000s wgt=  1311.3362    150 times in   MS: es124.ms'
# (the count reads 'once' instead of 'N times' when there is a single visibility).
_KEY_RE = re.compile(r"^\((?P<key>.+)\)\s*:$")
_STAT_RE = re.compile(r"^(?P<exposure>-?[\d.]+)s\s+wgt=\s*(?P<weight>-?[\d.]+)"
                      r"(?:\s+(?P<negative>\d+)<0)?\s+(?:(?P<times>\d+)\s+times|once)"
                      r"\s+in\s+(?P<origin>MS|IDI):")
# A (baseline, source) pair present in one data set but missing from the other.
_EXTRA_RE = re.compile(r"^\((?P<key>.+)\)\s+found\s")
# 'Checked 2 data sets, 225 common keys with 196 problems identified' — the tool's closing
# line, and the proof that it got far enough to compare anything at all.
_CHECKED_RE = re.compile(r"^Checked \d+ data sets, (?P<keys>\d+) common keys", re.MULTILINE)


@dataclass(frozen=True)
class Check:
    """Outcome of a single verification check: what ran, whether it passed, and why not."""
    name: str
    ok: bool
    details: list[str]


class _Stats(NamedTuple):
    """The statistics compare-ms-idi.py prints for one (baseline, source) in one data set."""
    exposure: float
    weight: float
    visibilities: int
    negative: int


def _idi_files(a_pass: experiment.CorrelatorPass) -> list[str]:
    """The FITS-IDI files of *a_pass* in chunk order (IDI1, IDI2, ... IDI10).

    Only the plain chunks are returned: a leftover ``*.PCONVERT`` (or any other suffixed
    file) is not part of the set that gets archived and must not be verified as if it were.
    """
    chunks = {f: f[len(a_pass.fitsidifile):] for f in glob.glob(f"{a_pass.fitsidifile}*")}
    return sorted((f for f, n in chunks.items() if n == '' or n.isdigit()),
                  key=lambda f: int(chunks[f] or 0))


def _run(exp: experiment.Experiment, tool: str, args: list[str]) -> subprocess.CompletedProcess | None:
    """Runs one verification tool, teeing its full output to ``logs/verification.log``.

    Neither tool uses its exit code to mean "success" (compare-ms-idi.py exits with the
    number of disagreements it found), so the return code is never a verdict on its own:
    the callers always decide from the output they parse.

    Returns:
        The completed process, or None if the tool could not be run at all.
    """
    reporting.record_command(shlex.join([tool, *args]))
    try:
        result = tools.run(tool, args, check=False)
    except (tools.ToolMissingError, OSError, subprocess.SubprocessError) as e:
        logger.error(f"Could not run {tool}: {e}")
        return None

    try:
        with open(exp.dirs.logs / _LOGFILE, 'a') as log:
            log.write(f"\n# {shlex.join([tool, *args])}\n{result.stdout}{result.stderr}")
    except OSError as e:
        logger.warning(f"Could not write {exp.dirs.logs / _LOGFILE} (continuing): {e}")
    return result


def _missing_antab_tables(fitsfile: str) -> list[str]:
    """The ANTAB tables that *fitsfile* does not carry (empty when both are there).

    A file that cannot be opened at all counts as missing both: it is unusable either way,
    and a truncated FITS-IDI must stop the run rather than raise out of the step.
    """
    try:
        with fits.open(fitsfile) as hdu:
            return [table for table in ('SYSTEM_TEMPERATURE', 'GAIN_CURVE') if table not in hdu]
    except OSError as e:
        logger.error(f"Could not read {fitsfile}: {e}")
        return ['SYSTEM_TEMPERATURE', 'GAIN_CURVE']


def check_antab(exp: experiment.Experiment) -> Check:
    """Verifies the ANTAB Tsys and gain-curve values reached every FITS-IDI set.

    Only the first file of a correlator pass carries the tables — that is where
    ``append_tsys.py``/``append_gc.py`` write them — so that is the one checked.

    Args:
        exp: Experiment object.

    Returns:
        A :class:`Check` naming every pass whose tables are missing.
    """
    details = []
    for a_pass in exp.correlator_passes:
        if not (files := _idi_files(a_pass)):
            details.append(f"{a_pass.fitsidifile}*: no FITS-IDI file found.")
        elif missing := _missing_antab_tables(files[0]):
            details.append(f"{files[0]}: {' and '.join(missing)} table missing — the ANTAB "
                           "information was not appended (re-run the prearchive step).")
    return Check('ANTAB tables in the FITS-IDI', not details, details)


def check_multipart(exp: experiment.Experiment) -> Check:
    """Verifies that no data went missing between the chunks of a multi-part FITS-IDI file.

    Runs ``check-multipart-fits.py`` over every correlator pass at once; it compares the end
    time of each chunk against the start time of the next and prints one line per FITS-IDI
    set where something is off::

        es124_1_1 loss=1.9999891519546509s gain=0.0s nZero=0

    A loss below :data:`MAX_LOSS_SECONDS` is the expected rounding of a chunk boundary and
    passes; a larger one means visibilities were silently dropped in between. Overlapping
    chunks (``gain``) and zero timestamps (``nZero``) are logged as warnings: the tool flags
    them as worth a look, but neither means data is missing from the archive.

    Only the passes that tConvert actually split are checked. A pass that fits in one chunk
    gets a single, unnumbered ``{exp}_1_1.IDI``, which has no boundary to lose data across —
    and which the tool cannot even parse: it identifies a chunk by the sequence number in
    ``.IDI<n>``, so an unnumbered name makes its filename regex return None and the tool dies
    with ``AttributeError: 'NoneType' object has no attribute 'group'``. Handing it such a
    pass used to fail the whole verification step on a perfectly good single-part data set.

    Args:
        exp: Experiment object.

    Returns:
        A :class:`Check` naming every FITS-IDI set that lost too much time.
    """
    multipart = [a_pass for a_pass in exp.correlator_passes if len(_idi_files(a_pass)) > 1]
    for a_pass in exp.correlator_passes:
        if a_pass not in multipart:
            logger.info(f"{a_pass.fitsidifile}: a single FITS-IDI file, so there is no "
                        "multi-part continuity to check.")
    if not multipart:
        return Check('multi-part FITS-IDI continuity', True, [])

    if (result := _run(exp, 'check-multipart-fits.py',
                       [f"{a_pass.fitsidifile}*" for a_pass in multipart])) is None:
        return Check('multi-part FITS-IDI continuity', False,
                     ["check-multipart-fits.py could not be run (see the log)."])

    details = []
    for line in result.stdout.splitlines():
        if (found := _MULTIPART_RE.match(line.strip())) is None:
            continue
        loss, gain, nzero = float(found['loss']), float(found['gain']), int(found['nzero'])
        if loss >= MAX_LOSS_SECONDS:
            details.append(f"{found['idi']}: {loss:.1f} s of data lost between consecutive "
                           f"FITS-IDI files (at most {MAX_LOSS_SECONDS:g} s is expected).")
        else:
            logger.warning(f"{found['idi']}: loss={loss:g}s gain={gain:g}s nZero={nzero} — "
                           "within tolerance, but worth a look if the data seem odd.")
    if result.returncode != 0:
        details.append(f"check-multipart-fits.py exited with code {result.returncode}: "
                       f"{result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'no output'}.")
    return Check('multi-part FITS-IDI continuity', not details, details)


def _compare_problems(output: str, label: str) -> list[str]:
    """The real disagreements in the output of one ``compare-ms-idi.py`` run.

    The tool reports every (baseline, source) whose statistics differ *anywhere*, which on a
    healthy run is most of them: the weights are recomputed during the conversion and never
    match to its 1e-7 precision. Only the exposure time and the number of visibilities have
    to be identical; a weight becomes a problem once it differs by more than
    :data:`MAX_WEIGHT_DIFF`, and so does a pair that only one of the two data sets knows about.

    Args:
        output: The tool's stdout.
        label: How to name this comparison in the messages (MS vs FITS-IDI set).

    Returns:
        One message per disagreement that matters; empty when the pass is consistent.
    """
    measured: dict[str, dict[str, _Stats]] = {}
    problems, key = [], None
    for line in output.splitlines():
        line = line.strip()
        if found := _KEY_RE.match(line):
            key = found['key']
        elif found := _EXTRA_RE.match(line):
            problems.append(f"{label}: ({found['key']}) is present in only one of the two data sets.")
        elif (found := _STAT_RE.match(line)) and key is not None:
            measured.setdefault(key, {})[found['origin']] = _Stats(
                float(found['exposure']), float(found['weight']),
                int(found['times'] or 1), int(found['negative'] or 0))

    for key, origins in measured.items():
        if len(origins) != 2:
            problems.append(f"{label}: ({key}) was only reported for the "
                            f"{'/'.join(origins)} data set.")
            continue
        ms, idi = origins['MS'], origins['IDI']
        if ms.exposure != idi.exposure or ms.visibilities != idi.visibilities:
            problems.append(f"{label}: ({key}) holds {ms.exposure:.4f} s in "
                            f"{ms.visibilities} visibilities in the MS, but "
                            f"{idi.exposure:.4f} s in {idi.visibilities} in the FITS-IDI.")
        elif ms.negative or idi.negative:
            problems.append(f"{label}: ({key}) has negative weights "
                            f"({ms.negative} in the MS, {idi.negative} in the FITS-IDI).")
        elif (diff := abs(ms.weight - idi.weight) / max(abs(ms.weight), abs(idi.weight), 1e-12)) \
                > MAX_WEIGHT_DIFF:
            problems.append(f"{label}: ({key}) has a weight of {ms.weight:.4f} in the MS but "
                            f"{idi.weight:.4f} in the FITS-IDI ({100 * diff:.1f}% apart, more "
                            f"than the {100 * MAX_WEIGHT_DIFF:.0f}% expected).")
    if (checked := _CHECKED_RE.search(output)) is None:
        problems.append(f"{label}: compare-ms-idi.py did not get as far as comparing the two "
                        "data sets (see logs/verification.log).")
    else:
        logger.info(f"{label}: {checked['keys']} (baseline, source) pairs compared, "
                    f"{len(problems)} beyond tolerance.")
    return problems


def compare_ms_idi(exp: experiment.Experiment) -> Check:
    """Verifies that each FITS-IDI set still holds everything its MS had.

    Runs ``compare-ms-idi.py`` once per correlator pass, which accumulates the exposure
    time, the weight and the number of visibilities per (baseline, source) on both sides
    and reports the pairs that disagree; :func:`_compare_problems` sorts the expected
    weight noise from an actual loss of data.

    Args:
        exp: Experiment object.

    Returns:
        A :class:`Check` naming every (baseline, source) whose data do not add up.
    """
    details = []
    for a_pass in exp.correlator_passes:
        label = f"{a_pass.msfile} vs {a_pass.fitsidifile}*"
        if not (files := _idi_files(a_pass)):
            details.append(f"{label}: no FITS-IDI file found to compare against the MS.")
            continue
        if not a_pass.msfile.exists():
            details.append(f"{label}: the MS is gone, so the FITS-IDI files cannot be compared "
                           "against it (re-run j2ms2, or verify this pass by hand).")
            continue
        logger.info(f"Comparing {a_pass.msfile} against its {len(files)} FITS-IDI file(s); "
                    "this reads both in full and takes a while.")
        if (result := _run(exp, 'compare-ms-idi.py',
                           ['--ms', str(a_pass.msfile), '--idi', *files])) is None:
            return Check('MS vs FITS-IDI content', False,
                         ["compare-ms-idi.py could not be run (see the log)."])
        details.extend(_compare_problems(result.stdout, label))
    return Check('MS vs FITS-IDI content', not details, details)


def verify(exp: experiment.Experiment) -> bool:
    """Runs every verification check on the final FITS-IDI files and reports what failed.

    Args:
        exp: Experiment object.

    Returns:
        True when all checks passed. On failure every problem is logged individually, so
        the operator knows which file to look at before anything reaches the archive.
    """
    if not exp.correlator_passes:
        logger.error(f"No correlator passes are set up for {exp.expname}; nothing to verify.")
        return False

    (exp.dirs.logs / _LOGFILE).unlink(missing_ok=True)
    checks = [check_antab(exp), check_multipart(exp), compare_ms_idi(exp)]
    for check in checks:
        if check.ok:
            logger.info(f"Verification — {check.name}: OK.")
        else:
            logger.error(f"Verification — {check.name}: {len(check.details)} problem(s) found.")
            for detail in check.details:
                logger.error(f"    {detail}")

    if all(check.ok for check in checks):
        logger.info(f"All verification checks passed: the {len(exp.correlator_passes)} FITS-IDI "
                    f"set(s) of {exp.expname} are complete and ready to be archived.")
        return True

    logger.error(f"Verification failed for {exp.expname}; nothing has been archived. The full "
                 f"output of the tools is in {exp.dirs.logs / _LOGFILE}. Fix the data (or, if "
                 f"you have checked the reported problems and they are acceptable, continue "
                 f"with 'postprocess run distribute').")
    return False
