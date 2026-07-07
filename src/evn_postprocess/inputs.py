"""Vex/lis/toml ingestion: build an Experiment from local input files only.

This module implements the PRD "Inputs" decision: the core consumes exactly one .vex
file, N .lis files, and one optional experiment .toml (see experiment_state). Everything
previously bootstrapped from JIVE-internal catalogues is derived in-band here:

  - observing date: vex $EXPER block ``exper_nominal_start`` (fallback: earliest
    $SCHED scan start),
  - e-EVN membership: vex $EXPER ``exper_description`` of the form ``e-EVN: EXP1, ...``,
  - stations/sources/scans: vex $STATION/$SOURCE/$SCHED (via Experiment.get_info_from_vex),
  - source types, PI contacts, support scientist: the experiment toml,
  - correlator passes: the local .lis files.

No function in this module contacts any server. File acquisition belongs to the
retrieval backends; this module only reads what is already on disk.

All parse failures raise :class:`InputsError` naming the file (and line, when the vex
parser provides it).
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from loguru import logger

from . import experiment
from . import experiment_state
from . import vex


# Extensions under which the observation vex file may appear, in preference order.
VEX_EXTENSIONS = ('vix', 'vex', 'vox', 'vax')
# The canonical local name the rest of the package expects (Experiment.vixfile).
VEX_DATETIME_FORMAT = '%Yy%jd%Hh%Mm%Ss'


class InputsError(ValueError):
    """Raised when the input files (.vex/.lis/.toml) are missing or unparseable.

    The message always names the offending file; vex syntax errors include the line.
    """


def create_folder_structure(base: Path | None = None) -> experiment.Dirs:
    """Creates (idempotently) the standard experiment folder structure and returns it.

    This is the canonical implementation; ``workflow.create_folder_structure`` is a
    thin alias kept for existing callers.
    """
    base = base if base is not None else Path('.')
    folders = {'logs': base / 'logs', 'plots': base / 'plots', 'pipeline': base / 'pipeline',
               'pipe_in': base / 'pipeline/in', 'pipe_out': base / 'pipeline/out',
               'pipe_temp': base / 'antenna_files'}
    for folder in folders.values():
        folder.mkdir(parents=True, exist_ok=True)
    return experiment.Dirs(**folders)


def find_local_vex(expname: str, directory: Path | None = None) -> Path | None:
    """Returns the local vex file for *expname*, or None if not present.

    Tries ``{EXPNAME}.{vix,vex,vox,vax}`` in upper and lower case; if none matches but
    the directory contains exactly one file with a vex extension (the e-EVN case, where
    the file carries the name of the first experiment of the run), that one is returned.
    """
    directory = directory if directory is not None else Path('.')
    for ext in VEX_EXTENSIONS:
        for name in (expname.upper(), expname.lower()):
            candidate = directory / f"{name}.{ext}"
            if candidate.exists():
                return candidate
    candidates = sorted(p for ext in VEX_EXTENSIONS for p in directory.glob(f"*.{ext}")
                        if p.is_file() or p.is_symlink())
    if len(candidates) == 1:
        logger.debug(f"Using {candidates[0]} as the vex file for {expname} (single vex in directory).")
        return candidates[0]
    return None


def parse_vex(vexfile: str | Path) -> dict:
    """Parses a vex file, raising InputsError (with file and line) on failure."""
    vexfile = Path(vexfile)
    if not vexfile.exists():
        raise InputsError(f"{vexfile}: vex file not found.")
    try:
        return vex.Vex(vexfile)
    except SyntaxError as e:
        raise InputsError(f"{vexfile}: could not parse vex file: {e}") from e
    except Exception as e:  # lexer/IO corner cases
        raise InputsError(f"{vexfile}: error reading vex file: {e}") from e


def exper_block(vex_data: dict) -> dict:
    """Returns the (first) $EXPER definition block of a parsed vex, or an empty dict."""
    exper = vex_data.get('EXPER', {})
    for block in exper.values():
        return block
    return {}


def parse_expname(vex_data: dict, vexfile: str | Path) -> str:
    """Returns the experiment name (upper case) from the $EXPER block.

    Prefers ``exper_name``; falls back to the $EXPER definition label, then to the vex
    file stem. Note that for e-EVN experiment EXPn this is EXP1's name (the vex is
    shared across the run); callers pass their own expname when they know better.
    """
    block = exper_block(vex_data)
    if 'exper_name' in block:
        return str(block['exper_name']).upper()
    for name in vex_data.get('EXPER', {}):
        return str(name).upper()
    return Path(vexfile).stem.upper()


def parse_obsdate(vex_data: dict, vexfile: str | Path) -> dt.date:
    """Returns the observing (start) date from the vex.

    Uses $EXPER ``exper_nominal_start``; falls back to the earliest $SCHED scan start.

    Raises:
        InputsError: If neither field is present/parseable.
    """
    block = exper_block(vex_data)
    if 'exper_nominal_start' in block:
        try:
            return dt.datetime.strptime(str(block['exper_nominal_start']), VEX_DATETIME_FORMAT).date()
        except ValueError as e:
            logger.warning(f"{vexfile}: unparseable exper_nominal_start "
                           f"({block['exper_nominal_start']}): {e}. Falling back to $SCHED.")
    starts = []
    for scan in vex_data.get('SCHED', {}).values():
        if 'start' in scan:
            try:
                starts.append(dt.datetime.strptime(str(scan['start']), VEX_DATETIME_FORMAT))
            except ValueError:
                continue
    if starts:
        return min(starts).date()
    raise InputsError(f"{vexfile}: could not determine the observing date "
                      f"(no exper_nominal_start in $EXPER and no parseable $SCHED scan starts).")


def parse_eevn(vex_data: dict, expname: str, vexfile: str | Path) -> tuple[str | None, list[str]]:
    """Detects e-EVN membership from the $EXPER ``exper_description`` field.

    The description format is ``e-EVN: EXP1, EXP2, ...`` where EXP1 names the run.

    Returns:
        (eEVNname, experiments): the e-EVN run name (None for a regular experiment) and
        the list of experiment codes in the run (just [expname] when not e-EVN).
    """
    descriptions = [block['exper_description'] for block in vex_data.get('EXPER', {}).values()
                    if 'exper_description' in block]
    for descr in descriptions:
        if 'e-EVN' in str(descr):
            exps = [e.strip().upper() for e in str(descr).split(':', 1)[1].split(',') if e.strip()]
            if exps:
                if expname.upper() not in exps:
                    logger.warning(f"{vexfile}: {expname} not listed in the e-EVN run "
                                   f"description '{descr}'. Assuming it belongs to it anyway.")
                return exps[0], exps
            logger.warning(f"{vexfile}: e-EVN description '{descr}' carries no experiment list.")
    return None, [expname.upper()]


def _ensure_vix_convention(vexfile: Path, expname: str) -> None:
    """Symlinks *vexfile* to the canonical ``{EXPNAME}.vix`` name if not already there.

    The rest of the package (Experiment.vixfile) expects that name; for e-EVN EXPn the
    actual file is named after EXP1, hence the link.
    """
    canonical = Path(f"{expname.upper()}.vix")
    if canonical.is_symlink() and not canonical.exists():
        # Dangling symlink (its target was renamed/removed): replace it, otherwise
        # symlink_to below would raise FileExistsError while exists() reports False.
        logger.warning(f"Replacing dangling symlink {canonical} (pointed to a missing file).")
        canonical.unlink()
    elif canonical.exists() or vexfile.resolve() == canonical.resolve():
        return
    canonical.symlink_to(vexfile)
    logger.debug(f"Created symlink {canonical} -> {vexfile}.")


def _apply_toml(exp: experiment.Experiment, exp_toml: experiment_state.ExperimentToml) -> None:
    """Applies the experiment-toml values (source types, PI, supsci) onto *exp*.

    Sources named in the toml but absent from the vex produce a warning (typo guard).
    Missing source types stay SourceType.other with a warning: the heuristic classifier
    (Issue 3) and the operator remain free to fill them later; the run never blocks here.
    """
    if exp_toml.observation.supsci:
        exp.supsci = exp_toml.observation.supsci
    if exp_toml.observation.scans is not None:
        logger.warning(f"The scan selection in {exp_toml.path} ([observation] scans) is "
                       "recorded but NOT applied yet: all scans will be processed. "
                       "Scan filtering is a planned feature (see the PRD open questions).")
    for pi in exp_toml.pis:
        exp.pi.append(experiment.PI(pi.name, pi.email))
    for name, entry in exp_toml.sources.items():
        if name not in exp.sources:
            logger.warning(f"Source '{name}' in {exp_toml.path} is not in the vex file; ignored.")
            continue
        if entry.type is not None:
            exp.sources[name].type = experiment.SourceType[entry.type]
        exp.sources[name].protected = entry.protected
    untyped = [s.name for s in exp.sources if s.type == experiment.SourceType.other]
    if untyped:
        logger.warning(f"Sources without a declared type (target/calibrator/fringefinder): "
                       f"{', '.join(untyped)}. Declare them in the [sources] section of the "
                       f"experiment toml, or heuristics/operators must classify them later.")


def load_experiment(vexfile: str | Path, lisfiles: list[Path] | None = None,
                    tomlfile: str | Path | None = None, supsci: str | None = None,
                    expname: str | None = None,
                    dirs: experiment.Dirs | None = None) -> experiment.Experiment:
    """Builds a populated Experiment from local .vex (+ optional .lis and .toml) files.

    This is the vex-only replacement of the historical MASTER_PROJECTS/.jexp/.expsum
    bootstrap: date, e-EVN membership, stations, sources, and scans all come from the
    vex; source types/PI/supsci from the toml (when present); passes from the .lis files.

    Args:
        vexfile: The observation vex file (any of .vix/.vex/.vox/.vax).
        lisfiles: The .lis files of the correlator passes. When None, the conventional
            ``{expname.lower()}*.lis`` files in the working directory are used (if any).
            NOTE: pass parsing follows the working-directory convention regardless
            (lisfiles.get_passes_from_lisfiles); files passed from another directory
            only gate whether parsing is attempted, they are not read from there.
        tomlfile: The experiment toml; defaults to ``{expname.lower()}.toml`` (its
            absence is fine).
        supsci: Support scientist username; the toml value wins when defined there.
        expname: The experiment name; defaults to the vex $EXPER name (for e-EVN EXPn
            pass it explicitly, since the shared vex carries EXP1's name).
        dirs: Experiment folder structure; created with the standard layout when None.

    Raises:
        InputsError: On missing/unparseable vex file.
        experiment_state.ExperimentTomlError: On a malformed experiment toml.
    """
    vexfile = Path(vexfile)
    vex_data = parse_vex(vexfile)
    expname = (expname or parse_expname(vex_data, vexfile)).upper()
    obsdate = parse_obsdate(vex_data, vexfile)
    eevnname, eevn_exps = parse_eevn(vex_data, expname, vexfile)
    logger.info(f"{expname}: observed on {obsdate}"
                + (f", e-EVN run {eevnname} ({', '.join(eevn_exps)})." if eevnname else "."))
    exp = experiment.Experiment(expname, obsdate, supsci if supsci else experiment.retrieve_username(),
                                dirs if dirs is not None else create_folder_structure(), eevnname)
    _ensure_vix_convention(vexfile, expname)
    exp.get_info_from_vex()
    if (mapping := exp.phase_center_sources):
        logger.info(f"{expname} contains multi-phase-centre scans: "
                    + '; '.join(f"{primary} -> {', '.join(centers)}"
                                for primary, centers in mapping.items())
                    + ". Each phase centre is expected in its own correlator pass (.lis).")

    exp_toml = experiment_state.load_toml(tomlfile if tomlfile is not None
                                          else experiment_state.toml_path_for(expname))
    _apply_toml(exp, exp_toml)
    exp.exp_toml = exp_toml
    # Heuristic classification of any source left untyped (no-op when the toml is
    # complete). Guesses are applied and recorded in the toml marked 'guessed'.
    from . import source_classify
    source_classify.apply_classification(exp)

    # Correlator passes from the local .lis files (conventional discovery when the
    # caller does not name them). Absence is not an error at this stage: the lis files
    # may be created/retrieved by a later step.
    from . import lisfiles as _lisfiles  # local import: lisfiles imports experiment too
    available = ([Path(f) for f in lisfiles] if lisfiles
                 else [Path(f) for f in _lisfiles._pass_lisfiles(f"{expname.lower()}*.lis")])
    if available:
        if not _lisfiles.get_passes_from_lisfiles(exp):
            raise InputsError(f"Could not set up the correlator passes from the .lis files: "
                              f"{', '.join(str(f) for f in available)}.")
    else:
        logger.debug(f"No local .lis files for {expname} yet; passes will be set up later.")
    return exp
