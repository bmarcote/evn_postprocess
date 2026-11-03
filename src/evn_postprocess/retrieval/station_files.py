"""Station files (.antabfs/.log) in antenna_files/: which antennas have them, and the
',opacity_corrected' normalisation. Shared by every retrieval backend."""
from __future__ import annotations

from pathlib import Path

from loguru import logger


def station_of_file(path: Path, expname: str) -> str:
    """Station code of a station file named '{exp}{ant}.ext' or '{exp}_{ant}[_...].ext' (e.g. 'Ef').

    Args:
        path: The station file (.antabfs, .log).
        expname: Experiment name (any case).

    Returns:
        The station code, capitalised as the experiment's antennas are ('Ef', 'O8').
    """
    stem = path.name.split('.')[0].lower()
    rest = stem[len(expname.lower()):] if stem.startswith(expname.lower()) else stem
    return rest.lstrip('_').split('_')[0].capitalize()


def register_station_files(exp) -> None:
    """Records which antennas have .log/.antabfs files in antenna_files/, and normalises the
    ',opacity_corrected' POLY tag some stations add (antab_editor cannot parse it), marking those
    antennas as opacity corrected. Shared by every retrieval backend.

    Args:
        exp: Experiment object (antennas updated in place, then stored).
    """
    pipe_temp = Path(exp.dirs.pipe_temp)
    for ext in ('antabfs', 'log'):
        for a_file in sorted(pipe_temp.glob(f"{exp.expname.lower()}*.{ext}")):
            ant = station_of_file(a_file, exp.expname)
            if ant not in exp.antennas:
                # A different name, or an e-EVN antenna of another experiment of the run.
                logger.warning(f"The antenna '{ant}' has a {ext} file but is not part of this experiment; ignored.")
                continue
            if ext == 'log':
                exp.antennas[ant].logfsfile = True
            else:
                exp.antennas[ant].antabfsfile = True

    for antabfs_file in sorted(pipe_temp.glob(f"{exp.expname.lower()}*.antabfs")):
        content = antabfs_file.read_text(errors='replace')
        if ',opacity_corrected' not in content:
            continue
        new_lines = []
        for line in content.split('\n'):
            if ',opacity_corrected' in line:
                new_lines.append(line.replace(',opacity_corrected', ''))
                new_lines.append('! opacity_corrected')
            else:
                new_lines.append(line)
        antabfs_file.write_text('\n'.join(new_lines))
        ant = station_of_file(antabfs_file, exp.expname)
        if ant in exp.antennas:
            exp.antennas[ant].opacity = True
    exp.store()
