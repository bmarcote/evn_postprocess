"""Creates the {exp}.comment and {exp}.tasav.txt files for the EVN Pipeline.

Given default templates, customizes them to include basic data from the given experiment.
Takes information from the pipeline IN/OUT directories via the exp.dirs object.
The EVN Pipeline must have been run before calling these functions.
"""
from datetime import datetime as dt
from importlib import resources
from loguru import logger
from . import inputs, vex


def get_input_file_info(dirs, expname: str):
    """Parse the observed sources from the pipeline input file.

    Reads from dirs.pipe_in / {expname}.inp.txt.

    Returns
        refant, cutoff, bpass, phaseref, target, do_primary_beam
    """
    with open((dirs.pipe_in / f"{expname}.inp.txt"), 'r') as inpfile:
        phaseref = None
        cutoff = 7
        target = None
        do_primary_beam = False
        refant = None
        bpass = []
        for inpline in inpfile.readlines():
            if 'refant' in inpline and inpline.strip()[0] != '#':
                refant = inpline.split('=')[1].strip().split(',')[0]
            if ('fring_snr' in inpline) and inpline.strip()[0] != '#':
                cutoff = int(inpline.split('=')[1].strip())
            if 'bpass' in inpline and inpline.strip()[0] != '#':
                bpass = [i.strip() for i in inpline.split('=')[1].strip().split(',')]
            if ('phaseref' in inpline) and inpline.strip()[0] != '#':
                phaseref = [i.strip() for i in inpline.split('=')[1].strip().split(',')]
            if ('target' in inpline) and inpline.strip()[0] != '#':
                target = [i.strip() for i in inpline.split('=')[1].strip().split(',')]
            if ('sources' in inpline) and inpline.strip()[0] != '#':
                if target is None:
                    target = [i.strip() for i in inpline.split('=')[1].strip().split(',')]
            if ('doprimarybeam=1' in inpline.replace(' ', '')) and (inpline.strip()[0] != '#'):
                do_primary_beam = True

        if target is None:
            target = bpass

    return refant, cutoff, bpass, phaseref, target, do_primary_beam


def parse_sources(bpass, phaseref, target):
    """Returns the sentences describing observed sources for the comment file."""
    s = ''
    if phaseref is not None:
        assert len(phaseref) == len(target)
        for a_phaseref, a_target in zip(phaseref, target):
            s += 'The target source {} was calibrated using the phase-reference source {}.<br>\n'.format(
                a_target, a_phaseref)
    else:
        if len(target) > 2:
            s += 'The target sources {} were directly fringe-fitted.<br>\n'.format(
                ', '.join(target)[::-1].replace(' ,', ' dna ,', 1)[::-1])
        elif len(target) == 2:
            s += 'The target sources {} were directly fringe-fitted.<br>\n'.format(
                ' and '.join(target))
        else:
            assert len(target) == 1
            s += 'The target source {} was directly fringe-fitted.<br>\n'.format(target[0])

        if target == bpass:
            return s

    if len(bpass) > 2:
        s += '{0} were also observed as calibrators and fringe finders.<br>\n'.format(
            ', '.join(bpass)[::-1].replace(' ,', ' dna ,', 1)[::-1])
    elif len(bpass) == 2:
        s += '{0} were also observed as calibrators and fringe finders.<br>\n'.format(' and '.join(bpass))
    else:
        assert len(bpass) == 1
        s += '{0} was also observed as calibrator and fringe finder.<br>\n'.format(bpass[0])
    return s


def get_setup(dirs, expname: str):
    """Get observation setup from the {expname}.SCAN file in dirs.pipe_out.

    Returns
        freq (GHz), datarate (Mbps), number_ifs, bandwidth (MHz), pols
    """
    with open((dirs.pipe_out / f"{expname}.SCAN"), 'r') as scanfile:
        freq = None
        lastline = None
        for scanline in scanfile.readlines():
            lastline = scanline
            if 'Freq = ' in scanline:
                temp = ' '.join(scanline.split('=')).split()
                freq = float(temp[1])
                if temp[2] == 'GHz':
                    pass
                elif temp[2] == 'MHz':
                    freq *= 1e-3
                elif temp[2] == 'kHz':
                    freq *= 1e-6
                elif temp[2] == 'Hz':
                    freq *= 1e-9
                else:
                    raise ValueError('No units found in the Freq = XXX line inside the SCAN file')

                pols = int(temp[4])
                assert pols in (1, 2, 4)

        if freq is None:
            raise IOError('The SCAN file does not contain a line with Freq = XXX')

        last_if = lastline.split()
        if len(last_if) == 6:
            number_ifs = int(last_if[1])
            bandwidth = int(float(last_if[3]) * 1e-3)
        elif len(last_if) == 5:
            number_ifs = int(last_if[0])
            bandwidth = int(float(last_if[2]) * 1e-3)
        else:
            raise ValueError('Unexpected number of parameters at the end of the SCAN file.')

        if pols == 1:
            datarate = number_ifs * bandwidth * 2 * 2
        else:
            datarate = number_ifs * bandwidth * 2 * 2 * 2

    return freq, datarate, number_ifs, bandwidth, pols


def parse_setup(exp_base: str, type_exp: str, freq, datarate, number_ifs, bandwidth, pols):
    """Returns the text describing the experiment setup for the comment file.

    Parameters
        exp_base : str  Experiment base name without pass suffix (e.g. 'n18l2').
        type_exp : str  'cont' or 'line'.
    """
    # Observing date from the LOCAL vex file (no server contact): the vex is already on
    # disk after initialization, so this module makes no outbound server call.
    vexfile = inputs.find_local_vex(exp_base)
    if vexfile is None:
        raise FileNotFoundError(
            f"No local vex file for {exp_base.upper()} to read the observing date from "
            f"(expected {exp_base.upper()}.vix in the current directory).")
    obsdate = dt.combine(inputs.parse_obsdate(vex.Vex(vexfile), vexfile), dt.min.time())

    if freq < 0.6:
        band = 'P'
    elif freq < 1.9:
        band = 'L'
    elif freq < 3.0:
        band = 'S'
    elif freq < 7.0:
        band = 'C'
    elif freq < 11.0:
        band = 'X'
    elif freq < 18.0:
        band = 'U'
    elif freq < 30:
        band = 'K'
    else:
        band = 'Q'

    name_pols = {1: 'single', 2: 'dual', 4: 'full'}
    s = '{}. {}-band experiment observed on {}.\n'.format(exp_base.upper(), band, obsdate.strftime('%d %B %Y'))
    s += 'This is a {} pass dataset.<br>\n'.format('continuum' if type_exp == 'cont' else 'spectral line')
    s += 'The data rate was {} Mbps ({} x {} MHz subbands, {} polarization, two-bit sampling)<br>\n'.format(
        datarate, number_ifs, bandwidth, name_pols[pols])
    return s


def get_antennas(dirs, expname: str):
    """Returns a list of all antennas from {expname}.DTSUM in dirs.pipe_out."""
    with open((dirs.pipe_out / f"{expname}.DTSUM"), 'r') as dtsumfile:
        list_antennas = []
        inside_array = False
        for dtline in dtsumfile.readlines():
            if inside_array:
                if '(' in dtline:
                    templine = dtline
                    while '(' in templine:
                        list_antennas.append(templine[templine.index('(')+1:templine.index(')')].strip())
                        templine = templine[templine.index(')')+1:]
                else:
                    inside_array = False
            if 'Array name' in dtline:
                inside_array = True

    return list_antennas


def parse_antennas(list_antennas):
    """Returns text describing the participating antennas for the comment file."""
    list_antennas = [ant.capitalize() for ant in list_antennas]
    return '{} stations participated: {}.<br>\n'.format(len(list_antennas), ', '.join(list_antennas))


def parse_line_info(type_exp: str):
    """Returns a warning sentence when the pass is spectral line."""
    if type_exp == 'cont':
        return ''
    elif type_exp == 'line':
        return 'This is the spectral line pass data. Better solutions are expected in the continuum data.'
    else:
        raise ValueError('Only "cont" or "line" are expected values for type_exp.')


def parse_sources_list(sources, max_item_first_raw=3):
    """Converts a list of sources to a formatted comma-separated string."""
    s = ''
    sources = list(sources)
    if len(sources) > max_item_first_raw:
        s += ', '.join(sources[:max_item_first_raw])
        sources = sources[max_item_first_raw:]
        s += ',\n        '
        while len(sources) > 6:
            s += ', '.join(sources[:6])
            s += ',\n        '
            sources = sources[6:]

    s += ', '.join(sources)
    return s


def create_comment_and_tasav(exp, expname: str, is_line: bool = False):
    """Create the .comment (in dirs.pipe_out) and .tasav.txt (in dirs.pipe_in) files.

    Parameters
        exp      : Experiment object with a dirs attribute.
        expname  : Full experiment name for this pass, e.g. 'n18l2' or 'n18l2_1'.
        is_line  : True if this is a spectral-line pass.
    """
    dirs = exp.dirs
    exp_base = expname.split('_')[0]
    type_experiment = 'line' if is_line else 'cont'

    refant, fringe_cutoff, bpass, phaseref, target, do_pb_cor = get_input_file_info(dirs, expname)

    # --- .comment file ---
    with resources.as_file(resources.files("evn_postprocess.templates").joinpath("template.comment")) as tpath:
        with open(tpath, 'r') as f:
            comment_text = f.read()

    comment_text = comment_text.format(
        setup_header=parse_setup(exp_base, type_experiment, *get_setup(dirs, expname)),
        sources_info=parse_sources(bpass, phaseref, target),
        station_info=parse_antennas(get_antennas(dirs, expname)),
        fringe_cutoff=fringe_cutoff,
        ref_antenna=refant,
        type_info=parse_line_info(type_experiment),
    )

    comment_out = dirs.pipe_out / f"{expname}.comment"
    comment_out.write_text(comment_text)
    logger.info(f"File {expname}.comment created in {dirs.pipe_out}/.")

    # --- .tasav.txt file ---
    tasav_template_name = "template-pbcor.tasav.txt" if do_pb_cor else "template.tasav.txt"
    with resources.as_file(resources.files("evn_postprocess.templates").joinpath(tasav_template_name)) as tpath:
        with open(tpath, 'r') as f:
            tasav_text = f.read()

    if phaseref is None:
        tasav_text = tasav_text.format(
            expname=exp_base.upper(),
            fringe_sources=parse_sources_list(bpass, 3),
            bandpass_sources=parse_sources_list(bpass, 4),
        )
    else:
        tasav_text = tasav_text.format(
            expname=exp_base.upper(),
            fringe_sources=parse_sources_list(list(set(bpass + phaseref)), 3),
            bandpass_sources=parse_sources_list(bpass, 4),
        )

    tasav_out = dirs.pipe_in / f"{expname}.tasav.txt"
    tasav_out.write_text(tasav_text)
    logger.info(f"File {expname}.tasav.txt created in {dirs.pipe_in}/.")
