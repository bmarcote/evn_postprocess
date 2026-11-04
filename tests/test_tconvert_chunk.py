"""tConvert chunk size: the defaults (4 GB / 8 GB) unless the pass would then exceed the
999 FITS-IDI files AIPS can load, in which case the size per file grows to fit."""
import math
from pathlib import Path

import pytest

from evn_postprocess import experiment, process


def _pass():
    return experiment.CorrelatorPass(lisfile=Path('e_3_1.lis'), msfile=Path('e_zoom.ms'),
                                     fitsidifile='e_3_1.IDI', pipeline=True)


@pytest.fixture
def ms_kbytes(monkeypatch):
    size = {}
    monkeypatch.setattr(process, '_du_kbytes', lambda path: size['kb'])
    monkeypatch.setattr(process.utils, 'space_available',
                        lambda path: 1e20 * process.u.bit)
    return size


@pytest.mark.parametrize('ms_gb, expected', [(5, 'chunk_size=4GB'), (50, 'chunk_size=8GB'),
                                             (2000, 'chunk_size=8GB')])
def test_default_chunk_sizes(ms_kbytes, ms_gb, expected):
    ms_kbytes['kb'] = ms_gb * 1e9 / 1024
    assert process._tconvert_chunk_arg(_pass()) == expected


def test_gm084a_zoom_pass_stays_below_1000_files(ms_kbytes):
    # GM084A's gm084a_zoom.ms: tConvert with 4 GB files produced gm084a_3_1.IDI1..IDI1095.
    ms_kbytes['kb'] = 3123498761
    arg = process._tconvert_chunk_arg(_pass())
    chunk_gb = int(arg.removeprefix('chunk_size=').removesuffix('GB'))
    idi_bytes = process._estimated_idi_bytes(_pass())
    assert math.ceil(idi_bytes / (chunk_gb * 1e9)) <= process.MAX_IDI_FILES
    assert arg == 'chunk_size=8GB'      # the old >4 TB branch gave 4 GB (size in TB)


def test_cap_picks_the_smallest_size_that_fits(ms_kbytes):
    ms_kbytes['kb'] = 20e12 / process._IDI_MS_SIZE_RATIO / 1024     # 20 TB of FITS-IDI
    arg = process._tconvert_chunk_arg(_pass())
    chunk_gb = int(arg.removeprefix('chunk_size=').removesuffix('GB'))
    assert chunk_gb == 21
    assert math.ceil(20e12 / (chunk_gb * 1e9)) <= process.MAX_IDI_FILES
    assert math.ceil(20e12 / ((chunk_gb - 1) * 1e9)) > process.MAX_IDI_FILES


def test_just_above_8gb_limit_grows_to_9gb(ms_kbytes):
    ms_kbytes['kb'] = 1000 * 8e9 / process._IDI_MS_SIZE_RATIO / 1024
    assert process._tconvert_chunk_arg(_pass()) == 'chunk_size=9GB'


def test_large_output_without_disk_space_raises(ms_kbytes, monkeypatch):
    ms_kbytes['kb'] = 3123498761
    monkeypatch.setattr(process.utils, 'space_available', lambda path: 1e9 * process.u.bit)
    with pytest.raises(IOError):
        process._tconvert_chunk_arg(_pass())
