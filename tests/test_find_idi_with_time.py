"""AIPS day numbers in find_idi_with_time count from the observation date (UV_DATA DATE-OBS).

Each FITS-IDI file has its own reference date in its DATE column (a file starting after midnight
refers to the next day), so counting from it shifted every time after midnight by one day.
"""
import datetime as dt

import numpy as np
from astropy.io import fits

from evn_postprocess.scripts import find_idi_with_time as find_idi

JD_0606 = 2461199.5  # 2026-06-08 00:00 UT


def _write_idi(path, date_jd, time_start, time_end, date_obs='2026-06-08'):
    cols = [fits.Column(name='DATE', format='D', array=np.array([date_jd, date_jd])),
            fits.Column(name='TIME', format='D', array=np.array([time_start, time_end]))]
    uvdata = fits.BinTableHDU.from_columns(cols, name='UV_DATA')
    if date_obs is not None:
        uvdata.header['DATE-OBS'] = date_obs
    fits.HDUList([fits.PrimaryHDU(), uvdata]).writeto(path)
    return str(path)


def _files(tmp_path, date_obs='2026-06-08'):
    # 22:00-23:30 on 8 June, 23:30-00:50 across midnight, 00:50-02:00 on 9 June (own DATE = 9 June)
    return [_write_idi(tmp_path / 'e_1_1.IDI1', JD_0606, 22/24, 23.5/24, date_obs),
            _write_idi(tmp_path / 'e_1_1.IDI2', JD_0606, 23.5/24, 1 + 50/1440, date_obs),
            _write_idi(tmp_path / 'e_1_1.IDI3', JD_0606 + 1, 50/1440, 2/24, date_obs)]


def test_time_after_midnight_in_file_starting_after_midnight(tmp_path):
    files = _files(tmp_path)
    assert find_idi.find_idi_with_time(files, aipstime=[1, 1, 31, 0], verbose=False) == files[2]


def test_times_across_the_observation(tmp_path):
    files = _files(tmp_path)
    assert find_idi.find_idi_with_time(files, aipstime=[0, 22, 30, 0], verbose=False) == files[0]
    assert find_idi.find_idi_with_time(files, aipstime=[1, 0, 10, 0], verbose=False) == files[1]
    assert find_idi.find_idi_with_time(files, aipstime=[1, 3, 0, 0], verbose=False) is None


def test_subset_of_files_still_counts_from_observation_date(tmp_path):
    files = _files(tmp_path)
    assert find_idi.find_idi_with_time(files[2:], aipstime=[1, 1, 31, 0], verbose=False) == files[2]


def test_falls_back_to_first_record_date_without_date_obs(tmp_path):
    files = _files(tmp_path, date_obs=None)
    assert find_idi.find_idi_with_time(files[:1], aipstime=[0, 22, 30, 0], verbose=False) == files[0]


def test_datetime_search(tmp_path):
    files = _files(tmp_path)
    assert find_idi.find_idi_with_time(files, datetime=dt.datetime(2026, 6, 9, 1, 31),
                                       verbose=False) == files[2]
