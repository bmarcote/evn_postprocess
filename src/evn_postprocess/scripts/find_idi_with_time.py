#!/usr/bin/python
import sys
import glob
import datetime as dt
from astropy.io import fits


def date2mjd(date):
  origin = dt.datetime(1858,11,17)
  return ((date-origin).days + (date-origin).seconds/86400.0)

def mjd2date(mjd):
  return (dt.datetime(1858,11,17) + dt.timedelta(mjd))

def jd2mjd(date):
    return date - 2400000.5

def get_timerange_in_ididata(datafits):
    """Returns the time range of the data recorded in this particular FITS data.

    Input
    -----
    datafits : astropy.io.fits.fitsrec.FITS_rec
        Data from a FITS-IDI file (obtained from e.g. astropy.io.fits.open(idifile)[#].data)

    Output
    ------
    starttime : datetime
        Starting time of the first scan in the file (in UT).
    endtime : datetime
        Ending time of the last scan in the file (in UT).
    """
    starttime = mjd2date(jd2mjd(datafits['DATE'][0] + datafits['TIME'][0]))
    return starttime, mjd2date(jd2mjd(datafits['DATE'][-1] + datafits['TIME'][-1]))


def get_reference_date(uvdata_hdu):
    """Returns the date that AIPS times (day number) are counted from: the observation start date.

    It is the DATE-OBS keyword of the UV_DATA table, which the correlator writes with the same
    value in every FITS-IDI file of an experiment. The DATE column cannot be used for this: each
    file has its own reference date (files starting after midnight refer to the next day), so
    counting from it would shift every time after midnight by one day in those files.
    It falls back to the date of the first record if DATE-OBS is missing.

    Input
    -----
    uvdata_hdu : astropy.io.fits.BinTableHDU
        The UV_DATA table of a FITS-IDI file.

    Output
    ------
    refdate : datetime.date
    """
    if (date_obs := uvdata_hdu.header.get('DATE-OBS')):
        return dt.date.fromisoformat(str(date_obs).strip()[:10])

    return get_timerange_in_ididata(uvdata_hdu.data)[0].date()




def find_idi_with_time(idi_files, datetime=None, aipstime=None, verbose=True):
    """Returns the name of the IDI file that contains data corresponding to the given time.

    Input
    -----
    idi_files : list
        Path to all the FITS-IDI files. e.g. ['exp_1_1.IDI1', 'exp_1_1.IDI2', ..]
    datetime : datetime (optional, but either datetime or time must be provided)
        Date and time (in UT) to search for in the FITS-IDI files.
    aipstime : list (optional, but either datetime or time must be provided)
        Time to search for in the FITS-IDI files, in a AIPS time format (list with D, H, M, S).
        The day is counted from the observation start date (see get_reference_date).
    verbose : bool (default: True)
        Print messages.

    Output
    ------
    idi_file : str
        Path to the FITS-IDI file that contains the given time.
        It returns None if none of the files contain the given time.
    """
    # idi_files = glob.glob(path_idi)
    for idi_file in idi_files:
        if verbose:
            print('Opening {} file'.format(idi_file))
        with fits.open(idi_file, mode='readonly') as hdul:
            inittime, endtime = get_timerange_in_ididata(hdul['UV_DATA'].data)
            refdate = get_reference_date(hdul['UV_DATA'])

        if datetime is not None:
            the_time = datetime
        elif aipstime is not None:
            the_time = dt.datetime.combine(refdate + dt.timedelta(days=aipstime[0]),
                                           dt.time(*aipstime[1:]))
        else:
            raise Exception('Either datetime or time must be provided')

        if inittime < the_time < endtime:
            if verbose:
                print('{} contains {} UT'.format(idi_file, the_time.strftime('%d-%m-%Y %H:%M')))

            return idi_file

    if verbose:
        print('None of the files contain the requested time.')

    return None


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Two parameters are required!')
        print('%proc IDI_files YYYY/mm/dd/HH:MM')
        print('- IDI_files: path to the FITS-IDI files (wildcards accepted e.g. exp_1_1.IDI*)')
        print('- YYYY/mm/dd/HH:MM: time in UT to search for in the files.')
        sys.exit(1)

    the_idi_files = sys.argv[1:-1]
    if (len(the_idi_files) == 1) and ('*' in the_idi_files[0] or '?' in the_idi_files[0]):
        the_idi_files = glob.glob(the_idi_files[0])
    the_time = dt.datetime.strptime(sys.argv[-1], '%Y/%m/%d/%H:%M')
    find_idi_with_time(the_idi_files, the_time, verbose=True)


