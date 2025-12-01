import datetime
from enum import IntEnum
from pathlib import Path
from contextlib import contextmanager
from pyrap import tables as pt


class Stokes(IntEnum):
    """The Stokes types defined as in the enum class from casacore code.
    """
    Undefined = 0 # Undefined value
    I = 1 # standard stokes parameters  # noqa: E741
    Q = 2
    U = 3
    V = 4
    RR = 5 # circular correlation products
    RL = 6
    LR = 7
    LL = 8
    XX = 9 # linear correlation products
    XY = 10
    YX = 11
    YY = 12
    RX = 13 # mixed correlation products
    RY = 14
    LX = 15
    LY = 16
    XR = 17
    XL = 18
    YR = 19
    YL = 20
    PP = 21 # general quasi-orthogonal correlation products
    PQ = 22
    QP = 23
    QQ = 24
    RCircular = 25 # single dish polarization types
    LCircular = 26
    Linear = 27
    Ptotal = 28 # Polarized intensity ((Q^2+U^2+V^2)^(1/2))
    Plinear = 29 #  Linearly Polarized intensity ((Q^2+U^2)^(1/2))
    PFtotal = 30 # Polarization Fraction (Ptotal/I)
    PFlinear = 31 # linear Polarization Fraction (Plinear/I)
    Pangle = 32 # linear polarization angle (0.5  arctan(U/Q)) (in radians)


def mjd2date(mjd: float) -> datetime.datetime:
    """Returns the datetime for the given MJD date.
    """
    origin = datetime.datetime(1858, 11, 17)
    return origin + datetime.timedelta(mjd)


def date2mjd(date: datetime.datetime) -> float:
    """Returns the MJD day associated to the given datetime.
    """
    origin = datetime.datetime(1858, 11, 17)
    return  (date-origin).days + (date-origin).seconds/86400.0


def chunkert(first: int, last: int, chunk_size: int, verbose: bool = True):
    """Generate chunks for iterating through data in blocks.
    
    Args:
        first (int): Starting index.
        last (int): Ending index (exclusive).
        chunk_size (int): Chunk size.
        verbose (bool): Unused parameter, kept for compatibility. Default True.
    
    Yields:
        tuple: (start_index, number_of_rows) for each chunk.
    """
    while first < last:
        n = min(chunk_size, last - first)
        yield (first, n)
        first = first + n


def parse_time(time_str: str | None) -> datetime.datetime | None:
    """Parse a time string into a datetime object.
    It expects the time string to be in the format YYYY/MM/DD/hh:mm:ss or YYYY/DOY/hh:mm:ss. Where the seconds (ss) are optional.

    Args:
        time_str (str): The time string to parse.
        In the format YYYY/MM/DD/hh:mm:ss or YYYY/DOY/hh:mm:ss. Where the seconds (ss) are optional.
    Returns:
        datetime.datetime: The parsed datetime object.
    """
    if time_str is None:
        return None

    if time_str.count('/') not in (2,3):
        raise ValueError("Time string must be in the format YYYY/MM/DD/hh:mm[:ss] or YYYY/DOY/hh:mm[:ss]")

    return datetime.datetime.strptime(time_str, f"%Y/{'%j' if time_str.count('/') == 2 else '%m/%d'}/"
                                      f"%H:%M{':%S' if time_str.count(':') == 2 else ''}")


@contextmanager
def table(msfile: str | Path, readonly: bool = True, ack: bool = False):
    """Context manager for safely opening and closing pyrap table objects.
    
    Provides a safer interface than manually opening and closing tables,
    ensuring proper cleanup even if exceptions occur.
    
    Args:
        msfile (str | Path): Path to the MS file or table.
        readonly (bool): Open table in read-only mode. Default True.
        ack (bool): Acknowledge opening. Default False.
    
    Yields:
        pyrap.tables.table: Opened table object.
    """
    with pt.table(msfile if isinstance(msfile, str) else str(msfile), readonly=readonly, ack=ack) as ms:
        try:
            yield ms
        finally:
            # Just because I am not sure if casacore implements this automatically
            # Definitely NRAO folks did not do it in casatools. Really upsetting
            ms.close()


# def _open_close(msfile: str | Path, msobj, nomodify: bool = True, lock: str = 'default'):
#     try:
#         msobj.open(msfile if isinstance(msfile, str) else str(msfile), nomodify=nomodify,
#                      lockoptions={'option': lock})
#         yield msobj
#     finally:
#         msobj.done()
#         msobj.close()


# @contextmanager
# def ms(msfile: str | Path, nomodify: bool = True, lock: str = 'default'):
#     """Wrapper function that saves the user to do the shit thing that CASA developers coded,
#     of loading the ms() then open, and not being save of closing it."""
#     yield from _open_close(msfile, casatools.ms(msfile if isinstance(msfile, str) else str(msfile)),
#                            nomodify=nomodify, lock=lock)


# @contextmanager
# def msmd(msfile: str | Path, nomodify: bool = True, lock: str = 'default'):
#     """Wrapper function that saves the user to do the shit thing that CASA developers coded,
#     of loading the msmd() then open, and not being save of closing it."""
#     yield from _open_close(msfile, casatools.msmetadata(msfile if isinstance(msfile, str) else str(msfile)),
#                            nomodify=nomodify, lock=lock)