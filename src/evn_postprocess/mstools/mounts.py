from rich import print as rprint
from . import misc
from .msdata import Ms


def print_mounts(msfile: str, verbose: bool = True):
    """Print mount type information from all antennas in MS file.
    
    Displays station name and mount type (e.g., ALT-AZ, EQUATORIAL) for each antenna
    in the Measurement Set.
    
    Args:
        msfile (str): Path to Measurement Set file to read.
        verbose (bool): If True, print mount type for each antenna. Default True.
    """
    with misc.table(msfile) as ms:
        with misc.table(ms.getkeyword('ANTENNA')) as ant_table:
            mounts_dict = dict(zip(ant_table.getcol('STATION'), ant_table.getcol('MOUNT')))
            if verbose:
                print('\n'.join(f"{antenna}: {mount}" for antenna, mount in mounts_dict.items()))
    
    return mounts_dict


def modify_mounts(msfile: str, antenna: str, mount: str, verbose: bool = True) -> bool:
    """Fix mount type information for specified antenna in MS file.
    
    Updates the mount type (e.g., ALT-AZ, EQUATORIAL) for a specific antenna in the
    Measurement Set.
    
    Args:
        msfile (str): Path to Measurement Set file to modify.
        antenna (str): Antenna name to update (case insensitive).
        mount (str): New mount type to set (e.g., 'ALT-AZ', 'EQUATORIAL').
        verbose (bool): If True, print mount type for each antenna. Default True.
    """

    with misc.table(msfile, readonly=False) as ms:
        with misc.table(ms.getkeyword('ANTENNA'), readonly=False) as ant_table:
            stations = [i for i in ant_table.getcol('NAME')]
            mounts = ant_table.getcol('MOUNT')
            def getmount(stations: list[str], station: str, mounts: list[str]) -> str:
                """Returns the mount for the given stations
                """
                return mounts[stations.index(station)]

            # Function to get directly the position of a station in the array to get its mount
            try:
                if getmount(stations, antenna, mounts) == mount:
                    rprint(f"{antenna} has already the right mount ({mount})")
                else:
                    rprint(f"Changing {antenna} mount from {getmount(stations, antenna, mounts)} to {mount}")
                    mounts[stations.index(antenna)] = mount
            except ValueError:
                rprint(f"[bold red]{antenna} was not found in the MS while executing ysfocus[/bold red]")
                return False

            # In case no station has been found in the MS
            if len([ant for ant in stations if ant == antenna]) == 0:
                rprint(r"[yellow]{antenna} was found in the MS, no ysfocus.py required.[/yellow]")
            else:
                ant_table.putcol('MOUNT', mounts)
                ant_table.flush()
                rprint(f"[green]Fixed mount type for {antenna} to {mount}.[/green]")

    return True


def fix_yebes_mount(msfile: str, verbose: bool = True) -> bool:
    """Fix mount type information for the Yebes (Ys) antenna in the MS file.
    Changes the MOUNT field in the ANTENNA table for Ys to 'ALT-AZ-NASMYTH-RH'.
    It allows tConvert to put MNTSTA=4 into the FITS AN table, to handle the
    parallactic angle correction for Ys's Nasmyth focus correctly.

    It will check for 'Ys', 'YS', or 'YEBES40M' as name of the station.
    
    Args:
        msfile (str): Path to Measurement Set file to modify.
        verbose (bool): If True, print mount type for each antenna. Default True.
    
    Returns:
        bool: True if the mount type was successfully updated, False otherwise.
    """
    ms = Ms(msfile, runstats=False)
    ysname = [n for n in ('Ys', 'YS', 'YEBES40M') if n in ms.antennas.names]
    if len(ysname) == 0:
        raise ValueError('Ys antenna not found in the MS file.')

    return all([modify_mounts(msfile, ys, 'ALT-AZ-NASMYTH-RH', verbose=verbose) for ys in ysname])


def fix_hobart_mount(msfile: str, verbose: bool = True) -> bool:
    """Fix mount type information for the Hobart (Ho) antenna in the MS file.
    Changes Hobart X_YEW to X-YEW expected by tConvert (MNTSTA=3).
    It will check for 'HOBART', 'HO', or 'HOB_DBBC' as name of the station.
    
    Args:
        msfile (str): Path to Measurement Set file to modify.
        verbose (bool): If True, print mount type for each antenna. Default True.
    
    Returns:
        bool: True if the mount type was successfully updated, False otherwise.
    """
    ms = Ms(msfile, runstats=False)
    honame = [n for n in ('HOBART', 'HO', 'HOB_DBBC') if n in ms.antennas.names]
    if len(honame) == 0:
        raise ValueError('Ys antenna not found in the MS file.')

    return all([modify_mounts(msfile, ho, 'X-YEW', verbose=verbose) for ho in honame])