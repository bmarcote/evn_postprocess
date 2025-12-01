from rich import print as rprint
import misc
from msdata import Ms


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


def modify_mounts(msfile: str, antenna: str, mount: str, verbose: bool = True):
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
            ant_table.putcol('MOUNT', mount, rownumbers=ant_table.getcol('STATION').index(antenna))
            #TODO: to test 
            rprint("[yellow]THIS HAS NOT BEEN TESTED[/yellow]")
            if verbose:
                rprint(f"[green]Fixed mount type for {antenna} to {mount}.[/green]")


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

    for ys in ysname:
        modify_mounts(msfile, ys, 'ALT-AZ-NASMYTH-RH', verbose=verbose)
    
    return True


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

    for ho in honame:
        modify_mounts(msfile, ho, 'X-YEW', verbose=verbose)
    
    return True