from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import numpy as np
from rich import progress
from rich import print as rprint
from . import misc


def get_polarizations(msfile: str | Path) -> tuple[misc.Stokes]:
    """Returns the polarizations available in the MS file.
    
    Args:
        msdata (str | Path): Path to Measurement Set file.
    
    Returns:
        stokes (tuple[misc.Stokes]): Tuple with the Stokes parameters
    """
    with misc.table(msfile, readonly=False) as ms:
        with misc.table(ms.getkeyword('POLARIZATION')) as ms_pol:
            return tuple([misc.Stokes(i) for i in ms_pol.getcol('CORR_TYPE')[0]])


def polswap(msfile: str | Path, antenna: str, starttime: dt.datetime | None = None,
            endtime: dt.datetime | None = None):
    """Swap polarizations for specified antennas in MS file.
    
    Fixes polarizations labeled incorrectly (R↔L or X↔Y). Modifies DATA, FLOAT_DATA, FLAG, 
    SIGMA_SPECTRUM, WEIGHT_SPECTRUM, WEIGHT, and SIGMA columns in the MS file.
    
    Args:
        msdata (str | Path): Path to Measurement Set file.
        antenna (str): Antenna name to swap polarizations (case insensitive).
        starttime (datetime.datetime, optional): Start time.
        endtime (datetime.datetime, optional): End time.
    
    Raises:
        ValueError: If polarization types are not circular nor linear.
    """
    def _get_nedded_move(products, ant_order: int):
        """Returns the transposing necessary to do a polswap in one of the stations.
        
        Args:
            products (2-D array-like): The CORR_PRODUCT of the MS data. Sets the mapping of the stokes given
                two stations. e.g. [[0, 0], [1, 1], [0, 1], [1, 0]] represents that there
                are four stokes products, where the first two rows are the direct hands
                between antenna 1 (first column) and antenna 2 (second column).
            ant_order (int): The position of the antenna in the CORR_PRODUCT (if the antenna to change
                is the ANT1 or ANT2, i.e. 0 or 1.

        Returns:
            1-D array-like: The transposition of the columns necessary to make a swap pol for the antenna
                specified in ant_order.
                e.g. for the case mentioned before, if ANT1 is the one that needs to be converted,
                then the output 'changes' is [3, 2, 1, 0], as the stokes wanted at the end are:
                [[1, 0], [0, 1], [1, 1], [0, 0]]
        """
        pols_prod = list([list(j) for j in products])
        temp = np.copy(products)
        temp[:,ant_order] = products[:,ant_order] ^ 1
        pols_prod_mod = list([list(j) for j in temp])
        return np.array([pols_prod.index(i) for i in pols_prod_mod])

    with misc.table(msfile, readonly=False) as ms:
        changes = None
        with misc.table(ms.getkeyword('ANTENNA')) as ms_ant:
            antenna_number = [i.upper() for i in ms_ant.getcol('NAME')].index(antenna.upper())

        with misc.table(ms.getkeyword('POLARIZATION')) as ms_pol:
            pols_order = [misc.Stokes(i) for i in ms_pol.getcol('CORR_TYPE')[0]]
            # Only change it if circular or linear pols.
            for a_pol_order in pols_order:
                if (a_pol_order not in (misc.Stokes.RR, misc.Stokes.RL, misc.Stokes.LR, misc.Stokes.LL)) and \
                   (a_pol_order not in (misc.Stokes.XX, misc.Stokes.XY, misc.Stokes.YX, misc.Stokes.YY)) and \
                   (a_pol_order not in (misc.Stokes.RX, misc.Stokes.RY, misc.Stokes.LX, misc.Stokes.LY)) and \
                   (a_pol_order not in (misc.Stokes.XR, misc.Stokes.XL, misc.Stokes.YR, misc.Stokes.YL)):
                    rprint("[red bold]Polswap only works for circular or linear pols[/red bold]")
                    rprint(f"[ref]These data contain the following stokes: {pols_order}[/red]")
                    raise ValueError('Wrong stokes type.')

            pols_prod = ms_pol.getcol('CORR_PRODUCT')[0]
            changes = [_get_nedded_move(pols_prod, i) for i in (0, 1)]

        with misc.table(ms.getkeyword('OBSERVATION')) as ms_obs:
            time_range = (dt.datetime(1858, 11, 17, 0, 0, 2) + ms_obs.getcol('TIME_RANGE')*dt.timedelta(seconds=1))[0]

        if starttime is not None:
            datetimes_start = starttime
        else:
            datetimes_start = time_range[0] - dt.timedelta(seconds=1)
        if endtime is not None:
            datetimes_end = endtime
        else:
            datetimes_end = time_range[1] + dt.timedelta(seconds=1)

        # shapes of DATA, FLOAT_DATA, FLAG, SIGMA_SPECTRUM, WEIGHT_SPECTRUM: (nrow, npol, nfreq)
        # shapes of WEIGHT, SIGMA: (nrow, npol)
        # Only leave the ones that are in the MS. Not all of them are always present.
        columns = [a_col for a_col in ('DATA', 'FLOAT_DATA', 'FLAG', 'SIGMA_SPECTRUM', 'WEIGHT_SPECTRUM',
                   'WEIGHT', 'SIGMA') if a_col in ms.colnames()]
        print('\nThe following columns will be modified: {}.\n'.format(', '.join(columns)))
        with progress.Progress() as progress_bar:
            task = progress_bar.add_task("[green]Processing...", total=len(ms))
            for (start, nrow) in misc.chunkert(0, len(ms), 100):
                progress_bar.update(task, advance=nrow)
                for changei, antpos in zip(changes, ('ANTENNA1','ANTENNA2')):
                    ants = ms.getcol(antpos, startrow=start, nrow=nrow)
                    datetimes = dt.datetime(1858, 11, 17, 0, 0, 2) + \
                                ms.getcol('TIME', startrow=start, nrow=nrow)*dt.timedelta(seconds=1)
                    cond = np.where((ants == antenna_number) & (datetimes > datetimes_start) & \
                                    (datetimes < datetimes_end))
                    if len(cond[0]) > 0:
                        for a_col in columns:
                            ms_col = ms.getcol(a_col, startrow=start, nrow=nrow)
                            if len(ms_col.shape) == 3:
                                ms_col[cond,] = ms_col[cond,][:,:,:,changei,]
                            elif len(ms_col.shape) == 2:
                                ms_col[cond,] = ms_col[cond,][:,:,changei,]
                            elif len(ms_col.shape) == 1:
                                ms_col[cond,] = ms_col[cond,][:,changei,]
                            else:
                                raise ValueError('Unexpected dimensions for {} column.'.format(a_col))
                            ms.putcol(a_col, ms_col, startrow=start, nrow=nrow)

        print(f"[green]\n{msfile} modified correctly.[/green]")


def copy_pol(msfile: str | Path, antenna: str, polfrom: str):
    """Copy the data from one polarization to the other one for a particular antenna.
    It is useful when a polarization is down or wrong, and you still want to recover Intensity maps.
    Specially knowing that CASA does not like having single-pol antennas in many of their functions.
    
    Args:
        msfile (str | Path): Path to the Measurement Set file.
        antenna (str): Antenna name to copy polarizations (case insensitive).
        polfrom (str): Polarization to copy from ('R', 'L', 'X', or 'Y', case insensitive).
    """
    rprint("[bold yellow]copy_pol has not been tested yet.[/bold yellow]")
    if not all([polfrom.lower() in ('r', 'l', 'x', 'y')]):
        raise ValueError(f"'polfrom' must be R, L, X, or Y. But is {polfrom}.")
    
    # It only works with linear or circular polarization information
    with misc.table(msfile, readonly=False) as ms:
        with misc.table(ms.getkeyword('ANTENNA')) as ms_ant:
            antenna_number = [i.upper() for i in ms_ant.getcol('NAME')].index(antenna.upper())

        with misc.table(ms.getkeyword('POLARIZATION')) as ms_pol:
            pols = [misc.Stokes(i) for i in ms_pol.getcol('CORR_TYPE')[0]]
            if not all([5 <= p <= 20 for p in pols]):
                rprint("[red bold]copy_pol only works for circular or linear pols[/red bold]")
                rprint(f"[red]These data contain the following stokes: {pols}[/red]")
                raise ValueError('Wrong stokes type.')
            
            pols_prod = ms_pol.getcol('CORR_PRODUCT')[0]

        # Determine which polarizations to copy based on polfrom
        polfrom_upper = polfrom.upper()
        
        # Map the polfrom to the corresponding Stokes parameters
        # For circular: R=0, L=1; For linear: X=0, Y=1
        if polfrom_upper in ('R', 'X'):
            pol_index = 0
        else:  # 'L' or 'Y'
            pol_index = 1
        
        # Determine copy operations based on antenna position
        # copy_map[antenna_position] = [(source_pol_idx, target_pol_idx), ...]
        # antenna_position: 0 for ANTENNA1, 1 for ANTENNA2
        copy_map = {}
        
        for ant_pos in [0, 1]:  # 0=ANTENNA1, 1=ANTENNA2
            copies = []
            for pol_idx, pol_prod in enumerate(pols_prod):
                # pol_prod is [ant1_pol, ant2_pol]
                if pol_prod[ant_pos] == pol_index:
                    # This polarization product has the source pol at this antenna position
                    # Find the corresponding cross-pol to copy to
                    target_prod = pol_prod.copy()
                    target_prod[ant_pos] = 1 - pol_index  # Flip the polarization
                    
                    # Find the index of the target polarization product
                    for target_idx, target_pol_prod in enumerate(pols_prod):
                        if np.array_equal(target_pol_prod, target_prod):
                            copies.append((pol_idx, target_idx))
                            break
            
            copy_map[ant_pos] = copies
        
        columns = [a_col for a_col in ('DATA', 'FLOAT_DATA', 'FLAG', 'SIGMA_SPECTRUM', 'WEIGHT_SPECTRUM', \
                   'WEIGHT', 'SIGMA') if a_col in ms.colnames()]
        print(f'\nThe following columns will be modified: {", ".join(columns)}.\n')
        with progress.Progress() as progress_bar:
            task = progress_bar.add_task("[green]Processing...", total=len(ms))
            for (start, nrow) in misc.chunkert(0, len(ms), 100):
                progress_bar.update(task, advance=nrow)
                
                for ant_pos, antcol in enumerate(['ANTENNA1', 'ANTENNA2']):
                    ants = ms.getcol(antcol, startrow=start, nrow=nrow)
                    cond = np.where(ants == antenna_number)[0]
                    
                    if len(cond) > 0:
                        for a_col in columns:
                            ms_col = ms.getcol(a_col, startrow=start, nrow=nrow)
                            for src_pol, tgt_pol in copy_map[ant_pos]:
                                if len(ms_col.shape) == 3:  # (nrow, npol, nfreq)
                                    ms_col[cond, tgt_pol, :] = ms_col[cond, src_pol, :]
                                elif len(ms_col.shape) == 2:  # (nrow, npol)
                                    ms_col[cond, tgt_pol] = ms_col[cond, src_pol]
                                else:
                                    raise ValueError(f'Unexpected dimensions for {a_col} column.')
                            
                            ms.putcol(a_col, ms_col, startrow=start, nrow=nrow)
        
        print(f"[green]\n{msfile} modified correctly.[/green]")


def scale1bit(msfile: str | Path, antenna: str | list[str], undo: bool = False, scale_weights: bool = True):
    """Scales 1-bit data to correct for quantization losses.
    
    When one or both antennas in a baseline use 1-bit sampling, the data needs to be scaled
    to correct for quantization losses. The scaling factor depends on whether one or both
    antennas use 1-bit sampling.
    
    Args:
        msfile (str | Path): Path to the Measurement Set file.
        antenna (str | list[str]): Antenna name or list of antenna names to scale.
        undo (bool, optional): If True, undo the scaling. Defaults to False.
        scale_weights (bool, optional): If True, also scale WEIGHT column. Defaults to False.
    """
    # Factors taken from the old Glish program (updated on 20 Nov 2015)
    # factor1b1b: both antennas are 1-bit
    # factor1b2b: only one antenna is 1-bit
    factor1b1b = np.pi/2.0/1.1329552
    factor1b2b = np.sqrt(factor1b1b)
    if isinstance(antenna, str):
        antenna = [antenna]
    
    with misc.table(msfile, readonly=False) as ms:
        with misc.table(ms.getkeyword('ANTENNA')) as ms_ant:
            antenna_names = [name.upper() for name in ms_ant.getcol('NAME')]
            antenna_numbers = []
            for ant in antenna:
                try:
                    antenna_numbers.append(antenna_names.index(ant.upper()))
                except ValueError:
                    raise ValueError(f"Antenna '{ant}' not found in MS. Available antennas: {antenna_names}")
        
        factor_both = 1.0/factor1b1b if undo else factor1b1b
        factor_one = 1.0/factor1b2b if undo else factor1b2b
        columns_to_scale = ['DATA']
        if scale_weights and 'WEIGHT' in ms.colnames():
            columns_to_scale.append('WEIGHT')
        
        columns_to_scale = [col for col in columns_to_scale if col in ms.colnames()]
        with progress.Progress() as progress_bar:
            task = progress_bar.add_task("[green]1-bit scaling...", total=len(ms))
            for (start, nrow) in misc.chunkert(0, len(ms), 100):
                progress_bar.update(task, advance=nrow)
                ant1 = ms.getcol('ANTENNA1', startrow=start, nrow=nrow)
                ant2 = ms.getcol('ANTENNA2', startrow=start, nrow=nrow)
                ant1_is_1bit = np.isin(ant1, antenna_numbers)
                ant2_is_1bit = np.isin(ant2, antenna_numbers)
                # Only process baselines where at least one antenna is 1-bit and ant1 != ant2
                both_1bit = ant1_is_1bit & ant2_is_1bit & (ant1 != ant2)
                one_1bit = (ant1_is_1bit ^ ant2_is_1bit) & (ant1 != ant2)
                if np.any(both_1bit) or np.any(one_1bit):
                    for col in columns_to_scale:
                        data = ms.getcol(col, startrow=start, nrow=nrow)
                        if np.any(both_1bit):
                            data[both_1bit] *= factor_both

                        if np.any(one_1bit):
                            data[one_1bit] *= factor_one
                        
                        ms.putcol(col, data, startrow=start, nrow=nrow)
        
        print(f"[green]\n1-bit {"unscaled" if undo else "scaled"} for "
              f"{', '.join(antenna)} in {msfile} done.[/green]")


def invert_subband(msfile: str | Path, antenna: str | list[str], starttime: dt.datetime | None = None, 
                   endtime: dt.datetime | None = None):
    """Inverts the frequency subbands for a given antenna in the MS file.
    
    This reverses the frequency axis for all baselines involving the specified antenna(s).
    Useful for correcting frequency ordering issues.
    
    Args:
        msfile (str | Path): Path to the Measurement Set file.
        antenna (str | list[str]): Antenna name or list of antenna names to invert subbands.
        starttime (datetime.datetime, optional): Start time to apply the inversion.
        endtime (datetime.datetime, optional): End time to apply the inversion.
    """
    if isinstance(antenna, str):
        antenna = [antenna]
    
    with misc.table(msfile, readonly=False) as ms:
        with misc.table(ms.getkeyword('ANTENNA')) as ms_ant:
            antenna_names = [name.upper() for name in ms_ant.getcol('NAME')]
            antenna_numbers = []
            for ant in antenna:
                try:
                    antenna_numbers.append(antenna_names.index(ant.upper()))
                except ValueError:
                    raise ValueError(f"Antenna '{ant}' not found in MS. Available antennas: {antenna_names}")

        with misc.table(ms.getkeyword('OBSERVATION')) as ms_obs:
            time_range = (dt.datetime(1858, 11, 17, 0, 0, 2) + 
                         ms_obs.getcol('TIME_RANGE')*dt.timedelta(seconds=1))[0]

        if starttime is not None:
            datetimes_start = starttime
        else:
            datetimes_start = time_range[0] - dt.timedelta(seconds=1)

        if endtime is not None:
            datetimes_end = endtime
        else:
            datetimes_end = time_range[1] + dt.timedelta(seconds=1)

        columns = [a_col for a_col in ('DATA', 'FLOAT_DATA', 'FLAG', 'SIGMA_SPECTRUM', 
                                        'WEIGHT_SPECTRUM', 'WEIGHT', 'SIGMA') 
                   if a_col in ms.colnames()]
        print(f'\nThe following columns will be modified: {", ".join(columns)}.\n')

        with progress.Progress() as progress_bar:
            task = progress_bar.add_task("[green]Inverting subbands...", total=len(ms))
            for (start, nrow) in misc.chunkert(0, len(ms), 100):
                progress_bar.update(task, advance=nrow)
                
                for antcol in ('ANTENNA1', 'ANTENNA2'):
                    ants = ms.getcol(antcol, startrow=start, nrow=nrow)
                    datetimes = dt.datetime(1858, 11, 17, 0, 0, 2) + \
                               ms.getcol('TIME', startrow=start, nrow=nrow)*dt.timedelta(seconds=1)
                    cond = np.where(np.isin(ants, antenna_numbers) & 
                                   (datetimes > datetimes_start) & 
                                   (datetimes < datetimes_end))[0]
                    if len(cond) > 0:
                        for a_col in columns:
                            ms_col = ms.getcol(a_col, startrow=start, nrow=nrow)
                            if len(ms_col.shape) == 3:  # (nrow, npol, nfreq)
                                ms_col[cond, :, :] = ms_col[cond, :, ::-1]
                            elif len(ms_col.shape) == 2:  # (nrow, npol) or (nrow, nfreq)
                                ms_col[cond, :] = ms_col[cond, ::-1]
                            else:
                                raise ValueError(f'Unexpected dimensions for {a_col} column.')

                            ms.putcol(a_col, ms_col, startrow=start, nrow=nrow)

    print(f"[green]\nSubbands inverted for {', '.join(antenna)} in {msfile}.[/green]")



def flag_weights(msfile: str | Path, threshold: float, apply: bool = True) -> tuple[int, float, float]:
    """Flag data based on weight threshold.
    
    Flags visibilities where the weight is below a specified threshold.
    This is useful for removing low-quality data from the dataset.
    
    Args:
        msfile (str | Path): Path to the Measurement Set file.
        threshold (float): Weight threshold below which data will be flagged. In the interval (0, 1).
        apply (bool, optional): If True, apply the flags to the MS. If False, only report statistics.
        Defaults to True.

    Returns:
        tuple[int, float, float]: Tuple containing the total number of visibilities, the percentage of visibilities
                                  flagged, and the percentage of visibilities with non-zero weights flagged.
    """
    if threshold <= 0 or threshold >= 1:
        raise ValueError("Threshold must be in the interval (0, 1).")

    @dataclass
    class Flagged:
        """Internal class to track flagging statistics.
        
        Attributes:
            before (int): Number of visibilities flagged before processing.
            after (int): Number of visibilities flagged after processing.
            nonzero (int): Number of non-zero weight visibilities flagged.
            total (int): Total number of visibilities processed (different from nvis if multiple polarizations/spw/etc).
        """
        before: int = 0
        after: int = 0
        nonzero: int = 0
        total: int = 0 # note that total is different from nvis if there are multiple polarizations/spw/etc

    with misc.table(msfile, readonly=False) as ms:
        flagged = Flagged()
        flagged_nonzero_after = 0
        # WEIGHT: (nrow, npol)
        # WEIGHT_SPECTRUM: (nrow, npol, nfreq)
        # flags[weight < threshold] = True
        weightcol = 'WEIGHT_SPECTRUM' if 'WEIGHT_SPECTRUM' in ms.colnames() else 'WEIGHT'
        transpose = (lambda x:x) if weightcol == 'WEIGHT_SPECTRUM' else (lambda x: x.transpose((1, 0, 2)))
        with progress.Progress() as progress_bar:
            task = progress_bar.add_task("[green]Flagging weights...", total=len(ms))
            for (start, nrow) in misc.chunkert(0, len(ms), 100):
                progress_bar.update(task, advance=nrow)
                # shape: (nrow, npol, nfreq)
                flags = transpose(ms.getcol("FLAG", startrow=start, nrow=nrow))
                flagged.total += int(np.prod(flags.shape))
                # count how much data is already flagged
                flagged.before += np.sum(flags)
                # extract weights and compute new flags based on threshold
                weights = ms.getcol(weightcol, startrow=start, nrow=nrow)
                # how many non-zero did we flag
                # join with existing flags and count again
                flags = np.logical_or(flags, weights < threshold)
                flagged.after += np.sum(flags)
                flagged_nonzero_after = np.logical_and(flags, weights > 0.001)
                # Saving the total of nonzero flags (in this and previous runs)
                # flagged_nonzero += np.sum(np.logical_xor(flagged_nonzero_before, flagged_nonzero_after))
                flagged.nonzero += np.sum(flagged_nonzero_after)
                # one thing left to do: write the updated flags to disk
                #flags = ms.putcol("FLAG", flags.transpose((1, 0 , 2)), startrow=start, nrow=nrow)
                if apply:
                    ms.putcol("FLAG", transpose(flags), startrow=start, nrow=nrow)

        pct_total = 100.0 * flagged.after / flagged.total if flagged.total > 0 else 0.0
        pct_new = 100.0 * (flagged.after - flagged.before) / flagged.total if flagged.total > 0 else 0.0
        pct_nonzero = 100.0 * flagged.nonzero / flagged.total if flagged.total > 0 else 0.0
        print(f"\nGot {flagged.total:11} visibilities ({flagged.before}, {flagged.after})")
        print(f"Got {flagged.after - flagged.before:11} visibilities to flag using threshold {threshold}\n")
        print(f"{pct_total:.2f}% total vis. flagged ({pct_new:.2f}% to flag in this execution).")
        print(f"{pct_nonzero:.2f}% data with non-zero weights flagged.\n")
        
        if apply:
            rprint('[green]Flags have been applied.[/green]')
        else:
            rprint('[yellow]Flags have not been applied (dry run).[/yellow]')
        
        return (flagged.total, pct_total, pct_nonzero)


def change_project_name(msfile: str | Path, new_name: str):
    """Changes the project name in the MS file.
    
    Args:
        msfile (str | Path): Path to the Measurement Set file.
        new_name (str): New project name.
    """
    with misc.table(msfile, readonly=True) as ms:
        with misc.table(ms.getkeyword('OBSERVATION'), readonly=False) as ms_obs:
            old_projectname = ms_obs.getcol('PROJECT')[0]  # should always be one-element list
            ms_obs.putcol('PROJECT', [new_name])
            ms_obs.flush()

            # Sometimes it can happen that the OBSERVER is also the PROJECT, although not always
            if ms_obs.getcol('OBSERVER')[0] == old_projectname:
                ms_obs.putcol('OBSERVER', [new_name])
    
    rprint(f'[green]Project name changed from {old_projectname} to {new_name} in {msfile}.[/green]')


def change_source_name(msfile: str | Path, oldname: str, newname: str):
    """Changes the name of a given source in the MS file.

    Args:
        msfile (str | Path): Path to the Measurement Set file.
        oldname (str): Existing source name.
        newname (str): New source name.
    """
    with misc.table(msfile, readonly=True) as ms:
        with misc.table(ms.getkeyword('FIELD'), readonly=False) as ms_field:
            srcnames = ms_field.getcol('NAME')
            try:
                idx = srcnames.index(oldname)
            except ValueError:
                rprint(f"[bold red]ERROR: [/bold red] [red]The source {oldname} is not present in the MS[/red]")
                rprint(f"[red]The only source names found are: {', '.join(srcnames)}[/red]")
                return
            srcnames[idx] = newname
            # The previous implementation modified the list in memory but never wrote it
            # back, so the rename silently did nothing. Persist it to disk.
            ms_field.putcol('NAME', srcnames)
            ms_field.flush()

    rprint(f"[green]Source '{oldname}' renamed to '{newname}' in {msfile}.[/green]")