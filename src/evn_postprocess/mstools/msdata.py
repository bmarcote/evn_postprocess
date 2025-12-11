from dataclasses import dataclass
from pathlib import Path
import datetime as dt
import functools
import json
from collections import defaultdict
from typing import overload, Self
import numpy as np
from astropy import units as u
from astropy import coordinates as coord
import blessed
from rich import progress
from pyrap import tables as pt
from . import misc
from . import operations

@dataclass
class ObsEpoch:
    """Represents an observation epoch with start and end times.
    
    Attributes:
        starttime (datetime.datetime): Observation start time.
        endtime (datetime.datetime): Observation end time.
    """
    starttime: dt.datetime
    endtime: dt.datetime
    
    @property
    def epoch(self) -> dt.date:
        """Date of the observation (from starttime).
        
        Returns:
            datetime.date: Observation date.
        """
        return self.starttime.date()

    @property
    def ymd(self) -> str:
        """Date formatted as YYYYMMDD string.
        
        Returns:
            str: Date in YYYYMMDD format.
        """
        return self.epoch.strftime("%Y%m%d")
    
    @property
    def mjd(self) -> float:
        """Modified Julian Date of the observation start.
        
        Returns:
            float: MJD value.
        """
        return misc.date2mjd(self.starttime)
    
    @property
    def doy(self) -> int:
        """Day of year of the observation.
        
        Returns:
            int: Day of year (1-366).
        """
        return int(self.epoch.strftime("%j"))
    
    @property
    def duration(self) -> u.Quantity:
        """Duration of the observation.
        
        Returns:
            astropy.units.Quantity: Observation duration in hours.
        """
        return ((self.endtime - self.starttime).total_seconds() * u.s).to(u.h)


@dataclass
class Source:
    """Represents an astronomical source/field in an observation.
    
    Attributes:
        name (str): Source name.
        coordinates (astropy.coordinates.SkyCoord): Source sky coordinates.
        intent (str | None): Observation intent (e.g., 'TARGET', 'CALIBRATOR'). Optional.
    """
    name: str
    coordinates: coord.SkyCoord
    intent: str | None = None


class Sources(list):
    """Container class for Source objects, providing convenient access by name or index."""
    
    def __init__(self, *args):
        """Initialize Sources list with optional Source objects.
        
        Args:
            *args: Variable number of Source objects.
        """
        super().__init__(args)

    @overload
    def __getitem__(self, item: int) -> Source: ...
    
    @overload
    def __getitem__(self, item: str) -> Source: ...
    
    @overload
    def __getitem__(self, item: slice) -> list[Source]: ...
    
    def __getitem__(self, item: int | str | slice) -> Source | list[Source]:
        """Get a source by name or index.
        
        Args:
            item (int | str): Source index or name.
        
        Returns:
            Source: The requested source.
        
        Raises:
            TypeError: If item is neither int nor str.
        """
        match item:
            case str():
                return super().__getitem__(self.names.index(item))
            case int() | slice():
                return super().__getitem__(item)
            case _:
                raise TypeError(f"Unsupported type {type(item)} for item index (should be int, str, or slice).")

    def __contains__(self, item: object) -> bool:
        """Check if a source name or Source object is in the list.
        
        Args:
            item (str | Source): Source name or Source object to check.
        
        Returns:
            bool: True if the source is in the list.
        """
        if isinstance(item, str):
            return any(s.name == item for s in self)

        return super().__contains__(item)
    
    def __str__(self) -> str:
        """Return string representation of Sources.
        
        Returns:
            str: String showing all source names.
        """
        return f"Sources < {', '.join(self.names)} >"

    @property
    def names(self) -> list[str]:
        """List of all source names.
        
        Returns:
            list[str]: Source names.
        """
        return [s.name for s in self]

    @property
    def coordinates(self) -> list[coord.SkyCoord]:
        """List of all source coordinates.
        
        Returns:
            list[astropy.coordinates.SkyCoord]: Source sky coordinates.
        """
        return [s.coordinates for s in self]

    @property
    def intents(self) -> list[str | None]:
        """List of all source intents.
        
        Returns:
            list[str | None]: Source observation intents.
        """
        return [s.intent for s in self]


@dataclass
class Antenna:
    """Defines an antenna.
    It has three parameters:
        name : str
            Name of the antenna
        observed : bool
            If the antenna has observed (has no-null data).
        subbands : tuple
            Tuple with the subbands numbers where the antenna observed.
            It may be all subbands covered in the observation or a subset of them.
    """
    name: str
    scheduled: bool = True
    observed: bool = True
    subbands: tuple = ()
    weights: tuple = ()
    polswap: bool = False
    polconvert: bool = False
    onebit: bool = False
    logfsfile: bool = False
    antabfsfile: bool = False
    opacity: bool = False  # if data have opacity correction in the ANTAB file

class Antennas(list):
    """Container class for Antenna objects, providing convenient access by name or index."""
    
    def __init__(self, *args):
        """Initialize Antennas list with optional Antenna objects.
        
        Args:
            *args: Variable number of Antenna objects.
        """
        super().__init__(args)

    @overload
    def __getitem__(self, item: str) -> Antenna: ...
    
    @overload
    def __getitem__(self, item: int) -> Antenna: ...
    
    @overload
    def __getitem__(self, item: slice) -> list[Antenna]: ...
    
    def __getitem__(self, item: str | int | slice) -> Antenna | list[Antenna]:
        """Get an antenna by name or index.
        
        Args:
            item (str | int | slice): Antenna name, index, or slice.
        
        Returns:
            Antenna | list[Antenna]: The requested antenna(s).
        
        Raises:
            KeyError: If antenna name is not found.
        """
        if isinstance(item, str):
            for antenna in self:
                if antenna.name == item:
                    return antenna
            raise KeyError(f"Antenna '{item}' not found")
        
        return super().__getitem__(item)

    def __contains__(self, item: str | Antenna) -> bool:
        """Check if an antenna name or Antenna object is in the list.
        
        Args:
            item (str | Antenna): Antenna name or Antenna object to check.
        
        Returns:
            bool: True if the antenna is in the list.
        """
        if isinstance(item, str):
            return any(s.name == item for s in self)

        return super().__contains__(item)
    
    def __str__(self) -> str:
        """Return string representation of Antennas.
        
        Returns:
            str: String showing all antenna names.
        """
        return f"Antennas < {', '.join(self.names)} >"

    @property
    def names(self) -> list[str]:
        """List of all antenna names.
        
        Returns:
            list[str]: Antenna names.
        """
        return [s.name for s in self]
    
    @property
    def observed(self) -> list[str]:
        """List of names of antennas that actually observed.
        
        Returns:
            list[str]: Names of antennas that have observed data.
        """
        return [s.name for s in self if s.observed]
    
    @property
    def subbands(self) -> list[tuple[int]]:
        """List of subband tuples for antennas that observed.
        
        Returns:
            list[tuple[int]]: Subband numbers for each observing antenna.
        """
        return [s.subbands for s in self if s.observed]
    

@dataclass
class Scan:
    """Defines a scan in the experiment."""
    scanno: int
    starttime: dt.datetime
    duration_s: int
    source: str
    stations: tuple[str]


class Scans(list[Scan]):
    """A list of scans in the experiment."""
    pass


@dataclass
class FreqSetup:
    """Defines the frequency setup of a given observation with the following data:
        - meanfreq : astropy.units.Quantity
            Mean frequency of the observation.
        - bandwidth : astropy.units.Quantity
            Total bandwidth of the observation.
        - nspw :  int
            Number of subbands.
        - nchan : int
            Number of channels per subband.
        - polarizations : tuple[misc.Stokes]
            Tuple of polarization/Stokes parameters in the observation.
    """
    meanfreq: u.Quantity
    bandwidth: u.Quantity
    nspw: int
    nchan: int
    polarizations: tuple[misc.Stokes]


class OperationsProxy:
    """Proxy class that binds msfile to all operations module functions."""
    
    def __init__(self, msfile: Path):
        """Initialize the OperationsProxy with an MS file path.
        
        Args:
            msfile (Path): Path to the Measurement Set file.
        """
        self._msfile = msfile
    
    def __getattr__(self, name: str):
        """Intercept attribute access and wrap operations functions with bound msfile.
        
        Args:
            name (str): Name of the operation function to access.
        
        Returns:
            functools.partial: Operation function with msfile pre-bound as first argument.
        
        Raises:
            AttributeError: If the operation does not exist in the operations module.
        """
        if hasattr(operations, name):
            func = getattr(operations, name)
            if callable(func):
                return functools.partial(func, self._msfile)
        raise AttributeError(f"module 'operations' has no attribute '{name}'")


class Ms:
    """Measurement Set wrapper class with lazy-loading metadata.
    Reads MS metadata on-demand when properties are accessed for the first time.
    
    Attributes:
        msfile (str): Path to the Measurement Set file.
        freqsetup (FreqSetup): Frequency setup of the observation.
        antennas (Antennas): Antennas in the observation.
        sources (Sources): Sources/fields in the observation.
        epoch (ObsEpoch): Observation time range.
        operations (OperationsProxy): Access to operations functions with msfile pre-bound.
    
    Example:
        >>> ms = Ms('mydata.ms')
        >>> ms.print_blessed()  # summary of the MS information in the terminal screen
        >>> ms.antenna   # lists all antennas in the MS.
        >>> ms.operations.polswap('antenna1')  # msfile is automatically passed
        >>> ms.operations.scale1bit(['Ef', 'Wb'])
        >>> ms.operations.flag_weights(threshold=0.1)
    """
    _freqsetup: FreqSetup
    _antennas: Antennas
    _sources: Sources
    _obsepoch: ObsEpoch
    _projectname: str
    scans: dict[int, set[str]] = {}
    def __init__(self, msfile: str | Path, runstats: bool = False):
        """Initialize Ms object with path to MS file.
        
        Args:
            msfile (str): Path to the Measurement Set file.
            runstats (bool): If True, runs the stats tool on the MS file, it may take a while (default False).
        """
        self._msfile: Path = msfile if isinstance(msfile, Path) else Path(msfile)
        
        if not self._msfile.exists():
            raise FileNotFoundError(f"Measurement Set file {self._msfile} not found.")

        self.operations = OperationsProxy(self._msfile)
        self.get_msmetadata()
        if runstats:
            self.run_stats()

    def get_msmetadata(self):
        """Read metadata from the MS file and populate internal attributes.
        
        Reads frequency setup, antennas, sources, and observation epoch from the MS.
        Only runs once - subsequent calls are ignored if metadata already loaded.
        """
        with misc.table(self.msfile) as msdata:
            with misc.table(msdata.getkeyword('DATA_DESCRIPTION')) as spw_table:
                spw_names = tuple(spw_table.getcol('SPECTRAL_WINDOW_ID'))
            
            with misc.table(msdata.getkeyword('SPECTRAL_WINDOW')) as spw_table:
                n_channels = spw_table.getcol('NUM_CHAN')[0]
                chan_freqs = spw_table.getcol('CHAN_FREQ') * u.Hz
                total_bw = spw_table.getcol('TOTAL_BANDWIDTH')[0] * u.Hz
                self._freqsetup = FreqSetup(meanfreq=np.mean(chan_freqs), bandwidth=total_bw,
                                            nspw=len(spw_names), nchan=n_channels,
                                            polarizations=operations.get_polarizations(self.msfile))
            
            with misc.table(msdata.getkeyword('ANTENNA')) as ant_table:
                ant_names = ant_table.getcol('NAME')
                self._antennas = Antennas()
                for ant_name in ant_names:
                    self._antennas.append(Antenna(name=ant_name, observed=True,
                                          subbands=tuple(int(spw) for spw in spw_names)))
            
            with misc.table(msdata.getkeyword('FIELD')) as field_table:
                src_names = field_table.getcol('NAME')
                src_coords = field_table.getcol('PHASE_DIR')
                self._sources = Sources()
                for src_name, src_ra, src_dec in zip(src_names, src_coords[0,0,:], src_coords[1,0,:]):
                    self._sources.append(Source(name=src_name,
                                                coordinates=coord.SkyCoord(src_ra, src_dec, unit=(u.rad, u.rad))))
            
            with misc.table(msdata.getkeyword('OBSERVATION')) as obs_table:
                time_range = obs_table.getcol('TIME_RANGE')
                # This is to avoid some corner cases where the shape is (2,1) or (1,2)
                indx = time_range.shape[0]
                origin = dt.datetime(1858, 11, 17, 0, 0, 2)
                starttime = origin + dt.timedelta(seconds=float(time_range[0, 0]))
                endtime = origin + dt.timedelta(seconds=float(time_range[indx-1, indx % 2]))
                self._obsepoch = ObsEpoch(starttime=starttime, endtime=endtime)
                self._projectname = obs_table.getcol('PROJECT')[0]  # should always be one-element list
            
    def run_stats(self, chunkert: int = 100):
        """Runs basic statistics over the MS data.

        Computes:
        - For each scan, which antennas participated (auto-correlations with |DATA| > 1e-5).
        - For each antenna, which spectral windows (SPWs) were observed (same criterion).
        - For each antenna, global weight statistics in the following bins:
          <1e-3, [1e-3,0.2), [0.2,0.4), [0.4,0.6), [0.6,0.8), [0.8,0.99), >=0.99.

        Returns a dictionary with the results and updates antenna ``subbands`` and
        ``observed`` flags based on detected auto-correlation data.
        """
        n_ants = len(self.antennas.names)
        ant_names = self.antennas.names
        antenna_spws: dict[str, set[int]] = {name: set() for name in ant_names}
        weight_stats = np.zeros((n_ants, 7), dtype=np.int64)
        scan_antennas: dict[int, set[str]] = defaultdict(set)
        bins_edges = np.array([0.0, 1e-3, 0.2, 0.4, 0.6, 0.8, 0.99, np.inf])

        with misc.table(self.msfile, readonly=True, ack=False) as msdata:
            with progress.Progress() as progress_bar:
                task = progress_bar.add_task("[yellow]Reading MS...", total=len(msdata))
                for (start, nrow) in misc.chunkert(0, len(msdata), chunkert):
                    ants1 = msdata.getcol('ANTENNA1', startrow=start, nrow=nrow)
                    ants2 = msdata.getcol('ANTENNA2', startrow=start, nrow=nrow)
                    spws = msdata.getcol('DATA_DESC_ID', startrow=start, nrow=nrow)
                    data = msdata.getcol('DATA', startrow=start, nrow=nrow)
                    scans = msdata.getcol('SCAN_NUMBER', startrow=start, nrow=nrow)
                    weights = msdata.getcol('WEIGHT', startrow=start, nrow=nrow)

                    auto_indices = np.where((ants1 == ants2) & (np.max(np.abs(data), axis=(1, 2)) > 1e-5))[0]
                    for idx in auto_indices:
                        ant_idx = ants1[idx]
                        ant_name = ant_names[ant_idx]
                        scan_antennas[int(scans[idx])].add(ant_name)
                        antenna_spws[ant_name].add(spws[idx])

                    weights_flat = weights.reshape(nrow, -1)
                    for ant_idx in range(n_ants):
                        ant_mask = (ants1 == ant_idx) | (ants2 == ant_idx)
                        if np.any(ant_mask):
                            ant_weights = weights_flat[ant_mask].ravel()
                            hist, _ = np.histogram(ant_weights, bins=bins_edges)
                            weight_stats[ant_idx] += hist

                    progress_bar.update(task, advance=nrow)

        for ant_idx, ant in enumerate(self.antennas):
            spws_sorted = sorted(antenna_spws[ant.name])
            ant.subbands = tuple(spws_sorted)
            ant.observed = len(spws_sorted) > 0
            ant.weights = weight_stats[ant_idx]
        
        self.scans = scan_antennas
        

    @property
    def msfile(self) -> Path:
        """Path to the Measurement Set file.
        
        Returns:
            Path: The MS file path.
        """
        return self._msfile
    
    @property
    def projectname(self) -> str:
        """Project name from the observation table.
        
        Returns:
            str: Project name.
        """
        return self._projectname
    
    @property
    def freqsetup(self) -> FreqSetup:
        """Frequency setup of the observation."""
        return self._freqsetup

    @property
    def antennas(self) -> Antennas:
        """Antennas in the observation."""
        return self._antennas
    
    @property
    def sources(self) -> Sources:
        """Sources/fields in the observation."""
        return self._sources
    
    @property
    def time(self) -> ObsEpoch:
        """Observation time range."""
        return self._obsepoch

    def __repr__(self) -> str:
        """Return representation string for Ms object.
        
        Returns:
            str: Object representation.
        """
        return f"Ms<'{self.msfile}'>"
    
    def __str__(self) -> str:
        """Return string representation of Ms object.
        
        Returns:
            str: Human-readable string.
        """
        return f"MS: {self.msfile}"

    def overview(self):
        """Pretty print of the full experiment with all available data.
        
        Displays an interactive fullscreen terminal view showing:
        - Project name and MS file path
        - Observation date and time range
        - Frequency setup (central frequency, bandwidth, subbands, channels)
        - Sources with their coordinates
        - Antennas that observed
        
        Returns:
            bool: True if completed successfully, False if user cancelled with 'q'.
        """
        term = blessed.Terminal()
        with term.fullscreen(), term.cbreak():
            s = term.red_on_bright_black(term.center(f"{term.bold(self.projectname)} - {term.bold(str(self.msfile))}"))
            s += f"{term.normal}\n\n{term.normal}"
            s += term.bright_black('Obs date: ') + self.time.epoch.strftime('%d/%m/%Y')
            s += f" {'-'.join([t.time().strftime('%H:%M') for t in (self.time.starttime, self.time.endtime)])} UTC\n\n"

            s += term.bold_green('SETUP\n')
            s += term.bright_black('Central Frequency: ') + f"{self.freqsetup.meanfreq:0.04}\n"
            s += term.bright_black('Frequency Range: ') + \
                 f"{(self.freqsetup.meanfreq - self.freqsetup.bandwidth/2).value:0.04}-" \
                 f"{self.freqsetup.meanfreq + self.freqsetup.bandwidth/2:0.04}.\n"
            s += term.bright_black('Bandwidth: ') + \
                 f"{self.freqsetup.nspw} x " \
                 f"{self.freqsetup.bandwidth*self.freqsetup.nspw:0.04} subbands " \
                 f"(total bandwidth of {self.freqsetup.bandwidth:0.04}). " \
                 f"{self.freqsetup.nchan} channels each.\n"
            s += term.bright_black('Polarizations: ') + \
                 f"{', '.join([pol.name for pol in self.freqsetup.polarizations])}\n\n"

            s += term.bold_green('SOURCES\n')
            for src in self.sources:
                s += f"{src.name}: {term.bright_black(src.coordinates.to_string('hmsdms'))}\n"

            s += '\n'
            s += term.bold_green('ANTENNAS\n')
            s += f"{', '.join([ant.name for ant in self.antennas if ant.observed])}\n"
            missing_ants = [ant.name for ant in self.antennas if not ant.observed]
            if missing_ants:
                s += term.bright_black('Did not observe: ') + \
                     f"{', '.join(missing_ants) if len(missing_ants) > 0 else 'None'}\n\n"

            # In case of antennas not observing the full bandwidth (this may be per correlator pass)
            ss = ""
            for antenna in self.antennas:
                if 0 < len(antenna.subbands) < self.freqsetup.nspw:
                    ss += f"    {antenna.name}: {antenna.subbands}\n"

            if ss != "":
                s += term.bright_black('Antennas with smaller bandwidth:\n') + ss

            s_final = term.wrap(s, width=term.width)

            def print_all(ss):
                """Print a page of the overview and wait for user input.
                
                Args:
                    ss (list[str]): Lines to print on current page.
                
                Returns:
                    str: User's key input.
                """
                print(term.clear)
                for a_ss in ss:
                    print(a_ss)

                print(term.move_y(term.height - 3) + \
                      term.center(term.on_bright_black('press any key to continue (or Q to cancel)')).rstrip())
                return term.inkey()#.strip()

            # Fitting the terminal
            i, i_width = 0, term.height - 5
            while i < len(s_final):
                value = print_all(s_final[i:min(i+i_width, len(s_final)+1)])
                if value.lower() == 'q':
                    return False
                elif value.is_sequence and (value.name == 'KEY_UP'):
                    i = max(0, i-i_width)
                else:
                    i += i_width

            return True
    
    def json(self) -> dict:
        """Convert all MS metadata to a JSON-compatible dictionary.
        
        Returns:
            dict: Dictionary containing all MS information including:
                - msfile: Path to MS file
                - projectname: Project name
                - observation: Start time, end time, epoch, MJD, DOY, duration
                - frequency_setup: Mean frequency, bandwidth, number of subbands, channels, polarizations
                - sources: List of sources with names, coordinates, and intents
                - antennas: List of antennas with names, observation status, and subbands
        """
        return {
            'msfile': str(self.msfile),
            'projectname': self.projectname,
            'observation': {
                'starttime': self.time.starttime.isoformat(),
                'endtime': self.time.endtime.isoformat(),
                'epoch': self.time.epoch.isoformat(),
                'mjd': self.time.mjd,
                'doy': self.time.doy,
                'duration_hours': self.time.duration.value,
            },
            'frequency_setup': {
                'mean_frequency': self.freqsetup.meanfreq.to(u.Hz).value,
                'bandwidth': self.freqsetup.bandwidth.to(u.Hz).value,
                'n_subbands': self.freqsetup.nspw,
                'n_channels': self.freqsetup.nchan,
                'polarizations': [pol.name for pol in self.freqsetup.polarizations]
            },
            'sources': [
                {
                    'name': src.name,
                    'ra_deg': src.coordinates.ra.deg,
                    'dec_deg': src.coordinates.dec.deg,
                    'intent': src.intent
                } for src in self.sources
            ],
            'antennas': [
                {
                    'name': ant.name,
                    'observed': ant.observed,
                    'subbands': list(ant.subbands)
                } for ant in self.antennas
            ]
        }
    
    def save(self, filepath: str | Path | None = None, indent: int = 2) -> Path:
        """Save MS metadata to a JSON file.
        
        Args:
            filepath (str | Path | None): Path to output JSON file. 
                If None, saves to '<msfile_name>.json' in the same directory as the MS file.
            indent (int): Number of spaces for JSON indentation. Default is 2.
        
        Returns:
            Path: Path to the saved JSON file.
        """
        with open(self.msfile.parent / f"{self.msfile.stem}.json" if filepath is None else filepath, 'w') as f:
            json.dump(self.json(), f, indent=indent)
        
        return self.msfile.parent / f"{self.msfile.stem}.json" if filepath is None else filepath if isinstance(filepath, Path) else Path(filepath)

    @classmethod
    def load(cls, filepath: str | Path) -> Self:
        """Load MS metadata from a JSON file and create an Ms object.
        
        The object is recreated using only the information stored in the
        JSON file, without accessing the underlying MS on disk.
        
        Args:
            filepath (str | Path): Path to the JSON file created by ``save_json``.
        
        Returns:
            Ms: Reconstructed Ms object.
        """
        filepath = Path(filepath) if isinstance(filepath, str) else filepath

        with open(filepath, 'r') as f:
            data = json.load(f)

        obj = cls.__new__(cls)

        obj._msfile = Path(data['msfile'])
        obj._projectname = data['projectname']

        obs = data['observation']
        starttime = dt.datetime.fromisoformat(obs['starttime'])
        endtime = dt.datetime.fromisoformat(obs['endtime'])
        obj._obsepoch = ObsEpoch(starttime=starttime, endtime=endtime)

        freq = data['frequency_setup']
        obj._freqsetup = FreqSetup(
            meanfreq=freq['mean_frequency'] * u.Hz,
            bandwidth=freq['bandwidth'] * u.Hz,
            nspw=freq['n_subbands'],
            nchan=freq['n_channels'],
            polarizations=tuple(misc.Stokes[pol] for pol in freq['polarizations']),
        )

        src_objs = []
        for src in data['sources']:
            coord_src = coord.SkyCoord(src['ra_deg'], src['dec_deg'], unit=(u.deg, u.deg))
            src_objs.append(Source(name=src['name'], coordinates=coord_src, intent=src.get('intent')))
        obj._sources = Sources(*src_objs)

        ant_objs = []
        for ant in data['antennas']:
            ant_objs.append(
                Antenna(
                    name=ant['name'],
                    observed=ant['observed'],
                    subbands=tuple(ant['subbands']),
                )
            )
        obj._antennas = Antennas(*ant_objs)

        obj.operations = OperationsProxy(obj._msfile)
        obj._datastats = None

        return obj
